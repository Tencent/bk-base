# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
License for BK-BASE 蓝鲸基础平台:
--------------------------------------------------------------------
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
import datetime
import hashlib
import json
import time
from abc import ABCMeta
from collections import namedtuple
from typing import Dict

from django.conf import settings
from django.core.cache import cache
from django.utils.module_loading import import_string
from django.utils.translation import ugettext as _

from common.api.modules.utils import (
    add_app_info_before_request,
    add_header_before_request,
)
from common.exceptions import ApiRequestError, ApiResultError
from common.local import get_request_id
from common.log import logger
from common.resource import interapi_session, interapi_session_without_retry


class DataResponse:
    """response for data api request"""

    def __init__(self, request_response):
        """
        初始化一个请求结果
        @param request_response: 期待是返回的结果字典
        """
        self.response = request_response

    def is_success(self):
        return bool(self.response.get("result", False))

    @property
    def message(self):
        return self.response.get("message", None)

    @property
    def code(self):
        return self.response.get("code", None)

    @property
    def data(self):
        return self.response.get("data", None)

    @property
    def errors(self):
        return self.response.get("errors", None)


class DataAPI:
    """Single API for DATA"""

    HTTP_STATUS_OK = 200

    def __init__(
        self,
        method,
        url,
        module,
        description="",
        default_return_value=None,
        before_request=None,
        after_request=None,
        max_response_record=None,
        max_query_params_record=None,
        url_keys=None,
        default_timeout=180,
        custom_headers=None,
        output_standard=True,
        request_auth=None,
        cache_time=0,
        timeout_auto_retry=True,
    ):
        """
        初始化一个请求句柄
        @param {string} method 请求方法, GET or POST，大小写皆可
        @param {string} url 请求地址
        @param {string} module 对应的模块，用于后续查询标记
        @param {string} description 中文描述
        @param {object} default_return_value 默认返回值，在请求失败情形下将返回该值
        @param {function} before_request 请求前的回调
        @param {function} after_request 请求后的回调
        @param {int} max_response_record 最大记录返回长度，如果为None将记录所有内容
        @param {int} max_query_params_record 最大请求参数记录长度，如果为None将记录所有的内容
        @param {array.<string>} url_keys 请求地址中存在未赋值的 KEYS
        @param {dict} custom_headers 以定义Header内容
        @param {bool} output_standard 输出是否标准
        @param {int} cache_time 缓存时间
        @param {bool} timeout_auto_retry 是否超时重试
        """
        self.url = url
        self.module = module
        self.method = method
        self.default_return_value = default_return_value

        self.before_request = before_request
        self.after_request = after_request

        self.description = description
        self.max_response_record = max_response_record if max_response_record else 500
        self.max_query_params_record = max_query_params_record

        self.url_keys = url_keys
        self.custom_headers = custom_headers
        self.output_standard = output_standard
        self.default_timeout = default_timeout

        self.request_auth = request_auth
        self.cache_time = cache_time
        self.timeout_auto_retry = timeout_auto_retry

    def __call__(self, params=None, timeout=None, raw=False, raise_exception=False, files=None):
        """
        调用传参

        @param {Boolean} raw 是否返回原始内容
        """
        if params is None:
            params = {}

        timeout = timeout or self.default_timeout

        start_time = time.time()
        result = self._send_request(params, timeout, files)
        end_time = time.time()

        # 记录请求日志
        self._log(start_time, end_time, result)

        if not result.result:
            raise result.exc

        response = result.response

        if raw:
            return response.response

        # 统一处理返回内容，根据平台既定规则，断定成功与否
        if raise_exception and not response.is_success():
            raise ApiResultError(response.message, code=response.code)

        return response

    def _send_request(self, params, timeout, files=None):
        Result = namedtuple("Result", ["result", "response", "params", "exc"])

        # 请求前的参数清洗处理
        if self.before_request is not None:
            params = self.before_request(params)

        params = add_app_info_before_request(params)

        # 是否有默认返回，调试阶段可用
        if self.default_return_value is not None:
            return Result(True, DataResponse(self.default_return_value()), params, None)

        # 缓存
        try:
            cache_key = None
            if self.cache_time:
                cache_key = self._build_cache_key(params)
                result = self._get_cache(cache_key)
                if result is not None:
                    # 有缓存时返回
                    return DataResponse(result)
        except Exception as err:
            logger.exception(f"Fail to save cache when set cache_key: {err}")

        response = None
        actual_url = self.build_actual_url(params)

        try:
            raw_response = self._send(params, timeout, files)
        except Exception as err:
            # 如果是请求异常了,这里也会有一个时间记录
            error_message = "Error occurred when requesting method->[{}] url->[{}] reason->[{}]".format(
                self.method,
                actual_url,
                err,
            )
            logger.exception(error_message)
            return Result(False, response, params, ApiRequestError(error_message))

        # http层面的处理结果
        if raw_response.status_code != self.HTTP_STATUS_OK:
            error_message = "data api response status code error code->[{}] url->[{}] content->[{}]".format(
                raw_response.status_code,
                actual_url,
                raw_response.text,
            )
            logger.error(error_message)
            return Result(False, response, params, ApiRequestError(error_message))

        # 结果层面的处理结果
        try:
            response_result = raw_response.json()

            if "result" not in response_result and not self.output_standard:
                response_result = {
                    "data": response_result,
                    "result": True,
                    "message": "",
                    "code": "00",
                    "errors": "",
                }

            # 只有正常返回才会调用 after_request 进行处理
            if response_result["result"]:

                # 请求完成后的清洗处理
                if self.after_request is not None:
                    response_result = self.after_request(response_result)

            if self.cache_time and cache_key:
                self._set_cache(cache_key, response_result)

            response = DataResponse(response_result)
            return Result(True, response, params, None)
        except Exception as err:
            error_message = "data api response not json format url->[{}] content->[{} reason->[{}]]".format(
                actual_url, raw_response.text, err
            )
            logger.exception(error_message)
            return Result(False, response, params, ApiResultError(error_message))

    def _log(self, start_time: float, end_time: float, result: namedtuple):
        """
        记录请求日志
        :return:
        """
        response = result.response
        params = result.params
        exc = result.exc
        error_message = exc.message if exc is not None else ""

        # 判断是否需要记录,及其最大返回值
        if response is not None:
            response_data = (
                json.dumps(response.data)[: self.max_response_record]
                if self.max_response_record is not None
                else json.dumps(response.data)
            )

            # 防止部分平台不规范接口搞出大新闻
            if response.code is None:
                response.response["code"] = "00"

        params_data = (
            json.dumps(params)[: self.max_query_params_record]
            if self.max_query_params_record is not None
            else json.dumps(params)
        )

        # 增加流水的记录
        _info = {
            "request_datetime": datetime.datetime.fromtimestamp(start_time),
            "url": self.build_actual_url(params),
            "module": self.module,
            "method": self.method,
            "query_params": params_data,
            "request_id": get_request_id(),
            "response_result": response.is_success() if response is not None else False,
            "response_code": response.code if response is not None else -1,
            "response_data": response_data if response is not None else "",
            "response_errors": getattr(response, "errors", "") if response is not None else "",
            "response_message": response.message if response is not None else error_message[:1023],
            "cost_time": (end_time - start_time),
        }

        _log = "[PIZZA-API request log] {info}".format(
            info="&&".join("{}: {}".format(_k, _v) for _k, _v in list(_info.items()))
        )
        logger.info(_log)

    def _send(self, params, timeout, files=None):
        """
        发送和接收返回请求的包装
        @param params: 请求的参数,预期是一个字典
        @return: requests response
        """
        session = interapi_session if self.timeout_auto_retry else interapi_session_without_retry
        headers = {}

        if self.custom_headers is not None:
            headers.update(self.custom_headers)

        # 补充默认的 Header
        headers.update(add_header_before_request())

        url = self.build_actual_url(params)
        # 发出请求并返回结果
        if self.method.upper() == "GET":
            return session.request(
                method=self.method,
                url=url,
                params=params,
                timeout=timeout,
                auth=self.request_auth,
                verify=False,
                headers=headers,
            )
        elif self.method.upper() == "POST":
            if not files:
                headers.update({"Content-Type": "application/json; chartset=utf-8"})
                data = json.dumps(params)
            else:
                data = params
            return session.request(
                method=self.method,
                url=url,
                data=data,
                timeout=timeout,
                auth=self.request_auth,
                files=files,
                verify=False,
                headers=headers,
            )
        elif self.method.upper() == "DELETE":
            # DELETE分支，目前DELETE与PUT,PATCH处理方式相同，但是为了方便扩展还是保留分支写法
            headers.update({"Content-Type": "application/json; chartset=utf-8"})
            return session.request(
                method=self.method,
                url=url,
                data=json.dumps(params),
                timeout=timeout,
                auth=self.request_auth,
                verify=False,
                headers=headers,
            )
        elif self.method.upper() == "PUT":
            # PUT分支，目前DELETE与PUT,PATCH处理方式相同，但是为了方便扩展还是保留分支写法
            headers.update({"Content-Type": "application/json; chartset=utf-8"})
            return session.request(
                method=self.method,
                url=url,
                data=json.dumps(params),
                timeout=timeout,
                auth=self.request_auth,
                verify=False,
                headers=headers,
            )
        elif self.method.upper() == "PATCH":
            # PATCH分支，目前DELETE与PUT,PATCH处理方式相同，但是为了方便扩展还是保留分支写法
            headers.update({"Content-Type": "application/json; chartset=utf-8"})
            return session.request(
                method=self.method,
                url=url,
                data=json.dumps(params),
                timeout=timeout,
                auth=self.request_auth,
                verify=False,
                headers=headers,
            )
        else:
            raise ApiRequestError(_("异常请求方式，{method}").format(method=self.method))

    def build_actual_url(self, params):
        if self.url_keys is not None:
            kvs = {}
            for key in self.url_keys:
                if key not in params:
                    raise ApiRequestError(_("请求参数中请包含参数{key}").format(key=key))
                kvs[key] = params[key]
                # del params[key]
            url = self.url.format(**kvs)
        else:
            url = self.url

        return url

    def _build_cache_key(self, params):
        """
        缓存key的组装方式，保证URL和参数相同的情况下返回是一致的
        :param params:
        :return:
        """
        # 缓存
        cache_str = "url_{url}__params_{params}".format(url=self.build_actual_url(params), params=json.dumps(params))
        hash_md5 = hashlib.new("md5")
        hash_md5.update(cache_str)
        cache_key = hash_md5.hexdigest()
        return cache_key

    def _set_cache(self, cache_key, data):
        """
        获取缓存
        :param cache_key:
        :return:
        """
        cache.set(cache_key, data, self.cache_time)

    @staticmethod
    def _get_cache(cache_key):
        """
        获取缓存
        :param cache_key:
        :return:
        """
        _cache = cache.get(cache_key)
        if _cache:
            return _cache


DRF_DATAAPI_CONFIG = [
    "description",
    "default_return_value",
    "before_request",
    "after_request",
    "max_response_record",
    "max_query_params_record",
    "default_timeout",
    "custom_headers",
    "output_standard",
    "request_auth",
    "timeout_auto_retry",
]


class DataAPISet(metaclass=ABCMeta):
    pass


class DRFActionAPI:
    def __init__(self, detail=True, url_path=None, method=None, **kwargs):
        """
        资源操作申明，类似 DRF 中的 detail_route、list_route，透传 DataAPI 配置
        """
        self.detail = detail
        self.url_path = url_path
        self.method = method

        for k, v in list(kwargs.items()):
            if k not in DRF_DATAAPI_CONFIG:
                raise Exception(
                    "Not support {k} config, SUPPORT_DATAAPI_CONFIG:{config}".format(k=k, config=DRF_DATAAPI_CONFIG)
                )

        self.dataapi_kwargs = kwargs


class DataDRFAPISet(DataAPISet):
    """
    For djangorestframework api set in the backend
    """

    BASE_ACTIONS = {
        "list": DRFActionAPI(detail=False, method="get"),
        "create": DRFActionAPI(detail=False, method="post"),
        "update": DRFActionAPI(method="put"),
        "partial_update": DRFActionAPI(method="patch"),
        "delete": DRFActionAPI(method="delete"),
        "retrieve": DRFActionAPI(method="get"),
    }

    def __init__(self, url, module, primary_key, custom_config=None, url_keys=None, **kwargs):
        """
        申明具体某一资源的 restful-api 集合工具，透传 DataAPI 配置

        @param {String} url 资源路径
        @param {String} primary_key 资源ID
        @param {Dict} custom_config 给请求资源增添额外动作，支持定制化配置
            {
                'start': DRFActionAPI(detail=True, method='post')
            }
        @params kwargs 支持 DRF_DATAAPI_CONFIG 定义的配置项，参数说明请求参照 DataAPI
        @paramExample
            DataDRFAPISet(
                url= 'http://example.com/insts/',
                primary_key='inst_id',
                module='meta',
                description='inst操作集合',
                custom_config={
                    'start': {detail=True, 'method': 'post'}
                }
            )
        @returnExample 返回实例具有以下方法
            inst.create          ---- POST {http://example.com/insts/}
            inst.list            ---- GET {http://example.com/insts/}
            inst.retrieve        ---- GET {http://example.com/insts/:inst_id/}
            inst.update          ---- PUT {http://example.com/insts/:inst_id/}
            inst.partial_update  ---- PATCH {http://example.com/insts/:inst_id/}
            inst.start           ---- POST {http://example.com/insts/:inst_id/}start/
        """
        self.url = url
        self.url_keys = url_keys
        self.custom_config = custom_config
        self.module = module
        self.primary_key = primary_key

        for k, v in list(kwargs.items()):
            if k not in DRF_DATAAPI_CONFIG:
                raise Exception(
                    "Not support {k} config, SUPPORT_DATAAPI_CONFIG:{conf}".format(k=k, conf=DRF_DATAAPI_CONFIG)
                )

        self.dataapi_kwargs = kwargs

    def to_url(self, action_name, action, is_standard=False):
        target_url_keys = []
        # 注意 target_url_keys 需要深复制
        if self.url_keys:
            target_url_keys = [key for key in self.url_keys]

        if action.detail:
            target_url = "{root_path}{{{pk}}}/".format(root_path=self.url, pk=self.primary_key)
            target_url_keys.append(self.primary_key)
        else:
            target_url = "{root_path}".format(root_path=self.url)

        if not is_standard:
            sub_path = action.url_path if action.url_path else action_name
            target_url += "{sub_path}/".format(sub_path=sub_path)

        return target_url, target_url_keys

    def __getattr__(self, key):
        """
        通过 ins.create 调用DRF方法，会在该函数进行分发
        """
        action, url, url_keys = None, "", []
        if key in self.BASE_ACTIONS:
            action = self.BASE_ACTIONS[key]
            url, url_keys = self.to_url(key, action, is_standard=True)

        if self.custom_config is not None and key in self.custom_config:
            action = self.custom_config[key]
            url, url_keys = self.to_url(key, self.custom_config[key])

        if action is None:
            raise Exception(_("请求方法{key}不存在").format(key=key))

        method = action.method

        dataapi_kwargs = {
            "method": method,
            "url": url,
            "url_keys": url_keys,
            "module": self.module,
        }
        dataapi_kwargs.update(self.dataapi_kwargs)
        dataapi_kwargs.update(action.dataapi_kwargs)

        return DataAPI(**dataapi_kwargs)


class ProxyDataAPI:
    """
    代理 DataAPI，会根据 dataapi_settings.RUN_VERSION 环境信息加上 self.proxy 路由配置找到对应的 API
    """

    def __init__(self, description="", params=None):
        """
        初始化方法

        :param description:
        :param params:
        """
        self.params = params
        self.description = description


class ProxyBaseApi(DataAPISet):
    """
    api基类，支持ProxyDataAPI代理类API
    """

    def __init__(self, default: DataAPISet, proxy: Dict):
        """
        初始化方法

        :param default: 默认请求类，如：BkLoginApi
        :param proxy: 根据环境信息路由的请求类，如：{"tencent": "extend.tencent.common.api.modules.bk_login.BkLoginApi"}
        """
        self.default = default
        self.proxy = proxy

    def __getattribute__(self, item):
        attr = super(ProxyBaseApi, self).__getattribute__(item)
        if isinstance(attr, ProxyDataAPI):
            default_mod = self.default
            version_mod = None

            # 代理类的DataApi，各个版本需要重载有差异的方法
            if settings.RUN_VERSION in self.proxy:
                module_str = self.proxy[settings.RUN_VERSION]
                try:
                    version_mod = import_string(module_str)()
                except ImportError:
                    raise NotImplementedError("{} is not implemented".format(module_str))

            # 如果对应版本的模块和方法存在，则调用对应的模块方法
            if version_mod is not None and hasattr(version_mod, item):
                return getattr(version_mod, item)

            return getattr(default_mod, item)

        return attr

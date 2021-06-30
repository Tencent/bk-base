# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
import hashlib
import json
import time
from abc import ABCMeta
from typing import Dict

import requests
from django.conf import settings
from django.core.cache import cache
from django.utils import timezone, translation
from django.utils.module_loading import import_string
from django.utils.translation import ugettext as _
from requests.exceptions import ReadTimeout

from apps.common.log import logger
from apps.exceptions import ApiRequestError, ApiResultError
from apps.utils.local import get_request_id
from apps.utils.time_handler import timestamp_to_datetime

from .exception import DataAPIException
from .models import DataAPIRecord
from .modules.utils import add_esb_info_before_request


class DataResponse(object):
    """response for data api request"""

    def __init__(self, request_response, request_id):
        """
        初始化一个请求结果
        @param request_response: 期待是返回的结果字典
        """
        self.response = request_response
        self.request_id = request_id

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


class DataAPI(object):
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
        max_response_record=5000,
        max_query_params_record=5000,
        method_override=None,
        url_keys=None,
        after_serializer=None,
        cache_time=0,
        default_timeout=300,
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
        @param {serializer} after_serializer 请求后的清洗
        @param {int} max_response_record 最大记录返回长度，如果为None将记录所有内容
        @param {int} max_query_params_record 最大请求参数记录长度，如果为None将记录所有的内容
        @param {string} method_override 重载请求方法，注入到 header（X-HTTP-METHOD-OVERRIDE）
        @param {array.<string>} url_keys 请求地址中存在未赋值的 KEYS
        @param {int} cache_time 缓存时间
        @param {int} default_timeout 默认超时时间
        """
        self.url = url
        self.module = module
        self.method = method
        self.default_return_value = default_return_value

        self.before_request = before_request
        self.after_request = after_request

        self.after_serializer = after_serializer

        self.description = description
        self.max_response_record = max_response_record
        self.max_query_params_record = max_query_params_record

        self.method_override = method_override
        self.url_keys = url_keys

        self.cache_time = cache_time
        self.default_timeout = default_timeout

    def __call__(self, params=None, files=None, raw=False, timeout=None):
        """
        调用传参

        @param {Boolean} raw 是否返回原始内容
        """
        if params is None:
            params = {}

        timeout = timeout or self.default_timeout

        request_id = get_request_id()
        try:
            response = self._send_request(params, timeout, request_id)
            if raw:
                return response.response

            if not response.is_success():

                # 请求错误码可以表示是哪个系统出错
                err_msg = "【{module}】{msg}".format(module=self.module, msg=response.message)
                exception = ApiResultError(err_msg, request_id, code=response.code, errors=response.errors)

                # ESB返回特定的错误码，错误信息定制化处理
                if str(response.code) in ["1306201", "1308203"]:
                    logger.error(
                        "[Reqeust Error] {msg}, request_id={request_id}".format(msg=err_msg, request_id=request_id)
                    )

                    exception = ApiRequestError(
                        _("【{module}】{msg}".format(module=self.module, msg=ApiRequestError.MESSAGE)),
                        request_id,
                        code=response.code,
                    )
                raise exception

            return response.data
        except DataAPIException as e:
            raise ApiRequestError(e.error_message, request_id)

    def _send_request(self, params, timeout, request_id):

        # 请求前的参数清洗处理
        if self.before_request is not None:
            params = self.before_request(params)

        # 是否有默认返回，调试阶段可用
        if self.default_return_value is not None:
            return DataResponse(self.default_return_value, request_id)

        # 缓存
        try:
            cache_key = self._build_cache_key(params)
            if self.cache_time:
                result = self._get_cache(cache_key)
                if result is not None:
                    # 有缓存时返回
                    return DataResponse(result, request_id)
        except Exception as error:
            logger.exception("Fail to save cache when set cache_key: {}".format(error))
            cache_key = None

        response = None
        session = None
        error_message = ""
        # 发送请求
        # 开始时记录请求时间
        start_time = time.time()
        try:
            try:
                raw_response, session = self._send(params, timeout, request_id)
            except ReadTimeout as e:
                error_message = "Error occurred when requesting method->[{}] url->[{}] reason->[{}]".format(
                    self.method, self.url, e
                )
                logger.exception(error_message)
                raise DataAPIException(self, _("API请求超时，请管理员排查问题"))
            except Exception as e:
                error_message = "Error occurred when requesting method->[{}] url->[{}] reason->[{}]".format(
                    self.method, self.url, e
                )
                logger.exception(error_message)
                if settings.SHOW_EXCEPTION_DETAIL:
                    raise DataAPIException(self, error_message)
                else:
                    raise DataAPIException(self, _("API请求异常，请管理员排查问题"))

            # http层面的处理结果
            if raw_response.status_code != self.HTTP_STATUS_OK:
                error_message = "data api response status code error code->[{}] url->[{}] content->[{}]".format(
                    raw_response.status_code,
                    self.url,
                    raw_response.text,
                )
                logger.error(error_message)

                if settings.SHOW_EXCEPTION_DETAIL:
                    message = _("模块【{module}】，请求路径【{url}】，" "API请求异常，错误信息（{error_message})").format(
                        module=self.module, url=self.url, error_message=error_message
                    )
                else:
                    message = _("API请求异常，状态码（{code}），请求路径【{url}】，请管理员排查问题").format(
                        code=raw_response.status_code, url=self.url, message=error_message
                    )
                raise DataAPIException(self, message, response=raw_response)

            # 结果层面的处理结果
            try:
                response_result = raw_response.json()
            except Exception:
                error_message = "data api response not json format url->[{}] content->[{}]".format(
                    self.url, raw_response.text
                )
                logger.exception(error_message)

                raise DataAPIException(self, _("返回数据格式不正确，结果格式非json."), response=raw_response)
            else:
                # 只有正常返回才会调用 after_request 进行处理
                if response_result["result"]:

                    # 请求完成后的清洗处理
                    if self.after_request is not None:
                        response_result = self.after_request(response_result)

                    if self.after_serializer is not None:
                        serializer = self.after_serializer(data=response_result)
                        serializer.is_valid(raise_exception=True)
                        response_result = serializer.validated_data

                    if self.cache_time and cache_key:
                        self._set_cache(cache_key, response_result)

                response = DataResponse(response_result, request_id)
                return response
        finally:
            self._log(response, params, error_message, start_time, session, request_id)

    def _log(self, response, params, error_message, start_time, session, request_id):
        # 最后记录时间
        end_time = time.time()
        # 判断是否需要记录,及其最大返回值
        if response is not None:
            response_result = response.is_success()
            response_data = json.dumps(response.data)[: self.max_response_record]

            for _param in settings.SENSITIVE_PARAMS:
                params.pop(_param, None)
            try:
                params = json.dumps(params)[: self.max_query_params_record]
            except TypeError:
                params = ""

            if response.code is None:
                response.response["code"] = "00"
            # message不符合规范，不为string的处理
            if type(response.message) not in [str]:
                response.response["message"] = str(response.message)
            response_code = response.code
        else:
            response_data = ""
            response_code = -1
            response_result = False
        response_message = response.message if response is not None else error_message
        # 增加流水的记录

        _info = {
            "request_datetime": timestamp_to_datetime(start_time),
            "url": self.url,
            "module": self.module,
            "method": self.method,
            "method_override": self.method_override,
            "query_params": params,
            "headers": str(session.headers) if session else "",
            "response_result": response_result,
            "response_code": response_code,
            "response_data": response_data,
            "response_message": response_message[:1023],
            "cost_time": (end_time - start_time),
            "request_id": request_id,
        }

        DataAPIRecord.objects.create(**_info)

        _log = _("[FLOW-API-LOG] {info}").format(
            info=" && ".join("{}: {}".format(_k, _v) for _k, _v in list(_info.items()))
        )
        logger.info(_log)

    def _build_cache_key(self, params):
        """
        缓存key的组装方式，保证URL和参数相同的情况下返回是一致的
        :param params:
        :return:
        """
        # 缓存
        try:
            params_str = json.dumps(params)
        except TypeError:
            params_str = str(params)
        cache_str = "url_{url}__params_{params}".format(url=self.build_actual_url(params), params=params_str)
        hash_md5 = hashlib.new("md5")
        hash_md5.update(cache_str.encode("utf-8"))
        cache_key = hash_md5.hexdigest()
        return cache_key

    def _set_cache(self, cache_key, data):
        """
        获取缓存
        :param cache_key:
        :return:
        """
        cache.set(cache_key, data, self.cache_time)

    def _send(self, params, timeout, request_id):
        """
        发送和接受返回请求的包装
        @param params: 请求的参数,预期是一个字典
        @return: requests response
        """

        # 增加request id
        session = requests.session()
        session.headers.update({"X-DATA-REQUEST-ID": request_id})

        # headers 申明重载请求方法
        if self.method_override is not None:
            session.headers.update({"X-METHOD-OVERRIDE": self.method_override})
            # params['X_HTTP_METHOD_OVERRIDE'] = self.method_override

        session.headers.update(
            {
                "blueking-language": translation.get_language(),
                "blueking-timezone": str(timezone.get_current_timezone()),
                "request-id": get_request_id(),
            }
        )
        url = self.build_actual_url(params)

        # 发出请求并返回结果
        non_file_data, file_data = self._split_file_data(params)
        if self.method.upper() == "GET":
            result = session.request(method=self.method, url=url, params=params, verify=False, timeout=timeout)
        elif self.method.upper() in ["POST", "PUT", "PATCH"]:
            if not file_data:
                session.headers.update({"Content-Type": "application/json; chartset=utf-8"})
                params = json.dumps(non_file_data)
            else:
                params = non_file_data
            result = session.request(
                method=self.method, url=url, data=params, files=file_data, verify=False, timeout=timeout
            )
        elif self.method.upper() == "DELETE":
            session.headers.update({"Content-Type": "application/json; chartset=utf-8"})
            result = session.request(
                method=self.method, url=url, data=json.dumps(non_file_data), verify=False, timeout=timeout
            )
        else:
            raise ApiRequestError("异常请求方式，{method}".format(method=self.method))

        return result, session

    def build_actual_url(self, params):
        if self.url_keys is not None:
            kvs = {_k: params[_k] for _k in self.url_keys}
            url = self.url.format(**kvs)
        else:
            url = self.url

        return url

    @staticmethod
    def _split_file_data(data):
        file_data = {}
        non_file_data = {}
        for k, v in list(data.items()):
            if hasattr(v, "read"):
                # 一般认为含有read属性的为文件类型
                file_data[k] = v
            else:
                non_file_data[k] = v
        return non_file_data, file_data

    @staticmethod
    def _get_cache(cache_key):
        """
        获取缓存
        :param cache_key:
        :return:
        """
        if cache.get(cache_key):
            return cache.get(cache_key)


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
    "cache_time",
]


class DataAPISet(metaclass=ABCMeta):  # noqa
    pass


class DRFActionAPI(object):
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
                    "Not support {k} config, SUPPORT_DATAAPI_CONFIG:{conf}".format(k=k, conf=DRF_DATAAPI_CONFIG)
                )

        self.dataapi_kwargs = kwargs


class DataDRFAPISet(object):
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
        target_url_keys = self.url_keys[:] if self.url_keys else []
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

        if key in self.custom_config and key in self.BASE_ACTIONS:
            action = self.custom_config[key]
            url, url_keys = self.to_url(key, self.custom_config[key], is_standard=True)

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
    代理DataAPI，会根据settings.RUN_VER到sites.{run_ver}.api.proxy_modules找到对应的api
    """

    def __init__(self, description=""):
        self.description = description


class BaseApi(DataAPISet):
    """
    api基类，支持ProxyDataAPI代理类API
    """

    def __init__(self, default: DataAPISet, proxy: Dict):
        """
        初始化方法

        :param default: 默认请求类，如：BkLoginApi
        :param proxy: 根据环境信息路由的请求类，如：{"ieod": "extend.tencent.common.api.modules.bk_login.BkLoginApi"}
        """
        self.default = default
        self.proxy = proxy

    def __getattribute__(self, item):
        attr = super(BaseApi, self).__getattribute__(item)
        if isinstance(attr, ProxyDataAPI):
            mod = self.default
            # 代理类的DataApi，各个版本需要重载有差异的方法

            # module_path, module_name = self.__module__.rsplit(".", 1)
            # class_name = self.__class__.__name__
            if settings.RUN_VER in self.proxy:
                module_str = self.proxy[settings.RUN_VER]

                try:
                    mod = import_string(module_str)()
                except ImportError:
                    raise NotImplementedError("{} is not implemented".format(module_str))
            try:
                attr = getattr(mod, item)
            except AttributeError:
                raise NotImplementedError("{}.{} is not implemented".format(mod, item))
        return attr

    def __call__(self, *args, **kwargs):
        return self


class PassThroughAPI(DataAPI):
    """
    直接透传API
    """

    def __init__(self, module, method, url_prefix, sub_url, **kwargs):
        api_kwargs = {
            "method": method,
            "url": "".join([url_prefix, sub_url]),
            "module": module,
        }
        api_kwargs.update(**kwargs)
        if "before_request" not in api_kwargs:
            api_kwargs["before_request"] = add_esb_info_before_request
        super(PassThroughAPI, self).__init__(**api_kwargs)

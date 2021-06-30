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
import json
import logging
import sys
import time
from importlib import import_module

import requests
import six
from requests.adapters import HTTPAdapter

from common.exceptions import ApiRequestError, ApiResultError
from conf import settings

logger = logging.getLogger(__name__)

interapi_adapter = HTTPAdapter(
    pool_connections=getattr(settings, "INTERAPI_POOL_SIZE", 5),
    pool_maxsize=getattr(settings, "INTERAPI_POOL_MAXSIZE", 10),
    max_retries=3,
)
interapi_session = requests.session()
interapi_session.mount("https://", interapi_adapter)
interapi_session.mount("http://", interapi_adapter)


class DataResponse(object):
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
        max_response_record=None,
        max_query_params_record=None,
        url_keys=None,
        default_timeout=180,
        custom_headers=None,
        output_standard=True,
        request_auth=None,
        cache_time=0,
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

    def __call__(
        self,
        params=None,
        timeout=None,
        raw=False,
        raise_exception=False,
        files=None,
        retry_times=1,
        interval=1,
    ):
        """
        调用传参

        @param {Boolean} raw 是否返回原始内容
        """
        if params is None:
            params = {}

        timeout = timeout or self.default_timeout

        while retry_times > 0:
            retry_times -= 1
            try:
                response = self._send_request(params, timeout, files, raw)
                if raw:
                    return response.response

                # 统一处理返回内容，根据平台既定规则，断定成功与否
                if raise_exception and not response.is_success():
                    raise ApiResultError(response.message, code=response.code)

                return response
            except (ApiRequestError, ApiResultError) as e:
                logger.exception("api request error:{}".format(e.message))
                if retry_times == 0:
                    raise e

            time.sleep(interval)

    def _send_request(self, params, timeout, files=None, raw=False):
        # 请求前的参数清洗处理
        if self.before_request is not None:
            params = self.before_request(params)

        # 是否有默认返回，调试阶段可用
        if self.default_return_value is not None:
            return DataResponse(self.default_return_value())

        response = None
        error_message = ""
        start_time, end_time = time.time(), time.time()
        actual_url = self.build_actual_url(params)
        # 发送请求
        try:
            raw_response, start_time, end_time = self.get_raw_response_by_params(
                params, timeout, files, actual_url
            )

            self.check_status_code(raw_response, actual_url)

            if raw:
                # 增加流水的记录
                _info = {
                    "request_datetime": datetime.datetime.fromtimestamp(start_time),
                    "url": actual_url,
                    "module": self.module,
                    "method": self.method,
                    "query_params": params,
                    "cost_time": (end_time - start_time),
                }

                _log = u"[DataManager Request] {info}".format(
                    info=u"&&".join(
                        [u"{}: {}".format(_k, _v) for _k, _v in _info.items()]
                    )
                )
                logger.info(_log)
                return DataResponse(raw_response)

            # 结果层面的处理结果
            response = self.handle_standard_response(raw_response, actual_url)
            return response
        finally:
            self.record_response(
                response, params, actual_url, start_time, end_time, error_message
            )

    def get_raw_response_by_params(self, params, timeout, files, actual_url):
        """根据参数获取请求原始返回，并记录开始和结束时间"""
        try:
            start_time = time.time()
            raw_response = self._send(params, timeout, files)
            end_time = time.time()
        except Exception as e:
            # 如果是请求异常了,这里也会有一个时间记录
            end_time = time.time()

            error_message = "Error occurred when requesting method->[{}] url->[{}] reason->[{}]".format(
                self.method, actual_url, e
            )
            logger.error(error_message)
            raise ApiRequestError("API请求异常，请管理员排查问题")

        return raw_response, start_time, end_time

    def _send(self, params, timeout, files=None):
        """
        发送和接收返回请求的包装
        @param params: 请求的参数,预期是一个字典
        @return: requests response
        """
        session = interapi_session
        headers = {}

        if self.custom_headers is not None:
            headers.update(self.custom_headers)

        headers.update({"x-bkdata-module": settings.APP_NAME})

        url = self.build_actual_url(params)
        # 发出请求并返回结果
        if self.method.upper() == "GET":
            headers.update({"Content-Type": "application/json; chartset=utf-8"})
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
            raise ApiRequestError("异常请求方式，{method}".format(method=self.method))

    def check_status_code(self, raw_response, actual_url):
        """检测HTTP的状态码"""
        if raw_response.status_code != self.HTTP_STATUS_OK:
            error_message = "data api response status code error code->[{}] url->[{}] content->[{}]".format(
                raw_response.status_code,
                actual_url,
                raw_response.text,
            )
            logger.error(error_message)

            message = "API请求异常，状态码（{code}），请管理员排查问题".format(
                code=raw_response.status_code
            )
            raise ApiRequestError(message)

    def handle_standard_response(self, raw_response, actual_url):
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

            response = DataResponse(response_result)
            return response
        except Exception:
            error_message = (
                "data api response not json format url->[{}] content->[{}]".format(
                    actual_url, raw_response.text
                )
            )
            logger.error(error_message)

            raise ApiResultError("返回数据格式不正确，结果格式非json.")

    def build_actual_url(self, params):
        if self.url_keys is not None:
            kvs = {}
            for key in self.url_keys:
                if key not in params:
                    raise ApiRequestError("请求参数中请包含参数{key}".format(key=key))
                kvs[key] = params[key]
                # del params[key]
            url = self.url.format(**kvs)
        else:
            url = self.url

        return url

    def record_response(
        self, response, params, actual_url, start_time, end_time, error_message
    ):
        # 判断是否需要记录,及其最大返回值
        if response is not None:
            response_data = (
                json.dumps(response.data)[: self.max_response_record]
                if self.max_response_record is not None
                else json.dumps(response.data)
            )

            params = (
                json.dumps(params)[: self.max_query_params_record]
                if self.max_query_params_record is not None
                else json.dumps(params)
            )

            # 防止部分平台不规范接口
            if response.code is None:
                response.response["code"] = "00"

        # 增加流水的记录
        _info = {
            "request_datetime": datetime.datetime.fromtimestamp(start_time),
            "url": actual_url,
            "module": self.module,
            "method": self.method,
            "query_params": params,
            "response_result": response.is_success() if response is not None else False,
            "response_code": response.code if response is not None else -1,
            "response_data": response_data if response is not None else "",
            "response_errors": getattr(response, "errors", "")
            if response is not None
            else "",
            "response_message": response.message
            if response is not None
            else error_message[:1023],
            "cost_time": (end_time - start_time),
        }

        _log = u"[DataManager Request] {info}".format(
            info=u"&&".join([u"{}: {}".format(_k, _v) for _k, _v in _info.items()])
        )
        logger.info(_log)


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
    "url_keys",
]


class DRFActionAPI(object):
    def __init__(self, detail=True, url_path=None, method=None, **kwargs):
        """
        资源操作申明，类似 DRF 中的 detail_route、list_route，透传 DataAPI 配置
        """
        self.detail = detail
        self.url_path = url_path
        self.method = method

        for k, v in kwargs.items():
            if k not in DRF_DATAAPI_CONFIG:
                raise Exception(
                    "Not support {k} config, SUPPORT_DATAAPI_CONFIG:{config}".format(
                        k=k, config=DRF_DATAAPI_CONFIG
                    )
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

    def __init__(
        self, url, module, primary_key, custom_config=None, url_keys=None, **kwargs
    ):
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

        for k, v in kwargs.items():
            if k not in DRF_DATAAPI_CONFIG:
                raise Exception(
                    "Not support {k} config, SUPPORT_DATAAPI_CONFIG:{conf}".format(
                        k=k, conf=DRF_DATAAPI_CONFIG
                    )
                )

        self.dataapi_kwargs = kwargs

    def to_url(self, action_name, action, is_standard=False):
        target_url = ""
        target_url_keys = []
        # 注意 target_url_keys 需要深复制
        if self.url_keys:
            target_url_keys = [key for key in self.url_keys]

        if action.detail:
            target_url = "{root_path}{{{pk}}}/".format(
                root_path=self.url, pk=self.primary_key
            )
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
            raise Exception("请求方法{key}不存在".format(key=key))

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


class ProxyDataAPI(object):
    """
    代理DataAPI，会根据settings.versions.{run_ver}.proxy_modules找到对应的api
    """

    def __init__(self, description="", params=None):
        self.params = params
        self.description = description


def import_string(dotted_path):
    """
    Import a dotted module path and return the attribute/class designated by the
    last name in the path. Raise ImportError if the import failed.
    """
    try:
        module_path, class_name = dotted_path.rsplit(".", 1)
    except ValueError:
        msg = "%s doesn't look like a module path" % dotted_path
        six.reraise(ImportError, ImportError(msg), sys.exc_info()[2])

    module = import_module(module_path)

    try:
        return getattr(module, class_name)
    except AttributeError:
        msg = 'Module "{}" does not define a "{}" attribute/class'.format(
            module_path, class_name
        )
        six.reraise(ImportError, ImportError(msg), sys.exc_info()[2])


class ProxyBaseApi(object):
    """
    api基类，支持ProxyDataAPI代理类API
    """

    def __getattribute__(self, item):
        attr = super(ProxyBaseApi, self).__getattribute__(item)
        if isinstance(attr, ProxyDataAPI):
            # 代理类的DataApi，各个版本需要重载有差异的方法

            module_path, module_name = self.__module__.rsplit(".", 1)
            class_name = self.__class__.__name__
            module_str = "api.versions.{run_ver}.{mod}.{api}".format(
                run_ver=settings.RUN_VERSION, mod=module_name, api=class_name
            )
            try:
                import_module
                mod = import_string(module_str)()
                attr = getattr(mod, item)
            except (ImportError, AttributeError):
                raise NotImplementedError(
                    "{}.{} is not implemented".format(module_str, item)
                )
        return attr

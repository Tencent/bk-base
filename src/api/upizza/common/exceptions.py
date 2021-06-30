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
from django.utils.translation import ugettext_lazy as _

from .errorcodes import ErrorCode


class BaseAPIError(Exception):
    """
    在Meta类中申明必要属性
    """

    MODULE_CODE = None
    CODE = None
    MESSAGE = None

    def __init__(self, message=None, message_kv=None, code=None, errors=None):
        self._message_tmp = message if message is not None else self.MESSAGE
        self._message_kv = message_kv if message_kv is not None else {}
        self._errors = errors

        if code is not None:
            self._code = code
        else:
            self._code = "{}{}{}".format(ErrorCode.BKDATA_PLAT_CODE, self.MODULE_CODE, self.CODE)

    def __str__(self):
        return "[{}] {}".format(self.code, self.message)

    @property
    def code(self):
        return self._code

    @property
    def message(self):
        try:
            return self._message_tmp.format(**self._message_kv)
        except Exception:
            return self._message_tmp

    @property
    def errors(self):
        return self._errors


class CommonCode:
    PARAM_ERR = ("001", _("参数校验错误"))
    API_REQ_ERR = ("002", _("调用API异常"))
    API_RES_ERR = ("003", _("API返回异常"))

    DATA_NOT_FOUND_ERR = ("004", _("对象不存在：{table}[{column}={value}]"))
    DATA_ALREADY_EXIST_ERR = ("005", _("对象已存在，无法添加：{table}[{column}={value}]"))

    META_MODEL_HAS_REGISTERED = ("006", _("该模型已注册为需要进行数据同步的ORM模型"))
    META_SYNC_ERR = ("007", _("元数据信息同步失败"))
    META_SYNC_MODEL_REGISTER_ERR = ("008", _("元数据同步类注册失败"))

    HTTP_200 = ("200", _("正常响应"))
    HTTP_404 = ("404", _("API不存在"))
    HTTP_500 = ("500", _("系统异常"))

    DEMO_ERR = ("101", _("普通异常"))
    DEMO_KV_ERR = ("102", _("带有参数的异常，{param1}，{param2}"))
    TAG_NOT_EXIST = "350"
    TARGET_NOT_EXIST = "355"
    TARGET_TYPE_NOT_ALLOWED = "356"

    METADATA_RPC_CALL_ERROR = "380"
    METADATA_COMPLIANCE_ERROR = "381"


class CommonAPIError(BaseAPIError):
    MODULE_CODE = ErrorCode.BKDATA_COMMON


class ValidationError(CommonAPIError):
    CODE = CommonCode.PARAM_ERR[0]
    MESSAGE = CommonCode.PARAM_ERR[1]


class ApiRequestError(CommonAPIError):
    CODE = CommonCode.API_REQ_ERR[0]
    MESSAGE = CommonCode.API_REQ_ERR[1]


class ApiResultError(CommonAPIError):
    CODE = CommonCode.API_RES_ERR[0]
    MESSAGE = CommonCode.API_RES_ERR[1]


class MetaSyncError(CommonAPIError):
    CODE = CommonCode.META_SYNC_ERR[0]
    MESSAGE = CommonCode.META_SYNC_ERR[1]


class MetaSyncModelRegisterError(CommonAPIError):
    CODE = CommonCode.META_SYNC_MODEL_REGISTER_ERR[0]
    MESSAGE = CommonCode.META_SYNC_MODEL_REGISTER_ERR[1]


class DataNotFoundError(CommonAPIError):
    CODE = CommonCode.DATA_NOT_FOUND_ERR[0]
    MESSAGE = CommonCode.DATA_NOT_FOUND_ERR[1]


class DataAlreadyExistError(CommonAPIError):
    CODE = CommonCode.DATA_ALREADY_EXIST_ERR[0]
    MESSAGE = CommonCode.DATA_ALREADY_EXIST_ERR[1]


class MetaModelHasRegistered(CommonAPIError):
    CODE = CommonCode.META_MODEL_HAS_REGISTERED[0]
    MESSAGE = CommonCode.META_MODEL_HAS_REGISTERED[1]


class DemoError(CommonAPIError):
    CODE = CommonCode.DEMO_ERR[0]
    MESSAGE = CommonCode.DEMO_ERR[1]


class DemoKVError(CommonAPIError):
    CODE = CommonCode.DEMO_KV_ERR[0]
    MESSAGE = CommonCode.DEMO_KV_ERR[1]


class TargetTypeNotAllowedError(CommonAPIError):
    MESSAGE = _("实体类型({type})不支持标签")
    CODE = CommonCode.TARGET_TYPE_NOT_ALLOWED


class TargetNotExistError(CommonAPIError):
    MESSAGE = _("目标实体({type}:{id})不存在")
    CODE = CommonCode.TARGET_NOT_EXIST


class TagNotExistError(CommonAPIError):
    MESSAGE = _("标签({code})不存在")
    CODE = CommonCode.TAG_NOT_EXIST


class MetaDataRPCCallError(CommonAPIError):
    MESSAGE = _("操作元数据系统RPC接口失败。错误：{inner_error}")
    CODE = CommonCode.METADATA_RPC_CALL_ERROR


class MetaDataComplianceError(CommonAPIError):
    MESSAGE = _("请求的数据不合规。错误：{inner_error}")
    CODE = CommonCode.METADATA_COMPLIANCE_ERROR

# coding=utf-8
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

# Copyright © 2012-2018 Tencent BlueKing.
# All Rights Reserved.
# 蓝鲸智云 版权所有

from __future__ import absolute_import, print_function, unicode_literals


class ErrorCodes(object):
    # 数据平台的平台代码--7位错误码的前2位
    BKDATA_PLAT_CODE = "15"
    # 数据平台子模块代码--7位错误码的3，4位
    BKDATA_METADATA = "30"


class MetaDataErrorCodes(object):
    COMMON = 000
    CLIENT = 500

    def __init_(
        self,
        parent_err_codes=ErrorCodes,
    ):
        self.parent_err_codes = parent_err_codes
        for k, v in vars(self.__class__).items():
            if k.isupper():
                setattr(self, k, self.full_error_code(v))

    def full_error_code(self, sub_code):
        return int(self.parent_err_codes.BKDATA_PLAT_CODE + self.parent_err_codes.BKDATA_METADATA + str(sub_code))


class StandardErrorMixIn(Exception):
    code = None
    msg_template = None
    default_msg = None
    data = None

    def __init__(self, *args, **kwargs):
        if "message_kv" in kwargs:
            if not args:
                message = self.msg_template.format(**kwargs[str("message_kv")])
            elif len(args) == 1:
                message = args[0].format(**kwargs[str("message_kv")])
            else:
                raise TypeError("Multi args and message_kv is not allowed at same time in standard error.")
        elif args:
            if len(args) == 1:
                message = args[0]
            else:
                message = self.msg_template.format(*args)
        else:
            message = self.default_msg

        self.data = {}
        if "data" in kwargs:
            data = kwargs.pop(str("data"))
            if not isinstance(data, dict):
                self.data["content"] = data
            else:
                self.data.update(data)

        if message:
            super(StandardErrorMixIn, self).__init__(message.encode("utf8") if isinstance(message, str) else message)
        else:
            super(StandardErrorMixIn, self).__init__()


class MetaDataError(StandardErrorMixIn, Exception):
    code = MetaDataErrorCodes().COMMON
    msg_template = "MetaData System Error: {error}."


class MetaDataClientError(MetaDataError):
    code = MetaDataErrorCodes().CLIENT
    msg_template = "MetaData Client Error: {error}."


class RPCCallLogicError(MetaDataClientError):
    code = MetaDataErrorCodes().CLIENT + 1
    msg_template = "Error in Metadata call logic: {error}."


class RPCProcedureError(MetaDataClientError):
    code = MetaDataErrorCodes().CLIENT + 2
    msg_template = "Error in RPC procedure: {error}."


class LocalMetaDataTypeError(MetaDataClientError):
    code = MetaDataErrorCodes().CLIENT + 3
    msg_template = "Invalid metadata local type: {cls_name}."


class SyncHookError(MetaDataClientError):
    code = MetaDataErrorCodes().CLIENT + 4
    msg_template = "Error in sync hook : {error}."


class SyncHookNotEnabledError(MetaDataClientError):
    code = MetaDataErrorCodes().CLIENT + 4
    msg_template = "Sync hook is not enabled."


class EventSubscribeConfigError(MetaDataClientError):
    code = MetaDataErrorCodes().CLIENT + 5
    msg_template = "Invalid config for subscriber: {detail}"


class GetEndpointError(MetaDataClientError):
    code = MetaDataErrorCodes().CLIENT + 6
    msg_template = "Invalid key for endpoint: {detail}"

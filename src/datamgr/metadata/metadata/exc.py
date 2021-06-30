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

from metadata.util.i18n import lazy_selfish as _


class ErrorCodes(object):
    # 数据平台的平台代码--7位错误码的前2位
    BKDATA_PLAT_CODE = '15'
    # 数据平台子模块代码--7位错误码的3，4位
    BKDATA_METADATA = '30'


class MetaDataErrorCodes(object):
    COMMON = 000
    LAYER = 250
    INTERACTOR = 50
    STATE = 100
    MYSQL_BACKEND = 150
    DGRAPH_BACKEND = 200
    RAW_ATLAS = 600
    RAW_MYSQL = 900
    DATA_QUALITY = 300
    ERP = 350
    ANALYSE = 500
    SHUNT = 1000

    def __init__(
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
        if 'message_kv' in kwargs:
            if not args:
                message = self.msg_template.format(**kwargs[str('message_kv')])
            elif len(args) == 1:
                message = args[0].format(**kwargs[str('message_kv')])
            else:
                raise TypeError('Multi args and message_kv is not allowed at same time in standard error.')
        elif args:
            if len(args) == 1:
                message = args[0]
            else:
                message = self.msg_template.format(*args)
        else:
            message = self.default_msg

        self.data = {}
        if 'data' in kwargs:
            data = kwargs.pop(str('data'))
            if not isinstance(data, dict):
                self.data['content'] = data
            else:
                self.data.update(data)

        if message:
            super(StandardErrorMixIn, self).__init__(message.encode('utf8') if isinstance(message, str) else message)
        else:
            super(StandardErrorMixIn, self).__init__()

    # RPC 错误码支持。
    @property
    def jsonrpc_error_code(self):
        return self.code


class MetaDataError(StandardErrorMixIn, Exception):
    code = MetaDataErrorCodes().COMMON
    msg_template = _('MetaData System Error: {}.')


class NotImplementedByBackendError(MetaDataError):
    code = MetaDataErrorCodes().COMMON + 2
    msg_template = _('Function {} is not implemented in backend {}.')


class MetaDataTypeError(StandardErrorMixIn, Exception):
    code = MetaDataErrorCodes().COMMON + 3
    msg_template = _('Invalid metadata type {}.')


class MetaDataTypeConvertError(MetaDataTypeError):
    code = MetaDataErrorCodes().COMMON + 4
    msg_template = _('Error metadata converter.')


class StateError(MetaDataError):
    code = MetaDataErrorCodes().STATE
    msg_template = _('Error in system state: {}.')


class ValidWaitError(StateError):
    code = MetaDataErrorCodes().STATE + 1
    msg_template = _('Error in system state: {}.')


class StateManageError(StateError):
    code = MetaDataErrorCodes().STATE + 20
    msg_template = _('Error in state manager: {}.')


class LayerError(MetaDataError):
    code = MetaDataErrorCodes().LAYER


class MySQLBackendError(MetaDataError):
    code = MetaDataErrorCodes().MYSQL_BACKEND
    msg_template = _('MySQL Backend Error: {}.')


class DgraphBackendError(MetaDataError):
    code = MetaDataErrorCodes().DGRAPH_BACKEND
    msg_template = _('Dgraph Backend Error: {}.')


class DgraphClientError(DgraphBackendError):
    code = MetaDataErrorCodes().DGRAPH_BACKEND + 1
    msg_template = _('Error occurred in Dgraph Client: {}.')


class DgraphServerError(DgraphBackendError):
    code = MetaDataErrorCodes().DGRAPH_BACKEND + 2
    msg_template = _('Error occurred in Dgraph Server: {}.')


class DgraphDefConvertError(DgraphBackendError):
    code = MetaDataErrorCodes().DGRAPH_BACKEND + 3
    msg_template = _('Error in Dgraph type Converter.')


class DgraphInstanceConvertError(DgraphBackendError):
    code = MetaDataErrorCodes().DGRAPH_BACKEND + 4
    msg_template = _('Error in Dgraph instance Converter.')


class DgraphNodeNotExistedError(DgraphBackendError):
    code = MetaDataErrorCodes().DGRAPH_BACKEND + 5
    msg_template = _('Fail to parse retrieve protocol content.')


class DispatchBackendError(MetaDataError):
    code = MetaDataErrorCodes().SHUNT
    msg_template = _('Error in shunt strategy.')


class UnhealthyFilterRequest(MetaDataError):
    code = MetaDataErrorCodes().SHUNT + 1
    msg_template = _('filter request cause service is unhealthy.')


class InteractorError(MetaDataError):
    code = MetaDataErrorCodes().INTERACTOR + 0
    msg_template = _('Error in interactor.')


class InteractTimeOut(InteractorError):
    code = MetaDataErrorCodes().INTERACTOR + 1
    msg_template = _('Dispatch timeout in {}.')


class InteractorExecuteError(InteractorError):
    code = MetaDataErrorCodes().INTERACTOR + 3
    msg_template = _('Error during interactor execute.')


class InteractorOperatorError(InteractorError):
    code = MetaDataErrorCodes().INTERACTOR + 4
    msg_template = _('Error in interactor operator.')


class ReplyError(MetaDataError):
    code = MetaDataErrorCodes().COMMON + 5
    msg_template = _('Error in record replaying.')


class ERPError(LayerError):
    code = MetaDataErrorCodes().ERP
    msg_template = _('Fail to parse retrieve protocol content.')


class ERPParamsError(ERPError):
    code = MetaDataErrorCodes().ERP + 1
    default_msg = _('Invalid params in erp input.')


class DataQualityError(MetaDataError):
    code = MetaDataErrorCodes().DATA_QUALITY + 0
    msg_template = _('Error in data quality process.')


class TargetSearchCriterionError(LayerError):
    code = MetaDataErrorCodes().LAYER + 7
    msg_template = _('Error in target search criterion.')


class AnalyseError(MetaDataError):
    code = MetaDataErrorCodes().ANALYSE
    msg_template = _('Error in metadata analyse.')


class CompareError(MetaDataError):
    code = MetaDataErrorCodes().ANALYSE + 1
    msg_template = _('Error in metadata analyse.')

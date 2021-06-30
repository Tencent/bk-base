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

from common.errorcodes import ErrorCode
from common.exceptions import BaseAPIError
from django.utils.translation import ugettext_lazy as _


class CodeCheckCommonCode(object):
    # 参数异常
    ILLEGAL_ARGUMENT_EX = ("001", _(u"参数异常"))
    # 业务逻辑问题
    LOGIC_ERR = ("100", _(u"代码校验异常"))
    # 期望外异常
    UNEXPECT_EX = ("200", _(u"预期外异常"))


class CommonException(BaseAPIError):
    """
    Batch Base class for all batch exceptions.
    """

    MODULE_CODE = ErrorCode.BKDATA_DATAAPI_CODECHECK

    def __init__(self, message, code=000):
        self.CODE = code
        self.MESSAGE = message
        super(CommonException, self).__init__()

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
from common.errorcodes import ErrorCode


class DataapiResourceCenterCode(object):
    # 参数异常
    ILLEGAL_ARGUMENT_EX = "000"
    #  -元数据接口 43x
    CALL_MEATA_EX = "431"
    # 服务异常
    INNER_SERVER_EX = "500"
    OUTTER_SERVER_EX = "501"
    # 状态异常
    NOT_SUCCESS_EX = "601"
    # 非法操作（状态不允许该操作）
    ILLEGAL_STATUS_EX = "602"
    # 非法操作（ 权限不允许该操作）
    ILLEGAL_AUTH_EX = "603"
    # 资源异常
    # 期望外异常
    UNEXPECT_EX = "777"

    ILLEGAL_ARGUMENT_ERR = ("000", _("参数异常"))
    CALL_MEATA_ERR = ("431", _("元数据接口调用异常"))
    INNER_SERVER_ERR = ("500", _("内部服务异常"))
    OUTTER_SERVER_ERR = ("501", _("外部服务异常"))
    NOT_SUCCESS_ERR = ("601", _("调用状态异常"))
    ILLEGAL_STATUS_ERR = ("601", _("非法状态"))
    ILLEGAL_AUTH_ERR = ("601", _("无权限操作"))
    UNEXPECT_ERR = ("777", _("期望外异常"))


def get_codes():
    error_codes = []
    for name in dir(DataapiResourceCenterCode):
        if name.startswith("__"):
            continue
        value = getattr(DataapiResourceCenterCode, name)
        if type(value) == tuple:
            error_codes.append(
                {
                    "code": "{}{}{}".format(
                        ErrorCode.BKDATA_PLAT_CODE, ErrorCode.BKDATA_DATAAPI_RESOURCECENTER, value[0]
                    ),
                    "name": name,
                    "message": value[1],
                    "solution": value[2] if len(value) > 2 else "-",
                }
            )
    error_codes = sorted(error_codes, key=lambda _c: _c["code"])
    return error_codes

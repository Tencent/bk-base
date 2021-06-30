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


class ErrorCode:
    # 数据平台的平台代码--7位错误码的前2位
    BKDATA_PLAT_CODE = "15"
    # 数据平台子模块代码--7位错误码的3，4位
    BKDATA_COMMON = "00"
    BKDATA_DATABUS = "06"
    BKDATA_DATAAPI_DATABUS = "70"
    BKDATA_DATAAPI_STREAM = "71"
    BKDATA_DATAAPI_BATCH = "72"
    BKDATA_DATAAPI_ADMIN = "73"
    BKDATA_DATAAPI_FLOW = "74"
    BKDATA_DATAAPI_JOBNAVI = "75"
    BKDATA_DATAAPI_ACCESS = "76"
    BKDATA_DATAAPI_COLLECTOR = "77"
    BKDATA_DATAAPI_STOREKIT = "78"
    BKDATA_DATAAPI_MODELFLOW = "79"
    BKDATA_DATAAPI_DATALAB = "80"
    BKDATA_DATAAPI_UDF = "81"
    BKDATA_DATAAPI_DATACUBE = "82"
    BKDATA_DATAAPI_RESOURCECENTER = "84"
    BKDATA_DATAAPI_CODECHECK = "85"
    BKDATA_STREAM = "08"
    BKDATA_BATCH = "09"
    BKDATA_DMONITOR = "10"
    BKDATA_AUTH = "11"
    BKDATA_MODELFLOW = "12"
    BKDATA_META = "21"
    BKDATA_METADATA = "30"
    BKDATA_UC = "31"
    BKDATA_DATAQUERY = "32"
    BKDATA_OLD = "33"
    BKDATA_SUPERSET = "34"

    BKDATA_DATAAPI_APPS = {
        "access": BKDATA_DATAAPI_ACCESS,
        "auth": BKDATA_AUTH,
        "dataquery": BKDATA_DATAQUERY,
        "dataflow": BKDATA_DATAAPI_FLOW,
        "storekit": BKDATA_DATAAPI_STOREKIT,
        "datamanage": BKDATA_DATAAPI_ADMIN,
        "databus": BKDATA_DATAAPI_DATABUS,
        "jobnavi": BKDATA_DATAAPI_JOBNAVI,
        "datalab": BKDATA_DATAAPI_DATALAB,
        "meta": BKDATA_META,
        "old": BKDATA_OLD,
        "codecheck": BKDATA_DATAAPI_CODECHECK,
    }

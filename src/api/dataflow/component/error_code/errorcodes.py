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


class DataapiCommonCode(object):
    # 参数异常
    ILLEGAL_ARGUMENT_EX = "000"
    # 数据异常
    NO_INPUT_DATA_EX = "101"
    NO_OUTPUT_DATA_EX = "111"
    # DB操作异常
    #  -DSL 40x
    SELECT_EX = "401"
    INSERT_EX = "402"
    UPDATE_EX = "403"
    DELETE_EX = "404"
    #  -DDL 41x
    CREATE_EX = "411"
    #  -存查接口 42x
    CALL_SQ_CREATE_EX = "421"
    CALL_SQ_EXISTS_EX = "422"
    #  -元数据接口 43x
    CALL_MEATA_EX = "431"
    # 服务异常
    INNER_SERVER_EX = "500"
    OUTTER_SERVER_EX = "501"
    # 状态异常
    NOT_SUCCESS_EX = "601"
    ILLEGAL_STATUS_EX = "602"
    # 资源异常
    NOT_ENOUGH_RESOURCE_EX = "701"
    OVER_LIMIT_RESOURCE_EX = "702"
    LOWER_LIMIT_RESOURCE_EX = "703"
    NOT_FOUND_RESOURCE_EX = "704"
    # 期望外异常
    UNEXPECT_EX = "777"
    #

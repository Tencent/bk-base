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

import json

from datahub.accesshandlers.idata import IDataHandler

bk_biz_id = 1
table_name = ""
operator = ""
ser_name = ""


def test_get_table_list_by_cc_id():
    res = IDataHandler().get_table_list_by_cc_id(bk_biz_id, operator)
    print(json.dumps(res))


def test_get_table_list_by_service_name():
    res = IDataHandler().get_table_list_by_service_name(ser_name, operator)
    print(json.dumps(res))


def test_get_table_info_all():
    # get all tables
    res = IDataHandler().get_table_info(bk_biz_id)
    print(json.dumps(res))


def test_get_table_info():
    # get specific table
    res = IDataHandler().get_table_info(bk_biz_id, table_name)
    print(json.dumps(res))


def test_get_service_list():
    res = IDataHandler.get_service_list(operator, bk_biz_id)
    print(json.dumps(res))
    for item in res:
        assert int(item["cc_id"]) == bk_biz_id


def test_get_service_map_info():
    res_all = IDataHandler.get_service_map_info(operator)
    assert len(res_all["data"]) > 0

    # filter by bk_biz_id
    res = IDataHandler.get_service_map_info(operator, bk_biz_id)
    assert len(res_all["data"]) > len(res["data"]) > 0
    for item in res["data"]:
        assert int(item["cc_id"]) == bk_biz_id

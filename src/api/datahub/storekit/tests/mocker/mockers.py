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


def get_mock_hermes_result_succ():
    """mock请求hermes创建表成功的返回结果"""
    return '{"status":"SUCCESS","message":"config success!","code":0}'


def get_mock_hermes_result_failed():
    """mock请求hermes创建表失败的返回结果"""
    return "java.lang.NullPointerException"


def get_mock_hermes_conn_info():
    """mock hermes的连接信息"""
    return {"bid": "b_xxxxx", "tdbank_cluster_id": 90, "host": "host.ip", "port": 8080}


def get_mock_tspider_conn_info():
    """mock tspider的连接信息"""
    return {"host": "x.x.x.x", "password": "pwd\n", "port": 25000, "user": "test"}


def get_mock_mysql_conn_info():
    """mock mysql的连接信息"""
    return '{"host": "host.host", "password": "pwd", "port": 3306, "user": "user.user"}'


def get_mock_phy_tb_fields_delete():
    """mock rt对应的物理表结构, 字段删除情况"""
    from collections import OrderedDict

    columns = OrderedDict()
    columns["thedate"] = "int"
    columns["dtEventTime"] = "varchar"
    columns["dtEventTimeStamp"] = "bigint"
    columns["localTime"] = "varchar"
    columns["field1"] = "varchar"
    columns["field2"] = "int"
    columns["field3"] = "bigint"
    return columns


def get_mock_phy_tb_fields_no_modify():
    """mock rt对应的物理表结构, 字段没有修改, flow正常重启情况"""
    from collections import OrderedDict

    columns = OrderedDict()
    columns["thedate"] = "int"
    columns["dtEventTime"] = "varchar"
    columns["dtEventTimeStamp"] = "bigint"
    columns["localTime"] = "varchar"
    columns["field1"] = "varchar"
    columns["field2"] = "int"
    return columns


def get_mock_phy_tb_fields_modify():
    """mock rt对应的物理表结构, 字段修改情况(字段名称修改)"""
    from collections import OrderedDict

    columns = OrderedDict()
    columns["thedate"] = "int"
    columns["dtEventTime"] = "varchar"
    columns["dtEventTimeStamp"] = "bigint"
    columns["localTime"] = "varchar"
    columns["field1"] = "varchar"
    columns["field3"] = "bigint"
    return columns


def get_mock_phy_tb_type_modify():
    """mock rt对应的物理表结构, 字段修改情况(字段类型修改)"""
    from collections import OrderedDict

    columns = OrderedDict()
    columns["thedate"] = "int"
    columns["dtEventTime"] = "varchar"
    columns["dtEventTimeStamp"] = "bigint"
    columns["localTime"] = "varchar"
    columns["field1"] = "varchar"
    # field2之前在rt中的类型是string, 最新在rt中的类型是int
    columns["field2"] = "varchar"
    return columns


def get_mock_phy_tb_tail_add():
    """mock rt对应的物理表结构, 尾部增加字段的情况"""
    from collections import OrderedDict

    columns = OrderedDict()
    columns["thedate"] = "int"
    columns["dtEventTime"] = "varchar"
    columns["dtEventTimeStamp"] = "bigint"
    columns["localTime"] = "varchar"
    columns["field1"] = "varchar"
    return columns

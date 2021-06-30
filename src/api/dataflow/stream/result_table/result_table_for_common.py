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


def load_parents(processing_info):
    """
    返回 processing parent 信息
    @param processing_info:
    @return:
        parents_info: 所有父表详细信息
        data_parents: 数据流父表列表
    """
    data_parents = []
    parents_info = []
    for one_input in processing_info["inputs"]:
        if one_input["data_set_type"] == "result_table":
            tags = one_input.get("tags", [])
            if "static_join" not in tags:
                data_parents.append(one_input["data_set_id"])
            parents_info.append({"data_set_id": one_input["data_set_id"], "tags": tags})
    return parents_info, data_parents


def fix_filed_type(type):
    """
    databus 目前支持以下字段类型，其中timestamp只用在时间戳上，不需要做改变。

        double
        int
        long
        string
        text
        timestamp

    :param type:  eg. text
    :return:    eg. string
    """
    fix_types = {"text": "string"}
    return fix_types.get(type.lower(), type)

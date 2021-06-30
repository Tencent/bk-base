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


from auth.api import MetaApi


class _LineageHandler:
    def __init__(self):
        pass

    def search(self, data_set_type, data_set_id, direction):
        """
        查询血缘内容

        @param {String} direction 血缘追溯方向，OUTPUT 是正向 INPUT 是反向
        @param {String} data_set_type 数据集类型，目前仅支持 result_table / raw_data
        @param {String} data_set_id 数据集ID
        """
        return MetaApi.lineage(
            {
                "type": data_set_type,
                "qualified_name": data_set_id,
                "depth": -1,
                "direction": direction,
            }
        ).data

    def search_input_raw_data_arr(self, result_table_id):
        """
        从 RT_ID 触发，追溯来源 RAW_DATA 列表
        """
        content = self.search("result_table", result_table_id, "INPUT")

        nodes = list(content["nodes"].values())

        raw_data_arr = [{"raw_data_id": n["qualified_name"]} for n in nodes if n["type"] == "raw_data"]

        return raw_data_arr


LineageHandler = _LineageHandler()

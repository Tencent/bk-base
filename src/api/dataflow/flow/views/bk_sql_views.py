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

from common.decorators import list_route
from common.views import APIViewSet
from django.utils.translation import ugettext as _
from rest_framework.response import Response

import dataflow.flow.utils.db_helper as db
from dataflow.shared.language import translate_based_on_language_config as translate


class BksqlViewSet(APIViewSet):
    def get_functions_info(self):
        sql_filed_format = {
            "func_group": "func_group_en",
            "func_explain": "explain_en",
            "args": "args_en",
            "type_name": "type_name_en",
            "display": "display_en",
        }
        func_info = {}
        with db.open_cursor("default") as c:
            # 参数不统一
            rows = db.list(
                c,
                """
                SELECT
                  type.type_id,type.type_name,group_info.func_group,func_info.func_name,func_info.func_explain,
                  mapper.compute_type,mapper.assign_usage,mapper.args,mapper.return_type,group_display,type_display,
                  example.example,example.return_field,example.return_value
                FROM
                  (
                    SELECT func_type,func_group_id,func_name,`{func_explain}` as func_explain
                      FROM bksql_function_config
                    WHERE args_unify=0
                  ) func_info
                  LEFT JOIN
                  (
                      SELECT func_name,example,return_field,return_value
                        FROM bksql_function_example_config
                  ) example
                  ON func_info.func_name=example.func_name
                  LEFT JOIN
                  (
                      SELECT
                        func_name,
                        CASE
                          WHEN framework_name='storm' THEN 'realtime'
                          WHEN framework_name='flink' THEN 'realtime'
                          WHEN framework_name='spark sql' THEN 'batch'
                        END as compute_type,
                        assign_usage,{args} as args,return_type
                      FROM bksql_function_mapper_config
                  ) mapper
                  ON func_info.func_name=mapper.func_name
                  LEFT JOIN
                  (
                      SELECT group_id, {func_group} as func_group, {display} as group_display
                        FROM bksql_function_group_config
                  ) group_info
                  ON func_info.func_group_id=group_info.group_id
                  RIGHT JOIN
                  (
                      SELECT
                        type_id,{type_name} as type_name,{display} as type_display
                      FROM bksql_function_type_config
                  ) type
                  ON  func_info.func_type = type.type_id
                """.format(
                    **sql_filed_format
                ),
            )
            for row in rows:
                type_name = translate(row["type_name"])
                func_group = translate(row["func_group"])
                args = translate(row["args"])
                type_display = translate(row["type_display"])
                group_display = translate(row["group_display"])
                func_explain = translate(row["func_explain"])

                if type_name not in func_info:
                    func_info[type_name] = {}
                if func_group not in func_info[type_name]:
                    func_info[type_name][func_group] = {}
                if "display" not in func_info[type_name]:
                    func_info[type_name]["display"] = type_display
                if "id" not in func_info[type_name]:
                    func_info[type_name]["id"] = row["type_id"]
                if row["func_name"] not in func_info[type_name][func_group]:
                    func_info[type_name][func_group][row["func_name"]] = {}
                if "display" not in func_info[type_name][func_group]:
                    func_info[type_name][func_group]["display"] = group_display
                func_info[type_name][func_group][row["func_name"]]["name"] = str(row["func_name"])
                # example
                func_info[type_name][func_group][row["func_name"]]["usage_case"] = row["example"]
                func_info[type_name][func_group][row["func_name"]]["return_field"] = row["return_field"]
                func_info[type_name][func_group][row["func_name"]]["return_value"] = row["return_value"]
                if "usages" not in func_info[type_name][func_group][row["func_name"]]:
                    func_info[type_name][func_group][row["func_name"]]["usages"] = []
                if row["compute_type"] == "realtime":
                    compute_type = _("实时计算")
                elif row["compute_type"] == "batch":
                    compute_type = _("离线计算")
                elif row["compute_type"] == "model_app":
                    compute_type = _("模型应用")
                else:
                    compute_type = _("通用")
                func_info[type_name][func_group][row["func_name"]]["usages"].append(
                    {
                        "compute_type": compute_type,
                        "usage": row["assign_usage"],
                        "args": args,
                        "return_type": row["return_type"],
                    }
                )
                func_info[type_name][func_group][row["func_name"]]["explain"] = func_explain

            # 参数统一
            second_rows = db.list(
                c,
                """
                SELECT
                  type.type_id,type.type_name,group_info.func_group,func_info.func_name,func_info.func_explain,
                  mapper.assign_usage,mapper.args,mapper.return_type,group_display,type_display,
                  example.example,example.return_field,example.return_value
                FROM
                  (
                    SELECT func_type,func_group_id,func_name,`{func_explain}` as func_explain
                      FROM bksql_function_config
                    WHERE args_unify=1
                  ) func_info
                  LEFT JOIN (
                    SELECT func_name,example,return_field,return_value
                    FROM bksql_function_example_config
                  ) example
                  ON func_info.func_name=example.func_name
                  JOIN (
                    SELECT
                        DISTINCT func_name,assign_usage,{args} as args,return_type
                    FROM bksql_function_mapper_config
                  ) mapper
                  ON func_info.func_name=mapper.func_name
                  LEFT JOIN
                  (
                    SELECT group_id, {func_group} as func_group, {display} as group_display
                    FROM bksql_function_group_config
                  ) group_info
                  ON func_info.func_group_id=group_info.group_id
                  RIGHT JOIN
                  (
                    SELECT
                      type_id,{type_name} as type_name, {display} as type_display
                    FROM bksql_function_type_config
                  ) type
                  ON func_info.func_type = type.type_id
                """.format(
                    **sql_filed_format
                ),
            )
            for row in second_rows:
                type_name = translate(row["type_name"])
                func_group = translate(row["func_group"])
                args = translate(row["args"])
                type_display = translate(row["type_display"])
                group_display = translate(row["group_display"])
                func_explain = translate(row["func_explain"])

                if type_name not in func_info:
                    func_info[type_name] = {}
                if func_group not in func_info[type_name]:
                    func_info[type_name][func_group] = {}
                if "display" not in func_info[type_name]:
                    func_info[type_name]["display"] = type_display
                if "id" not in func_info[type_name]:
                    func_info[type_name]["id"] = row["type_id"]
                if row["func_name"] not in func_info[type_name][func_group]:
                    func_info[type_name][func_group][row["func_name"]] = {}
                if "display" not in func_info[type_name][func_group]:
                    func_info[type_name][func_group]["display"] = group_display
                func_info[type_name][func_group][row["func_name"]]["name"] = row["func_name"]
                # example
                func_info[type_name][func_group][row["func_name"]]["usage_case"] = row["example"]
                func_info[type_name][func_group][row["func_name"]]["return_field"] = row["return_field"]
                func_info[type_name][func_group][row["func_name"]]["return_value"] = row["return_value"]
                func_info[type_name][func_group][row["func_name"]]["usages"] = [
                    {
                        "usage": row["assign_usage"],
                        "args": args,
                        "return_type": row["return_type"],
                        "compute_type": _("通用"),
                    }
                ]
                func_info[type_name][func_group][row["func_name"]]["explain"] = func_explain

            # 规范化json返回格式
            res = []
            for s_type_name, d_func_groups in list(func_info.items()):
                l_func_groups = []
                res.append(
                    {
                        "type_name": s_type_name,
                        "func_groups": l_func_groups,
                        "display": d_func_groups.pop("display"),
                        "id": d_func_groups.pop("id"),
                    }
                )
                for s_group_name, d_functions in list(d_func_groups.items()):
                    l_functions = []
                    l_func_groups.append(
                        {
                            "group_name": s_group_name,
                            "func": l_functions,
                            "display": d_functions.pop("display"),
                        }
                    )
                    for s_func_name, d_function in list(d_functions.items()):
                        if s_func_name is None or s_func_name == "":
                            continue
                        l_functions.append(d_function)
            new_res = sorted(res, key=lambda k: k["id"])
        return new_res

    @list_route(methods=["get"], url_path="bksql_func_info")
    def list_bksql_functions_info(self, request):
        """
        @api {get} /dataflow/flow/bksql/bksql_func_info 获取bksql函数库信息
        @apiName get_bksql_func_info
        @apiGroup Flow
        @apiSuccessExample {json} 成功返回
        [
            {
                "func_groups": [
                    {
                        "func": [
                            {
                                "usages": [
                                    {
                                        "usage": "mod(LONG a,LONG b)",
                                        "return_type": "INT",
                                        "args": "a LONG，被除数；b LONG，除数。",
                                        "compute_type": "通用"
                                    }
                                ],
                                "explain": "求余数，返回对字段a除以b的余数。",
                                "name": "mod",
                                "usage_case": "SELECT mod(5,2) as result FROM TABLE",
                                "return_field": "result",
                                "return_value": "1"
                            }
                        ],
                        "display": "Calculate a set of values and return a single value",
                        "group_name": "数学函数"
                    }
                ],
                "type_name": "内置函数"
                "display": "Predefined function",
                "id": 0
            }
        ]
        """
        return Response(self.get_functions_info())

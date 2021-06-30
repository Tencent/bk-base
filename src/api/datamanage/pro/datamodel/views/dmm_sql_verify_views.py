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


import re

from datamanage.exceptions import ParamQueryMetricsError
from datamanage.pro.datamodel.handlers.verifier import SQLVerifier
from datamanage.pro.datamodel.serializers.data_model import (
    DataModelColumnProcessVerifySerializer,
)
from django.utils.translation import ugettext as _
from rest_framework.response import Response

from common.decorators import list_route, params_valid
from common.views import APIViewSet


class DataModelSQLVerifyViewSet(APIViewSet):

    PREFIX_CHAR_CNT = 12
    """
    sql片段在单字段完整`SELECT`语句中的位置前置偏移(i.e., 'SELECT cast(' + SQL_PART)
    """

    @list_route(methods=["post"], url_path="column_process")
    @params_valid(serializer=DataModelColumnProcessVerifySerializer)
    def verify(self, request, params):
        """
        @api {post} /datamanage/datamodel/sql_verify/column_process/ 数据模型字段加工校验

        @apiVersion 3.5.0
        @apiGroup DataModelSqlVerify
        @apiName datamodel_sql_verify_column_process
        @apiDescription 数据模型字段加工校验

        @apiParam {String} table_name 表名称
        @apiParam {List[]} verify_fields 需要进行校验的"输出"字段名列表
        @apiParam {List[]} scope_field_list sql执行环境(输入字段)列表
        @apiParam {String} scope_field_list.field_name 输入字段名称
        @apiParam {String} scope_field_list.field_type 输入字段类型
        @apiParam {Object[]} scope_field_list.field_clean_content (可选, 仅在require_field_list不存在时有效)sql片段表达式
        @apiParam {List[]} require_field_list 语句执行结果需要的表 (require_field_list 为可选字段,
        当require_field_list不存在时，默认输入输出格式相同，使用scope_field_list替代)
        @apiParam {String} require_field_list.field_name 输出字段名称
        @apiParam {String} require_field_list.field_type 输出字段类型
        @apiParam {Object[]} require_field_list.field_clean_content (可选)sql片段表达式
        @apiParamExample {json} 参数样例:
        {
            "table_name": "table_name_str",
            "verify_fields": ["price"],
            "scope_field_list":[
                {
                    "field_name":"price_num",
                    "field_type":"long",
                    "field_clean_content":{
                        "clean_option":"SQL",
                        "clean_content":"price_num * 100 as price"
                    }
                },
                {
                    "field_name":"channel_id",
                    "field_type":"int",
                    "field_clean_content":{},
                }
            ],
            "require_field_list":[
                {
                    "field_name":"price",
                    "field_type":"long",
                    "field_clean_content":{
                        "clean_option":"SQL",
                        "clean_content":"price_num * 100 as price"
                    }
                },
                {
                    "field_name":"channel_id",
                    "field_type":"string",
                    "field_clean_content":{},
                }
            ]
        }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "verify_result": True,
                    "origin_fields_dict": {}
                }
            }
        """

        table_name = params.get("table_name", None)
        verify_fields = params.get("verify_fields", [])
        scope_field_list = params.get("scope_field_list", [])
        require_field_list = params.get("require_field_list", [])
        require_field_list = require_field_list if require_field_list else scope_field_list
        # 目前只支持scope、require中都只包含一张表
        # 兼容field_clean_content字段的数据格式
        for field_item in require_field_list:
            if "field_clean_content" in field_item and not isinstance(field_item["field_clean_content"], dict):
                field_item["field_clean_content"] = {}

            # 如果清洗逻辑为空，则认为field_clean_content为空
            if not field_item["field_clean_content"].get("clean_content"):
                field_item["field_clean_content"] = {}
        verify_param_dict = dict(
            sql_parts=dict(
                table_name=table_name,
                fields={
                    field_item["field_name"]: field_item.get("field_clean_content", {}).get(
                        "clean_content", field_item["field_name"]
                    )
                    for field_item in require_field_list
                    if field_item["field_name"] in verify_fields
                },
            ),
            scopes={table_name: scope_field_list},
            auto_format=False,
        )
        require_schema = {
            table_name: [field_item for field_item in require_field_list if field_item["field_name"] in verify_fields]
        }
        verifier = SQLVerifier(**verify_param_dict)
        matched, match_info = verifier.check_res_schema(
            require_schema, "flink_sql", agg=False, extract_source_column=True
        )
        if not matched:
            err_message = self._deal_error_message_loc_info(match_info)
            raise ParamQueryMetricsError(message=_("校验失败: {detail};"), message_kv={"detail": err_message})
        return Response({"verify_result": True, "origin_fields_dict": match_info["source_columns"]})

    @classmethod
    def _deal_error_message_loc_info(cls, message):
        """
        从返回的错误信息中提取错误发生的行列号(如果有的话)

        :param message:
        :return: dict
        loc_dict {
            s_row   错误点行号(或范围错误开始行号)
            s_col   错误点列号(或范围错误开始列号)
            e_row   范围错误结束行号
            e_col   范围错误结束列号
            type    错误行列号模式类型
            seq     行列号序列(s_row, s_col, e_row, e_col顺序)
        }
        """
        loc_dict = dict(type="")
        pattern_list = [
            dict(pattern_type="point_pattern", pattern_expr=r"第(.+?)行, 第(.+?)列"),
            dict(
                pattern_type="point_pattern",
                pattern_expr=r"At line(.+?), column(.+?)[\W]+",
            ),
            dict(
                pattern_type="range_pattern",
                pattern_expr=r"第(.+?)行(.+?)列 至 第(.+?)行(.+?)列之间",
            ),
        ]
        match_groups = list()
        for pattern_item in pattern_list:
            pattern_type = pattern_item["pattern_type"]
            pattern = pattern_item["pattern_expr"]
            match_groups = re.findall(pattern, message, re.S)
            if not match_groups or not isinstance(match_groups, list):
                continue
            if cls._set_loc_info(match_groups[0], pattern_type, pattern, loc_dict):
                break
        # 先处理SQL片段错误行列数的矫正
        if loc_dict and "seq" in loc_dict:
            loc_seq = loc_dict.pop("seq")
            loc_pattern = loc_dict["loc_pattern"]
            rep_pattern = loc_pattern
            for seq_val in loc_seq:
                rep_pattern = rep_pattern.replace("(.+?)", str(seq_val), 1)
            message = re.sub(loc_pattern, rep_pattern, message)
        return message

    @classmethod
    def _set_loc_info(cls, match_groups, pattern_type, pattern_expr, loc_dict):
        """
        重新设置报错行列号再sql片段中的位置

        :param match_groups: 匹配到的完整sql中行列号组合
        :param pattern_type: 错误行列号模式类型
        :param pattern_expr: 模式表达式
        :param loc_dict: 行列号组合字典
        :return: boolean 设置是否成功
        """

        # 单点行列号组合描述
        if pattern_type == "point_pattern":
            row, col = int(match_groups[0]), int(match_groups[1])
            if not row:
                return False
            loc_dict["s_row"] = row
            loc_dict["s_col"] = col
            loc_dict["type"] = pattern_type
            loc_dict["loc_pattern"] = pattern_expr
            if row == 1:
                loc_dict["s_col"] = col - cls.PREFIX_CHAR_CNT
            loc_dict["seq"] = [loc_dict["s_row"], loc_dict["s_col"]]
        # 行列号范围描述
        elif pattern_type == "range_pattern":
            s_row, s_col = int(match_groups[0]), int(match_groups[1])
            e_row, e_col = int(match_groups[2]), int(match_groups[3])
            if not s_row or not e_row:
                return False
            loc_dict["s_row"] = s_row
            loc_dict["s_col"] = s_col
            loc_dict["e_row"] = e_row
            loc_dict["e_col"] = e_col
            loc_dict["type"] = pattern_type
            loc_dict["loc_pattern"] = pattern_expr
            if s_row == 1:
                loc_dict["s_col"] = s_col - cls.PREFIX_CHAR_CNT
            if e_row == 1:
                loc_dict["e_col"] = e_col - cls.PREFIX_CHAR_CNT
            loc_dict["seq"] = [
                loc_dict["s_row"],
                loc_dict["s_col"],
                loc_dict["e_row"],
                loc_dict["e_col"],
            ]
        else:
            return False
        return True

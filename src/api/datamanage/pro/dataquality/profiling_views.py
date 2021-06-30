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

from datamanage.pro.dataquality.config import (
    PROFILING_EXCLUDE_FIELDS,
    PROFILING_UDF_CONFIGS,
)
from datamanage.pro.dataquality.mixins.base_mixins import BaseMixin
from datamanage.pro.dataquality.serializers.profiling import (
    DatalabQuerysetProfilingSerializer,
    ProfilingResultSerializer,
)
from datamanage.pro.pizza_settings import (
    DATA_PROFILING_SESSION_KEY,
    SESSION_GEOG_AREA_CODE,
)
from datamanage.utils.api import DataflowApi, MetaApi
from rest_framework.response import Response

from common.decorators import list_route, params_valid
from common.views import APIViewSet


class DataProfilingViewSet(BaseMixin, APIViewSet):
    @list_route(methods=["get"], url_path="result_tables")
    def result_tables(self, request):
        """
        @api {get} /datamanage/dataquality/profiling/result_tables/ 获取剖析结果表列表

        @apiVersion 3.5.0
        @apiGroup DataQualityProfiling
        @apiName dataquality_profiling_result_table_list
        @apiDescription 获取剖析结果表列表

        @apiSuccess (200) {List} data 结果表ID列表

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    "1_profiling_table1",
                    "1_profiling_table2"
                ]
            }
        """
        result_tables = self.fetch_profiling_result_tables()
        return Response(result_tables)

    @list_route(methods=["post"], url_path="queryset")
    @params_valid(serializer=DatalabQuerysetProfilingSerializer)
    def datalab_queryset_profiling(self, request, params):
        """
        @api {post} /datamanage/dataquality/profiling/queryset/ 剖析Queryset结果表

        @apiVersion 3.5.0
        @apiGroup DataQualityProfiling
        @apiName dataquality_profiling_queryset
        @apiDescription 剖析Queryset结果表

        @apiParam {String} result_table_id Queryset结果表ID

        @apiSuccess (200) {List} data 剖析请求ID

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": [
                    "1_profiling_table1",
                    "1_profiling_table2"
                ]
            }
        """
        result_table_id = params["result_table_id"]

        fields = MetaApi.result_tables.fields({"result_table_id": result_table_id}, raise_exception=True).data

        session_server_id = self.get_session_server_id(DATA_PROFILING_SESSION_KEY)
        tmp_table = "{result_table_id}_tmp_table".format(result_table_id=result_table_id)
        profiling_sql = self.generate_profiling_sql(fields, tmp_table)

        codes = [
            """import json""",
            """df = spark_session.create_dataframe('{}')""".format(result_table_id),
            """df.createOrReplaceTempView('{}')""".format(tmp_table),
            """result_df = spark_session.sql('{}')""".format(profiling_sql),
            """profiling_results = result_df.collect()""",
            """print(json.dumps(profiling_results[0].asDict()))""",
        ]

        res = DataflowApi.interactive_codes.create(
            {
                "server_id": session_server_id,
                "code": "\n".join(codes),
                "geog_area_code": SESSION_GEOG_AREA_CODE,
            },
            raise_exception=True,
        )

        return Response(res.data.get("id"))

    def generate_profiling_sql(self, fields, table_name):
        select_parts = []
        for field in fields:
            if field.get("field_name") in PROFILING_EXCLUDE_FIELDS:
                continue

            for udf_key, udf_config in list(PROFILING_UDF_CONFIGS.items()):
                if field.get("field_type") in udf_config.get("field_types", []):
                    udf_result_field = "{}_{}".format(field.get("field_name"), udf_key)
                    select_parts.append(
                        "{udf_name}({field_name}) AS {udf_result_field}".format(
                            udf_name=udf_config.get("udf_name"),
                            field_name=field.get("field_name"),
                            udf_result_field=udf_result_field,
                        )
                    )
        return "SELECT {select_parts_str} FROM {table_name}".format(
            select_parts_str=", ".join(select_parts),
            table_name=table_name,
        )

    @list_route(methods=["get"], url_path="result")
    @params_valid(serializer=ProfilingResultSerializer)
    def profiling_result(self, request, params):
        """
        @api {get} /datamanage/dataquality/profiling/result/ 获取剖析异步请求状态及结果

        @apiVersion 3.5.0
        @apiGroup DataQualityProfiling
        @apiName dataquality_profiling_result
        @apiDescription 获取剖析异步请求状态及结果

        @apiParam {Int} profiling_request_id 剖析请求ID

        @apiSuccess (200) {Int} data.progress 剖析请求进度
        @apiSuccess (200) {String} data.state 剖析请求状态
        @apiSuccess (200) {Object} data.result 剖析请求结果
        @apiSuccess (200) {Object} data.result.field1 字段一剖析结果
        @apiSuccess (200) {Object} data.result.field1.descriptive_statistics 字段一描述性统计结果
        @apiSuccess (200) {Object} data.result.field1.data_distribution 字段一数据分布结果
        @apiSuccess (200) {Object} data.result.field1.cardinal_distribution 字段一基数分布结果

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                    "progress": 0.5,
                    "state": "running",
                    "result": {
                        "field1": {
                            "descriptive_statistics": {},
                            "data_distribution": {},
                            "cardinal_distribution": {}
                        }
                    }
                }
            }
        """
        profiling_request_id = params["profiling_request_id"]

        session_server_id = self.get_session_server_id(DATA_PROFILING_SESSION_KEY)

        res = DataflowApi.interactive_codes.retrieve(
            {
                "code_id": profiling_request_id,
                "server_id": session_server_id,
                "geog_area_code": SESSION_GEOG_AREA_CODE,
            },
            raise_exception=True,
        )

        profiling_result = {}
        if res.data.get("state") == "available":
            if res.data.get("output", {}).get("status") == "ok":
                code_results = json.loads(res.data.get("output", {}).get("data", {}).get("text/plain", {}))

                for udf_result_field, udf_result in list(code_results.items()):
                    for udf_key in list(PROFILING_UDF_CONFIGS.keys()):
                        if udf_result_field.endswith(udf_key):
                            # 这里减掉的1是下划线所占的长度
                            field = udf_result_field[: len(udf_result_field) - len(udf_key) - 1]
                            if field not in profiling_result:
                                profiling_result[field] = {}
                            profiling_result[field][udf_key] = json.loads(udf_result)

        return Response(
            {
                "progress": res.data.get("progress"),
                "state": res.data.get("state"),
                "result": profiling_result,
            }
        )

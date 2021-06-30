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

from common.api.base import DataAPI
from common.bklanguage import BkLanguage
from django.utils.translation import ugettext_lazy as _

from dataflow.pizza_settings import (
    BASE_BKSQL_EXTEND_URL,
    BASE_BKSQL_FLINKSQL_URL,
    BASE_BKSQL_MLSQL_URL,
    BASE_BKSQL_SPARKSQL_URL,
    BASE_BKSQL_STORMSQL_URL,
    BASE_BKSQL_URL,
)


class _BksqlApi(object):
    def __init__(self):

        language = BkLanguage.current_language()
        if language == BkLanguage.EN:
            # 英文
            bk_lang = "en"
        elif language == BkLanguage.CN:
            # 中文
            bk_lang = "zh-cn"
        else:
            bk_lang = "zh-cn"

        headers = {
            "Content-type": "application/json",
            "blueking_language": bk_lang,
            "blueking-language": bk_lang,
        }

        self.list_result_tables_by_sql_parser = DataAPI(
            url=BASE_BKSQL_FLINKSQL_URL + "bksql/api/convert/flink-sql",
            method="POST",
            module="bksql.flink-sql",
            description=_("flink实时bksql解析"),
        )

        self.list_rts_by_storm_sql_parser = DataAPI(
            url=BASE_BKSQL_STORMSQL_URL + "bksql/api/convert/storm-sql",
            method="POST",
            module="bksql.storm-sql",
            description=_("storm实时bksql解析"),
        )

        self.list_result_tables_by_mlsql_parser = DataAPI(
            url=BASE_BKSQL_MLSQL_URL + "bksql/api/convert/mlsql",
            method="POST",
            module="bksql.mlsql",
            description=_("mlsql实时bksql解析"),
            custom_headers=headers,
        )

        self.list_result_entity_by_mlsql_parser = DataAPI(
            url=BASE_BKSQL_MLSQL_URL + "bksql/api/convert/mlsql-entity-usage",
            method="POST",
            module="bksql.mlsql",
            description=_("mlsql实时bksql解析产出结果"),
            custom_headers=headers,
        )

        self.mlsql_sub_query_parser = DataAPI(
            url=BASE_BKSQL_MLSQL_URL + "bksql/api/convert/mlsql-sub-query",
            method="POST",
            module="bksql.mlsql",
            description=_("mlsql解析子查询相关信息"),
            custom_headers=headers,
        )

        self.mlsql_table_names_parser = DataAPI(
            url=BASE_BKSQL_MLSQL_URL + "bksql/api/convert/mlsql-table-names",
            method="POST",
            module="bksql.mlsql",
            description=_("mlsql所有相关表"),
            custom_headers=headers,
        )

        self.mlsql_column_names_parser = DataAPI(
            url=BASE_BKSQL_MLSQL_URL + "bksql/api/convert/mlsql-columns",
            method="POST",
            module="bksql.mlsql",
            description=_("mlsql解析子查询所有返回列"),
            custom_headers=headers,
        )

        self.mlsql_query_source_parser = DataAPI(
            url=BASE_BKSQL_MLSQL_URL + "bksql/api/convert/mlsql-query-source",
            method="POST",
            module="bksql.mlsql",
            description=_("mlsql解析子查询的时间范围等信息"),
            custom_headers=headers,
        )

        self.convert_by_common_sql_parser = DataAPI(
            url=BASE_BKSQL_URL + "bksql/api/v1/convert/common",
            method="POST",
            module="bksql.common",
            description=_("bksql通用解析"),
        )

        self.convert_dimension = DataAPI(
            url=BASE_BKSQL_EXTEND_URL + "bksql/api/convert/dimension",
            method="POST",
            module="bksql.dimension",
            description=_("bksql获取维度信息"),
        )

        self.sql_column = DataAPI(
            url=BASE_BKSQL_EXTEND_URL + "bksql/api/convert/sql-column",
            method="POST",
            module="bksql.sql_column",
            description=_("获取sql-column"),
        )

        self.sql_to_json_api = DataAPI(
            url=BASE_BKSQL_URL + "bksql/api/v1/convert/offline",
            method="POST",
            module="bksql.offline",
            description=_("离线bksql解析"),
            custom_headers=headers,
        )

        self.tdw_sql_to_json_api = DataAPI(
            url=BASE_BKSQL_URL + "bksql/api/v1/convert/offline-tdw",
            method="POST",
            module="bksql.offline",
            description=_("离线tdw-bksql解析"),
            custom_headers=headers,
        )

        self.storm_parser_api = DataAPI(
            url=BASE_BKSQL_URL + "bksql/api/v1/convert/real-time",
            method="POST",
            module="bksql.storm",
            description=_("实时 storm bksql 解析"),
            custom_headers=headers,
        )

        self.convert_function = DataAPI(
            url=BASE_BKSQL_EXTEND_URL + "bksql/api/convert/function",
            method="POST",
            module="bksql.udf",
            description="bksql function",
        )

        self.spark_sql_api = DataAPI(
            url=BASE_BKSQL_SPARKSQL_URL + "bksql/api/convert/spark-sql",
            method="POST",
            module="bksql.park-sql",
            description=_("离线spark-sql解析"),
            custom_headers=headers,
        )

        self.one_time_spark_sql_api = DataAPI(
            url=BASE_BKSQL_SPARKSQL_URL + "bksql/api/convert/one-time-spark-sql",
            method="POST",
            module="bksql.park-sql",
            description=_("离线one-time-spark-sql解析"),
            custom_headers=headers,
        )


BksqlApi = _BksqlApi()

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

from dataflow import pizza_settings
from dataflow.batch import settings
from dataflow.shared.api.modules.bksql import BksqlApi
from dataflow.shared.api.modules.databus import DatabusApi
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util
from dataflow.shared.log import batch_logger


class DatabusHelper(object):
    @staticmethod
    def is_rt_batch_import(result_table_id):
        res = DatabusApi.result_tables.is_batch_data({"result_table_id": result_table_id})
        res_util.check_response(res)
        batch_logger.info(res.data)
        return res.data

    @staticmethod
    def import_hdfs(result_table_id, data_dir, bk_username):
        request_args = {
            "result_table_id": result_table_id,
            "data_dir": data_dir,
            "bk_app_code": settings.PASS_APP_CODE,
            "bk_app_secret": settings.PASS_APP_TOKEN,
            "bk_username": bk_username,
        }
        get_execute_status_api = DataAPI(
            url=pizza_settings.BASE_DATABUS_URL + "import_hdfs/",
            method="POST",
            module="databus",
            description="hdfs数据导出",
        )
        res = get_execute_status_api(request_args)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def tdw_shipper(result_table_id, data_time, bk_username):
        request_args = {
            "result_table_id": result_table_id,
            "data_time": data_time,
            "bk_app_code": settings.PASS_APP_CODE,
            "bk_app_secret": settings.PASS_APP_TOKEN,
            "bk_username": bk_username,
        }
        get_execute_status_api = DataAPI(
            url=pizza_settings.BASE_DATABUS_URL + "tdw_shipper/",
            method="POST",
            module="databus",
            description="tdw数据导出",
        )
        res = get_execute_status_api(request_args)
        res_util.check_response(res)
        return res.data


class BksqlHelper(object):
    @staticmethod
    def select_to_json_batch(sql):
        res = BksqlApi.sql_to_json_api({"sql": sql})
        res_util.check_response(res)
        batch_logger.info(res.data)
        return res.data

    @staticmethod
    def select_to_json_batch_tdw(sql):
        res = BksqlApi.tdw_sql_to_json_api({"sql": sql})
        res_util.check_response(res)
        batch_logger.info(res.data)
        return res.data

    @staticmethod
    def spark_sql(sql, spark_sql_properties):
        res = BksqlApi.spark_sql_api({"sql": sql, "properties": spark_sql_properties})
        res_util.check_response(res)
        batch_logger.info(res.data)
        return res.data

    @staticmethod
    def one_time_spark_sql(sql, spark_sql_properties):
        res = BksqlApi.one_time_spark_sql_api({"sql": sql, "properties": spark_sql_properties})
        res_util.check_response(res)
        batch_logger.info(res.data)
        return res.data

    @staticmethod
    def sql_column(sql):
        res = BksqlApi.sql_column({"sql": sql})
        res_util.check_response(res)
        batch_logger.info(res.data)
        return res.data

    @staticmethod
    def convert_dimension(params):
        """
        获取sql的dimension信息

        :param params: {'sql': xxx}
        :return:
        """
        res = BksqlApi.convert_dimension(params)
        res_util.check_response(res)
        batch_logger.info(res.data)
        return res.data

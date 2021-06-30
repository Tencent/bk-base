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

from common.exceptions import ApiRequestError

from dataflow.shared.api.modules.bksql import BksqlApi
from dataflow.shared.log import udf_logger as logger
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper


class MetaApiHelper(object):
    @staticmethod
    def get_result_table(result_table_id, related=None, not_found_raise_exception=True):
        return ResultTableHelper.get_result_table(
            result_table_id,
            related=related,
            not_found_raise_exception=not_found_raise_exception,
        )


class BksqlHelper(object):
    @staticmethod
    def parse_sql_function(params):
        """
        parse sql's function

        :param params:
        :return:
        """
        res = BksqlApi.convert_function(params)
        logger.info("API[bksql convert function] : result is " + str(res.is_success()))
        logger.info("API[bksql convert function] : data is " + str(res.data))
        if not res.is_success():
            raise ApiRequestError(res.message)
        return res.data

    @staticmethod
    def parse_spark_sql(params):
        """
        parser spark sql

        :param params:
        :return:
        """
        res = BksqlApi.spark_sql_api(params)
        logger.info("API[bksql convert spark sql]: result is " + str(res.is_success()))
        logger.info("API[bksql convert spark sql]: data is " + str(res.data))
        if not res.is_success():
            raise ApiRequestError(res.message)
        return res.data

    @staticmethod
    def parse_flink_sql(params):
        """
        parser flink sql

        :param params:
        :return:
        """
        res = BksqlApi.list_result_tables_by_sql_parser(params)
        logger.info("API[bksql convert flink sql]: result is " + str(res.is_success()))
        logger.info("API[bksql convert flink sql]: data is " + str(res.data))
        if not res.is_success():
            raise ApiRequestError(res.message)
        return res.data

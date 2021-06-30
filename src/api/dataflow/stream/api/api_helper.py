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
from common.local import get_request_username

from dataflow.shared.access.access_helper import AccessHelper
from dataflow.shared.api.modules.bksql import BksqlApi
from dataflow.shared.api.modules.stream import StreamApi
from dataflow.shared.databus.databus_helper import DatabusHelper
from dataflow.shared.log import stream_logger as logger
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper
from dataflow.shared.meta.transaction.meta_transaction_helper import MetaTransactionHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper


class MetaApiHelper(object):
    @staticmethod
    def get_result_table(result_table_id, related=None, not_found_raise_exception=True):
        return ResultTableHelper.get_result_table(
            result_table_id,
            related=related,
            not_found_raise_exception=not_found_raise_exception,
        )

    @staticmethod
    def is_result_table_exist(result_table_id):
        return bool(MetaApiHelper.get_result_table(result_table_id, not_found_raise_exception=False))

    @staticmethod
    def get_result_table_fields(result_table_id):
        return ResultTableHelper.get_result_table_fields(result_table_id)

    @staticmethod
    def set_result_table(result_table):
        return ResultTableHelper.set_result_table(result_table)

    @staticmethod
    def update_result_table(result_table):
        return ResultTableHelper.update_result_table(result_table)

    @staticmethod
    def delete_result_table(result_table_id):
        ResultTableHelper.delete_result_table(result_table_id)

    @staticmethod
    def set_data_processing(params):
        return DataProcessingHelper.set_data_processing(params)

    @staticmethod
    def get_data_processing(processing_id):
        return DataProcessingHelper.get_data_processing(processing_id)

    @staticmethod
    def get_data_processing_via_erp(processing_id):
        return DataProcessingHelper.get_dp_via_erp(processing_id)

    @staticmethod
    def is_data_processing_exist(processing_id):
        return bool(MetaApiHelper.get_data_processing(processing_id))

    @staticmethod
    def update_data_processing(processing):
        return DataProcessingHelper.update_data_processing(processing)

    @staticmethod
    def delete_data_processing(processing_id):
        DataProcessingHelper.delete_data_processing(processing_id)

    @staticmethod
    def bulk_delete_data_processing(processings):
        DataProcessingHelper.bulk_delete_dp(processings)

    @staticmethod
    def meta_transaction(api_operate_list):
        return MetaTransactionHelper.create_meta_transaction(api_operate_list)

    @staticmethod
    def bulk_delete_dp_by_username(processings, bk_username):
        DataProcessingHelper.bulk_delete_dp_by_username(processings, bk_username)


class BksqlHelper(object):
    @staticmethod
    def list_result_tables_by_sql_parser(params):
        res = BksqlApi.list_result_tables_by_sql_parser(params)
        logger.info("API[flink sql] : result is " + str(res.is_success()))
        logger.info("API[flink sql] : data is " + str(res.data))
        if not res.is_success():
            raise ApiRequestError(res.message)
        return res.data

    @staticmethod
    def list_rts_by_storm_sql_parser(params):
        res = BksqlApi.list_rts_by_storm_sql_parser(params)
        logger.info("API[storm sql v3] : result is " + str(res.is_success()))
        logger.info("API[storm sql v3] : data is " + str(res.data))
        if not res.is_success():
            raise ApiRequestError(res.message)
        return res.data

    @staticmethod
    def convert_by_common_sql_parser(params):
        """
        调用通用 sql 解析
        @param params: {'sql': text}
        @return:
        """
        res = BksqlApi.convert_by_common_sql_parser(params)
        logger.info("API[common sql convert] : result is " + str(res.is_success()))
        logger.info("API[common sql convert] : data is " + str(res.data))
        if not res.is_success():
            raise ApiRequestError(res.message)
        return res.data

    @staticmethod
    def convert_dimension(params):
        """
        获取sql的dimension信息

        :param params: {'sql': xxx}
        :return:
        """
        res = BksqlApi.convert_dimension(params)
        logger.info("API[bksql convert dimension] : result is " + str(res.is_success()))
        logger.info("API[bksql convert dimension] : data is " + str(res.data))
        if not res.is_success():
            raise ApiRequestError(res.message)
        return res.data

    @staticmethod
    def select_token_by_storm_sql_parser(params):
        """
        调用 storm-sql-v1
        @param params: {'sql': text}
        @return:
        """
        res = BksqlApi.storm_parser_api(params)
        logger.info("API[storm sql v1] : result is " + str(res.is_success()))
        logger.info("API[storm sql v1] : data is " + str(res.data))
        if not res.is_success():
            raise ApiRequestError(res.message)
        return res.data


class DatabusApiHelper(object):
    @staticmethod
    def get_partition_for_result_table(result_table_id):
        try:
            partition_num = DatabusHelper.get_partition_num(result_table_id)
            logger.info("the result table {} partition is {}".format(result_table_id, partition_num))
            return partition_num
        except Exception as e:
            logger.exception("Failed to get topic partition num for {}, {}".format(result_table_id, e))
            return 1

    @staticmethod
    def get_channel_info(channel_id):
        data = DatabusHelper.get_channel_info(channel_id)
        logger.info("the channel id {} info is {}".format(channel_id, data))
        return data


class AccessApiHelper(object):
    @staticmethod
    def get_raw_data(rawdata_id, bk_username=None):
        if not bk_username:
            bk_username = get_request_username()
        data = AccessHelper.get_raw_data(rawdata_id, bk_username)
        return data


class StreamApiHelper(object):
    @staticmethod
    def cancel_job(params):
        res = StreamApi.jobs.cancel(params)
        return res.data


class SorekitApiHelper(object):
    @staticmethod
    def create_physical_table(**storage_config):
        StorekitHelper.create_physical_table(**storage_config)

    @staticmethod
    def update_physical_table(**storage_config):
        StorekitHelper.update_physical_table(**storage_config)

    @staticmethod
    def get_physical_table_name(result_table_id, processing_type):
        return StorekitHelper.get_physical_table_name(result_table_id, processing_type)

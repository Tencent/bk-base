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

import bkbase.dataflow.core.api.custom_http as http
from bkbase.dataflow.batch.conf import batch_conf
from bkbase.dataflow.batch.exception.batch_exception import BatchAPIHttpException
from bkbase.dataflow.batch.utils.batch_utils import batch_logger


def get_result_table_param(result_table_id):
    url = "{url_prefix}/dataflow/batch/jobs/{result_table_id}/get_param/".format(
        url_prefix=batch_conf.DATAFLOWAPI_URL_PREFIX, result_table_id=result_table_id
    )
    batch_logger.info("Try to call url: %s" % url)
    _, result = http.get(url)
    batch_logger.info(result)
    if result["result"]:
        return result["data"]
    else:
        raise Exception("Get the result table (%s) info failed. " % result_table_id + str(result))


def call_storekit_maintain(result_table_id, days):
    url = "{url_prefix}/storekit/hdfs/{result_table_id}/maintain/?delta_day={days}".format(
        url_prefix=batch_conf.STOREKITAPI_URL_PREFIX, result_table_id=result_table_id, days=days
    )
    batch_logger.info("Try to call url: %s" % url)
    status, result = http.get(url)
    batch_logger.info(result)
    if status and result["result"]:
        return result["data"]
    else:
        raise BatchAPIHttpException("Can not call storekit maintain for %s" % result_table_id)


def add_connector(result_table_id, data_dir, schedule_time):
    url = "{url_prefix}/databus/import_hdfs/".format(url_prefix=batch_conf.DATABUSAPI_URL_PREFIX)
    params = {"result_table_id": result_table_id, "data_dir": data_dir, "description": ""}
    batch_logger.info("Try to call url: {} with params {}".format(url, params))
    status, result = http.post(url, params)
    batch_logger.info(result)
    if status and result["result"]:
        return result["data"]
    else:
        add_retry_record(result_table_id, data_dir, schedule_time)
        raise BatchAPIHttpException("Can not call import_hdfs for %s" % result_table_id)


def add_retry_record(result_table_id, data_dir, schedule_time):
    base_url = "{url_prefix}/dataflow/batch/jobs/{result_table_id}/add_retry".format(
        url_prefix=batch_conf.DATAFLOWAPI_URL_PREFIX, result_table_id=result_table_id
    )
    params = "data_dir={data_dir}&schedule_time={schedule_time}&batch_storage_type=hdfs".format(
        data_dir=data_dir, schedule_time=schedule_time
    )
    url = "{base_url}/?{params}".format(base_url=base_url, params=params)
    batch_logger.info("Try to call url: %s" % url)
    _, result = http.get(url)
    batch_logger.info(result)


def send_monitor_report(params):
    url = "{url_prefix}/datamanage/dmonitor/metrics/report/".format(url_prefix=batch_conf.DATAMANAGEAPI_URL_PREFIX)
    batch_logger.info("Try to call url: {} with params {}".format(url, params))
    status, result = http.post(url, params)
    batch_logger.info(result)
    if not status or not result["result"]:
        message = "Monitor report error"
        if result and "data" in result:
            message = "{message}:{error}".format(message=message, error=result["data"])
        raise BatchAPIHttpException(message)

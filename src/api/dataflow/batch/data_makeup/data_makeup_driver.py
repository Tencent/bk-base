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

from dataflow.batch.api.api_helper import BksqlHelper
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import batch_logger


def submit_jobnavi_data_makeup(args):
    batch_logger.info("Try to submit jobnavi data makeup with args: {}".format(args))
    processing_id = args["processing_id"]
    rerun_processings = args["rerun_processings"]
    target_schedule_time = args["target_schedule_time"]
    source_schedule_time = args["source_schedule_time"]
    dispatch_to_storage = args["with_storage"]
    geog_area_code = args["geog_area_code"]
    rerun_model = "current_canvas"
    if "rerun_model" in args:
        rerun_model = args["rerun_model"]
    cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
    jobnavi = JobNaviHelper(geog_area_code, cluster_id)
    return jobnavi.data_makeup(
        processing_id,
        rerun_processings,
        rerun_model,
        target_schedule_time,
        source_schedule_time,
        dispatch_to_storage,
    )


def get_jobnavi_data_makeup_status(args):
    batch_logger.info("Try to submit jobnavi data makeup status list with args: {}".format(args))
    processing_id = args["processing_id"]
    data_start = args["data_start"]
    data_end = args["data_end"]
    geog_area_code = args["geog_area_code"]
    cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
    jobnavi = JobNaviHelper(geog_area_code, cluster_id)
    return jobnavi.data_makeup_status_list(processing_id, data_start, data_end)


def is_jobnavi_data_makeup_allowed(args):
    batch_logger.info("Try to submit jobnavi data makeup check allowed with args: {}".format(args))
    processing_id = args["processing_id"]
    schedule_time = args["schedule_time"]
    geog_area_code = args["geog_area_code"]
    cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
    jobnavi = JobNaviHelper(geog_area_code, cluster_id)
    return jobnavi.data_makeup_check_allowed(processing_id, schedule_time)


def submit_sql_column(args):
    batch_logger.info("Try to get sql column: {}".format(args))
    sql = args["sql"]
    return BksqlHelper.sql_column(sql)

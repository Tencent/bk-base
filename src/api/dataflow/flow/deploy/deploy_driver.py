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

from conf.dataapi_settings import API_URL

from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper


def deploy(geog_area_code):
    __clean_hdfs_clean(geog_area_code)
    __check_custom_calculate(geog_area_code)


def __clean_hdfs_clean(geog_area_code):
    schedule_id = "hdfs_uploaded_file_clean"
    command = "curl -d '' %s/v3/dataflow/flow/hdfs_uploaded_file_clean/" % API_URL
    cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
    jobnavi = JobNaviHelper(geog_area_code, cluster_id)
    jobnavi_args = {
        "schedule_id": schedule_id,
        "description": "system_tool",
        "type_id": "command",
        "period": {
            "timezone": "Asia/Shanghai",
            "frequency": 1,
            "period_unit": "d",
            # 2019-05-17 00:00:50:000
            "first_schedule_time": 1558022450000,
        },
        "active": True,
        "extra_info": command,
        # 'exec_oncreate': True
    }
    jobnavi.delete_schedule(schedule_id)
    jobnavi.create_schedule_info(jobnavi_args)


def __check_custom_calculate(geog_area_code):
    schedule_id = "check_custom_calculate"
    command = "curl %s/v3/dataflow/flow/check_custom_calculate/" % API_URL
    cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
    jobnavi = JobNaviHelper(geog_area_code, cluster_id)
    jobnavi_args = {
        "schedule_id": schedule_id,
        "description": "system_tool",
        "type_id": "command",
        "period": {
            "timezone": "Asia/Shanghai",
            "frequency": 10,
            "period_unit": "s",
            # 2019-05-17 00:00:50:000
            "first_schedule_time": 1558022450000,
        },
        "active": True,
        "extra_info": command,
        # 'exec_oncreate': True
    }
    jobnavi.delete_schedule(schedule_id)
    jobnavi.create_schedule_info(jobnavi_args)

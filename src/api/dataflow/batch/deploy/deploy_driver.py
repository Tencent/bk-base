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

from conf.dataapi_settings import DATAFLOW_API_HOST, DATAFLOW_API_PORT

from dataflow.batch.settings import HDFS_BACKUP_DIR
from dataflow.pizza_settings import RUN_VERSION
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper


def deploy(geog_area_code):
    __retry_hdfs_export(geog_area_code)
    if RUN_VERSION == "ee":
        __hdfs_backup()
    __recalculate_sync(geog_area_code)
    __recalculate_expire_data(geog_area_code)
    custom_jobs_expire_data(geog_area_code)


def __retry_hdfs_export(geog_area_code):
    command = "curl http://{}:{}/v3/dataflow/batch/jobs/retry_shipper_api/".format(
        DATAFLOW_API_HOST,
        DATAFLOW_API_PORT,
    )
    cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
    jobnavi = JobNaviHelper(geog_area_code, cluster_id)
    jobnavi_args = {
        "schedule_id": "retry_hdfs_export",
        "description": "system_tool",
        "type_id": "command",
        "period": {"timezone": "Asia/Shanghai", "frequency": 1, "period_unit": "h"},
        "active": True,
        "extra_info": command,
    }
    jobnavi.delete_schedule("retry_hdfs_export")
    jobnavi.create_schedule_info(jobnavi_args)


def __hdfs_backup():
    path = HDFS_BACKUP_DIR
    jobnavi_args = {
        "schedule_id": "hdfs_backup",
        "description": "system_tool",
        "type_id": "hdfs_backup",
        "period": {"timezone": "Asia/Shanghai", "frequency": 1, "period_unit": "d"},
        "active": True,
        "extra_info": """
           {
                "path":"%(path)s"
           }
        """
        % {"path": path},
    }
    for cluster_config in JobNaviHelper.list_jobnavi_cluster_config():
        geog_area_code = cluster_config["geog_area_code"]
        cluster_id = cluster_config["cluster_name"]
        jobnavi = JobNaviHelper(geog_area_code, cluster_id)
        jobnavi.delete_schedule("hdfs_backup")
        jobnavi.create_schedule_info(jobnavi_args)


def __recalculate_sync(geog_area_code):
    command = "curl http://{}:{}/v3/dataflow/batch/custom_calculates/sync/".format(
        DATAFLOW_API_HOST,
        DATAFLOW_API_PORT,
    )
    cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
    jobnavi = JobNaviHelper(geog_area_code, cluster_id)
    jobnavi_args = {
        "schedule_id": "custom_calculate_sync",
        "description": "system_tool",
        "type_id": "command",
        "period": {"timezone": "Asia/Shanghai", "frequency": 1, "period_unit": "m"},
        "active": True,
        "extra_info": command,
    }
    jobnavi.delete_schedule("custom_calculate_sync")
    jobnavi.create_schedule_info(jobnavi_args)


def __recalculate_expire_data(geog_area_code):
    command = "curl http://{}:{}/v3/dataflow/batch/custom_calculates/expire_data/".format(
        DATAFLOW_API_HOST,
        DATAFLOW_API_PORT,
    )
    cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
    jobnavi = JobNaviHelper(geog_area_code, cluster_id)
    jobnavi_args = {
        "schedule_id": "custom_calculate_expire_data",
        "description": "system_tool",
        "type_id": "command",
        "period": {"timezone": "Asia/Shanghai", "frequency": 1, "period_unit": "d"},
        "active": True,
        "extra_info": command,
    }
    jobnavi.delete_schedule("custom_calculate_expire_data")
    jobnavi.create_schedule_info(jobnavi_args)


def custom_jobs_expire_data(geog_area_code):
    command = "curl http://{}:{}/v3/dataflow/batch/custom_jobs/expire_data/ ".format(
        DATAFLOW_API_HOST,
        DATAFLOW_API_PORT,
    )
    cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
    jobnavi = JobNaviHelper(geog_area_code, cluster_id)
    jobnavi_args = {
        "schedule_id": "custom_jobs_expire_data",
        "description": "system_tool",
        "type_id": "command",
        "period": {"timezone": "Asia/Shanghai", "frequency": 1, "period_unit": "d"},
        "active": True,
        "extra_info": command,
    }
    jobnavi.delete_schedule("custom_jobs_expire_data")
    jobnavi.create_schedule_info(jobnavi_args)

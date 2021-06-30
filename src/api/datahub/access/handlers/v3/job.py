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
from __future__ import absolute_import, unicode_literals

from common.api.base import DataAPI
from conf.dataapi_settings import JOB_V3_API_URL as JOB_API_URL

JOB_API_URL = JOB_API_URL + "/api/c/compapi/v2/job/"


def enum(**enums):
    return type(str("Enum"), (), enums)


class _JobApi(object):
    MODULE = "JOB"

    def __init__(self):
        self.HostStatus = enum(
            RELEASING=0,
            REMOVING=1,
            COLLECTING=2,
            PUBLISH_EXCEPTION=3,
            SUCCESS=4,
            DELETE_EXCEPTION=5,
            STATUS_EXCEPTION=6,
            WAITING=7,
        )
        self.REQUEST_GET_PRO_STATUS = {}

        self.gse_proc_operate = DataAPI(
            method="POST",
            url=JOB_API_URL + "operate_proc",
            module=self.MODULE,
            description="进程操作任务",
        )
        self.get_proc_result = DataAPI(
            method="POST",
            url=JOB_API_URL + "get_proc_result",
            module=self.MODULE,
            description="查询任务结果",
        )
        self.get_agent_status = DataAPI(
            method="POST",
            url=JOB_API_URL + "get_agent_status",
            module=self.MODULE,
            description="查询Agent状态",
        )
        self.fast_push_file = DataAPI(
            method="POST",
            url=JOB_API_URL + "fast_push_file",
            module=self.MODULE,
            description="快速分发文件",
        )
        self.gse_push_file = DataAPI(
            method="POST",
            url=JOB_API_URL + "push_config_file",
            module=self.MODULE,
            description="GSE 文件内容推送",
        )
        self.get_task_result = DataAPI(
            method="GET",
            url=JOB_API_URL + "get_task_result",
            module=self.MODULE,
            description="查询任务结果",
        )
        self.get_task_ip_log = DataAPI(
            method="POST",
            url=JOB_API_URL + "get_job_instance_log",
            module=self.MODULE,
            description="查询ip任务结果日志",
        )
        self.get_proc_result = DataAPI(
            method="GET",
            url=JOB_API_URL + "get_proc_result",
            module=self.MODULE,
            description="查询进程结果",
        )
        self.fast_execute_script = DataAPI(
            method="POST",
            url=JOB_API_URL + "fast_execute_script",
            module=self.MODULE,
            description="快速执行脚本",
        )
        self.gse_process_manage = DataAPI(
            method="POST",
            url=JOB_API_URL + "manage_proc",
            module=self.MODULE,
            description="GSE进程托管注册和取消注册",
        )


JobApi = _JobApi()

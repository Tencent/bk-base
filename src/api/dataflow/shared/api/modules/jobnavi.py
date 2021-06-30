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

from common.api.base import DataDRFAPISet, DRFActionAPI

from dataflow.pizza_settings import BASE_JOBNAVI_URL


class _JobNaviApi(object):
    def __init__(self):
        self.schedule = DataDRFAPISet(
            url=BASE_JOBNAVI_URL + "cluster/{cluster_id}/schedule/",
            primary_key="schedule_id",
            url_keys=["cluster_id"],
            module="JobNavi",
            description="调度管理",
            custom_config={"force_schedule": DRFActionAPI(method="get", detail=True)},
        )

        self.schedule_without_retry = DataDRFAPISet(
            url=BASE_JOBNAVI_URL + "cluster/{cluster_id}/schedule/",
            primary_key="schedule_id",
            url_keys=["cluster_id"],
            timeout_auto_retry=False,
            module="JobNavi",
            description="调度管理",
        )

        self.execute = DataDRFAPISet(
            url=BASE_JOBNAVI_URL + "cluster/{cluster_id}/execute/",
            primary_key="execute_id",
            url_keys=["cluster_id"],
            module="JobNavi",
            description="任务执行管理",
            custom_config={
                "rerun": DRFActionAPI(method="post", detail=False),
                "list_recovery": DRFActionAPI(method="get", detail=False),
                "query_submitted_by_time": DRFActionAPI(method="get", detail=False),
                "run": DRFActionAPI(method="post", detail=False),
            },
        )

        self.event = DataDRFAPISet(
            url=BASE_JOBNAVI_URL + "cluster/{cluster_id}/event/",
            primary_key="event_id",
            url_keys=["cluster_id"],
            module="JobNavi",
            description="任务事件管理",
        )

        self.event_without_retry = DataDRFAPISet(
            url=BASE_JOBNAVI_URL + "cluster/{cluster_id}/event/",
            primary_key="event_id",
            url_keys=["cluster_id"],
            timeout_auto_retry=False,
            module="JobNavi",
            description="任务事件管理",
        )

        self.task_type = DataDRFAPISet(
            url=BASE_JOBNAVI_URL + "cluster/{cluster_id}/task_type/",
            primary_key="type_id",
            url_keys=["cluster_id"],
            module="JobNavi",
            description="任务类型管理",
            custom_config={"upload": DRFActionAPI(method="post")},
        )

        self.task_log = DataDRFAPISet(
            url=BASE_JOBNAVI_URL + "cluster/{cluster_id}/task_log/",
            primary_key="execute_id",
            url_keys=["cluster_id"],
            module="JobNavi",
            description="任务提交日志管理",
            custom_config={
                "query_tracking_url": DRFActionAPI(method="get"),
                "query_application_id": DRFActionAPI(method="get"),
                "query_file_size": DRFActionAPI(method="get"),
            },
        )

        self.cluster_config = DataDRFAPISet(
            url=BASE_JOBNAVI_URL + "cluster_config/",
            primary_key="cluster_name",
            module="JobNavi",
            description="集群配置",
        )

        self.admin = DataDRFAPISet(
            url=BASE_JOBNAVI_URL + "cluster/{cluster_id}/admin/",
            primary_key="admin_type",
            url_keys=["cluster_id"],
            module="JobNavi",
            description="admin操作",
            custom_config={"calculate_schedule_task_time": DRFActionAPI(method="post")},
        )

        self.data_makeup = DataDRFAPISet(
            url=BASE_JOBNAVI_URL + "cluster/{cluster_id}/data_makeup/",
            primary_key=None,
            url_keys=["cluster_id"],
            module="JobNavi",
            description="补齐数据",
            custom_config={"check_allowed": DRFActionAPI(method="get", detail=False)},
        )


JobNaviApi = _JobNaviApi()

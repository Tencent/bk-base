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


from datamanage.utils.api import AccessApi, DatabusApi
from datamanage.utils.dbtools.kafka_util import send_kafka_messages
from rest_framework.response import Response

from common.decorators import list_route
from common.views import APIViewSet


class ToolsViewSet(APIViewSet):

    USER_BEHAVIOR_RAW_DATA_NAME = "dataweb_user_behavior"

    USER_BEHAVIOR_QUEUE = None

    @list_route(methods=["post"], url_path="report_user_behavior")
    def report_user_behavior(self, request):
        """
        @api {post} /v3/datamanage/ops/report_user_behavior/ 前端上报用户操作记录
        @apiName report_user_behavior
        @apiGroup ops
        """
        content = request.data["content"].encode("utf-8")

        self.init_user_behavior_queue()
        svr_type = self.USER_BEHAVIOR_QUEUE["svr_type"]
        topic = self.USER_BEHAVIOR_QUEUE["topic"]

        send_kafka_messages(svr_type, topic, [content])

        return Response("ok")

    @classmethod
    def init_user_behavior_queue(cls):
        """
        根据 raw_data_name 初始化队列配置信息
        """
        if cls.USER_BEHAVIOR_QUEUE is None:
            raw_data_name = cls.USER_BEHAVIOR_RAW_DATA_NAME

            raw_data_info = AccessApi.rawdata.list(
                {"raw_data_name__icontains": raw_data_name}, raise_exception=True
            ).data[0]

            storage_channel_id = raw_data_info["storage_channel_id"]

            channel_info = DatabusApi.channels.retrieve(
                {"channel_cluster_config_id": storage_channel_id}, raise_exception=True
            ).data

            topic = str(raw_data_info.get("topic"))
            svr_type = "raw:{}:{}".format(channel_info["cluster_domain"], channel_info["cluster_port"])

            cls.USER_BEHAVIOR_QUEUE = {"topic": topic, "svr_type": svr_type}

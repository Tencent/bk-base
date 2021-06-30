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

from common.decorators import list_route
from common.django_utils import JsonResponse
from common.views import APIViewSet
from datahub.common.const import (
    CODE,
    DATA,
    DEST_CLUSTER_ID,
    GROUPID_LOWERCASE,
    KAFKA,
    MESSAGE,
    RESULT,
    SRC_CLUSTER_ID,
    TOKEN,
    TOPIC,
    TOPICS,
    TYPE,
    USER,
    VERSION,
)
from rest_framework.response import Response

from datahub.databus import queue_acl


class QueueSdkViewset(APIViewSet):
    """
    队列权限相关接口，包含sdk的token权限查询
    """

    @list_route(methods=["get"], url_path="init")
    def init_token(self, request):
        """
        @api {get} /databus/queue/v1/sdk/init/?token=xxxx sdk初始化请求
        @apiGroup QueueAuth
        @apiDescription sdk初始化请求
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
          "result": true,
          "message": "",
          "code": "00",
          "data": {
            "password": "xxxxxx",
            "groupId": "queue-xxx",
            "username": "xxxx"
          }
        }
        """
        token = str(request.GET.get(TOKEN, ""))
        result = queue_acl.get_token_init_info(token)

        return JsonResponse({RESULT: True, DATA: result, MESSAGE: "", CODE: "00"})

    @list_route(methods=["get"], url_path="producer")
    def get_producer_info(self, request):
        """
        @api {get} databus/queue/v1/sdk/producer/?user=userName&topics=topic1,topic2&version=1 sdk查询topic所在集群信息
        @apiGroup QueueAuth
        @apiDescription 查询指定主题的授权情况以及已授权主题所在集群信息
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
             "result": true,
             "message": "",
             "code": "00",
             "data": {
                 "clusterInfo": [ {"topic": "topic1","cluster": "x.x.x.x:9092","version": 1}],                ],
                 "noAuth": [{"topic": "topic2","cluster": "x.x.x.x:9092","version": 2}],
             }
         }
        """
        user = str(request.GET.get(USER, ""))
        topics = str(request.GET.get(TOPICS, ""))
        version = int(request.GET.get(VERSION, 0))
        result = queue_acl.get_producer_info(user, topics, version)

        return JsonResponse({RESULT: True, DATA: result, MESSAGE: "", CODE: "00"})

    @list_route(methods=["get"], url_path="consumer")
    def get_consumer_info(self, request):
        """
        @api {get} databus/queue/v1/sdk/consumer/?groupid=mygroup&topics=topic1,topic2&version=1 查询topic所在集群信息
        @apiGroup QueueAuth
        @apiDescription 查询指定主题的授权情况以及已授权主题所在集群信息
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
             "result": true,
             "message": "",
             "code": "00",
             "data": {
                 "clusterInfo": [ {"topic": "topic1","cluster": "x.x.x.x:9092","version": 1}],                ],
                 "noAuth": [{"topic": "topic2","cluster": "x.x.x.x:9092","version": 2}],
             }
         }
        """
        topics = str(request.GET.get(TOPICS, ""))
        group_id = str(request.GET.get(GROUPID_LOWERCASE, ""))
        cluster_type = str(request.GET.get(TYPE, KAFKA))
        is_pattern = bool(request.GET.get("is_pattern", False))
        version = int(request.GET.get(VERSION, 0))
        result = queue_acl.get_consumer_info(topics, group_id, version, cluster_type, is_pattern)

        return JsonResponse({RESULT: True, DATA: result, MESSAGE: "", CODE: "00"})

    @list_route(methods=["get"], url_path="report")
    def log_report(self, request):
        """
        @api {get} /databus/queue/v1/sdk/report/?key1=value1&key2=value2 客户端事件上报接口
        @apiGroup QueueAuth
        @apiDescription 客户端事件上报接口
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
             "result": true,
             "message": "",
             "code": "00",
             "data": "OK"
         }
        """
        return Response("OK")

    @list_route(methods=["get"], url_path="topic_transfer")
    def topic_transfer(self, request):
        """
        @api {get} /databus/queue/v1/sdk/topic_transfer/?topic=value1&src_cluster=value2&dest_cluster=value3 topic迁移接口
        @apiGroup QueueAuth
        @apiDescription 发布新授权，客户端访问有老集群迁移到新集群
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
             "result": true,
             "message": "",
             "code": "00",
             "data": "OK"
         }
        """
        src_cluster = int(request.GET.get(SRC_CLUSTER_ID))
        dest_cluster = int(request.GET.get(DEST_CLUSTER_ID))
        topic = str(request.GET.get(TOPIC, ""))
        queue_acl.transfer_topic(topic, src_cluster, dest_cluster)
        return Response("OK")

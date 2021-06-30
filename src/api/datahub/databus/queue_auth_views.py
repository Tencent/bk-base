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
from common.views import APIViewSet
from datahub.databus.serializers import QueueAuthSerializer
from rest_framework.response import Response

from datahub.databus import queue_acl


class QueueAuthViewset(APIViewSet):
    """
    队列权限相关接口，包含添加账号、授权队列访问权限等
    """

    def list(self, request):
        """
        @api {get} /databus/queue_auths/ 获取队列服务授权的列表信息
        @apiGroup QueueAuth
        @apiDescription 获取队列服务授权的列表信息
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "xx.xx.xx:2181/kafka-test-3": {
                    "topic": {
                        "Topic:queue_test2": "User:mytest Describe from hosts: *",
                        "Topic:queue_test2": "User:mytest Read from hosts: *"
                    },
                    "group": {
                        "Group:queue-version_monitor": "User:version_monitor Read from hosts: *",
                        "Group:queue-mytest": "User:mytest Read from hosts: *",
                        "Group:queue-logon_service": "User:logon_service Read from hosts: *"
                    }
                }
            }
        }
        """
        result = queue_acl.list_queue_auth()

        return Response(result)

    @list_route(methods=["post"], url_path="add_auths")
    def add_auths(self, request):
        """
        @api {post} /databus/queue_auths/add_auths/ 添加队列访问授权
        @apiGroup QueueAuth
        @apiDescription 添加授权，允许指定用户访问指定的队列
        @apiParam {string{小于128字符}} user 访问总线队列服务的用户名。
        @apiParam {list{小255字符}} result_table_ids 访问总线队列服务的队列对应的rt_id的列表。
        @apiParamExample {json} 参数样例:
        {
            "user": "data-app",
            "result_table_ids": ["101_etl_abc", "134_join_cal"]
        }

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": true,
            "result": true
        }
        """
        params = self.params_valid(serializer=QueueAuthSerializer)
        result = queue_acl.add_auth_for_queue(params["result_table_ids"], params["user"])

        return Response(result)

    @list_route(methods=["post"], url_path="remove_auths")
    def remove_auths(self, request):
        """
        @api {delete} /databus/queue_auths/remove_auths/ 删除队列服务授权
        @apiGroup QueueAuth
        @apiDescription 删除队列服务的授权
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": true
        }
        """
        params = self.params_valid(serializer=QueueAuthSerializer)
        result = queue_acl.remove_auth_for_queue(params["result_table_ids"], params["user"])

        return Response(result)

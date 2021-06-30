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

from common.views import APIViewSet
from datahub.databus.serializers import AddQueueUserSerializer
from rest_framework.response import Response

from datahub.databus import queue_acl


class QueueUserViewset(APIViewSet):
    """
    队列权限账户相关接口，包含添加账号、删除账号，查看账号列表等接口
    """

    lookup_field = "user"

    def list(self, request):
        """
        @api {get} /databus/queue_users/ 获取队列服务账户的列表信息
        @apiGroup QueueUser
        @apiDescription 获取队列服务账户的列表信息
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": {
                "xx.xx.xx:2181/kafka-test-3": ['data', 'user01']
            }
        }
        """
        result = queue_acl.list_queue_user()

        return Response(result)

    def create(self, request):
        """
        @api {post} /databus/queue_auths/user 添加使用队列服务的用户
        @apiGroup QueueAuth
        @apiDescription 添加使用队列服务的用户
        @apiParam {string{小于128字符}} user 访问总线队列服务的用户名。
        @apiParam {string{小255字符}} password 访问总线队列服务的密码。
        @apiParamExample {json} 参数样例:
        {
            "user": "data-app",
            "password": "adxxxxxxxxx"
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
        params = self.params_valid(serializer=AddQueueUserSerializer)
        result = queue_acl.add_queue_user(params["user"], params["password"])

        return Response(result)

    def destroy(self, request, user):
        """
        @api {delete} /databus/queue_users/:user/ 删除队列服务账户
        @apiGroup QueueUser
        @apiDescription 删除队列服务的账户
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
        result = queue_acl.remove_queue_user(user)

        return Response(result)

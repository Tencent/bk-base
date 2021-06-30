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


import uuid

from common.decorators import params_valid
from rest_framework.response import Response

from meta.basic.common import RPCViewSet
from meta.public.models.event import EventManager
from meta.public.serializers.common import EventReportSerializer


class EventViewSet(RPCViewSet):
    @params_valid(serializer=EventReportSerializer)
    def create(self, request, params):
        """
        @api {post} /v3/meta/event/ 数据管理事件
        @apiName Events
        @apiGroup Common
        @apiVersion 1.0.0
        @apiDescription 用于上报数据管理事件

        @apiParam {String} event_type 事件类型
        @apiParam {String} event_level 事件等级
        @apiParam {String} refer 事件来源模块
        @apiParam {String} expire 事件过期时间
        @apiParam {String} triggered_at 事件触发事件
        @apiParam {String} description 时间描述信息
        @apiParam {String} event_message 事件消息体

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": "d8ac4cd9-8af2-4767-853b-3d20640bd3b9",
                    "result": true
                }
        """
        msg_id = str(uuid.uuid4())
        params["msg_id"] = msg_id
        params["triggered_by"] = params["refer"]
        producer = EventManager(params)
        producer.publish_event()
        return Response(msg_id)


class EventSupportViewSet(RPCViewSet):
    @staticmethod
    def list(request):
        """
        @api {get} /meta/event_supports/ 事件系统支持
        @apiVersion 0.2.0
        @apiGroup Event
        @apiName 获取事件系统支持项目

        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": "amqp://user:pass@host:port/vhost",
                "result": true
            }
        """

        support_config = dict(addr=EventManager.get_encrypted_addr())
        return Response(support_config)

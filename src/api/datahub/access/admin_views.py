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

#

from common.decorators import list_route
from common.views import APIViewSet
from datahub.access import settings, tags
from datahub.access.collectors.factory import CollectorFactory
from datahub.access.models import AccessManagerConfig
from datahub.access.utils.init import init_blueking_biz_id, patch_scenario
from datahub.databus.pullers.factory import PullerFactory
from datahub.databus.script.kafka.__main__ import main_method
from datahub.databus.script.kafka.init_databus_topic import init_databus_conf_topic
from datahub.databus.shippers.factory import ShipperFactory
from rest_framework.response import Response


class AdminViewSet(APIViewSet):
    """
    管理接口
    """

    @list_route(methods=["get"], url_path="create_raw_data_tag")
    def create_raw_data_tag(self, request):
        """
        @api {get} /admin/create_raw_data_tag/ 创建raw_data的tag
        @apiGroup Admin
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
        }
        """
        target_id = int(request.GET.get("id", 0))
        tag = request.GET.get("tag", "")
        tags.create_tags(settings.TARGET_TYPE_ACCESS_RAW_DATA, target_id, [tag])
        return Response("ok")

    @list_route(methods=["get"], url_path="add_editable_app")
    def add_editable_app(self, request):
        """
        @api {get} /admin/add_editable_app/ 新增可编辑app
        @apiGroup Admin
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
        }
        """
        app = request.GET.get("app", "")
        if not app:
            return Response("ok")

        obj = AccessManagerConfig.objects.get(type="editable")
        apps = obj.names
        try:
            apps = apps.split(",")
            apps = set(apps)
            apps.add(app)
            obj.names = ",".join(apps)
            obj.save()
            return Response(apps)
        except Exception as e:
            return Response(e)

    @list_route(methods=["get"], url_path="del_editable_app")
    def del_editable_app(self, request):
        """
        @api {get} /admin/del_editable_app/ 删除可编辑app
        @apiGroup Admin
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
        }
        """
        app = request.GET.get("app", "")
        if not app:
            return Response("ok")

        obj = AccessManagerConfig.objects.get(type="editable")
        apps = obj.names
        try:
            apps = apps.split(",")
            apps = set(apps)
            apps.remove(app)
            obj.names = ",".join(apps)
            obj.save()
            return Response(apps)
        except Exception as e:
            return Response(str(e))

    @list_route(methods=["get"], url_path="support_component")
    def support_component(self, request):
        # access
        collector_factory = CollectorFactory.get_collector_factory()
        access_collector = {}
        for k in collector_factory.registered_collectors.keys():
            access_collector[k] = str(collector_factory.registered_collectors[k])
        access_task = {}
        task_factory = collector_factory.get_task_factory()
        for k in task_factory.registered_tasks.keys():
            access_task[k] = str(task_factory.registered_tasks[k])

        # databus
        shipper_factory = ShipperFactory.get_shipper_factory()
        databus_shipper = {}
        for k in shipper_factory.registered_shippers.keys():
            databus_shipper[k] = str(shipper_factory.registered_shippers[k])

        shipper_factory = ShipperFactory.get_shipper_factory()
        databus_shipper = {}
        for k in shipper_factory.registered_shippers.keys():
            databus_shipper[k] = str(shipper_factory.registered_shippers[k])

        puller_factory = PullerFactory.get_puller_factory()
        databus_pullers = {}
        for k in puller_factory.registered_pullers.keys():
            databus_pullers[k] = str(puller_factory.registered_pullers[k])

        results = {
            "access_collectors": access_collector,
            "access_tasks": access_task,
            "databus_shippers": databus_shipper,
            "databus_pullers": databus_pullers,
        }
        return Response(results)

    @list_route(methods=["get"], url_path="init_databus_topic_config")
    def init_databus_topic_config(self, request):
        """
        @api {get} /admin/init_databus_topic_config/ init databus_connector config topics
        @apiGroup Admin
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
        }
        """
        init_databus_conf_topic()
        return Response("OK")

    @list_route(methods=["get"], url_path="init_access_api")
    def init_access_api(self, request):
        """
        @api {get} /admin/init_access_api/ init access_api
        @apiGroup Admin
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
        }
        """
        # 初始化蓝鲸业务
        init_blueking_biz_id()

        # 根据配置文件调整支持的场景
        patch_scenario()
        return Response("OK")

    @list_route(methods=["get"], url_path="init_databus_topic_config")  # noqa
    def init_databus_topic_config(self, request):
        """
        @api {get} /admin/init_databus_topic_config/ init databus_connector config topics
        @apiGroup Admin
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
        }
        """
        main_method()
        return Response("OK")

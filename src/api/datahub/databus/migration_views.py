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

import time

from common.decorators import detail_route, list_route
from common.log import logger
from common.transaction import auto_meta_sync
from common.views import APIViewSet
from datahub.common.const import DEFAULT
from datahub.databus.exceptions import (
    MigrationCannotOperatError,
    MigrationNotFoundError,
)
from datahub.databus.task.task_utils import check_task_auth
from django.forms import model_to_dict
from rest_framework.response import Response

from datahub.databus import exceptions, migration, models, rt, serializers, settings


class MigrationViewset(APIViewSet):
    """
    对于资源 REST 操作逻辑统一放置在 APIViewSet 中提供接口
    """

    serializer_class = serializers.MigrateCreateSerializer
    # 对于URL中实例ID变量名进行重命名，默认为 pk
    lookup_field = "id"

    def create(self, request):
        """
        @apiGroup migration
        @api {post} /databus/migrations/ 创建迁移任务
        @apiDescription 创建迁移任务
        @apiParam {string} result_table_id result_table_id
        @apiParam {string} source 源存储
        @apiParam {string} dest 目标存储
        @apiParam {string} start 起始时间
        @apiParam {string} end 结束时间
        @apiParam {int} parallelism 【选填】处理并发数，默认3
        @apiParam {boolean} overwrite 是否覆盖已有数据
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": [{}],
                    "message": "ok",
                    "code": "1500200",
                }
        """
        args = self.params_valid(serializer=serializers.MigrateCreateSerializer)
        rt_id = args["result_table_id"]
        check_task_auth(rt_id)

        rt_info = rt.get_databus_rt_info(rt_id)

        if not rt_info:
            raise exceptions.NotFoundRtError()

        # 源存储配置检查
        if args["source"] not in rt_info["storages.list"]:
            raise exceptions.TaskStorageNotFound(
                message_kv={
                    "result_table_id": args["result_table_id"],
                    "storage": args["source"],
                }
            )

        # 目的存储配置检查
        if args["dest"] not in rt_info["storages.list"]:
            raise exceptions.TaskStorageNotFound(
                message_kv={
                    "result_table_id": args["result_table_id"],
                    "storage": args["dest"],
                }
            )

        task_label = migration.create_task(rt_info, args)
        objs = models.DatabusMigrateTask.objects.filter(task_label=task_label).values()
        return Response(objs)

    def partial_update(self, request, id):
        """
        @apiGroup migration
        @api {patch} /databus/migrations/:task_id/ 更新迁移任务
        @apiDescription 更新迁移任务
        @apiParam {string} status 状态
        @apiParam {int} parallelism 处理并发数
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": {},
                    "message": "ok",
                    "code": "1500200",
                }
        """
        args = self.params_valid(serializer=serializers.MigrateUpdateSerializer)
        obj = models.DatabusMigrateTask.objects.get(id=id)
        with auto_meta_sync(using=DEFAULT):
            if args["status"] != "":
                obj.status = args["status"]
            if args["parallelism"] > 0:
                obj.parallelism = args["parallelism"]
            obj.save()
        return Response(model_to_dict(models.DatabusMigrateTask.objects.get(id=id)))

    def list(self, request):
        """
        @apiGroup migration
        @api {patch} /databus/migrations/ 查询未完成任务
        @apiDescription 查询未完成任务
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": [{}],
                    "message": "ok",
                    "code": "1500200",
                }
        """
        tasks = models.DatabusMigrateTask.objects.exclude(status__in=["finish"]).values(
            "id",
            "task_label",
            "task_type",
            "result_table_id",
            "parallelism",
            "dest",
            "dest_config",
            "overwrite",
            "start",
            "end",
            "status",
        )
        for task_obj in tasks:
            if task_obj["task_type"] != "overall":
                task_obj["source_config"] = ""
                task_obj["dest_config"] = ""
        return Response(tasks)

    def retrieve(self, request, id):
        """
        @apiGroup migration
        @api {patch} /databus/migrations/:task_id/ 查询任务
        @apiDescription 查询任务
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": [{}],
                    "message": "ok",
                    "code": "1500200",
                }
        """
        task_id = 0
        try:
            task_id = int(id)
        except Exception:
            pass

        if task_id == 0:
            return Response(
                models.DatabusMigrateTask.objects.filter(result_table_id=id, task_type="overall")
                .order_by("task_label")
                .values(
                    "id",
                    "task_type",
                    "result_table_id",
                    "source",
                    "dest",
                    "start",
                    "end",
                    "created_at",
                    "created_by",
                    "updated_at",
                    "status",
                )
            )
        else:
            obj = models.DatabusMigrateTask.objects.get(id=task_id)
            return Response(
                models.DatabusMigrateTask.objects.filter(task_label=obj.task_label).values(
                    "id",
                    "task_type",
                    "result_table_id",
                    "source",
                    "dest",
                    "input",
                    "output",
                    "start",
                    "end",
                    "created_at",
                    "created_by",
                    "updated_at",
                    "status",
                )
            )

    @detail_route(methods=["get"], url_path="start")
    def start_task(self, request, id):
        """
        @apiGroup migration
        @api {post} /databus/migrations/:task_id/start/ 启动任务
        @apiDescription 启动任务
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": True,
                    "message": "ok",
                    "code": "1500200",
                }
        """
        args = self.params_valid(serializer=serializers.MigrateTaskTypeSerializer)
        task_obj = migration.get_task(id)

        if task_obj.status in ["finish"]:
            raise MigrationCannotOperatError(message_kv={"type": task_obj.status})

        return Response(migration.start_task(task_obj, args["type"]))

    @detail_route(methods=["get"], url_path="stop")
    def stop_task(self, request, id):
        """
        @apiGroup migration
        @api {post} /databus/migrations/:task_id/stop/ 停止任务
        @apiDescription 停止任务
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": True,
                    "message": "ok",
                    "code": "1500200",
                }
        """
        args = self.params_valid(serializer=serializers.MigrateTaskTypeSerializer)
        task_obj = migration.get_task(id)
        migration.stop_task(task_obj, args["type"])

        return Response(True)

    @detail_route(methods=["get"], url_path="status")
    def get_status(self, request, id):
        """
        @apiGroup migration
        @api {get} /databus/migrations/:task_id/status/ 查询任务pulsar运行状态
        @apiDescription 查询任务pulsar运行状态
        @apiParam {string} type 查询的任务类型，选值：all—所有类型任务，默认；source；sink
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": {"source":{},
                             "sink":{}},
                    "message": "ok",
                    "code": "1500200",
                }
        """
        args = self.params_valid(serializer=serializers.MigrateTaskTypeSerializer)
        task_obj = migration.get_task(id)
        result = migration.get_task_status(task_obj, args["type"])

        return Response(result)

    @list_route(methods=["get"], url_path="get_clusters")
    def get_clusters(self, request):
        """
        @apiGroup migration
        @api {get} /databus/migrations/get_clusters/ 获取result_table_id支持迁移集群列表
        @apiDescription 获取result_table_id支持迁移集群列表
        @apiParam {string} result_table_id result_table_id
        @apiParam {string} type 查询的任务类型，选值：all—所有类型任务，默认；source；sink
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": {"source":[],
                             "sink":[]},
                    "message": "ok",
                    "code": "1500200",
                }
        """
        args = self.params_valid(serializer=serializers.TasksRtIdSerializer)
        result = {"source": [], "dest": []}
        # 查询已配置存储集群列表
        rt_info = rt.get_rt_fields_storages(args["result_table_id"])
        if not rt_info or not rt_info.get("storages"):
            return Response(result)

        storages = rt_info.get("storages")
        for storage_type in storages.keys():
            if storage_type in settings.migration_source_supported:
                result["source"].append(storage_type)
            elif storage_type in settings.migration_dest_supported:
                result["dest"].append(storage_type)

        return Response(result)

    @list_route(methods=["get"], url_path="get_tasks")
    def get_tasks(self, request):
        """
        @apiGroup migration
        @api {get} /databus/migrations/get_tasks/ 获取dataid下迁移任务列表
        @apiDescription 获取dataid下迁移任务列表
        @apiParam {string} raw_data_id 数据id
        @apiParam {string} result_table_id result_table_id, 当存在raw_data_id时无效
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": {{}},
                    "message": "ok",
                    "code": "1500200",
                }
        """
        args = self.params_valid(serializer=serializers.MigrationGetTasksVerifySerializer)
        if "raw_data_id" in args:
            # query by raw_data_id
            raw_data_id = args["raw_data_id"]
            objects = models.DatabusClean.objects.filter(raw_data_id=raw_data_id)
            rts = [obj.processing_id for obj in objects]
        elif "result_table_id" in args:
            rts = [args["result_table_id"]]
        else:
            return Response([])

        result = models.DatabusMigrateTask.objects.filter(result_table_id__in=rts, task_type="overall").values(
            "id",
            "task_type",
            "result_table_id",
            "source",
            "dest",
            "start",
            "end",
            "created_at",
            "created_by",
            "updated_at",
            "status",
        )

        return Response(result)

    @list_route(methods=["post"], url_path="update_task_status")
    def update_task_status(self, request):
        """
        @apiGroup migration
        @api {post} /databus/migrations/update_task_status/ 更新任务状态
        @apiDescription 更新任务状态
        @apiParam {int} task_id 任务id
        @apiParam {string} status 状态
        @apiParam {int}  input [选填]source任务处理数量，默认0
        @apiParam {int} output [选填]sink任务处理数量，默认0
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": {{}},
                    "message": "ok",
                    "code": "1500200",
                }
        """
        args = self.params_valid(serializer=serializers.MigrateUpdateStateSerializer)
        try:
            obj = models.DatabusMigrateTask.objects.get(id=args["task_id"])
        except models.DatabusMigrateTask.DoesNotExist:
            raise MigrationNotFoundError()

        with auto_meta_sync(using=DEFAULT):
            obj.status = args["status"]
            obj.input = args["input"]
            obj.output = args["output"]
            obj.updated_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            logger.info("update task:{} status:{} input:{} output:{}".format(obj.id, obj.status, obj.input, obj.output))
            obj.save()

        return Response("ok")

    @list_route(methods=["get"], url_path="get_support_clusters")
    def get_support_clusters(self, request):
        """
        @apiGroup migration
        @api {get} /databus/migrations/get_support_clusters/ 获取当前支持的迁移列表
        @apiDescription 更新任务状态
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": {"source" : ["tspider"],
                             "dest": ["hdfs"]},
                    "message": "ok",
                    "code": "1500200",
                }
        """
        return Response(
            {
                "source": settings.migration_source_supported,
                "dest": settings.migration_dest_supported,
            }
        )

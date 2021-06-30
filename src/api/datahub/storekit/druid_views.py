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

from rest_framework.response import Response

from common.decorators import detail_route, list_route
from common.views import APIViewSet
from datahub.common.const import (
    CLUSTER_NAME,
    DELTA_DAY,
    DRUID,
    EXPIRES,
    MERGE_DAYS,
    RESULT_TABLE_ID,
    STORAGES,
    TASK_TYPE,
    TIMEOUT,
    TYPE,
    VERSION,
)
from datahub.storekit import druid, model_manager, util
from datahub.storekit.exceptions import RtStorageNotExistsError
from datahub.storekit.serializers import (
    CompactionDaysSerializer,
    CreateTableSerializer,
    DruidExpiresSerializer,
    DruidTaskSerializer,
    ExecuteTimeoutSerializer,
    MaintainDaysSerializer,
)
from datahub.storekit.settings import (
    EXECUTE_TIMEOUT,
    MAINTAIN_DELTA_DAY,
    MERGE_DAYS_DEFAULT,
)


class DruidClusterSet(APIViewSet):
    lookup_field = CLUSTER_NAME
    lookup_value_regex = "[^/]+"

    @detail_route(methods=["get"], url_path="get_roles")
    def get_roles(self, request, cluster_name):
        """
        @api {post} v3/storekit/cluster/druid/:cluster_name/get_roles 获取Druid不同角色
        @apiGroup Druid
        @apiDescription 获取druid不同角色
        @apiName ListRoles
        @apiVersion 1.0.0
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": "",
            "message": "ok",
            "code": "1500200",
        }
        """
        data = druid.get_roles(cluster_name)
        return Response(data=data)

    @detail_route(methods=["get"], url_path="get_datasources")
    def get_datasources(self, request, cluster_name):
        """
        @api {post} v3/storekit/cluster/druid/:cluster_name/get_datasources 获取datasources
        @apiGroup Druid
        @apiDescription 获取datasources
        @apiName GetDatasources
        @apiVersion 1.0.0
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": "",
            "message": "ok",
            "code": "1500200",
        }
        """
        data = druid.get_datasources(cluster_name)
        return Response(data=data)

    @detail_route(methods=["get"], url_path="get_workers")
    def get_workers(self, request, cluster_name):
        """
        @api {post} v3/storekit/cluster/druid/:cluster_name/get_workers 查询workers
        @apiGroup Druid
        @apiDescription 查询运行的workers
        @apiName GetWorkers
        @apiVersion 1.0.0
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": "",
            "message": "ok",
            "code": "1500200",
        }
        """
        data = druid.get_workers(cluster_name)
        return Response(data=data)

    @detail_route(methods=["get"], url_path="get_tasks")
    def get_tasks(self, request, cluster_name):
        """
        @api {post} v3/storekit/cluster/druid/:cluster_name/get_tasks 查询任务
        @apiGroup Druid
        @apiDescription 查询正在运行的任务
        @apiName GetRunningTasks
        @apiVersion 1.0.0
        @apiParam {String} task_type 任务类型
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": "",
            "message": "ok",
            "code": "1500200",
        }
        """
        params = self.params_valid(serializer=DruidTaskSerializer)
        data = druid.get_tasks(cluster_name, params[TASK_TYPE])
        return Response(data=data)

    @detail_route(methods=["get"], url_path="clean_deepstorage")
    def clean_deepstorage(self, request, cluster_name):
        """
        @api {get} v3/storekit/cluster/druid/:cluster_name/clean_deepstorage 清理deepstorage中的无用数据
        @apiGroup Druid
        @apiDescription 清理指定druid集群deepstorage中的无用数据
        @apiName cleanDeepStorage
        @apiVersion 1.0.0
        @apiParam {String} cluster_name 集群名称
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": "",
            "message": "ok",
            "code": "1500200",
        }
        """
        params = self.params_valid(serializer=ExecuteTimeoutSerializer)
        timeout = params[TIMEOUT]
        timeout = EXECUTE_TIMEOUT if timeout <= 0 else timeout

        cluster = model_manager.get_storage_cluster_config(cluster_name, DRUID)
        druid.clean_unused_segments(cluster_name, cluster[VERSION], timeout)
        return Response(cluster_name)

    @detail_route(methods=["get"], url_path="kill_pending_tasks")
    def kill_pending_tasks(self, request, cluster_name):
        """
        @api {get} v3/storekit/cluster/druid/:cluster_name/kill_pending_tasks kill所有pending状态的任务
        @apiGroup Druid
        @apiDescription kill所有pending状态的任务
        @apiName killPendingTasks
        @apiVersion 1.0.0
        @apiParam {String} task_type 任务类型
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": "",
            "message": "ok",
            "code": "1500200",
        }
        """
        druid.kill_pending_tasks(cluster_name)
        return Response(cluster_name)

    @detail_route(methods=["get"], url_path="kill_waiting_tasks")
    def kill_waiting_tasks(self, request, cluster_name):
        """
        @api {get} v3/storekit/cluster/druid/:cluster_name/kill_waiting_tasks kill所有waiting状态的任务
        @apiGroup Druid
        @apiDescription kill所有waiting状态的任务
        @apiName killWaitingTasks
        @apiVersion 1.0.0
        @apiParam {String} task_type 任务类型
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": "",
            "message": "ok",
            "code": "1500200",
        }
        """
        druid.kill_waiting_tasks(cluster_name)
        return Response(cluster_name)

    @detail_route(methods=["get"], url_path="compaction")
    def compaction(self, request, cluster_name):
        """
        @api {get} v3/storekit/cluster/druid/:cluster_name/compaction 合并segment
        @apiGroup Druid
        @apiDescription 合并segment
        @apiName compaction
        @apiVersion 1.0.0
        @apiParam {String} task_type 任务类型
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": "",
            "message": "ok",
            "code": "1500200",
        }
        """
        params = self.params_valid(serializer=MaintainDaysSerializer)
        delta_day = params[DELTA_DAY]
        delta_day = MAINTAIN_DELTA_DAY if delta_day <= 1 else delta_day

        params = self.params_valid(serializer=CompactionDaysSerializer)
        merge_days = params[MERGE_DAYS]
        merge_days = MERGE_DAYS_DEFAULT if merge_days <= 1 else merge_days

        params = self.params_valid(serializer=ExecuteTimeoutSerializer)
        timeout = params[TIMEOUT]
        timeout = EXECUTE_TIMEOUT if timeout <= 0 else timeout

        druid.segments_compaction(cluster_name, delta_day, merge_days, timeout)
        return Response(cluster_name)


class DruidSet(APIViewSet):
    # 结果表ID
    lookup_field = RESULT_TABLE_ID
    cluster_type = DRUID

    def create(self, request):
        """
        @api {post} v3/storekit/druid/ 初始化druid存储
        @apiGroup Druid
        @apiDescription 创建rt的一些元信息，关联对应的druidl存储
        @apiParam {string{小于128字符}} result_table_id 结果表名称
        @apiError (错误码) 1578104 result_table_id未关联存储druid
        @apiParamExample {json} 参数样例:
        {
            "result_table_id": "591_test_rt"
        }

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data": true,
            "result":true
        }
        """
        params = self.params_valid(serializer=CreateTableSerializer)
        rt = params[RESULT_TABLE_ID]
        rt_info = util.get_rt_info(rt)
        if rt_info and self.cluster_type in rt_info[STORAGES]:
            result = druid.initialize(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: rt, TYPE: self.cluster_type})

    def retrieve(self, request, result_table_id):
        """
        @api {get} v3/storekit/druid/:result_table_id/ 获取rt的druid信息
        @apiGroup Druid
        @apiDescription 获取result_table_id关联druid存储的元数据信息和物理表结构
        @apiError (错误码) 1578104 result_table_id未关联存储druid
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "code": "1500200",
            "data": {},
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and self.cluster_type in rt_info[STORAGES]:
            result = druid.info(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: self.cluster_type})

    def update(self, request, result_table_id):
        """
        @api {put} v3/storekit/druid/:result_table_id/ 更新rt的druid存储
        @apiGroup Druid
        @apiDescription 变更rt的一些元信息
        @apiError (错误码) 1578104 result_table_id未关联存储druid
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data": true,
            "result":true
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and self.cluster_type in rt_info[STORAGES]:
            result = druid.alter(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: self.cluster_type})

    def destroy(self, request, result_table_id):
        """
        @api {delete} v3/storekit/druid/:result_table_id/ 删除rt的druid存储
        @apiGroup Druid
        @apiParam {String} expires expires, 如30d
        @apiDescription 删除rt对应druid存储上的数据，以及已关联的元数据
        @apiError (错误码) 1578104 result_table_id未关联存储druid
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data": true,
            "result":true
        }
        """
        params = self.params_valid(serializer=DruidExpiresSerializer)
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and self.cluster_type in rt_info[STORAGES]:
            result = druid.delete(rt_info, params[EXPIRES])
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: self.cluster_type})

    @detail_route(methods=["get"], url_path="prepare")
    def prepare(self, request, result_table_id):
        """
        @api {get} v3/storekit/druid/:result_table_id/prepare/ 准备rt的druid存储
        @apiGroup Druid
        @apiDescription 准备rt关联的druid存储，例如创建库表
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data": true,
            "result":true
        }
        """

        rt_info = util.get_rt_info(result_table_id)
        if rt_info and self.cluster_type in rt_info[STORAGES]:
            result = druid.prepare(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: self.cluster_type})

    @detail_route(methods=["get"], url_path="check_schema")
    def check_schema(self, request, result_table_id):
        """
        @api {get} v3/storekit/druid/:result_table_id/check_schema/ 对比rt和druid的schema
        @apiGroup Druid
        @apiDescription 校验RT字段修改是否满足druid限制（flow调试时会使用）
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data": true,
            "result":true
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and self.cluster_type in rt_info[STORAGES]:
            result = druid.check_schema(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: self.cluster_type})

    @detail_route(methods=["get"], url_path="maintain")
    def maintain(self, request, result_table_id):
        """
        @api {post} v3/storekit/druid/:result_table_id/maintain/  维护druid表
        @apiGroup Druid
        @apiDescription 维护rt对应的druid表，增减分区
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": "",
            "message": "ok",
            "code": "1500200",
        }
        """
        params = self.params_valid(serializer=MaintainDaysSerializer)
        delta_day = params[DELTA_DAY]
        delta_day = MAINTAIN_DELTA_DAY if delta_day <= 1 else delta_day

        params = self.params_valid(serializer=CompactionDaysSerializer)
        merge_days = params[MERGE_DAYS]
        merge_days = MERGE_DAYS_DEFAULT if merge_days <= 1 else merge_days

        params = self.params_valid(serializer=ExecuteTimeoutSerializer)
        timeout = params[TIMEOUT]
        timeout = EXECUTE_TIMEOUT if timeout <= 0 else timeout

        rt_info = util.get_rt_info(result_table_id)
        if rt_info and self.cluster_type in rt_info[STORAGES]:
            result = druid.maintain(rt_info, delta_day, timeout, merge_days)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: self.cluster_type})

    @list_route(methods=["get"], url_path="maintain_all")
    def maintain_all(self, request):
        """
        @api {get} v3/storekit/druid/maintain_all/ 维护所有druid存储
        @apiGroup Druid
        @apiDescription 维护所有的druid存储
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": [],
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        params = self.params_valid(serializer=MaintainDaysSerializer)
        delta_day = params[DELTA_DAY]
        delta_day = MAINTAIN_DELTA_DAY if delta_day <= 1 else delta_day
        result = druid.maintain_all(delta_day)
        return Response(result)

    @list_route(methods=["get"], url_path="clusters")
    def clusters(self, request):
        """
        @api {get} v3/storekit/druid/clusters/ druid存储集群列表
        @apiGroup Druid
        @apiDescription 获取druid存储集群列表
        @apiSuccessExample {json} 成功返回:
        {
            "code": "1500200",
            "data": [],
            "errors": null,
            "message": "ok",
            "result": true
        }
        """
        result = druid.clusters()
        return Response(result)

    @detail_route(methods=["post"], url_path="create_task")
    def create_task(self, request, result_table_id):
        """
        @api {post} v3/storekit/druid/:result_table_id/create_task 创建task
        @apiGroup Druid
        @apiDescription 创建task
        @apiName create_task
        @apiVersion 1.0.0
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": "",
            "message": "ok",
            "code": "1500200",
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and self.cluster_type in rt_info[STORAGES]:
            result = druid.create_task(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: self.cluster_type})

    @detail_route(methods=["post"], url_path="shutdown_task")
    def shutdown_task(self, request, result_table_id):
        """
        @api {post} v3/storekit/druid/:result_table_id/shutdown_task 停止task
        @apiGroup Druid
        @apiDescription 停止task
        @apiName shutdown_task
        @apiVersion 1.0.0
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": "",
            "message": "ok",
            "code": "1500200",
        }
        """
        rt_info = util.get_rt_info(result_table_id)
        if rt_info and self.cluster_type in rt_info[STORAGES]:
            result = druid.shutdown_task(rt_info)
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: self.cluster_type})

    @detail_route(methods=["post"], url_path="update_expires")
    def update_expires(self, request, result_table_id):
        """
        @api {post} v3/storekit/druid/:result_table_id/update_expires 更新datasource的数据过期时间
        @apiGroup Druid
        @apiDescription 更新datasource的数据过期时间
        @apiName UpdateExpires
        @apiVersion 1.0.0
        @apiParam {String} expires expires, 如P30D
        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "result": true
            "data": "",
            "message": "ok",
            "code": "1500200",
        }
        """
        params = self.params_valid(serializer=DruidExpiresSerializer)
        rt_info = util.get_rt_info(result_table_id)

        if rt_info and self.cluster_type in rt_info[STORAGES]:
            result = druid.update_expires(rt_info, params[EXPIRES])
            return Response(result)
        else:
            raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: result_table_id, TYPE: self.cluster_type})

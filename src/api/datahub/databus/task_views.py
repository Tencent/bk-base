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
from common.log import logger
from common.views import APIViewSet
from datahub.common.const import FOR_DATALAB
from datahub.databus.exceptions import TaskParamMissing
from django.forms import model_to_dict
from django.utils.translation import ugettext as _
from rest_framework.response import Response

from datahub.databus import models, serializers

from .task import datanode_task, puller_task, storage_task, task_utils, transport_task
from .task.task import delete_task


class TaskViewset(APIViewSet):
    """
    对于资源 REST 操作逻辑统一放置在 APIViewSet 中提供接口
    """

    serializer_class = serializers.TasksCreateSerializer
    lookup_field = "rt_id"

    # 对于URL中实例ID变量名进行重命名，默认为 pk

    def create(self, request):
        """
        @apiGroup task
        @api {post} /databus/tasks/ 创建清洗分发任务
        @apiDescription 创建清洗，分发任务
        @apiParam {string} result_table_id result_table_id
        @apiParam {list} storages [可选]分发任务的存储列表。该参数没有时，启动所有配置存储的分发任务
        @apiParamExample {json} Request-Example:
            {"result_table_id":"xxx_xxx",
             "storages":["es"]
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": "任务(result_table_id：:result_table_id)启动成功!",
                    "message": "ok",
                    "code": "1500200",
                }
        """
        args = self.params_valid(serializer=serializers.TasksCreateSerializer)
        rt_id = args["result_table_id"]
        task_utils.check_task_auth(rt_id)
        storage_task.create_storage_task(rt_id, args["storages"])
        return Response(_(u"任务(result_table_id:{})启动成功!").format(rt_id))

    def destroy(self, request, rt_id):
        """
        @apiGroup task
        @api {delete} /databus/tasks/:rt_id/ 停止清洗，分发任务
        @apiDescription 停止清洗，分发任务
        @apiParam {list} storages [可选]分发任务的存储列表。该参数没有时，启动所有配置存储的分发任务
        @apiParamExample {json} Request-Example:
            {"storages":["es"]}
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": "result_table_id为:rt_id的任务已成功停止!",
                    "message": "ok",
                    "code": "1500200",
                }
        """
        args = self.params_valid(serializer=serializers.TasksStoragesSerializer)
        task_utils.check_task_auth(rt_id)
        delete_task(rt_id, args.get("storages", []))
        return Response(_(u"result_table_id:{},任务已成功停止!").format(rt_id))

    def list(self, request):
        """
        @apiGroup task
        @api {get} /databus/tasks/ 任务列表（admin使用）
        @apiDescription 任务列表（admin使用）
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                 {
                    "code":"1500200",
                    "message":"ok",
                    "errors":null,
                    "data":{
                        "count":12,
                        "results":[
                            {
                                "status":"running",
                                "connector_task_name":"hdfs-table_10000_test_clean_rt",
                                "task_type":"puller",
                                "description":"",
                                "created_at":"2019-04-02T04:36:31",
                                "updated_at":null,
                                "created_by":"",
                                "data_sink":"hdfs#adb",
                                "cluster_name":"hdfs-kafka_cluster_name-M",
                                "source_type":"kafka",
                                "data_source":"kafka#123",
                                "processing_id":"10000_test_clean_rt",
                                "id":2437,
                                "sink_type":"hdfs",
                                "updated_by":""
                            }
                        ]
                    },
                    "result":true
                }
        """

        page = int(request.GET.get("page", 0))
        page_size = int(request.GET.get("page_size", 0))
        cluster_name = request.GET.get("cluster_name")
        status = request.GET.get("status")

        tasks = models.DatabusConnectorTask.objects.filter()
        if cluster_name:
            tasks = tasks.filter(cluster_name__exact=cluster_name)

        if status:
            tasks = tasks.filter(status__exact=status)

        total_size = tasks.count()

        if page != 0:
            tasks = tasks.order_by("id")[(page - 1) * page_size : page * page_size]

        task_list = []
        for one_task in tasks:
            task_list.append(model_to_dict(one_task))

        return Response(
            {
                "page_total": total_size,
                "page": 1 if page == 0 else page,
                "tasks": task_list,
            }
        )

    def retrieve(self, request, rt_id):
        """
        @apiGroup task
        @api {get} /databus/tasks/:rt_id/ 通过result table id查询kafka connect任务路由信息
        @apiParams {string} storage 存储类型
        @apiDescription 查询connector路由信息
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": [
                       {
                            "status": "delete",
                            "cluster_name": "xxx",
                            "task_type": "shipper",
                            "description": "",
                            "sink_type": "kafka",
                            "created_by": "",
                            "data_sink": "xxx",
                            "connector_task_name": "xxx",
                            "source_type": "kafka",
                            "data_source": "xxx",
                            "id": 11,
                            "processing_id": "xxx",
                            "updated_by": ""
                       }
                    ],
                    "message": "",
                    "code": "1500200",
                }
        """
        args = self.params_valid(serializer=serializers.TasksStorageSerializer)
        if args["storage"]:
            storage = args["storage"]
            storages = ["es", "eslog"] if args["storage"] == "es" else [args["storage"]]
            obj = models.DatabusConnectorTask.objects.filter(processing_id__exact=rt_id, sink_type__in=storages)

            if len(obj) > 1:
                logger.error("({}, {}) task count is wrong! {}".format(rt_id, storage, len(obj)))
                return Response({})
            elif len(obj) == 0:
                logger.error("({}, {}) task NOT FOUND!".format(rt_id, storage))
                return Response({})

            rst_info = model_to_dict(obj[0])
            try:
                obj = models.DatabusCluster.objects.get(cluster_name=rst_info["cluster_name"])
            except models.DatabusCluster.DoesNotExist:
                return Response({})
            rst_info["module"] = obj.module
            rst_info["component"] = obj.component
            return Response(rst_info)
        else:
            return Response(models.DatabusConnectorTask.objects.filter(processing_id__exact=rt_id).values())

    @list_route(methods=["post"], url_path="datanode")
    def datanode_task(self, request):
        """
        @apiGroup task
        @api {post} /databus/tasks/datanode/ 创建固化节点任务
        @apiDescription 创建固化节点任务
        @apiParam {string} result_table_id result_table_id
        @apiParamExample {json} Request-Example:
            {"result_table_id":"xxx_xxx"}
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": "connectors for result_table_id :result_table_id started!",
                    "message": "ok",
                    "code": "1500200",
                }
        """
        args = self.params_valid(serializer=serializers.TasksRtIdSerializer)
        rt_id = args["result_table_id"]
        task_utils.check_task_auth(rt_id)

        datanode_task.process_datanode_task(rt_id)
        return Response(u"任务(result_table_id：%s)启动成功!" % rt_id)

    @list_route(methods=["get"], url_path="component")
    def task_component(self, request):
        """
        @apiGroup task
        @api {get} /databus/tasks/component/?result_table_id=id1&result_table_id=id2 查询result_table_id所有任务的component
        @apiDescription 创建tdw拉取任务
        @apiParam {string} result_table_id result_table_id
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result":true,
                    "data":{
                        "id1":{
                            "mysql":{
                                "component":"",
                                "module":""
                            },
                            "es":{
                                "component":"",
                                "module":""
                            }
                        },
                        "id2":{
                            "mysql":{
                                "component":"",
                                "module":""
                            },
                            "es":{
                                "component":"",
                                "module":""
                            }
                        }
                    },
                    "message":"",
                    "code":"00"
                }
        """
        result_table_ids = request.query_params.getlist("result_table_id", None)
        if result_table_ids is None:
            raise TaskParamMissing(message_kv={"param": "result_table_id"})

        result = task_utils.get_task_component(result_table_ids)
        return Response(result)


class PullerViewset(APIViewSet):
    lookup_field = "data_id"

    def create(self, request):
        """
        @apiGroup task
        @api {post} /databus/tasks/puller/ 创建拉取任务
        @apiDescription 创建tdw拉取任务
        @apiParam {string} data_id raw_data_name
        @apiParamExample {json} Request-Example:
            {"data_id":"xxx_xxx"}
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": "任务(data_id：%s)启动成功!",
                    "message": "",
                    "code": "00",
                }
        """

        args = self.params_valid(serializer=serializers.TasksDataIdSerializer)
        data_id = args["data_id"]

        logger.info("create puller task for %s" % data_id)
        puller_task.process_puller_task(data_id)
        return Response(u"任务(data_id：%s)启动成功!" % data_id)

    def destroy(self, request, data_id):
        """
         @apiGroup task
         @api {delete} /databus/tasks/puller/:data_id/ 停止拉取任务
         @apiDescription 停止拉取任务
        @apiSuccessExample {json} Success-Response:
             HTTP/1.1 200 OK
                 {
                     "result": true,
                     "data": "任务(data_id：%s)停止成功!",
                     "message": "ok",
                     "code": "1500200",
                 }
        """
        logger.info("stop puller task for %s" % data_id)
        puller_task.stop_puller_task(data_id)
        return Response(u"任务(data_id：%s)停止成功!" % data_id)


class TransportViewSet(APIViewSet):
    lookup_field = "connector_name"

    def create(self, request):
        """
        @apiGroup task
        @api {post} /databus/tasks/transport/ 创建迁移任务
        @apiDescription 创建同一rt_id且不同存储类型之间的迁移任务，或者创建不同rt_id指定存储类型之间的迁移任务
        @apiParam {string{小于128字符}}
        @apiError (错误码) 1578104 result_table_id未关联存储hdfs。
        @apiParamExample {json} 参数样例:
        {
            "source_rt_id": "xxxx",
            "source_type": "hdfs",
            "sink_rt_id": "xxx",
            "sink_type": "clickhouse"
        }
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message":"ok",
            "code":"1500200",
            "data": "xxxx", // 返回connector_name，用于删数和查看任务
            "result":true
        }
        """
        params = self.params_valid(serializer=serializers.TasksTransportSerializer)
        connctor_name = transport_task.process_transport_task(params)
        return Response(connctor_name)

    def destroy(self, request, connector_name):
        """
        @apiGroup task
        @api {delete} /databus/tasks/transport/:connector_name/ 停止并删除迁移任务
        @apiDescription 停止并删除迁移任务
        @apiParam {string{小于128字符}}
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "result": true,
            "data": "任务(data_id：%s)停止成功!",
            "message": "ok",
            "code": "1500200",
        }
        """
        logger.info("to stop transport task %s " % connector_name)
        transport_task.stop_transport(connector_name)
        transport_task.delete_transport_task_status(connector_name)
        return Response(u"任务(data_id：%s)停止成功!" % connector_name)

    def retrieve(self, request, connector_name):
        """
        @apiGroup task
        @api {get} /databus/tasks/transport/:connector_name/?for_datalab=False 查看指定connector状态
        @apiDescription 查看指定connector状态
        @apiParam {string{小于128字符}}
        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "status": "success",
                "percentage" : 100,
                "stages": [
                    {
                        "update_time": "2020-11-02T16:32:56.521",
                        "description": "执行数据迁移",
                        "last_read_file_name": "xxx.snappy.parquet",
                        "stage_status": "success",
                        "processed_rows": 31484,
                        "finished": 100,
                        "source_rt_id": "xxx",
                        "stage_seq": 1,
                        "last_read_line_number": 7871,
                        "time_taken": 1,  // 单位s
                        "finish_time": "2020-11-02T16:32:56.520",
                        "create_rime": "2020-11-02T16:22:29.869",
                        "stage_type": "execute",
                        "sink_rt_id": "xxx"
                    }
                ]
             },
             "result": true
        }
        """
        params = self.params_valid(serializer=serializers.TasksTransportStatusSerializer)
        result = transport_task.get_transport_task_status(connector_name, params[FOR_DATALAB])
        return Response(result)

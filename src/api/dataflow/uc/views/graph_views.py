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

from common.decorators import detail_route, params_valid

from dataflow.uc.graph.graph_controller import GraphController
from dataflow.uc.graph.graph_job_controller import GraphJobController
from dataflow.uc.graph.graph_serializers import CreateGraph, GetLatestDeployData, GetOperateResult
from dataflow.uc.views.base_views import BaseViewSet


class GraphViewSet(BaseViewSet):
    """
    uc graph rest api
    """

    lookup_field = "graph_id"

    @params_valid(serializer=CreateGraph)
    def create(self, request, params):
        """
        @api {post} /dataflow/uc/graphs/ 创建 graph
        @apiName create graph
        @apiGroup uc
        @apiParam {int} project_id 项目ID
        @apiParam {list} nodes 组成graph的node配置
        @apiParamExample {json} 参数样例:
            {
                "project_id": 123,
                "nodes": [
                    "processing_id": "processing_id",
                    "component_type": "spark",
                    "processor_type": "code",
                    "programming_language": "python",
                    "tags": ["inland"],
                    "children": [], # 如果为叶子节点，则不需要此字段
                    "data_link": {
                        "inputs": [],
                        "outputs": [],
                        "static_data": [],
                        "storage": []
                    },
                    "processor_logic": {
                        "user_main_class": "service",
                        "user_args": "args0 args1",
                        "user_package_path": "hdfs://xx.zip",
                        "window": {},
                        "sql": ""
                    },
                    "deploy_config": {
                        "cluster_group": "xx",
                        "resource": {},
                        "checkpoint": {},
                        "advanced": {}
                    },
                    "schedule_info": {
                        "schedule_period": "",
                        "result_tables": {}
                    }
                ]
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "graph_id": "abc"
                    },
                    "result": true
                }
        """
        graph_id = GraphController.create_graph(params)
        return self.return_ok({"graph_id": graph_id})

    @params_valid(serializer=CreateGraph)
    def update(self, request, graph_id, params):
        """
        @api {put} /dataflow/uc/graphs/:graph_id 更新 graph
        @apiName update graph
        @apiGroup uc
        @apiParam {int} project_id 项目ID
        @apiParam {list} nodes 组成graph的node配置
        @apiParamExample {json} 参数样例:
            {
                "project_id": 123,
                "nodes": [
                    "id": "processing_id",
                    "component_type": "spark",
                    "processor_type": "code",
                    "programming_language": "python",
                    "tags": ["inland"],
                    "children": [], # 如果为叶子节点，则不需要此字段
                    "data_link": {
                        "inputs": [],
                        "outputs": [],
                        "static_data": [],
                        "storage": []
                    },
                    "processor_logic": {
                        "user_main_class": "service",
                        "user_args": "args0 args1",
                        "user_package_path": "hdfs://xx.zip",
                        "window": {},
                        "sql": ""
                    },
                    "deploy_config": {
                        "cluster_group": "xx",
                        "resource": {},
                        "checkpoint": {},
                        "advanced": {}
                    },
                    "schedule_info": {
                        "schedule_period": "",
                        "result_tables": {}
                    }
                ]
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "graph_id": "abc"
                    },
                    "result": true
                }
        """
        graph_id = GraphController(graph_id).update_graph(params)
        return self.return_ok({"graph_id": graph_id})

    def destroy(self, request, graph_id):
        """
        @api {delete} /dataflow/uc/graphs/:graph_id/ 删除graph
        @apiName graphs/:graph_id/
        @apiGroup Stream
        @apiParamExample {json} 参数样例:
            {}
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": None,
                    "result": true
                }
        """
        GraphController(graph_id).delete_graph()
        return self.return_ok()

    @detail_route(methods=["post"], url_path="start")
    def start(self, request, graph_id):
        """
        @api {post} /dataflow/uc/graphs/:graph_id/start 启动 graph 任务
        @apiName graphs/:graph_id/start
        @apiGroup uc
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "task_id": 1
                    },
                    "result": true
                }
        """
        result = GraphController(graph_id).start_graph()
        return self.return_ok(result)

    @detail_route(methods=["post"], url_path="stop")
    def stop(self, request, graph_id):
        """
        @api {post} /dataflow/uc/graphs/:graph_id/stop 停止 graph 任务
        @apiName graphs/:graph_id/stop
        @apiGroup uc
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "task_id": 1
                    },
                    "result": true
                }
        """
        result = GraphController(graph_id).stop_graph()
        return self.return_ok(result)

    @detail_route(methods=["get"], url_path="latest_deploy_data")
    @params_valid(serializer=GetLatestDeployData)
    def get_latest_deploy_data(self, request, graph_id, params):
        """
        @api {get} /dataflow/uc/graphs/:graph_id/latest_deploy_data 获取 graph 最近部署信息
        @apiName graphs/:graph_id/latest_deploy_data
        @apiGroup uc
        @apiParam {int} task_id  graph 任务操作的 task id
        @apiParamExample {json} 参数样例:
            {
                "operate": "start/stop"
                "task_id": 1
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "status": "running/finished/failed",
                        "info": None  # status为failed时，info里面有报错信息
                    },
                    "result": true
                }
        """
        operate = params["operate"]
        task_id = params["task_id"]
        result = GraphController(graph_id).get_latest_deploy_data(operate, task_id)
        return self.return_ok(result)


class GraphJobViewSet(BaseViewSet):
    lookup_field = "job_id"

    @detail_route(methods=["post"], url_path="start")
    def start(self, request, graph_id, job_id):
        """
        @api {post} /dataflow/uc/graphs/:graph_id/jobs/:job_id/start/ 启动graph中的一个任务
        @apiName graphs/:graph_id/jobs/:job_id/start/
        @apiGroup uc
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {"operate": "start", "operate_id": "123"},
                    "result": true
                }
        """
        result = GraphJobController(graph_id, job_id).start()
        return self.return_ok(result)

    @detail_route(methods=["post"], url_path="stop")
    def stop(self, request, graph_id, job_id):
        """
        @api {post} /dataflow/uc/graphs/:graph_id/jobs/:job_id/stop/ 停止graph中的一个任务
        @apiName graphs/:graph_id/jobs/:job_id/stop/
        @apiGroup uc
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {"operate": "stop", "operate_id": "123"},
                    "result": true
                }
        """
        result = GraphJobController(graph_id, job_id).stop()
        return self.return_ok(result)

    @detail_route(methods=["get"], url_path="operate_result")
    @params_valid(serializer=GetOperateResult)
    def get_operate_result(self, request, graph_id, job_id, params):
        """
        @api {get} /dataflow/uc/graphs/:graph_id/jobs/:job_id/operate_result/ 获取graph中job的操作(start/stop)结果
        @apiName graphs/:graph_id/jobs/:job_id/start/
        @apiGroup uc
        @apiParamExample {json} 参数样例:
            {
                "operate": "start/stop",
                "operate_id": "123"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                    },
                    "result": true
                }
        """
        result = GraphJobController(graph_id, job_id).get_operate_result(params)
        return self.return_ok(result)

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
from common.views import APIViewSet
from rest_framework.response import Response

from dataflow.batch.interactive_server import interactive_servers_driver
from dataflow.batch.serializer.serializers import (
    InteractiveCodeCancelSerializer,
    InteractiveCodeStatusSerializer,
    InteractiveCreateSerializer,
    InteractiveCreateSessionSerializer,
    InteractiveKillServersSerializer,
    InteractiveRunCodeSerializer,
    InteractiveServerCreateSerializer,
    InteractiveServersStatusSerializer,
    InteractiveSessionStatusSerializer,
)


class InteractiveServersViewSet(APIViewSet):
    lookup_field = "server_id"
    lookup_value_regex = r"\w+"

    @params_valid(serializer=InteractiveCreateSerializer)
    def create(self, request, params):
        """
        @api {post} /dataflow/batch/interactive_servers/ 创建交互式server以及session
        @apiName start_server_and_session
        @apiGroup interactive_servers
        @apiParam {string} server_id server ID
        @apiParam {string} bk_user user
        @apiParam {string} geog_area_code geog_area_code
        @apiParamExample {json} 参数样例:
            {
                "server_id": "xxx",
                "bk_user": "xxx",
                "geog_area_code": "inland",
                "cluster_group": "default",
                "node_label": xxx,
                "bk_project_id": 123456,
                "preload_py_files": ["file1.py", "file1.py"],
                "preload_files": ["file1", "file2"],
                "preload_jars": ["file1.jar", "file2.jar"],
                "engine_conf": {
                    "spark.bkdata.auth.project.id.enable": "true",
                    "spark.bkdata.auth.user.id.enable": "true",
                    "spark.bkdata.auth.cross.business.enable": "true",
                    "spark.bkdata.auth.storage.path.enable": "true"
                }
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200"
                    "data": "started",
                    "result": true
                }
        """
        res = interactive_servers_driver.create_server_and_session(params)
        return Response(res)

    @params_valid(serializer=InteractiveServersStatusSerializer)
    def retrieve(self, request, server_id, params):
        """
        @api {get} /dataflow/batch/interactive_servers/:server_id 获取交互式server状态
        @apiName get_session_status
        @apiGroup interactive_servers
        @apiParam {string} server_id server ID
        @apiParam {string} geog_area_code geog_area_code
        @apiParamExample {json} 参数样例:
            {
                "geog_area_code": "inland"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200"
                    "data": "started",
                    "result": true
                }
        """
        res = interactive_servers_driver.retrieve_server_status(server_id, params)
        return Response(res)

    @detail_route(methods=["post"], url_path="server")
    @params_valid(serializer=InteractiveServerCreateSerializer)
    def server(self, request, server_id, params):
        """
        @api {post} /dataflow/batch/interactive_servers/:server_id/server 创建交互式server
        @apiName start_server
        @apiGroup interactive_servers
        @apiParam {string} server_id server ID
        @apiParam {string} geog_area_code geog_area_code
        @apiParam {string} node_label node_label
        @apiParam {string} cluster_group cluster_group
        @apiParamExample {json} 参数样例:
            {
                "geog_area_code": "inland",
                "bk_user": "xxx",
                "node_label": xxx,
                "cluster_group": "default"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200"
                    "data": "3943417",
                    "result": true
                }
        """
        res = interactive_servers_driver.create_interactive_server(server_id, params)
        return Response(res)

    @detail_route(methods=["post"], url_path="session")
    @params_valid(serializer=InteractiveCreateSessionSerializer)
    def session(self, request, server_id, params):
        """
        @api {post} /dataflow/batch/interactive_servers/:server_id/session/ 创建交互式server session
        @apiName start_session
        @apiGroup interactive_servers
        @apiParam {string} server_id server ID
        @apiParam {string} bk_user user
        @apiParam {string} geog_area_code geog_area_code
        @apiParamExample {json} 参数样例:
            {
                "bk_user": "xxx",
                "geog_area_code": "inland",
                "cluster_group": "default",
                "preload_py_files": ["file1.py", "file1.py"],
                "preload_files": ["file1", "file2"],
                "preload_jars": ["file1.jar", "file2.jar"],
                "engine_conf": {
                    "spark.bkdata.auth.project.id.enable": "true",
                    "spark.bkdata.auth.user.id.enable": "true",
                    "spark.bkdata.auth.cross.business.enable": "true",
                    "spark.bkdata.auth.storage.path.enable": "true"
                }
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "kind": "pyspark",
                    "name": null,
                    "proxyUser": null,
                    "appInfo": {
                        "driverLogUrl": null,
                        "sparkUiUrl": null
                    },
                    "state": "starting",
                    "appId": null,
                    "owner": null,
                    "id": 0,
                    "log": [
                        "stdout: ",
                        "\nstderr: ",
                        "\nYARN Diagnostics: "
                    ]
                },
                "result": true
            }
        """
        res = interactive_servers_driver.create_spark_session(server_id, params)
        return Response(res)

    @detail_route(methods=["get"], url_path="session_status")
    @params_valid(serializer=InteractiveSessionStatusSerializer)
    def session_status(self, request, server_id, params):
        """
        @api {get} /dataflow/batch/interactive_servers/:server_id/session_status 获取交互式server session状态
        @apiName session_status
        @apiGroup interactive_servers
        @apiParam {string} server_id server ID
        @apiParam {string} geog_area_code geog_area_code
        @apiParamExample {json} 参数样例:
            {
                "geog_area_code": "inland"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                "kind": "pyspark",
                "name": null,
                "proxyUser": null,
                "appInfo": {
                    "driverLogUrl": "http://${driver_url}/node/containerlogs/${container_id}/root",
                    "sparkUiUrl": "http://${driver_url}/proxy/${application_id}/"
                },
                "state": "idle",
                "appId": "application_1584501917228_53812",
                "owner": null,
                "id": 0,
                "log": [
                    "20/04/29 19:50:58 INFO SparkEntries: Spark context finished initialization in 9122ms",
                    "20/04/29 19:50:58 INFO SparkEntries: Created Spark session.",
                    "20/04/29 19:51:58 INFO ExecutorAllocationManager: Request to remove executorIds: 1",
                    "20/04/29 19:53:40 INFO SparkEntries: Created SQLContext.",
                    "\nstderr: ",
                    "\nYARN Diagnostics: "
                ]
                },
                "result": true
            }
        """
        res = interactive_servers_driver.get_session_status(server_id, params)
        return Response(res)

    @params_valid(serializer=InteractiveKillServersSerializer)
    def destroy(self, request, server_id, params):
        """
        @api {delete} /dataflow/batch/interactive_servers/:server_id/ 删除交互式server
        @apiName delete_server
        @apiGroup interactive_servers
        @apiParam {string} server_id server ID
        @apiParam {string} geog_area_code geog_area_code
        @apiParamExample {json} 参数样例:
            {
                "geog_area_code": "inland"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": null,
                "result": true
            }
        """
        return Response(interactive_servers_driver.stop_server(server_id, params))


class InteractiveServersCodeViewSet(APIViewSet):
    lookup_field = "code_id"
    lookup_value_regex = r"\w+"

    @params_valid(serializer=InteractiveRunCodeSerializer)
    def create(self, request, server_id, params):
        """
        @api {post} /dataflow/batch/interactive_servers/:server_id/codes/ 交互式server执行代码
        @apiName run_code
        @apiGroup interactive_servers
        @apiParam {string} server_id server ID
        @apiParam {string} geog_area_code geog_area_code
        @apiParamExample {json} 参数样例:
            {
              "blacklist_group_name": 'xxx'
              "context": ["a = 1", "a + 1"]
              "code": "a + 1",
              "geog_area_code": "inland"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "code": "1 + 1",
                    "started": 0,
                    "completed": 0,
                    "state": "waiting",
                    "progress": 0,
                    "output": null,
                    "id": 0
                },
                "result": true
            }
        """
        res = interactive_servers_driver.run_pyspark_code(server_id, params)
        return Response(res)

    @params_valid(serializer=InteractiveCodeStatusSerializer)
    def retrieve(self, request, server_id, code_id, params):
        """
        @api {get} /dataflow/batch/interactive_servers/:server_id/codes/:code_id 获取交互式server代码执行状态
        @apiName get_code_status
        @apiGroup interactive_servers
        @apiParam {string} server_id server ID
        @apiParam {string} code_id code ID
        @apiParam {string} geog_area_code geog_area_code
        @apiParamExample {json} 参数样例:
            {
                "geog_area_code": "inland"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "code": "1 + 1",
                    "started": 1588164531296,
                    "completed": 1588164531297,
                    "state": "available",
                    "progress": 1,
                    "output": {
                        "status": "ok",
                        "execution_count": 0,
                        "data": {
                            "text/plain": "2"
                        }
                    },
                    "id": 0
                },
                "result": true
            }
        """
        res = interactive_servers_driver.get_code_status(code_id, server_id, params)
        return Response(res)

    @detail_route(methods=["post"], url_path="cancel")
    @params_valid(serializer=InteractiveCodeCancelSerializer)
    def cancel(self, request, server_id, code_id, params):
        """
        @api {post} /dataflow/batch/interactive_servers/:server_id/codes/:code_id/cancel 取消交互式server代码执
        @apiName cancel_code
        @apiGroup interactive_servers
        @apiParam {string} server_id server ID
        @apiParam {string} code_id code ID
        @apiParam {string} geog_area_code geog_area_code
        @apiParamExample {json} 参数样例:
            {
                "geog_area_code": "inland"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": null,
                "message": "ok",
                "code": "1500200",
                "data": {
                    "msg": "canceled"
                },
                "result": true
            }
        """
        res = interactive_servers_driver.cancel_code(code_id, server_id, params)
        return Response(res)

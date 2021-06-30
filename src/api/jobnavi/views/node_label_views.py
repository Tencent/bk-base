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
import json

from rest_framework.response import Response

from common.decorators import params_valid, list_route
from common.views import APIViewSet
from jobnavi.api.jobnavi_api import JobNaviApi
from jobnavi.exception.exceptions import InterfaceError
from jobnavi.views.serializers import (
    CommonSerializer,
    RunnerNodeLabelSerializer,
    RunnerNodeLabelDefinitionSerializer,
    DeleteRunnerNodeLabelSerializer,
    DeleteNodeLabelDefinitionSerializer,
)


class NodeLabelViewSet(APIViewSet):
    """
    @apiDefine runner_node_label
    Runner节点标签API
    """

    lookup_field = "runner_id"

    @params_valid(serializer=CommonSerializer)
    def list(self, request, cluster_id, params):
        """
        @api {get} /jobnavi/cluster/:cluster_id/node_label/ 获取Runner节点标签列表
        @apiName list_runner_node_label
        @apiGroup runner_node_label
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": {
                    "host-0": ["label_1", "label_2"],
                    "host-1": ["default"],
                    ...
                ],
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.list_runner_node_label()
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        return Response(json.loads(result.data))

    @params_valid(serializer=CommonSerializer)
    def retrieve(self, request, cluster_id, runner_id, params):
        """
        @api {get} /jobnavi/cluster/:cluster_id/node_label/:runner_id/ 获取指定Runner节点标签
        @apiName retrieve_runner_node_label
        @apiGroup runner_node_label
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": {
                    "host-0": ["label_1", "label_2"],
                    "host-1": ["default"],
                    ...
                ],
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.retrieve_runner_node_label(runner_id)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        return Response(json.loads(result.data))

    @params_valid(serializer=RunnerNodeLabelSerializer)
    def create(self, request, cluster_id, params):
        """
        @api {post} /jobnavi/cluster/:cluster_id/node_label/ 绑定Runner节点标签
        @apiName create_runner_node_label
        @apiGroup runner_node_label
        @apiParam {string} runner_id Runner节点ID
        @apiParam {string} node_label Runner节点标签
        @apiParam {string} description Runner节点标签描述
        @apiParamExample {json} 参数样例:
            {
                "runner_id": "host_1",
                "node_label": "label_1"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": null,
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        runner_id = params["runner_id"]
        node_label = params["node_label"]
        description = params["description"] if "description" in params else ""
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.create_runner_node_label(runner_id, node_label, description)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        return Response()

    @params_valid(serializer=DeleteRunnerNodeLabelSerializer)
    def destroy(self, request, cluster_id, runner_id, params):
        """
        @api {delete} /jobnavi/cluster/:cluster_id/node_label/:runner_id/ 删除指定Runner节点标签
        @apiName delete_runner_node_label
        @apiGroup runner_node_label
        @apiParamExample {json} 参数样例:
            {
                "node_label": "label_1"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": null,
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        node_label = params["node_label"]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.delete_runner_node_label(runner_id, node_label)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        return Response()

    @list_route(methods=["post"], url_path="create_label_definition")
    @params_valid(serializer=RunnerNodeLabelDefinitionSerializer)
    def create_label_definition(self, request, cluster_id, params):
        """
        @api {post} /jobnavi/cluster/:cluster_id/node_label/create_label_definition/ 添加Runner节点标签定义
        @apiName create_node_label_definition
        @apiGroup runner_node_label
        @apiParam {string} node_label Runner节点标签定义名
        @apiParam {string} description Runner节点标签定义描述
        @apiParamExample {json} 参数样例:
            {
                "node_label": "static_label_1"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": null,
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        node_label = params["node_label"]
        description = params["description"] if "description" in params else ""
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.create_runner_node_label_definition(node_label, description)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        return Response()

    @list_route(methods=["get"], url_path="list_label_definition")
    @params_valid(serializer=CommonSerializer)
    def list_label_definition(self, request, cluster_id, params):
        """
        @api {get} /jobnavi/cluster/:cluster_id/node_label/list_label_definition/ 获取Runner节点标签定义列表
        @apiName list_node_label_definition
        @apiGroup runner_node_label
        @apiParamExample {json} 参数样例:
            {
                "node_label": "label_1"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": {
                    "label_1": "description for label 1",
                    "label_2": "description for label 2",
                },
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.list_runner_node_label_definition()
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        return Response(json.loads(result.data))

    @list_route(methods=["post"], url_path="delete_label_definition")
    @params_valid(serializer=DeleteNodeLabelDefinitionSerializer)
    def delete_label_definition(self, request, cluster_id, params):
        """
        @api {post} /jobnavi/cluster/:cluster_id/node_label/delete_label_definition/ 删除Runner节点标签定义
        @apiName delete_node_label_definition
        @apiGroup runner_node_label
        @apiParam {string} node_label Runner节点标签定义名
        @apiParamExample {json} 参数样例:
            {
                "node_label": "label_1"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": null,
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        node_label = params["node_label"]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.delete_runner_node_label_definition(node_label)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        return Response()

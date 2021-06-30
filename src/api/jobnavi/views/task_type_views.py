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

from django.conf import settings
from common.decorators import params_valid, list_route, detail_route
from common.views import APIViewSet
from jobnavi.api.jobnavi_api import JobNaviApi
from jobnavi.exception.exceptions import ArgumentError, InterfaceError
from jobnavi.views.serializers import (
    CommonSerializer,
    UploadFileTaskSerializer,
    TaskTypeSerializer,
    DeleteTaskTypeTagSerializer,
    RetrieveTaskTypeTagAliasSerializer,
    TaskTypeTagAliasSerializer,
    DeleteTaskTypeTagAliasSerializer,
    RetrieveTaskTypeDefaultTagSerializer,
    TaskTypeDefaultTagSerializer,
    DeleteTaskTypeDefaultTagSerializer,
)


class TaskTypeViewSet(APIViewSet):
    """
    @apiDefine task_type
    任务类型管理API
    """

    lookup_field = "type_id"
    lookup_value_regex = "\\w+"

    @detail_route(methods=["post"], url_path="upload")
    @params_valid(serializer=UploadFileTaskSerializer)
    def upload(self, request, cluster_id, type_id, params):
        """
        @api {post} /jobnavi/cluster/:cluster_id/task_type/:type_id/upload 上传任务执行文件
        @apiName task_type_upload
        @apiGroup task_type
        @apiParam {string} type_id 任务
        @apiParamExample {json} 参数样例:
            {
                "task_id": xxx,
                "rtx_name": "xxx"
                "server": "xxxx"
                "app_group": "xxx"
                "file": ""
            }
        @apiSuccessExample {json} 成功返回，返回事件ID
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": 12,
                "result": true
            }
        """
        if settings.EXTENDED and type_id == "tdw":
            from jobnavi.extend.api.tdw_api import TDWApi
            tdw_api = TDWApi(cluster_id)
            tdw_api.upload_jar(request.data, self.request.FILES)
            return Response()
        else:
            raise ArgumentError(message="only support [tdw] task type.")

    @params_valid(serializer=CommonSerializer)
    def list(self, request, cluster_id, params):
        """
        @api {get} /jobnavi/cluster/:cluster_id/task_type/ 获取任务类型列表
        @apiName list_task_type
        @apiGroup task_type
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": [
                    {
                        "type_id": "type-1",
                        "tag": "stable",
                        "main": "com.main1",
                        "description": null,
                        "env": "env1",
                        "sys_env": null,
                        "language": "python",
                        "task_mode": "process",
                        "recoverable": true
                    }, {
                        "type_id": "type-2",
                        "tag": "stable",
                        "main": "com.main2",
                        "description": null,
                        "env": "env2",
                        "sys_env": null,
                        "language": "java",
                        "task_mode": "thread",
                        "recoverable":false
                    }
                ],
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.list_task_type()
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        return Response(json.loads(result.data))

    @params_valid(serializer=TaskTypeSerializer)
    def create(self, request, cluster_id, params):
        """
        @api {post} /jobnavi/cluster/:cluster_id/task_type/ 新增任务类型定义
        @apiName create_task_type
        @apiGroup task_type
        @apiParam {string} type_id 任务类型ID
        @apiParam {string} tag 任务类型tag
        @apiParam {string} main 任务入口定义
        @apiParam {string} env 任务环境配置
        @apiParam {string} sys_env 任务系统环境依赖配置
        @apiParam {string} language 任务运行语言
        @apiParam {string} task_mode 任务运行模式
        @apiParam {string} [recoverable] 是否可从故障自愈
        @apiParam {string} [created_by] 创建人
        @apiParam {string} [description] 任务类型描述
        @apiParamExample {json} 参数样例:
            {
                "type_id": "type-1",
                "tag": "stable",
                "main": "com.main1",
                "env": "env1",
                "sys_env": null,
                "language": "python",
                "task_mode": "process",
                "recoverable": true,
                "created_by": "admin",
                "description": null
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
        type_id = params["type_id"]
        tag = params["tag"]
        main = params["main"]
        env = params["env"]
        sys_env = params["sys_env"]
        language = params["language"]
        task_mode = params["task_mode"]
        recoverable = params["recoverable"] if "recoverable" in params else False
        created_by = params["created_by"] if "created_by" in params else None
        description = params["description"] if "description" in params else None
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.create_task_type(
            type_id, tag, main, env, sys_env, language, task_mode, recoverable, created_by, description
        )
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        return Response()

    @list_route(methods=["post"], url_path="delete_tag")
    @params_valid(serializer=DeleteTaskTypeTagSerializer)
    def delete_tag(self, request, cluster_id, params):
        """
        @api {post} /jobnavi/cluster/:cluster_id/task_type/delete_tag/ 删除任务类型tag定义
        @apiName delete_task_type_tag
        @apiGroup task_type
        @apiParam {string} type_id 任务类型ID
        @apiParam {string} tag 任务类型tag
        @apiParamExample {json} 参数样例:
            {
                "type_id": "type-1",
                "tag": "stable"
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
        type_id = params["type_id"]
        tag = params["tag"]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.delete_task_type_tag(type_id, tag)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        return Response()

    @list_route(methods=["get"], url_path="retrieve_tag_alias")
    @params_valid(serializer=RetrieveTaskTypeTagAliasSerializer)
    def retrieve_tag_alias(self, request, cluster_id, params):
        """
        @api {get} /jobnavi/cluster/:cluster_id/task_type/retrieve_tag_alias/ 查询任务类型tag定义别名
        @apiName retrieve_task_type_tag_alias
        @apiGroup task_type
        @apiParam {string} type_id 任务类型ID
        @apiParam {string} tag 任务类型tag
        @apiParamExample {json} 参数样例:
            {
                "type_id": "type-1",
                "tag": "stable"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": ["stable", "latest"],
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        type_id = params["type_id"]
        tag = params["tag"]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.retrieve_task_type_tag_alias(type_id, tag)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        return Response(json.loads(result.data))

    @list_route(methods=["post"], url_path="create_tag_alias")
    @params_valid(serializer=TaskTypeTagAliasSerializer)
    def create_tag_alias(self, request, cluster_id, params):
        """
        @api {post} /jobnavi/cluster/:cluster_id/task_type/create_tag_alias/ 新增任务类型tag定义
        @apiName create_task_type_tag_alias
        @apiGroup task_type
        @apiParam {string} type_id 任务类型ID
        @apiParam {string} tag 任务类型tag
        @apiParam {string} alias 任务类型tag别名
        @apiParam {string} [description] 任务类型tag别名描述
        @apiParamExample {json} 参数样例:
            {
                "type_id": "type-1",
                "tag": "stable",
                "alias": "latest"
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
        type_id = params["type_id"]
        tag = params["tag"]
        alias = params["alias"]
        description = params["description"] if "description" in params else None
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.create_task_type_tag_alias(type_id, tag, alias, description)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        return Response()

    @list_route(methods=["post"], url_path="delete_tag_alias")
    @params_valid(serializer=DeleteTaskTypeTagAliasSerializer)
    def delete_tag_alias(self, request, cluster_id, params):
        """
        @api {post} /jobnavi/cluster/:cluster_id/task_type/delete_tag_alias/ 删除任务类型tag定义别名
        @apiName delete_task_type_tag_alias
        @apiGroup task_type
        @apiParam {string} type_id 任务类型ID
        @apiParam {string} tag 任务类型tag
        @apiParamExample {json} 参数样例:
            {
                "type_id": "type-1",
                "tag": "stable",
                "alias": "latest"
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
        type_id = params["type_id"]
        tag = params["tag"]
        alias = params["alias"]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.delete_task_type_tag_alias(type_id, tag, alias)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        return Response()

    @list_route(methods=["get"], url_path="retrieve_default_tag")
    @params_valid(serializer=RetrieveTaskTypeDefaultTagSerializer)
    def retrieve_default_tag(self, request, cluster_id, params):
        """
        @api {get} /jobnavi/cluster/:cluster_id/task_type/retrieve_default_tag/ 查询任务类型默认tag
        @apiName retrieve_task_type_default_tag
        @apiGroup task_type
        @apiParam {string} type_id 任务类型ID
        @apiParam {string} node_label Runner节点标签
        @apiParamExample {json} 参数样例:
            {
                "type_id": "type-1",
                "node_label": "default"
            }
        @apiSuccessExample {json} 成功返回
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200",
                "data": ["stable", "latest"],
                "result": true
            }
        """
        geog_area_code = params["tags"][0]
        type_id = params["type_id"]
        node_label = params["node_label"] if "node_label" in params else None
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.retrieve_task_type_default_tag(type_id, node_label)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        return Response(json.loads(result.data))

    @list_route(methods=["post"], url_path="create_default_tag")
    @params_valid(serializer=TaskTypeDefaultTagSerializer)
    def create_default_tag(self, request, cluster_id, params):
        """
        @api {post} /jobnavi/cluster/:cluster_id/task_type/create_default_tag/ 新增任务类型tag定义
        @apiName create_task_type_default_tag
        @apiGroup task_type
        @apiParam {string} type_id 任务类型ID
        @apiParam {string} [node_label] Runner节点标签
        @apiParam {string} default_tag 任务类型默认tag
        @apiParamExample {json} 参数样例:
            {
                "type_id": "type-1",
                "node_label": "default",
                "default_tag": "latest"
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
        type_id = params["type_id"]
        node_label = params["node_label"] if "node_label" in params else None
        default_tag = params["default_tag"]
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.create_task_type_default_tag(type_id, node_label, default_tag)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        return Response()

    @list_route(methods=["post"], url_path="delete_default_tag")
    @params_valid(serializer=DeleteTaskTypeDefaultTagSerializer)
    def delete_default_tag(self, request, cluster_id, params):
        """
        @api {post} /jobnavi/cluster/:cluster_id/task_type/delete_default_tag/ 删除任务类型tag定义别名
        @apiName delete_task_type_default_tag
        @apiGroup task_type
        @apiParam {string} type_id 任务类型ID
        @apiParam {string} node_label Runner节点标签
        @apiParamExample {json} 参数样例:
            {
                "type_id": "type-1",
                "node_label": "default"
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
        type_id = params["type_id"]
        node_label = params["node_label"] if "node_label" in params else None
        jobnavi = JobNaviApi(geog_area_code, cluster_id)
        result = jobnavi.delete_task_type_default_tag(type_id, node_label)
        if not result or not result.is_success():
            raise InterfaceError(message=result.message)
        return Response()

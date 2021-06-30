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


import datetime
import json

from common.auth import UserPerm, perm_check
from common.auth.objects import is_sys_scopes
from common.auth.perms import ProjectPerm
from common.base_utils import model_to_dict
from common.decorators import params_valid
from common.django_utils import CustomJSONEncoder
from common.local import get_request_username
from common.transaction import auto_meta_sync
from django.conf import settings
from meta import exceptions as meta_errors
from meta.basic.common import RPCViewSet
from meta.public.common import translate_project_name
from meta.public.models import Project, ProjectDel
from meta.public.serializers.common import (
    DestroySerializer,
    ProjectSerializer,
    ProjectUpdateSerializer,
)
from meta.utils.basicapi import parseresult
from meta.utils.common import paged_sql
from rest_framework.decorators import action
from rest_framework.response import Response


class ProjectViewSet(RPCViewSet):
    lookup_field = "project_id"

    @action(detail=False, methods=["get"], url_path="mine")
    def mine(self, request):
        """
        @api {get} /meta/projects/mine 获取项目列表
        @apiVersion 0.2.0
        @apiGroup Project
        @apiName get_project_list_mine

        @apiParam {Number} active 项目是否有效[1:有效;0:无效]
        @apiParam {Number} [page] 页码
        @apiParam {Number} [page_size] 分页大小

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": [
                    {
                        "project_id": 1,
                        "project_name": "test1",
                        "bk_app_code": "data",
                        "description": "dddd",
                        "created_by":"admin",
                        "created_at": "2018-01-01 00:00:00",
                        "updated_by": "lisi",
                        "updated_at": "2018-01-01 00:00:01",
                        "active": 1,
                        "deleted_by": "wangwu",
                        "deleted_at": "2018-01-01 00:00:02",
                        "tags":{"manage":{"geog_area":[{"code":"NA","alias":"北美"}]}}
                    }
                ],
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }
        """
        # 根据需要进行的操作，返回相应的对象列表
        action_id = request.query_params.get("action_id", "project.retrieve")
        scopes = UserPerm(get_request_username()).list_scopes(action_id)

        # 判断是否全局范围，若是，则返回全部对象
        if is_sys_scopes(scopes):
            return self.list(request)

        # 组装全部有权限的 project_id
        project_ids = [s["project_id"] for s in scopes]
        # print ('mine_project_ids:', project_ids)
        # 返回有权限的项目列表
        if project_ids:
            return self.list(request, project_id_list=project_ids)

        return Response([])

    def list(self, request, project_id_list=None):
        """
        @api {get} /meta/projects/ 获取项目列表
        @apiVersion 0.2.0
        @apiGroup Project
        @apiName get_project_list

        @apiParam {Number} [active] 项目是否有效[1:有效;0:无效]
        @apiParam {Number} [page] 页码
        @apiParam {Number} [page_size] 分页大小
        @apiParam {Number[]} [project_ids] 项目ID列表

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": [
                    {
                        "project_id": 1,
                        "project_name": "test1",
                        "bk_app_code": "data",
                        "description": "dddd",
                        "created_by":"admin",
                        "created_at": "2018-01-01 00:00:00",
                        "updated_by": "lisi",
                        "updated_at": "2018-01-01 00:00:01",
                        "active": 1,
                        "deleted_by": "wangwu",
                        "deleted_at": "2018-01-01 00:00:02",
                        "tdw_app_groups":["test","test2"],
                        "tags":{"manage":{"geog_area":[{"code":"NA","alias":"北美"}]}}
                    }
                ],
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }
        """
        active = request.query_params.get("active")
        page = request.query_params.get("page")
        page_size = request.query_params.get("page_size")
        project_ids = request.query_params.getlist("project_ids")

        sql = parseresult.get_project_info_sql()
        if project_id_list:
            sql += " and project_id in("
            for project_id in project_id_list:
                sql += str(project_id) + ","
            sql = sql.rstrip(",") + ") "

        if active:
            sql += " and active=" + parseresult.escape_string(active)
        if project_ids:
            sql += " and project_id in("
            for p_id in project_ids:
                sql += parseresult.escape_string(p_id) + ","
            sql = sql.rstrip(",") + ") "

        rpc_response = self.entity_complex_search(paged_sql(sql, page=page, page_size=page_size))
        ret = parseresult.parse_field_to_boolean(rpc_response.result, "active")
        project_ids = {item["project_id"]: item for item in ret}
        for item in ret:
            translate_project_name(item)

        if settings.ENABLED_TDW and project_ids:
            from meta.extend.tencent.tdw.views.utils_project_mixin import (
                mixin_app_group_name,
            )

            mixin_app_group_name(project_ids)
            # tdw_ret = self.entity_complex_search(
            #     "select app_group_name,project_id from tdw_app_group where project_id in ({})".format(
            #         ",".join(str(item) for item in project_ids)
            #     )
            # ).result
            #
            # tdw_app_groups_by_id = defaultdict(list)
            # for item in tdw_ret:
            #     tdw_app_groups_by_id[item["project_id"]].append(item["app_group_name"])
            # for k, v in tdw_app_groups_by_id.items():
            #     project_ids[k]["tdw_app_groups"] = v

        parseresult.add_manage_tag_to_project(ret)
        return Response(ret)

    @perm_check("project.create", detail=False)
    @params_valid(serializer=ProjectSerializer)
    def create(self, request, params):
        """
        @api {post} /meta/projects/ 新建项目
        @apiVersion 0.2.0
        @apiGroup Project
        @apiName create_project

        @apiParam {String} bk_username 用户名
        @apiParam {String} project_name 项目名称
        @apiParam {String} bk_app_code 项目来源，一般为app_code
        @apiParam {description} description 描述信息
        @apiParam {String} [tdw_app_groups] tdw应用组
        @apiParam {String[]} tags 标签code列表

        @apiParamExample {json} 参数样例:
            {
                "bk_username": "admin",
                "project_name": "测试项目",
                "bk_app_code": "data",
                "description": "test"
                "tdw_app_groups":["test","test2"],
                "tags": ["NA"]
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": 3481,
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }
        """
        username = params.pop("bk_username", get_request_username())
        tdw_app_groups = params.pop("tdw_app_groups", [])
        tags = parseresult.get_tag_params(params)
        with auto_meta_sync(using="bkdata_basic"):
            params["created_by"] = username
            params["active"] = True
            project = Project.objects.create(**params)
            parseresult.create_tag_to_project(tags, project.project_id)
            if settings.ENABLED_TDW:
                from meta.extend.tencent.tdw.views.utils_project_mixin import (
                    bind_app_groups,
                )

                bind_app_groups(request, project, tdw_app_groups)

        ProjectPerm(project.project_id).update_role_users(
            [
                {
                    "role_id": "project.manager",
                    "user_ids": [username],
                }
            ]
        )

        return Response(project.project_id)

    def retrieve(self, request, project_id):
        """
        @api {get} /meta/projects/:project_id/ 获取项目
        @apiVersion 0.2.0
        @apiGroup Project
        @apiName get_project

        @apiParam {String} [show_app_group_detail] 显示tdw应用组详细。

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": {
                    "project_id": 1,
                    "project_name": "test1",
                    "bk_app_code": "data",
                    "description": "dddd",
                    "created_by":"admin",
                    "created_at": "2018-01-01 00:00:00",
                    "updated_by": "lisi",
                    "updated_at": "2018-01-01 00:00:01",
                    "active": 1,
                    "deleted_by": "wangwu",
                    "deleted_at": "2018-01-01 00:00:02",
                    "tdw_app_groups":["test","test2"],
                    "tags":{"manage":{"geog_area":[{"code":"NA","alias":"北美"}]}}
                },
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }
        """
        show_app_group_detail = request.query_params.get("show_app_group_detail", False)
        sql = parseresult.get_project_info_sql()
        sql += " and project_id=" + parseresult.escape_string(project_id)
        rpc_response = self.entity_complex_search(sql)
        query_result = parseresult.parse_field_to_boolean(rpc_response.result, "active")
        if query_result:
            retrieve_project = query_result[0]
            if settings.ENABLED_TDW:
                project_items = {project_id: retrieve_project}
                from meta.extend.tencent.tdw.views.utils_project_mixin import (
                    mixin_app_group_name,
                )

                mixin_app_group_name(project_items, show_app_group_detail, request)
                # tdw_ret = self.entity_complex_search(
                #     "select app_group_name from tdw_app_group where project_id={}".format(project_id)
                # ).result
                # query_result[0]["tdw_app_groups"] = [item["app_group_name"] for item in tdw_ret]
                # query_result[0]["tdw_app_groups_detail"] = {}
                # if show_app_group_detail:
                #     for app_group_name in query_result[0]["tdw_app_groups"]:
                #         request_proxy = ObjectProxy(request)
                #         request_proxy.GET = {}
                #         ret = TdwAppGroupViewSet().retrieve(request=request_proxy, group_name=app_group_name)
                #         query_result[0]["tdw_app_groups_detail"] = ret.data
            parseresult.add_manage_tag_to_project(retrieve_project)
            translate_project_name(retrieve_project)
            return Response(retrieve_project)
        return Response({})

    @perm_check("project.update")
    @params_valid(serializer=ProjectUpdateSerializer)
    def update(self, request, project_id, params):
        """
        @api {put} /meta/projects/:project_id/ 修改项目信息
        @apiVersion 0.2.0
        @apiGroup Project
        @apiName update_project

        @apiParam {String} bk_username 用户名
        @apiParam {String} [project_name] 项目名称
        @apiParam {String} [bk_app_code] 项目来源，一般为app_code
        @apiParam {String} [description] 描述信息

        @apiParamExample {json} 参数样例:
            {
                "bk_username": "admin",
                "project_name": "测试项目",
                "bk_app_code": "data",
                "description": "test",
                "tdw_app_groups":["test3","test4"]
            }

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": 3481,
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }
        """
        tdw_app_groups = params.pop("tdw_app_groups", None)
        try:
            project = Project.objects.get(project_id=project_id)
        except Project.DoesNotExist:
            raise meta_errors.ProjectNotExistError(message_kv={"project_id": project_id})

        with auto_meta_sync(using="bkdata_basic"):
            params["updated_by"] = params.pop("bk_username", get_request_username())
            for key, value in list(params.items()):
                setattr(project, key, value)
            project.save()

            if settings.ENABLED_TDW and tdw_app_groups:
                from meta.extend.tencent.tdw.views.utils_project_mixin import (
                    rebind_app_groups,
                )

                rebind_app_groups(project, tdw_app_groups)
                # app_groups_now = {
                #     t.app_group_name
                #     for t in TdwAppGroup.objects.filter(
                #         project_id=project.project_id,
                #     ).all()
                # }
                # to_del = app_groups_now - set(tdw_app_groups)
                # to_add = set(tdw_app_groups) - app_groups_now
                # for app_group in to_add:
                #     TdwAppGroup.objects.create(project_id=project.project_id, app_group_name=app_group)
                # for app_group in to_del:
                #     TdwAppGroup.objects.filter(project_id=project.project_id, app_group_name=app_group)[0].delete()

        return Response(project_id)

    @perm_check("project.delete")
    @params_valid(serializer=DestroySerializer)
    def destroy(self, request, project_id, params):
        """
        @api {delete} /meta/projects/:project_id/ 删除项目
        @apiVersion 0.2.0
        @apiGroup Project
        @apiName delete_project

        @apiParam {String} bk_username 用户名

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": 3481,
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }

        @apiError 1521060 项目不存在
        """

        try:
            project = Project.objects.get(project_id=project_id)
        except Project.DoesNotExist:
            raise meta_errors.ProjectNotExistError(message_kv={"project_id": project_id})

        with auto_meta_sync(using="bkdata_basic"):
            ProjectDel.objects.create(
                project_id=project.project_id,
                project_name=project.project_name,
                project_content=json.dumps(model_to_dict(project), cls=CustomJSONEncoder),
                status="deleted",
                deleted_by=get_request_username(),
            )
            project.delete()
            if settings.ENABLED_TDW:
                from meta.extend.tencent.tdw.views.utils_project_mixin import (
                    unbind_app_groups,
                )

                unbind_app_groups(project)
                # items = TdwAppGroup.objects.filter(project_id=project_id)
                # for item in items:
                #     item.delete()

        return Response(project_id)

    @perm_check("project.manage")
    @action(detail=True, methods=["put", "patch"], url_path="disabled")
    @params_valid(serializer=DestroySerializer)
    def disabled(self, request, project_id, params):
        """
        @api {put/patch} /meta/projects/:project_id/disabled/ 软删除项目
        @apiVersion 0.2.0
        @apiGroup Project
        @apiName disabled_project

        @apiParam {String} bk_username 用户名

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": 3481,
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }

        @apiError 1521060 项目不存在
        """
        try:
            project = Project.objects.get(project_id=project_id)
        except Project.DoesNotExist:
            raise meta_errors.ProjectNotExistError(message_kv={"project_id": project_id})

        with auto_meta_sync(using="bkdata_basic"):
            project.active = False
            project.deleted_by = get_request_username()
            project.deleted_at = datetime.datetime.now()
            project.save()

        return Response(project_id)

    @perm_check("project.manage")
    @action(detail=True, methods=["put", "patch"], url_path="enabled")
    @params_valid(serializer=DestroySerializer)
    def enabled(self, request, project_id, params):
        """
        @api {put/patch} /meta/projects/:project_id/enabled/ 恢复项目
        @apiVersion 0.2.0
        @apiGroup Project
        @apiName enabled_project

        @apiParam {String} bk_username 用户名

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "data": 3481,
                "result": true,
                "message": "",
                "code": 1500200,
                "errors": null
            }

        @apiError 1521060 项目不存在
        """
        try:
            project = Project.objects.get(project_id=project_id)
        except Project.DoesNotExist:
            raise meta_errors.ProjectNotExistError(message_kv={"project_id": project_id})

        with auto_meta_sync(using="bkdata_basic"):
            project.active = True
            project.updated_by = get_request_username()
            project.updated_at = datetime.datetime.now()
            project.save()

        return Response(project_id)

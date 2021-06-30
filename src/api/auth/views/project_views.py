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
from auth.config.ticket import PROJECT_BIZ, RESOURCE_GROUP_USE
from auth.constants import FAILED, PROCESSING, SUCCEEDED, SubjectTypeChoices
from auth.core.ticket import TicketFactory
from auth.handlers.object_classes import ObjectFactory
from auth.handlers.project import ProjectHandler
from auth.handlers.role import RoleHandler
from auth.models.base_models import RoleConfig
from auth.services.role import RoleService
from auth.utils.base import local_dgraph_data
from auth.views.auth_serializers import (
    ProjectAddDataSerializer,
    ProjectClusterGroupCheckSerializer,
    ProjectDataCheckSerializer,
    ProjectDataSerializer,
    ProjectDataTicketSerializer,
    ProjectUsersUpdateSerializer,
)
from common.business import Business
from common.decorators import detail_route, params_valid
from common.local import get_request_username
from common.log import logger
from common.views import APIViewSet
from rest_framework.response import Response


class ProjectViewSet(APIViewSet):
    lookup_field = "project_id"

    @detail_route(methods=["GET", "PUT"])
    def role_users(self, request, project_id):
        if request.method == "GET":
            return self._get_role_users(request, project_id)

        if request.method == "PUT":
            return self._update_role_users(request, project_id)

    def _get_role_users(self, request, project_id):
        """
        @api {get} /auth/projects/:project_id/role_users/ 列举项目相关用户
        @apiName list_project_role_user
        @apiGroup ProjectUser
        @apiSuccessExample {json} Success-Response.data
            [
                {
                    'role_id': 'project.manager',
                    'role_name': '项目管理员',
                    'user_ids': ['user00', 'user01']
                }
            ]
        """
        roles = RoleConfig.objects.filter(object_class="project")
        role_users = [
            {
                "role_id": _role.role_id,
                "role_name": _role.role_name,
                "users": RoleHandler.list_users(_role.role_id, scope_id=project_id),
            }
            for _role in roles
        ]
        return Response(role_users)

    @params_valid(ProjectUsersUpdateSerializer, add_params=False)
    def _update_role_users(self, request, project_id):
        """
        @api {put} /auth/projects/:project_id/role_users/ 更新项目相关用户
        @apiName update_project_role_user
        @apiGroup ProjectUser
        @apiParam {object[]} role_users 用户列表
        @apiParamExample {json} role_users 参数例子
            {
                role_users: [
                    {
                        'role_id': 'project.manager',
                        'user_ids': ['user00', 'user01']
                    },
                    {
                        'role_id': 'project.flow_member',
                        'user_ids': ['user00', 'user01']
                    }
                ]
            }
        @apiSuccessExample {json} Success-Response.data
            'ok'
        """
        role_users = request.cleaned_params["role_users"]

        username = get_request_username()
        for _users in role_users:
            _role_id = _users["role_id"]
            _user_ids = _users["user_ids"]
            RoleService.update_roles(username, _role_id, _user_ids, project_id, force_no_ticket=True)

        return Response("ok")

    @detail_route(methods=["GET"])
    @params_valid(ProjectDataSerializer, add_params=False)
    def data(self, request, project_id):
        """
        @api {get} /auth/projects/:project_id/data/ 列举项目相关数据
        @apiName list_project_data
        @apiGroup ProjectData
        @apiParam {int} [bk_biz_id] 业务属性
        @apiParam {bool} [with_queryset] 是否返回 QuerySet 类型结果表，默认 False
        @apiParam {string} [action_id] 动作方式，默认 result_table.query_data 查询结果数据，可选 raw_data.query_data 查询数据源
        @apiParam {bool} [extra_fields] 是否返回额外字段，默认 False
        @apiSuccessExample {json} Success-Response.data 当 extra_fields=False
            [
                {
                    'bk_biz_id': 111,
                    'result_table_id': '111_xx'
                }
            ]
        @apiSuccessExample {json} Success-Response.data 当 extra_fields=True
            [
                {
                    'bk_biz_id': 111,
                    'result_table_id': '111_xx',
                    'result_table_name': 'xxx',
                    'description': 'ddddd',
                    'created_at': '',
                    'created_by': '',
                    'updated_at': '',
                    'updated_by': ''
                }
            ]
        """
        bk_biz_id = request.cleaned_params.get("bk_biz_id")
        page = request.cleaned_params.get("page")
        with_queryset = request.cleaned_params.get("with_queryset")
        action_id = request.cleaned_params.get("action_id")
        extra_fields = request.cleaned_params.get("extra_fields")

        data = ProjectHandler(project_id=project_id).get_data(
            bk_biz_id=bk_biz_id, with_queryset=with_queryset, action_id=action_id, extra_fields=extra_fields
        )

        # 来自 dgraph 的时间字段需要处理
        if extra_fields:
            for d in data:
                local_dgraph_data(d, ("created_at", "updated_at"))

        if page:
            return Response({"has_next": False, "acc_tdw": False, "data": data})

        return Response(data)

    @detail_route(methods=["GET"])
    def bizs(self, request, project_id):
        """
        @api {get} /auth/projects/:project_id/bizs/ 列举项目相关的业务
        @apiName list_project_bizs
        @apiGroup ProjectData
        @apiSuccessExample {json} Success-Response.data
        [
            {
                'bk_biz_id': 222,
                'bk_biz_name': 'xxx'
            }
        ]
        """
        bizs = ProjectHandler(project_id=project_id).get_bizs()
        return Response(Business.wrap_biz_name(bizs))

    @detail_route(methods=["POST"], url_path="data/add")
    @params_valid(ProjectAddDataSerializer, add_params=False)
    def add_data(self, request, project_id):
        """
        @api {post} /v3/auth/projects/:project_id/data/add/  添加项目数据
        @apiName add_project_data
        @apiGroup ProjectData
        @apiParam {int} bk_biz_id 业务ID
        @apiParam {string} object_id 对象ID，可选 result_table_id, raw_data_id
        @apiParam {string} [action_id] 动作方式，默认 result_table.query_data 查询结果数据，可选 raw_data.query_data 查询数据源
        @apiSuccessExample {json} Success-Response.data
            'ok'
        """
        bk_biz_id = request.cleaned_params["bk_biz_id"]
        object_id = request.cleaned_params["object_id"]
        action_id = request.cleaned_params["action_id"]

        inst = ObjectFactory.init_instance(action_id, object_id)

        scope = {"bk_biz_id": bk_biz_id, inst.input_val_key: inst.object_id}
        ProjectHandler(project_id=project_id).add_data(scope, action_id=action_id)
        logger.warning(
            "[Extra Auth] Add project data from inner call: "
            "project_id={}, bk_biz_id={}, object_id={}".format(project_id, bk_biz_id, object_id)
        )
        return Response("ok")

    @detail_route(methods=["get"])
    @params_valid(ProjectDataTicketSerializer, add_params=False)
    def data_tickets(self, request, project_id):
        """
        @api {get} /v3/auth/projects/:project_id/data_tickets/ 项目已申请的业务数据单据信息
        @apiName list_project_data_ticket
        @apiGroup ProjectData
        @apiParam {int} bk_biz_id 业务ID
        @apiParam {string} [action_ids]
            功能列表，默认 result_table.query_data 可多选 'result_table.query_data,raw_data.query_data'
        @apiSuccessExample {json} 查询结果样例
        {
            "591_test_rt": {
              "status": "processing",
              "created_at": "2019-04-03 16:22:27",
              "ticket_id": 44,
              "state_id": 1,
            },
            "593_test_rt": {
              "status": "failed",
              "created_at": "2019-04-03 16:42:31",
              "ticket_id": 46,
              "state_id": 1,
            },
            "592_test_rt": {
              "status": "succeeded",
              "created_at": "2019-04-03 16:22:50",
              "ticket_id": 45,
              "state_id": 1,
            }
        }
        """
        action_ids = request.cleaned_params["action_ids"]
        bk_biz_id = request.cleaned_params["bk_biz_id"]

        action_id_arr = action_ids.split(",")

        data = {}
        for action_id in action_id_arr:
            data[action_id] = TicketFactory(PROJECT_BIZ).ticket_obj.distinct_data(
                project_id=project_id, bk_biz_id=bk_biz_id, action_id=action_id
            )

        if len(data) > 1:
            return Response(data)
        else:
            return Response(list(data.values())[0])

    @detail_route(methods=["get"])
    @params_valid(ProjectDataTicketSerializer, add_params=False)
    def biz_tickets_count(self, request, project_id):
        """
        @api {get} /v3/auth/projects/:project_id/biz_tickets_count/ 项目申请的业务数据统计（按业务维度）
        @apiName get_project_biz_tickets_count
        @apiGroup ProjectData
        @apiParam {string} [action_ids]
            功能列表，默认 result_table.query_data，可多选 'result_table.query_data,raw_data.query_data'  # noqa
        @apiSuccessExample {json} 查询结果样例
        [
            {
              bk_biz_id: "591",
              count: {
                failed: 0,
                processing: 1,
                succeeded: 0
              },
              bk_biz_name: "蓝鲸基础计算平台",
              description: "枫叶对内版本的配置管理"
            }
        ]
        """
        action_ids = request.cleaned_params["action_ids"]
        action_id_arr = action_ids.split(",")

        tickets = []
        for action_id in action_id_arr:
            ticket_obj = TicketFactory(PROJECT_BIZ).ticket_obj
            project_tickets = ticket_obj.distinct_data(project_id, action_id=action_id)
            tickets.extend(list(project_tickets.values()))

        d_count = self.ticket_count_group_by_biz(tickets)

        biz_list = Business.list()
        for biz in biz_list:
            int_bk_biz_id = int(biz["bk_biz_id"])
            biz["int_bk_biz_id"] = int_bk_biz_id
            biz["count"] = d_count.get(int_bk_biz_id, {})

        # 排序，以总数为优先，成功申请的为次优先
        biz_list = sorted(
            biz_list,
            key=lambda _biz: (
                -_biz["count"].get("total", 0),
                -_biz["count"].get(SUCCEEDED, 0),
                _biz["int_bk_biz_id"],
            ),
        )

        return Response(biz_list)

    @staticmethod
    def ticket_count_group_by_biz(data):
        """
        根据项目申请的数据， 其中 result_table_id 由 {bk_biz_id}_{table_name} 组成
        @param data: 见类方法 distinct_data() 的返回
        @return:
        {
            "591": {
                "total":1,
                "failed": 0,
                "processing":0,
                "succeeded":1
            }
        }
        """
        counts = {}
        count_status = [PROCESSING, SUCCEEDED, FAILED]
        for _data in data:

            biz_id = _data.get("bk_biz_id")
            status = _data.get("status")

            if status not in count_status:
                continue

            if biz_id not in counts:
                counts[biz_id] = {_status: 0 for _status in count_status}
                counts[biz_id]["total"] = 0

            counts[biz_id][status] += 1
            counts[biz_id]["total"] += 1

        return counts

    @detail_route(methods=["POST"], url_path="data/check")
    @params_valid(ProjectDataCheckSerializer, add_params=False)
    def data_check(self, request, project_id):
        """
        @api {post} /auth/projects/:project_id/data/check/ 检查项目是否有结果表权限
        @apiName check_project_data
        @apiGroup ProjectData
        @apiParam {string} object_id 鉴权对象id，比如 result_table_id, raw_data_id
        @apiParam {string} [action_id] 动作方式，默认 result_table.query_data 查询结果数据，可选 raw_data.query_data 查询数据源
        @apiSuccessExample {json} Sucees-Response.data
            True | False
        """
        object_id = request.cleaned_params["object_id"]
        action_id = request.cleaned_params["action_id"]

        ret = ProjectHandler(project_id=project_id).check_data_perm(object_id, action_id=action_id)
        return Response(ret)

    @detail_route(methods=["get"])
    def resource_group_tickets(self, request, project_id):
        """
        @api {get} /v3/auth/projects/:project_id/resource_group_tickets/ 项目已申请的资源组单据信息
        @apiName list_project_resource_group_ticket
        @apiGroup ProjectClusterGroup
        @apiSuccessExample {json} 查询结果样例
        {
            "tgqa": {
              "status": "processing",
              "created_at": "2019-04-03 16:22:27",
              "ticket_id": 44,
              "state_id": 1,
            },
            "bkdata_test": {
              "status": "failed",
              "created_at": "2019-04-03 16:42:31",
              "ticket_id": 46,
              "state_id": 1,
            }
        }
        """
        ticket_controller = TicketFactory(RESOURCE_GROUP_USE).ticket_obj
        summaries = ticket_controller.list_authoriazed_tickets(
            subject_class=SubjectTypeChoices.PROJECT, subject_id=project_id
        )

        return Response(summaries)

    @detail_route(methods=["GET"])
    def cluster_group(self, request, project_id):
        """
        @api {get} /auth/projects/:project_id/cluster_group/ 列举项目有权限的集群组
        @apiName list_project_cluster_group
        @apiGroup ProjectClusterGroup
        @apiSuccessExample {json} Success-Response.data
            [
                {
                    "resource_group_id": "mysql_lol_1",
                    "group_name": "mysql_lol_1",
                    "group_type": "private",

                    "cluster_group_id": "mysql_lol_1",
                    "cluster_group_name": "mysql_lol_1",
                    "cluster_group_alias": "mysql_lol_1",
                    "scope": "private",
                    "created_by": "asdf",
                    "created_at": "2018-11-27 17:21:20",
                    "updated_by": "",
                    "updated_at": null,
                    "description": null
                },
                {
                    "resource_group_id": "mysql_lol_2",
                    "group_name": "mysql_lol_2",
                    "group_type": "public",

                    "cluster_group_id": "mysql_lol_2",
                    "cluster_group_name": "mysql_lol_2",
                    "cluster_group_alias": "mysql_lol_2",
                    "scope": "public",
                    "created_by": "asdf",
                    "created_at": "2018-11-27 17:33:35",
                    "updated_by": null,
                    "updated_at": null,
                    "description": null
                }
            ]
        """
        return Response(ProjectHandler(project_id).list_cluster_group())

    @detail_route(methods=["POST"], url_path="cluster_group/check")
    @params_valid(ProjectClusterGroupCheckSerializer, add_params=False)
    def cluster_group_check(self, request, project_id):
        """
        @api {post} /auth/projects/:project_id/cluster_group/check/ 检查项目是否有集群组权限
        @apiName check_project_cluster_group
        @apiGroup ProjectClusterGroup
        @apiParam {string} cluster_group_id 集群组ID
        @apiSuccessExample {json} Sucees-Response.data
            True | False
        """
        cluster_group_id = request.cleaned_params["cluster_group_id"]
        ret = ProjectHandler(project_id).check_cluster_group(cluster_group_id)
        return Response(ret)

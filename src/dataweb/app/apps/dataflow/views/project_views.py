# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""


from rest_framework import serializers
from rest_framework.response import Response

from apps import forms as data_forms
from apps.api import BKLoginApi, MetaApi
from apps.common.views import detail_route, list_route
from apps.dataflow.handlers.business import Business
from apps.dataflow.handlers.project import Project
from apps.dataflow.handlers.result_table import ResultTable
from apps.dataflow.models import ResultTableQueryRecord
from apps.dataflow.permissions import ProjectPermissions
from apps.generic import APIViewSet
from apps.utils.time_handler import SelfDRFDateTimeField


class ProjectViewSet(APIViewSet):
    class QueryHistorySerializer(serializers.ModelSerializer):
        time = SelfDRFDateTimeField(required=False)

        class Meta:
            model = ResultTableQueryRecord

    lookup_value_regex = r"\d+"
    lookup_field = "project_id"
    permission_classes = (ProjectPermissions,)

    @detail_route(methods=["get"], url_path="check")
    def check(self, request, project_id=None):
        return Response(True)

    @detail_route(methods=["get"])
    def list_rt_as_source(self, request, project_id=None):
        """
        @api {get} /projects/:project_id/list_rt_as_source/ 获取用户有权限的项目
        @apiName list_rt_as_source
        @apiParam {Int} source_type 数据源类型 rtsource etl_source stream_source batch_source kv_source
        @apiGroup ProjectSet
        @apiSuccessExample {json} 成功返回
        []
        """
        source_type = request.query_params.get("source_type")
        bk_biz_id = request.query_params.get("bk_biz_id")
        rts = ResultTable.list_as_source_by_project_id(
            project_id=project_id, source_type=source_type, bk_biz_id=bk_biz_id
        )
        # 补充业务名称
        _biz_name_dict = {}
        for _rt in rts:
            if not _biz_name_dict:
                _biz_name_dict = Business.get_name_dict()
            _biz_id = _rt["bk_biz_id"]
            _rt["bk_biz_name"] = _biz_name_dict.get(_biz_id, _biz_id)

        return Response(rts)

    @list_route(methods=["get"])
    def list_my_projects(self, request):
        """
        @api {get} /projects/list_my_projects/ 获取用户有权限的项目
        @apiName list_my_projects
        @apiParam {Int} [page] 分页页码
        @apiParam {Int} [page_size] 每一页显示的数量
        @apiParam {Int} [add_flow_info] 显示关联 DataFlow 数量，1表示开启，0表示不开启
        @apiParam {Int} [add_model_info] 显示关联 ModelFlow 数量，1表示开启，0表示不开启
        @apiParam {Int} [add_note_info] 显示关联 DataLab 数量，1表示开启，0表示不开启
        @apiParam {String} [search] 项目信息模糊查询，支持项目ID、项目名称、创建人
        @apiParam {String} [disabled] 支持删除状态过滤，1表示显示删除项目，0表示显示正常项目
        @apiParam {String} [role] 当前用户在项目中的角色，只是角色过滤，支持 admin、member
        @apiGroup ProjectSet
        @apiSuccessExample {json} 成功返回:
        {
            "avatar_url": "http://xxx.com/avators/admin.png",
            "username": "admin",
            "projects": [
                {
                    "project_id": 32,
                    "is_deleted": false,
                    "project_name": "1111111111",
                    "biz_id": null,
                    "is_public": 0,
                    "description": "1111111111",
                    "created_by": "admin",
                    "created_at": "2017-10-18T15:27:34",
                    "updated_by": "admin",
                    "updated_at": "2017-10-18T15:27:34",
                    "deleted_by": null,
                    "deleted_at": null,
                    "delete_time": null,
                    "is_valid": 1,
                    "is_open": "N",
                    "public_id": null,
                    "status": 0,
                    "tag_name": "common",
                    "app_code": "data",

                    # 当设置 add_biz_info 返回该字段
                    "biz_count": 0,

                    # 当设置 add_flow_info 返回该字段
                    "flow_count": 3,
                    "running_count": 2,
                    "no_start_count": 1,
                    "normal_count": 1,
                    "exception_count": 1,

                    # 当设置 add_del_info 返回该字段
                    "deleting": true,
                    "delete_at": "2017-11-19 21:29:28",

                    # 当设置 add_model_info 返回该字段
                    "modelflow_counts": {
                        "building": {
                            "count": 0,
                            "status_display": "构建中"
                        },
                        "project_id": 3481,
                        "complete": {
                            "count": 0,
                            "status_display": "已完成"
                        },
                        "project_url": 'xxxx'
                    },

                    # 当设置 add_note_info 返回该字段
                    "query_count": 0,
                    "notebook_count": 0

                }
            ],
            "count": 1,
        }
        """
        add_flow_info = int(request.query_params.get("add_flow_info", 0))
        add_note_info = int(request.query_params.get("add_note_info", 0))

        # todo 暂时注释掉模型获取逻辑，后续加上需要优化这段获取逻辑
        # add_model_info = bool(request.query_params.get('add_model_info', False))
        role = request.query_params.get("role", "member")

        action_id = "project.manage" if role == "admin" else "project.retrieve"

        username = request.user.username
        projects = MetaApi.projects.mine({"active": int(request.query_params.get("active", 1)), "action_id": action_id})

        # 给项目补充Flow数据
        if add_flow_info and len(projects) > 0:
            Project.wrap_flow_count(projects)

        # 给项目补充 ModelFlow 数据
        # if add_model_info and len(projects) > 0:
        #     _project_ids = json.dumps([_p['project_id'] for _p in projects])
        #     try:
        #         _model_count = AlgoApi.get_model_stats({'project_id_list': _project_ids})
        #         _m_model_count = {int(_c['project_id']): _c for _c in _model_count}

        #         # note 这里依赖挖掘返回所有要查询项目模型
        #         for _p in projects:
        #             if _p['project_id'] in _m_model_count:
        #                 _p['modelflow_counts'] = _m_model_count[_p['project_id']]
        #             else:
        #                 _p['modelflow_counts'] = None
        #     except exceptions.DataError as err:
        #         logger.exception(u"ModelFlow MODEL_STATS_API Exception: {}".format(err))
        #         for _p in projects:
        #             _p['modelflow_counts'] = None

        projects = data_forms.SortForm(request.query_params).sort(projects, "project_id")

        # 项目分页
        page_projects = self.do_paging(request, projects)
        count = page_projects["count"]
        projects = page_projects["results"]

        # 添加DataLab的查询数和笔记数
        if add_note_info and len(projects) > 0:
            Project.wrap_lab_count(projects)

        data = {"username": username, "projects": projects, "count": count}
        return Response(data)

    @list_route(methods=["get"], url_path="list_all_user")
    def list_all_user(self, request):
        users = BKLoginApi.get_all_user()
        return Response([user["username"] for user in users])

    @list_route(methods=["get"], url_path="all_projects")
    def all_projects(self, request):

        res_tmp = MetaApi.project_list.list({})
        res = []
        for each_project in res_tmp:
            res.append(
                {
                    "project_id": each_project.get("project_id"),
                    "project_name": each_project.get("project_name", ""),
                }
            )

        return Response(res)

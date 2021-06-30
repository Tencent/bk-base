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

from common.base_utils import model_to_dict
from common.decorators import list_route
from common.local import get_request_username
from common.log import sys_logger
from common.views import APIViewSet
from datalab.constants import (
    BK_USERNAME,
    COMMON,
    COUNT,
    GET,
    MINE,
    PERSONAL,
    PROJECT_ID,
    PROJECT_TYPE,
)
from datalab.notebooks.models import DatalabNotebookTaskModel
from datalab.projects.models import DatalabProjectModel
from datalab.projects.serializers import ProjectModelSerializer
from datalab.queries.models import DatalabQueryTaskModel
from datalab.utils import check_required_params
from django.db.models import Count
from django.utils.translation import ugettext as _
from rest_framework.response import Response


class DatalabProjectViewSet(APIViewSet):

    serializer_class = ProjectModelSerializer

    @list_route(methods=[GET], url_path=MINE)
    @check_required_params([BK_USERNAME])
    def mine(self, request):
        """
        @api {get} /v3/datalab/projects/mine/ 获取指定用户的个人探索项目
        @apiName user_personal_project
        @apiGroup projects
        @apiDescription 获取指定用户的个人探索项目

        @apiSuccess {int} project_id 项目Id
        @apiSuccess {string} project_name 项目名称
        @apiSuccess {string} project_type 项目类型
        @apiSuccess {string} bind_to 绑定的用户
        @apiSuccess {int} active 记录是否有效 0：失效，1：有效

        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "project_id": 11,
                    "project_name": "xxx的探索",
                    "project_type": "personal",
                    "active": 1,
                    "created_at": "2019-10-28 00:00:00",
                    "created_by": "xxx",
                    "updated_at": "2019-10-30 00:00:00",
                    "updated_by": "yyy",
                    "description": "ddd"
                },
                {
                    "project_id": 26,
                    "project_name": "aaaaa",
                    "project_type": "personal",
                    "active": 1,
                    "created_at": "2019-10-27 00:00:00",
                    "created_by": "xxx",
                    "updated_at": "2019-10-28 00:00:00",
                    "updated_by": "yyy",
                    "description": "ddd"
                }
            ],
            "result": true
        }
        """
        bk_username = get_request_username()
        # 获取个人默认项目
        project_objs = list(DatalabProjectModel.objects.filter(bind_to=bk_username).values())
        if project_objs:
            return Response(project_objs)
        else:
            sys_logger.info("create personal project by bk_username: %s" % bk_username)
            # 新建个人项目
            project_obj = DatalabProjectModel.objects.create(
                project_name=_("%s的探索") % bk_username,
                project_type=PERSONAL,
                bind_to=bk_username,
                created_by=bk_username,
            )
            result = model_to_dict(project_obj)
            return Response([result])

    def list(self, request):
        """
        @api {get} /v3/datalab/projects/ 获取平台所有探索项目列表(内部用，不注册api-gw)
        @apiName project_list
        @apiGroup projects
        @apiDescription 获取平台所有探索项目列表

        @apiSuccess {int} project_id 项目Id
        @apiSuccess {string} project_name 项目名称
        @apiSuccess {int} active 记录是否有效 0：失效，1：有效

        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "project_id": 16,
                    "project_name": "aaaaa",
                    "active": 1,
                    "created_at": "2019-10-28 00:00:00",
                    "created_by": "xxx",
                    "updated_at": "2019-10-30 00:00:00",
                    "updated_by": "yyy",
                    "description": "ddd"
                },
                {
                    "project_id": 26,
                    "project_name": "bbbbb",
                    "active": 1,
                    "created_at": "2019-10-27 00:00:00",
                    "created_by": "xxx",
                    "updated_at": "2019-10-28 00:00:00",
                    "updated_by": "yyy",
                    "description": "ddd"
                }
            ],
            "result": true
        }
        """
        return Response(list(DatalabProjectModel.objects.all().values()))

    @list_route(methods=[GET], url_path=COUNT)
    def count(self, request):
        """
        @api {get} /v3/datalab/projects/count?project_id=1&project_id=2 获取指定项目查询和笔记的数量，支持批量
        @apiName project_count
        @apiGroup projects
        @apiDescription 获取指定项目查询和笔记的数量，支持批量

        @apiSuccess {int} project_id 项目Id

        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "project_id": 1,
                    "query_count": 11,
                    "notebook_count": 3
                },
                {
                    "project_id": 2,
                    "query_count": 8,
                    "notebook_count": 6
                }
            ],
            "result": true
        }
        """
        query_dict = request.query_params
        ids = query_dict.getlist(PROJECT_ID)
        kwargs = {PROJECT_TYPE: COMMON}
        if ids:
            kwargs["project_id__in"] = ids
        else:
            # 如果传参没有project_id，默认返回所有公共项目的count值
            ids = (
                DatalabQueryTaskModel.objects.filter(project_type=COMMON)
                .values(PROJECT_ID)
                .distinct()
                .order_by(PROJECT_ID)
            )
        # 对应SELECT project_id, count(project_id) AS query_count FROM table WHERE xx GROUP BY project_id
        queries_set = (
            DatalabQueryTaskModel.objects.filter(**kwargs)
            .values(PROJECT_ID)
            .annotate(query_count=Count(PROJECT_ID))
            .order_by(PROJECT_ID)
        )
        notebooks_set = (
            DatalabNotebookTaskModel.objects.filter(**kwargs)
            .values(PROJECT_ID)
            .annotate(notebook_count=Count(PROJECT_ID))
            .order_by(PROJECT_ID)
        )
        ids_count = []
        for _id in ids:
            if isinstance(_id, dict):
                _id = _id[PROJECT_ID]
            _id_count = {PROJECT_ID: _id}
            query_set = queries_set.filter(project_id=_id)
            # 指定项目下不一定存在查询或笔记，需要做判断
            _id_count["query_count"] = query_set[0]["query_count"] if query_set else 0
            notebook_set = notebooks_set.filter(project_id=_id)
            _id_count["notebook_count"] = notebook_set[0]["notebook_count"] if notebook_set else 0
            ids_count.append(_id_count)

        return Response(ids_count)

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
    CANCEL,
    DELETE,
    PERSONAL,
    PROJECT_ID,
    PROJECT_TYPE,
    RESULT_TABLE_ID,
)
from datalab.favorites.models import DatalabFavoriteModel
from datalab.favorites.serializers import FavoriteModelSerializer
from datalab.utils import check_required_params
from django.utils.translation import ugettext as _
from rest_framework.response import Response


class DatalabFavoriteViewSet(APIViewSet):

    # 查询项目的id，用于唯一确定一个查询项目
    lookup_field = PROJECT_ID
    serializer_class = FavoriteModelSerializer

    @check_required_params([PROJECT_ID])
    def create(self, request):
        """
        @api {post} /v3/datalab/favorites/ 新增置顶表
        @apiName create_favorite
        @apiGroup favorites
        @apiDescription 新增置顶表
        @apiParam {int} project_id 项目id
        @apiParam {string} project_type 项目类型，common或者personal
        @apiParam {string} result_table_id 置顶结果表id

        @apiParamExample {json} 参数样例:
        {
            "project_id": 11,
            "project_type": "personal",
            "result_table_id": "591_xxx"
        }

        @apiSuccess {int} id 数据表主键id
        @apiSuccess {int} project_id 所属项目id
        @apiSuccess {string} project_type 所属项目类型，common或者personal
        @apiSuccess {string} result_table_id 置顶结果表id
        @apiSuccess {int} active 记录是否有效 0：失效，1：有效

        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "id": 1
                "result_table_id": "591_xxx",
                "project_id": 11,
                "project_type": "personal",
                "active": 1,
                "created_at": "2019-10-27 00:00:00",
                "created_by": "xxx",
                "updated_at": "",
                "updated_by": "",
                "description": "ddd"
            },
            "result": true
        }
        """
        params = self.request.data
        # 如果传参project_type没有指定，默认为个人项目
        project_type = params.get(PROJECT_TYPE, PERSONAL)
        # 判断置顶表在指定项目中是否已经存在
        is_exists = DatalabFavoriteModel.objects.filter(
            project_id=params[PROJECT_ID], project_type=project_type, result_table_id=params[RESULT_TABLE_ID]
        ).exists()
        if is_exists:
            sys_logger.warning(
                "datalab favorite is already exists|project_id: %s|project_type: %s|result_table_id %s!"
                % (params[PROJECT_ID], project_type, params[RESULT_TABLE_ID])
            )
            return Response()
        else:
            # 新增置顶表
            favorite_obj = DatalabFavoriteModel.objects.create(
                project_id=params[PROJECT_ID],
                project_type=project_type,
                result_table_id=params[RESULT_TABLE_ID],
                created_by=get_request_username(),
            )
            # 获取置顶表信息
            obj = DatalabFavoriteModel.objects.get(id=favorite_obj.id)
            result = model_to_dict(obj)
            return Response(result)

    @list_route(methods=[DELETE], url_path=CANCEL)
    @check_required_params([PROJECT_ID, RESULT_TABLE_ID])
    def cancel(self, request):
        """
        @api {delete} /v3/datalab/favorites/cancel/ 取消置顶表
        @apiName cancel_query_favorite
        @apiGroup favorites
        @apiDescription 取消置顶表
        @apiParam {int} project_id 所属项目id
        @apiParam {string} project_type 项目类型，common或者personal
        @apiParam {string} result_table_id 置顶结果表id

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "errors": {},
            "message": "ok",
            "code": "1500200",
            "result": true,
            "data": true
        }
        """
        params = self.request.data
        project_type = params.get(PROJECT_TYPE, PERSONAL)
        try:
            obj = DatalabFavoriteModel.objects.get(
                project_id=params[PROJECT_ID], project_type=project_type, result_table_id=params[RESULT_TABLE_ID]
            )
            obj.delete()
            return Response(True)
        except DatalabFavoriteModel.DoesNotExist:
            raise Exception(_("置顶表不存在"))

    def retrieve(self, request, project_id):
        """
        @api {get} /v3/datalab/favorites/:project_id/ 获取项目的置顶表列表
        @apiName favorite_list
        @apiGroup favorites
        @apiDescription 获取项目的置顶表列表
        @apiParam {int} project_id 项目id
        @apiParam {string} project_type 项目类型，common或者personal

        @apiSuccess {int} id 数据表主键id
        @apiSuccess {int} project_id 所属项目id
        @apiSuccess {string} project_type 项目类型，common或者personal
        @apiSuccess {string} result_table_id 置顶结果表id
        @apiSuccess {int} active 记录是否有效 0：失效，1：有效

        @apiSuccessExample {json} 成功返回:
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": [
                {
                    "id": 1,
                    "result_table_id": "591_xxx",
                    "project_id": 11,
                    "project_type": "personal",
                    "active": 1,
                    "created_at": "2019-10-27 00:00:00",
                    "created_by": "xxx",
                    "updated_at": "2019-10-28 00:00:00",
                    "updated_by": "yyy",
                    "description": "ddd"
                },
                {
                    "id": 2,
                    "result_table_id": "591_yyy"
                    "project_id": 26,
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
        project_type = request.query_params.get(PROJECT_TYPE, PERSONAL)
        try:
            obj = (
                DatalabFavoriteModel.objects.filter(project_id=project_id, project_type=project_type)
                .order_by("-id")
                .values()
            )
            return Response(list(obj))
        except DatalabFavoriteModel.DoesNotExist:
            sys_logger.error("failed to find favorite table list by project_id: %s" % project_id)
            return Response()

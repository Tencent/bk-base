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
from rest_framework.response import Response
from django.utils.translation import ugettext as _
from common.decorators import params_valid, detail_route
from common.views import APIViewSet
from resourcecenter.auth.auth_helper import AuthHelper
from resourcecenter.error_code.errorcodes import DataapiResourceCenterCode
from resourcecenter.exceptions.base_exception import ResourceCenterException
from resourcecenter.handlers import resource_group_info
from resourcecenter.serializers.serializers import ProjectAddResourceGroupSerializer


class ProjectAuthViewSet(APIViewSet):
    """
    @apiDefine project_auth
    项目授权API
    """

    lookup_field = "project_id"

    @detail_route(methods=["get"], url_path="resource_groups")
    def resource_groups(self, request, project_id):
        """
        @api {get} /resourcecenter/project_auth/:project_id/resource_groups/ 查询项目的资源组授权
        @apiName resource_groups
        @apiGroup project_auth
        @apiParam {string} project_id 资源组ID
        @apiParamExample {json} 参数样例:
            {
              "project_id": "project_id"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "errors": null,
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "aiops": {
                          "status": "processing",
                          "group_name": "智能运维"
                          "created_at": "2019-04-03 16:22:27",
                          "ticket_id": 44,
                          "state_id": 1,
                        },
                        "bkdata_test": {
                          "status": "failed",
                          "group_name": "内部测试资源组"
                          "created_at": "2019-04-03 16:42:31",
                          "ticket_id": 46,
                          "state_id": 1,
                        },
                        "tgpa": {
                          "status": "succeeded",
                          "group_name": "TGPA",
                          "created_at": "2019-04-03 16:22:50",
                          "ticket_id": 45,
                          "state_id": 1,
                        }
                    },
                    "result": true
                }
        """
        resource_group_list = resource_group_info.filter_list(status="succeed")
        res_data = AuthHelper.get_project_resource_group_tickets(project_id=project_id)
        if res_data is not None and resource_group_list is not None:
            for resource_group in resource_group_list:
                # 补充资源组名称信息
                if resource_group.resource_group_id in list(res_data.keys()):
                    res_data[resource_group.resource_group_id]["group_name"] = resource_group.group_name
        return Response(res_data)

    @detail_route(methods=["post"], url_path="add_resource_group")
    @params_valid(ProjectAddResourceGroupSerializer)
    def add_resource_group(self, request, project_id, params):
        """
        @api {post} /resourcecenter/project_auth/:project_id/add_resource_group 项目申请资源组
        @apiName add_resource_group
        @apiGroup project_auth
        @apiParam {string} project_id 项目ID
        @apiParam {string} resource_group_id 资源组ID
        @apiParam {string} reason 申请原因
        @apiParam {string} bk_username 提交人
        @apiParamExample {json} 参数样例:
            {
              "project_id": "project_id",
              "resource_group_id": "resource_group_id",
              "reason": "reason",
              "bk_username": "user01"
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
        params["project_id"] = project_id
        # 验证是否已经申请拥有或者申请了该资源组
        res_data = AuthHelper.get_project_resource_group_tickets(project_id=project_id)
        if res_data is not None:
            if params["resource_group_id"] in list(res_data.keys()):
                if res_data[params["resource_group_id"]]["status"] not in ["failed"]:
                    raise ResourceCenterException(
                        message=_("已经申请过该资源组，无需重复申请。"), code=DataapiResourceCenterCode.UNEXPECT_EX
                    )
        resource_group = resource_group_info.filter_list(resource_group_id=params["resource_group_id"]).first()
        if resource_group is None:
            raise ResourceCenterException(message=_("资源组不存在，请确认后再申请。"), code=DataapiResourceCenterCode.UNEXPECT_EX)
        if resource_group.status in ["public"]:
            raise ResourceCenterException(message=_("该资源组是公开的无需申请。"), code=DataapiResourceCenterCode.UNEXPECT_EX)
        AuthHelper.project_apply_resource_group(project_id, params["resource_group_id"], params["reason"])

        return Response()

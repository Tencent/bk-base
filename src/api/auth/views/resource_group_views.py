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
from auth.models.auth_models import AuthResourceGroupRecord
from common.decorators import detail_route
from common.views import APIViewSet
from rest_framework.response import Response


class ResourceGroupViewSet(APIViewSet):
    @detail_route(methods=["get"], url_path="authorized_subjects")
    def list_authorized_subjects(self, request, pk):
        """
        @api {get} /auth/resource_groups/:resource_group_id/authorized_subjects/ 查找资源组授权主体
        @apiName list_authorized_subjects
        @apiGroup ResourceGroups
        @apiSuccessExample {json} Success-Response.data
            [
                {
                    'resource_group_id': 'mysql_lol_1',
                    'subject_type': 'project_info',
                    'subject_id': '1',
                    'description': None,
                    'created_at': '2020-03-31 18:21:16',
                    'created_by': 'user01',
                    'id': 18
                },
                {
                    'resource_group_id': 'mysql_lol_1',
                    'subject_type': 'access_raw_data',
                    'subject_id': '1011',
                    'description': None,
                    'created_at': '2020-03-31 18:21:16',
                    'created_by': 'user01',
                    'id': 18
                },
                {
                    'resource_group_id': 'mysql_lol_1',
                    'subject_type': 'bk_biz',
                    'subject_id': '591',
                    'description': None,
                    'created_at': '2020-03-31 18:21:16',
                    'created_by': 'user01',
                    'id': 18
                }
            ]
        """
        data = list(AuthResourceGroupRecord.objects.filter(resource_group_id=pk).values())
        return Response(data)

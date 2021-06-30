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

from .base import BaseTest


class TestAuth(BaseTest):
    def list_project_cluster_group(self):
        data = [
            {
                "updated_by": None,
                "created_at": "2018-12-06 17:52:55",
                "updated_at": None,
                "created_by": "xxx",
                "scope": "public",
                "cluster_group_id": "default",
                "cluster_group_alias": "xxx",
                "cluster_group_name": "default_group",
                "description": None,
            },
            {
                "updated_by": None,
                "created_at": "2018-12-06 17:52:55",
                "updated_at": None,
                "created_by": "xxx",
                "scope": "private",
                "cluster_group_id": "default2",
                "cluster_group_alias": "xxx",
                "cluster_group_name": "default_group",
                "description": None,
            },
            {
                "updated_by": None,
                "created_at": "2018-12-06 17:52:55",
                "updated_at": None,
                "created_by": "xxx",
                "scope": "private",
                "cluster_group_id": "default3",
                "cluster_group_alias": "xxx",
                "cluster_group_name": "default_group",
                "description": None,
            },
            {
                "updated_by": None,
                "created_at": "2018-12-06 17:52:55",
                "updated_at": None,
                "created_by": "xxx",
                "scope": "public",
                "cluster_group_id": "default4",
                "cluster_group_alias": "xxx",
                "cluster_group_name": "default_group",
                "description": None,
            },
        ]
        return self.success_response(data)

    def check_table_perm(self):
        return self.success_response({})

    def check_app_group_perm(self):
        return self.success_response({})

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


from django.db import models


class OperationStatus:
    DISABLED = "disabled"
    ACTIVE = "active"
    INVISIBLE = "invisible"


class OperationConfig(models.Model):
    """
    功能开关
    """

    operation_id = models.CharField(primary_key=True, max_length=255)
    operation_name = models.CharField(max_length=255)
    operation_alias = models.CharField(max_length=255)
    status = models.CharField(max_length=255, default=OperationStatus.DISABLED)
    description = models.TextField()
    users = models.TextField(null=True)

    @property
    def user_list(self):
        if self.users is None:
            return []

        return [u.strip() for u in self.users.split(",")]

    class Meta:
        managed = False
        app_label = "auth"
        db_table = "operation_config"

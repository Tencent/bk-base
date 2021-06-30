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


from auth.handlers.object_classes import AuthResultTable
from auth.models.outer_models import PROCESSING_TYPE


class BasePermissionMixin:
    def interpret_check(self, action_id, object_id, bk_app_code, **kwargs):
        """
        校验拦截器，类似中间件，返回非 None 时为有效返回

        @returnExample {Boolean} 成功拦截，返回有效鉴权结果
        @returnExample {None} 未被拦截
        """
        # 有关 RT 更新操作的权限控制，需要追溯到其他关联对象，先硬编码，后续看如何与现有框架机制结合
        if action_id in ["result_table.manage_task"]:
            auth_result_table = AuthResultTable(object_id)
            if auth_result_table.obj__processing_type in [PROCESSING_TYPE.CLEAN]:
                auth_raw_data = auth_result_table.get_parent_raw_data()
                return self.check(
                    "raw_data.etl",
                    object_id=str(auth_raw_data.object_id),
                    bk_app_code=bk_app_code,
                    raise_exception=True,
                )
            else:
                auth_project = auth_result_table.get_parent_project()
                return self.check(
                    "project.manage_flow",
                    object_id=str(auth_project.object_id),
                    bk_app_code=bk_app_code,
                    raise_exception=True,
                )

        # 内部版补丁，结果表包含_system_的查询结果表不校验权限
        if action_id == "result_table.query_data" and "_system_" in object_id:
            return True

        return None

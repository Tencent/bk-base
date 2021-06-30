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

from abc import ABCMeta, abstractmethod


class BaseUserBackend(metaclass=ABCMeta):
    @abstractmethod
    def check(self, user_id, action_id, object_id=None, bk_app_code=""):
        """
        校验主体是否具有操作对象的权限

        @param {String} user_id     用户ID
        @param {String} action_id   操作权限ID
        @param {String} object_id   操作对象ID
        @param {String} bk_app_code 应用编码，是否要对发起方进行限制

        @return
            - True
            - False

        @raise
            - PermissionDeniedError
            - ...
        """
        pass

    @abstractmethod
    def get_scopes(self, user_id, action_id):
        """
        获取主体有权限范围

        @param {String} user_id   用户ID
        @param {String} action_id 操作权限ID
        @return {Dict} 格式待定
            - action_id = result_table.query_data
            [
                {
                    "project_id": "111"
                },
                {
                    "bk_biz_id": "501"
                },
                {
                    "result_table_id": "105_xxxx"
                }
            ]
        """
        pass

    def can_handle_action(self, action_id):
        """
        是否可以处理传入的 action，不同 backend 依赖不同后端，需要实际的后端支持才可以启动，默认可以
        """
        return True

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
from django.conf import settings

RUN_MODE = settings.RUN_MODE


class BaseTest(object):
    def common_success(self):
        return self.success_response()

    def common_fail(self):
        return self.fail_response()

    def success_response(self, data=None):
        response = {"result": True, "message": "默认返回成功", "code": "1500200", "data": data}
        return response

    def fail_response(self, data=None):
        response = {"result": False, "message": "默认返回失败", "code": "1500500", "data": data}
        return response

    def set_return_value(self, func_name, is_mocking=False):
        """
        子类根据func_name继承
        @param func_name: 简单粗暴，即将调用的函数名称
        @param is_mocking: 是否mock
        @return:
        """
        # 默认本地环境下或is_mocking为true的情况下mock
        if RUN_MODE in ["LOCAL"] or is_mocking:
            return getattr(self, func_name, None)
        else:
            return None

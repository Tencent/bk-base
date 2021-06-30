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

from app_control.models import FunctionController


def func_check(func_code):
    """
    @summary: 检查功能是否开放
    @param func_code: 功能ID
    @return (1/2/3, message)
            #如下 (0, 功能未开启)
                  (1, 功能已开启)
    """
    result, enabled = FunctionController.objects.func_check(func_code)
    return (enabled, "功能已开启" if enabled else "功能未开启")

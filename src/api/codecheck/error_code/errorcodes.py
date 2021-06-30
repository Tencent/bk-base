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

from common.errorcodes import ErrorCode


def get_codes(code_class, module_code):
    """
    从框架的标准错误码管理类中提取标准化信息
    """
    error_codes = []
    for name in dir(code_class):
        if name.startswith("__"):
            continue
        value = getattr(code_class, name)
        if type(value) == tuple:
            error_codes.append(
                {
                    "code": "{}{}{}".format(ErrorCode.BKDATA_PLAT_CODE, module_code, value[0]),
                    "name": name,
                    "message": value[1],
                    "solution": value[2] if len(value) > 2 else "-",
                }
            )
    error_codes = sorted(error_codes, key=lambda _c: _c["code"])
    return error_codes

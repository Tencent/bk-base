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

import json
import time
from functools import wraps

from common.auth import check_perm
from common.log import sys_logger
from datalab.constants import (
    AUTH_INFO,
    BK_TICKET,
    COMMON,
    DELETE,
    PARAM,
    POST,
    PROJECT_MANAGE_FLOW,
)
from datalab.exceptions import ParamMissingError


def replace_name_prefix(bk_username):
    """
    兼容性方案：修复英文名带前缀时jupyterhub启动容器失败的问题

    :param bk_username: 笔记用户名
    :return: 转换后的用户名
    """
    return bk_username.replace("_", "1")


def check_project_auth(project_type, project_id, action_id=PROJECT_MANAGE_FLOW):
    """
    判断用户是否有相应操作的权限
    project.manage_flow：校验用户是否有数据开发的权限，如果无此权限，则没有对查询或笔记进行增删改的权限
    project.retrieve：校验用户是否有项目观察的权限，如果无此权限，则没有查看笔记报告的权限

    :param project_type: 项目类型
    :param project_id: 项目id
    :param action_id: 动作id
    """
    if project_type == COMMON:
        check_perm(action_id, project_id)


def check_required_params(required_params):
    """
    必传参数校验

    :param required_params: 必传参数列表
    """

    def _wrap(func):
        @wraps(func)
        def _deco(self, request, *args, **kwargs):
            request_params = request.data if request.method.lower() in [POST, DELETE] else request.query_params
            for param in required_params:
                if param not in request_params:
                    raise ParamMissingError(message_kv={PARAM: param})
                if param == AUTH_INFO and BK_TICKET not in json.loads(request_params[AUTH_INFO]):
                    raise ParamMissingError(message_kv={PARAM: BK_TICKET})
            return func(self, request, *args, **kwargs)

        return _deco

    return _wrap


def retry_deco(retry):
    """
    调用周边接口的重试机制

    :param retry: 重试次数
    """

    def _deco(func):
        @wraps(func)
        def _wrap(*arg, **kwargs):
            _retry = 1
            while True:
                try:
                    return func(*arg, **kwargs)
                except Exception as e:
                    sys_logger.error("重试第{}次失败: {}, ".format(_retry, e))
                    if _retry < retry:
                        # 重试间隔逐渐增大，最长不超过 3 秒
                        time.sleep(min(_retry, 3))
                        _retry = _retry + 1
                    else:
                        sys_logger.error("累计重试%s次失败" % retry)
                        raise e

        return _wrap

    return _deco

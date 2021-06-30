# -*- coding:utf-8 -*-
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
from __future__ import absolute_import, print_function, unicode_literals

import argparse
import sys

import click

from auth import models
from auth.bkiam.bkiam import SyncHandler
from auth.bkiam.manager import Manager
from common.db import connections

click.disable_unicode_literals_warning = True

if __name__ == "__main__":
    # 定义参数
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--user",
        "-b",
        default="",
        metavar="USER",
        help="Users who need to perform synchronization",
    )
    parser.add_argument(
        "--role",
        action="store",
        default="",
        type=str,
        nargs="?",
        help="Roles that require synchronization",
    )
    # 解析参数
    args = parser.parse_args()
    user = args.user
    role = args.role
    # 执行判断条件
    if user == "":
        sys.stderr.write("[ERROR]: please input user!")
        exit(1)
    if role == "":
        sys.stderr.write("[ERROR]: please input role!")
        exit(1)
    # 获取角色
    role_manager = Manager("roles")
    role_config = role_manager.filter(role_id=role)
    # 判断该角色是否存在
    if len(role_config) == 0:
        sys.stderr.write("[ERROR]: {} is not exist".format(role))
    sync = SyncHandler()
    with connections["basic"].session() as session:
        user_roles = (
            session.query(models.AuthUserRole)
            .filter(
                models.AuthUserRole.user_id == user, models.AuthUserRole.role_id == role
            )
            .all()
        )
        if not user_roles:
            sys.stderr.write("[ERROR]: Nothing to !")
        sync.sync_role(user_roles, role, sync.GRANT)

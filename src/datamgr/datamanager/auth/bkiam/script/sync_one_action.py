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
import traceback

import click

from auth.bkiam.bkiam import SyncHandler
from auth.bkiam.convert import Convert
from auth.bkiam.manager import Manager
from auth.bkiam.sync_users import SyncUser
from auth.models import AuthUserRole
from common.db import connections

click.disable_unicode_literals_warning = True


def sync_actions(user_id):
    """
    同步用户所有actions
    :param user_id: 用户id
    """

    with connections["basic"].session() as session:
        # 获取该用户下的所有权限
        user_roles = (
            session.query(AuthUserRole).filter(AuthUserRole.user_id == user_id).all()
        )
        sync_users = SyncUser()
        # 汇总权限信息
        local_action_resource_dict = sync_users.merge_data(user_roles)
        # 遍历所有action
        for user_action, resources in local_action_resource_dict.items():
            try:
                user_id = sync_users.parse_user_action_key(user_action, "user")
                action_id = sync_users.parse_user_action_key(user_action, "action")
                # 同步 action
                sync_users.batch_actions_grant_or_revoke(
                    user_id, action_id, resources, sync_users.sync_handler.GRANT
                )
                sys.stdout.write(
                    "[SyncUser remove_action_in_iam]: Succeed to sync action in iam, user_id={}, "
                    "action_id={}\n".format(user_id, action_id)
                )
            except Exception as err:
                traceback.print_exc()
                sys.stderr.write(
                    "[SyncUser remove_action_in_iam]: Fail to sync action in iam, user_id={}, "
                    "action_id={}, err={}\n".format(user_id, action_id, err)
                )


def sync_action(user_id, action):
    """
    同步用户单个action
    :param user_id: 用户id
    :param action: action_id
    """
    # 转换为 IAM ACTION
    sync_hanlder = SyncHandler()
    action = sync_hanlder.to_iam_action(action)
    try:
        with connections["basic"].session() as session:
            # 获取该用户下的所有权限
            user_roles = (
                session.query(AuthUserRole)
                .filter(AuthUserRole.user_id == user_id)
                .all()
            )
        sync_users = SyncUser()
        # 汇总权限信息
        local_action_resource_dict = sync_users.merge_data(user_roles)
        # 转换操作id
        key = Convert.get_user_action_key(user_id, action)
        resources = local_action_resource_dict[key]
        # 同步权限
        sync_users.batch_actions_grant_or_revoke(
            user_id, action, resources, sync_users.sync_handler.GRANT
        )
    except Exception:
        # 打印堆栈
        traceback.print_exc()
        exit(1)


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
        "--action",
        action="store",
        default="",
        type=str,
        nargs="?",
        help="Roles that require synchronization",
    )

    args = parser.parse_args()
    user = args.user
    action = args.action
    # 执行判断条件
    if user == "":
        sys.stderr.write("[ERROR]: please input user_id!")
        exit(1)
    if action != "":
        # 获取角色
        action_manager = Manager("actions")
        action_config = action_manager.filter(action_id=action)
        # 判断 action 是否存在
        if len(action_config) == 0:
            sys.stderr.write("[ERROR]: {} is not exist".format(action))
            exit(1)
        sync_action(user, action)
    else:
        sync_actions(user)

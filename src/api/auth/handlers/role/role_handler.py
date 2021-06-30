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

import queue
import threading
import time

from auth.bkiam import SUPPORT_IAM
from auth.bkiam.sync import RoleSync
from auth.config import AUTO_CONFIGURED_ROLES, CAN_HANDOVER_MANAGER_ROLES
from auth.exceptions import ActionCheckErr, BKIAMPolicesCountLimitErr
from auth.handlers.event import AddManagersEvent, DeleteManagersEvent, EventController
from auth.models.auth_models import AUTH_STATUS, UserRole
from auth.models.base_models import RoleConfig
from common.log import logger
from django.utils.translation import ugettext_lazy as _


class RoleHandler:

    SUPPORT_IAM = SUPPORT_IAM
    BKIAM_TOP_LIMIT = 5000

    GRANT = "grant"
    REVOKE = "revoke"

    def __init__(self):
        pass

    @classmethod
    def update_role_batch(cls, username, permissions):
        """
        批量添加角色权限

        @params {String} username 当前操作人
        @params {Dict[]} permssions 权限列表
        @paramExampe
            [
                {'user_id': 'user1', 'role_id': 'project.manager', 'scope_id': '1', 'operate': 'grant'},
                {'user_id': 'user2', 'role_id': 'project.manager', 'scope_id': '1', 'operate': 'revoke'}
            ]
        @returnExample
            [
                ({'user_id': 'user1', 'role_id': 'project.manager', 'scope_id': '1', 'operate': 'grant'}, True),
                ({'user_id': 'user2', 'role_id': 'project.manager', 'scope_id': '1', 'operate': 'revoke'}, True)
            ]
        """
        threads = []

        def wrap_execute_result(func, result_queue, perm):
            """
            记录结果的装饰器函数
            """

            def _deco(results, *args, **kwargs):
                try:
                    func(*args, **kwargs)
                    result_queue.put((perm, True, None))
                except Exception as err:
                    logger.exception(f"[Updata RoleAuth] Fail to update perm({perm}), {err}")
                    result_queue.put((perm, False, err.message))

            return _deco

        result_queue = queue.Queue()
        for perm in permissions:
            if perm["operate"] == cls.GRANT:
                t = threading.Thread(
                    target=wrap_execute_result(cls.add_role, result_queue, perm),
                    args=(cls, username, perm["role_id"], perm["user_id"], perm["scope_id"]),
                )
            elif perm["operate"] == cls.REVOKE:
                t = threading.Thread(
                    target=wrap_execute_result(cls.delete_role, result_queue, perm),
                    args=(cls, perm["role_id"], perm["user_id"], perm["scope_id"]),
                )
            else:
                t = None

            if t is not None:
                threads.append(t)

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        results = []
        while not result_queue.empty():
            results.append(result_queue.get())

        return results

    @classmethod
    def add_role(cls, username, role_id, user_id, scope_id):
        """
        添加角色
        @param username:
        @param role_id:
        @param user_id:
        @param scope_id:
        @return:
        """
        # 已存在角色则直接跳过
        if UserRole.objects.filter(scope_id=scope_id, role_id=role_id, user_id=user_id).exists():
            return
        try:
            role = RoleConfig.objects.get(pk=role_id)
        except RoleConfig.DoesNotExist:
            raise ActionCheckErr()

        UserRole.objects.create(scope_id=scope_id, role_id=role.role_id, user_id=user_id, created_by=username)

        # 开启 IAM 同步，此处需要同步至 IAM
        if cls.SUPPORT_IAM:
            # 目前 IAM 对单用户对某一功能有权限的策略数上限 10000 条的限制
            # 为了保障系统不受影响权限模块需要在用户与角色关系的数量加上 < 5000 条的限制
            if UserRole.objects.filter(role_id=role.role_id, user_id=user_id).count() > cls.BKIAM_TOP_LIMIT:
                raise BKIAMPolicesCountLimitErr(
                    _("暂不支持单个用户加入超过 {} 个同一资源角色，" "请前往蓝鲸权限中心，使用用户组+属性授权的方式进行授权").format(cls.BKIAM_TOP_LIMIT)
                )

            RoleSync().grant(user_id, role_id, scope_id)

        if role_id == "raw_data.manager":
            cls._push_raw_data_event(
                AddManagersEvent(
                    data_set_type="raw_data", data_set_id=scope_id, managers=[user_id], change_time=time.time()
                )
            )

    @classmethod
    def _push_raw_data_event(cls, event):
        """
        推送表更事件
        """
        try:
            EventController().push_event(event)
            logger.info(f"[PUSH EVENT] Succedd to push {event}")
        except Exception as err:
            logger.exception(f"[PUSH EVENT] Fail to push {event}, {err}")

    @classmethod
    def delete_role(cls, role_id, user_id, scope_id):
        """
        删除角色
        @param role_id:
        @param user_id:
        @param scope_id:
        @return:
        """
        count, _ = UserRole.objects.filter(scope_id=scope_id, role_id=role_id, user_id=user_id).delete()
        if count > 0 and cls.SUPPORT_IAM:
            RoleSync().revoke(user_id, role_id, scope_id)

        if count > 0 and role_id == "raw_data.manager":
            cls._push_raw_data_event(
                DeleteManagersEvent(
                    data_set_type="raw_data", data_set_id=scope_id, managers=[user_id], change_time=time.time()
                )
            )

    @classmethod
    def clear_roles(cls, user_id):
        """
        清除用户所有角色，设置非法状态位，不进行实际删除操作，便于记录，主要提供给审计使用

        @param user_id:
        @return:
        """
        relations = UserRole.objects.filter(user_id=user_id).exclude(role_id__in=AUTO_CONFIGURED_ROLES)
        instances = list(relations)
        num = relations.update(auth_status=AUTH_STATUS.INVALID)

        return num, instances

    @classmethod
    def handover_roles(cls, user_id, receiver):
        """
        移交所有管理角色
        """
        if user_id == receiver:
            return 0, []

        relations = UserRole.objects.filter(user_id=user_id).filter(role_id__in=CAN_HANDOVER_MANAGER_ROLES)
        instances = list(relations)

        num = relations.update(user_id=receiver)

        # 检查原始数据的变更事件
        raw_data_ids = [instance.scope_id for instance in instances if instance.role_id == "raw_data.manager"]
        for raw_data_id in raw_data_ids:
            cls._push_raw_data_event(
                DeleteManagersEvent(
                    data_set_type="raw_data", data_set_id=raw_data_id, managers=[user_id], change_time=time.time()
                )
            )
            cls._push_raw_data_event(
                AddManagersEvent(
                    data_set_type="raw_data", data_set_id=raw_data_id, managers=[receiver], change_time=time.time()
                )
            )

        return num, instances

    @classmethod
    def cmp_users(cls, user_ids, role_id, scope_id):
        """
        比较两个列表，发现删除和增加的内容
        @param user_ids:
        @param role_id:
        @param scope_id:
        @return:
        """
        olds = cls.list_users(role_id, scope_id=scope_id)
        result = {
            "add": [user_id for user_id in user_ids if user_id not in olds],
            "delete": [user_id for user_id in olds if user_id not in user_ids],
        }
        return result

    @classmethod
    def list_users(cls, role_id, scope_id=None):
        """
        查询某一角色底下的用户
        """
        rela_set = UserRole.objects.filter(role_id=role_id, scope_id=scope_id)
        return list(set(rela_set.values_list("user_id", flat=True)))

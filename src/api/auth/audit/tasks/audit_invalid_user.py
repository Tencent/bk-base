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


import time
import uuid

from auth.audit.audit_stats import AUDIT_TYPE, Stats
from auth.core.permission import RolePermission
from auth.handlers.staff import StaffManager
from auth.models.auth_models import AUTH_STATUS, UserRole
from common.log import logger
from django.db import transaction

try:
    from auth.extend.core.tdw.backend import TDWBackend
except ImportError as e:
    logger.info("环境中不存在 TDW 模块：{}".format(e))
    TDWBackend = None


def audit_invalid_user():
    audit_id = uuid.uuid4().hex + "_" + time.strftime("%Y%m%d%H%M%S", time.localtime())
    logger.info(f"[audit_invalid_user] Begin to audit, audit_id={audit_id}")
    # 需要审计的平台人员列表
    user_id_list = UserRole.objects.filter(auth_status=AUTH_STATUS.NORMAL).values_list("user_id", flat=True).distinct()
    logger.info(f"[audit_invalid_user] Audit users={user_id_list}")

    staff_manager = StaffManager()

    if user_id_list:
        for user_id in user_id_list:
            if staff_manager.is_dimission(user_id):
                with transaction.atomic(using="basic"):
                    user_role_num, user_role_models = RolePermission.clear(user_id)
                    if user_role_num > 0:
                        logger.info(f"[audit_invalid_user] Disable user_role, user={user_id}, num={user_role_num}")
                        Stats.add_to_audit_action(
                            audit_id, user_id, AUDIT_TYPE.USER_ROLE_DISABLED, user_role_num, user_role_models
                        )

                    if TDWBackend is not None:
                        tdw_user_num, auth_tdw_user_model = TDWBackend.clear(user_id)
                        if tdw_user_num > 0:
                            logger.info(f"[audit_invalid_user] Remove tdw_account, user={user_id}, num={tdw_user_num}")
                            Stats.add_to_audit_action(
                                audit_id, user_id, AUDIT_TYPE.TDW_USER_DISABLED, tdw_user_num, auth_tdw_user_model
                            )

                    # todo: 后续需要补充 Token 的冻结功能

    logger.info(f"[audit_invalid_user] end to audit, audit_id={audit_id}")

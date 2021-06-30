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
import uuid

from auth.models.audit_models import AuthAuditRecord
from common.base_utils import model_to_dict
from django.core.serializers.json import DjangoJSONEncoder


class AUDIT_TYPE:
    USER_ROLE_DISABLED = "user.role.disabled"
    USER_ROLE_HANDOVER = "user.role.handover"
    TDW_USER_DISABLED = "tdw.user.disabled"
    TOKEN_DISABLED = "token.disabled"
    TOKEN_RENEWAL = "token.renewal"


class Stats:
    def __init__(self):
        pass

    @classmethod
    def gene_audit_id(cls):
        return uuid.uuid4().hex + "_" + time.strftime("%Y%m%d%H%M%S", time.localtime())

    @classmethod
    def add_to_audit_action(cls, audit_id, audit_object_id, audit_type, audit_count, audit_models, operator=""):
        audit_log_list = [model_to_dict(audit_model) for audit_model in audit_models]
        audit_log = json.dumps(audit_log_list, cls=DjangoJSONEncoder)
        AuthAuditRecord.objects.create(
            audit_id=audit_id,
            audit_object_id=audit_object_id,
            audit_type=audit_type,
            audit_num=audit_count,
            audit_log=audit_log,
            created_by=operator,
            updated_by=operator,
        )

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

from enum import Enum

from django.conf import settings
from django.utils.translation import ugettext_lazy as _

# 系统自动过单身份ID
SYSTEM_USER = "__system__"


# 单据状态
SUCCEEDED = "succeeded"
FAILED = "failed"
PENDING = "pending"
PROCESSING = "processing"
STOPPED = "stopped"

STATUS = ((SUCCEEDED, _("已同意")), (FAILED, _("已驳回")), (PENDING, _("待处理")), (PROCESSING, _("处理中")), (STOPPED, _("已终止")))

DISPLAY_STATUS = ((SUCCEEDED, _("已同意")), (FAILED, _("已驳回")), (PROCESSING, _("处理中")), (STOPPED, _("已终止")))


class TokenPermissionStatus:
    ACTIVE = "active"
    INACTIVE = "inactive"
    APPLYING = "applying"

    # 有效或合法的状态
    VALID_STATUS = [
        ACTIVE,
    ]

    CHOICE = (
        (ACTIVE, _("有效")),
        (INACTIVE, _("无效")),
        (APPLYING, _("申请中")),
    )


# 权限对象
USER = "user"
APP = "app"
PROJECT = "project"
TOKEN = "token"


class SubjectTypeChoices:
    RAWDATA = "access_raw_data"
    PROJECT = "project_info"
    BIZ = "bk_biz"


SUBJECTS = (
    (USER, _("用户")),
    (APP, _("APP")),
    (PROJECT, _("项目")),
    (TOKEN, _("授权码")),
    (SubjectTypeChoices.PROJECT, _("项目")),
    (SubjectTypeChoices.RAWDATA, _("原始数据")),
    (SubjectTypeChoices.BIZ, _("业务")),
)


class Sensitivity(Enum):
    PUBLIC = "public"
    PRIVATE = "private"
    CONFIDENTIAL = "confidential"
    TOPSECRET = "topsecret"


SensitivityConfigs = [
    {
        "id": Sensitivity.PUBLIC.value,
        "name": _("公开"),
        "description": _("接入后平台所有用户均可使用此数"),
        "biz_role_id": "biz.manager",
        "active": Sensitivity.PUBLIC.value in settings.SUPPORT_SENSITIVITIES,
    },
    {
        "id": Sensitivity.PRIVATE.value,
        "name": _("业务私有"),
        "description": _("由业务负责人直接接入至平台，业务人员可见，平台用户均可申请此数据"),
        "biz_role_id": "biz.manager",
        "active": Sensitivity.PRIVATE.value in settings.SUPPORT_SENSITIVITIES,
    },
    {
        "id": Sensitivity.CONFIDENTIAL.value,
        "name": _("业务机密"),
        "description": _("由业务负责人接入平台，业务成员不可见，数据申请方需要leader级别"),
        "biz_role_id": "biz.leader",
        "active": Sensitivity.CONFIDENTIAL.value in settings.SUPPORT_SENSITIVITIES,
    },
    {
        "id": Sensitivity.TOPSECRET.value,
        "name": _("业务绝密"),
        "description": _("由业务负责人接入平台，仅授权人员可见，数据申请方需要总监级别"),
        "biz_role_id": "biz.leader",
        "active": Sensitivity.TOPSECRET.value in settings.SUPPORT_SENSITIVITIES,
    },
]

SensitivityMapping = {conf["id"]: conf for conf in SensitivityConfigs}

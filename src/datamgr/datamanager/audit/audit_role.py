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
import logging
import re
import time
from datetime import datetime

import attr

from api.modules.cmsi import CmsiApi
from audit.templates import render
from common.db import connections
from common.meta import metadata_client
from conf import settings
from models.meta import AccessRawData, ProjectInfo

logger = logging.getLogger(__name__)


class RoleManager(object):

    QQ_REGULAR_FORMAT = r"^\d+$"

    def __init__(self):
        pass

    def audit_manage_role(self):
        """
        审计管理角色授权关系
        """
        manage_roles = [
            {
                "role_id": "raw_data.manager",
                "object_class": "raw_data",
                "object_class_name": "原始数据",
                "model_id_field": "id",
                "model_name_field": "raw_data_name",
                "model_desc_field": "description",
            },
            {
                "role_id": "project.manager",
                "object_class": "project",
                "object_class_name": "项目",
                "model_id_field": "project_id",
                "model_name_field": "project_name",
                "model_desc_field": "description",
            }
            # 'function.manager'
        ]

        for _config in manage_roles:
            role_id = _config["role_id"]
            relations = self.get_role_relations(role_id)

            m_role_users = dict()
            for relation in relations:
                scope_id = relation["scope_id"]
                user_id = relation["user_id"]

                if scope_id not in m_role_users:
                    m_role_users[scope_id] = list()

                m_role_users[scope_id].append(user_id)

            # 当前系统对象列表
            m_objects = self.get_map_objects_by_role_id(role_id)

            self._audit_empty_members(_config, m_role_users, m_objects)
            self._audit_one_member(_config, m_role_users, m_objects)

    def _audit_empty_members(self, _config, m_role_users, m_objects):
        object_ids = m_objects.keys()
        object_class_name = _config["object_class_name"]
        title = "【IEG数据平台】您收到一封审计邮件"

        # 检查角色成员为空的情况
        logger.info("Start to check no members role...")
        auth_scope_ids = m_role_users.keys()
        empty_scope_ids = sorted(
            filter(lambda object_id: object_id not in auth_scope_ids, object_ids)
        )

        mail_objects = self._build_mail_objects(empty_scope_ids, _config, m_objects)
        if len(mail_objects) > 0:
            try:
                if settings.RUN_MODE != "PRODUCT":
                    title = "【IEG数据平台测试】您收到一封审计邮件"

                content = render(
                    "mail_empty_role_member.jinja",
                    {
                        "title": title,
                        "object_class_name": object_class_name,
                        "send_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "objects": mail_objects,
                    },
                )
                CmsiApi.send_mail(
                    {
                        "receivers": settings.PLAT_RECEIVERS,
                        "title": title,
                        "content": content,
                    }
                )
                # 避免同一时间点发送太多邮件导致公司邮件服务受影响，平滑发送
                time.sleep(1)
            except Exception as err:
                logger.exception(
                    "Fail to audit empty_members, object={}, err={}".format(
                        object_class_name, err
                    )
                )

        logger.info(
            "Audit empty_members task conclusion, object={}, object_ids={}".format(
                object_class_name, empty_scope_ids
            )
        )

    def _audit_one_member(self, _config, m_role_users, m_objects):
        object_class_name = _config["object_class_name"]
        object_class = _config["object_class"]
        title = "【IEG数据平台】您收到一封审计邮件"

        # 检查管理员仅一人的情况
        logger.info("Start to check one member role...")
        m_user_scope_ids = dict()
        for scope_id, user_ids in m_role_users.items():
            if len(user_ids) == 1:
                user_id = user_ids[0]
                if user_id not in m_user_scope_ids:
                    m_user_scope_ids[user_id] = []
                m_user_scope_ids[user_id].append(scope_id)

        one_member_object_ids = []
        send_count = 0
        for user_id, scope_ids in m_user_scope_ids.items():
            mail_objects = self._build_mail_objects(scope_ids, _config, m_objects)
            if len(mail_objects) > 0:
                try:
                    _receivers = [user_id]
                    if settings.RUN_MODE != "PRODUCT":
                        title = "【IEG数据平台测试】您收到一封审计邮件"
                        _receivers = settings.TEST_RECEIVERS

                    content = render(
                        "mail_one_role_member.jinja",
                        {
                            "title": title,
                            "object_class_name": object_class_name,
                            "object_class": object_class,
                            "send_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "objects": mail_objects,
                            "app_dataweb_url": settings.APP_DATAWEB_URL,
                        },
                    )

                    # 1分钟发送5封邮件，确认半小时内，不超过发送 200 封邮件
                    if send_count > 0 and send_count % 5 == 0:
                        logger.info(
                            "Waiting 1 minutes as mail send_count is {}".format(
                                send_count
                            )
                        )
                        time.sleep(60)

                    CmsiApi.send_mail(
                        {"receivers": _receivers, "title": title, "content": content}
                    )

                    send_count += 1
                    one_member_object_ids.extend([_obj["id"] for _obj in mail_objects])
                except Exception as err:
                    logger.exception(
                        (
                            "Fail to audit one_member, object={}, user_id={}, "
                            "scope_ids={}, err={}"
                        ).format(object_class_name, user_id, scope_ids, err)
                    )

        logger.info(
            "Audit one_member task conclusion , object={}, object_ids={}, "
            "send_count={}".format(object_class_name, one_member_object_ids, send_count)
        )

    def _build_mail_objects(self, object_ids, config, m_objects):
        """
        对于 ID 不在元数据对象库里的，无需考虑
        """
        return [
            {
                "id": getattr(m_objects[_id], config["model_id_field"]),
                "name": getattr(m_objects[_id], config["model_name_field"]),
                "description": getattr(m_objects[_id], config["model_desc_field"]),
            }
            for _id in object_ids
            if _id in m_objects
        ]

    def get_role_relations(self, role_id):
        """
        获取角色关系
        """
        with connections["basic"].session() as session:
            content = """
                SELECT scope_id, user_id
                FROM auth_user_role
                WHERE role_id = "{}"
                AND auth_status = "normal"
            """.format(
                role_id
            )

        result = session.execute(content)
        relations = [
            {"scope_id": item["scope_id"], "user_id": item["user_id"]}
            for item in result
        ]

        return filter(
            lambda _relation: not re.match(
                self.QQ_REGULAR_FORMAT, _relation["user_id"]
            ),
            relations,
        )

    def get_map_objects_by_role_id(self, role_id):
        """
        通过角色ID获取对象列表

        @resultExample
            # 此处会将 ID 转换为字符串
            {
                '1': AccessRawData(**kwargs),
                '2': AccessRawData(**kwargs)
            }
        """
        ROLE_MODEL_MAP = {
            "raw_data.manager": AccessRawData,
            "project.manager": ProjectInfo,
        }

        md = ROLE_MODEL_MAP[role_id]
        objects = metadata_client.list(md)

        identifier = md.metadata["identifier"].name

        # 目前仅审计内部版对象，且仅在官网创建的对象
        field_names = [f.name for f in attr.fields(md)]
        if "bk_biz_id" in field_names:
            objects = filter(lambda _obj: _obj.bk_biz_id < 200000, objects)

        if "bk_app_code" in field_names:
            objects = filter(
                lambda _obj: _obj.bk_app_code in ["data", "dataweb"], objects
            )

        if "active" in field_names:
            objects = filter(lambda _obj: _obj.active, objects)

        return {str(getattr(obj, identifier)): obj for obj in objects}


def audit_manage_role():
    """
    审计有管理权限的角色关系
    """
    RoleManager().audit_manage_role()

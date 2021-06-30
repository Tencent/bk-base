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
from __future__ import absolute_import, print_function, unicode_literals

from enum import Enum

import attr
from sqlalchemy import TIMESTAMP, Boolean, Column, String, Text, text
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class AuthStatus(Enum):
    NORMAL = "normal"
    EXPIRED = "expired"
    INVALID = "invalid"


class AuthUserRole(Base):
    __tablename__ = "auth_user_role"

    id = Column(INTEGER(11), primary_key=True)
    user_id = Column(String(64), nullable=False)
    role_id = Column(String(64), nullable=False)
    scope_id = Column(String(128), nullable=False)
    auth_status = Column(
        String(32), nullable=False, server_default=AuthStatus.NORMAL.value
    )
    created_by = Column(String(64), nullable=False)
    created_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    updated_by = Column(String(64))
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    description = Column(Text)


class AuthDataTokenStatus(Enum):
    # 过期的状态不直接写入DB, 通过过期时间expired_at得出
    EXPIRED = "expired"
    DISABLED = "disabled"
    ENABLED = "enabled"
    REMOVED = "removed"


class AuthDataToken(Base):
    """
    auth_data_token 表
    """

    __tablename__ = "auth_data_token"

    id = Column(INTEGER(11), primary_key=True)
    data_token = Column(String(64), nullable=False)
    data_token_bk_app_code = Column(String(64), nullable=False)
    status = Column(
        String(32), nullable=False, server_default=AuthDataTokenStatus.ENABLED.value
    )
    expired_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    created_by = Column(String(64), nullable=False)
    created_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    updated_by = Column(String(64), nullable=False)
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    description = Column(Text)


class AuthDataTokenPermissionStatus(Enum):
    ACTIVE = "active"
    APPLYING = "applying"


class AuthDataTokenPermission(Base):

    __tablename__ = "auth_data_token_permission"

    id = Column(INTEGER(11), primary_key=True)
    status = Column(
        String(32),
        nullable=False,
        server_default=AuthDataTokenPermissionStatus.ACTIVE.value,
    )
    action_id = Column(String(64), nullable=False)
    object_class = Column(String(32), nullable=False)
    scope_id_key = Column(String(64), nullable=False)
    scope_id = Column(String(128), nullable=False)
    created_by = Column(String(64), nullable=False)
    created_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    updated_by = Column(String(64), nullable=False)
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    description = Column(Text)
    data_token_id = Column(INTEGER(11), primary_key=True)


class AuthActionConfig(Base):

    __tablename__ = "auth_action_config"

    action_id = Column(String(64), primary_key=True)
    action_name = Column(String(64), nullable=False)
    object_class = Column(String(32), nullable=False)
    has_instance = Column(Boolean, server_default=text("True"))
    user_mode = Column(Boolean, server_default=text("False"))
    created_by = Column(String(64), nullable=False)
    created_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    updated_by = Column(String(64), nullable=False)
    updated_at = Column(
        TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    description = Column(Text)
    can_be_applied = Column(Boolean, server_default=text("True"))
    order = Column(INTEGER, server_default="1")


@attr.s
class ObjectConfig:
    """
    Object 配置表
    """

    object_class = attr.ib(type=str)
    object_name = attr.ib(type=str)
    has_object = attr.ib(type=bool)
    scope_id_key = attr.ib(type=str)
    scope_name_key = attr.ib(type=str)
    user_mode = attr.ib(type=bool)
    to_iam = attr.ib(type=bool)
    description = attr.ib(type=str, default="")


@attr.s
class ActionConfig:
    """
    Action 配置类
    """

    action_id = attr.ib(type=str)
    action_name = attr.ib(type=str)
    object_class = attr.ib(type=str)
    has_instance = attr.ib(type=bool)
    user_mode = attr.ib(type=bool)
    can_be_applied = attr.ib(type=bool)
    order = attr.ib(type=int)
    to_iam = attr.ib(type=bool)
    description = attr.ib(type=str, default="")
    object_class_id = attr.ib(type=str, init=False)


@attr.s
class RoleConfig:
    """
    权限角色配置表
    """

    role_id = attr.ib(type=str)
    role_name = attr.ib(type=str)
    object_class = attr.ib(type=str)
    description = attr.ib(type=str, default="")
    allow_empty_member = attr.ib(type=bool, default=True)
    user_mode = attr.ib(type=bool, default=False)
    order = attr.ib(type=int, default=1)
    to_iam = attr.ib(type=bool, default=False)


@attr.s
class RolePolicyInfo:
    """
    角色权限策略信息
    """

    role_id = attr.ib(type=str, init=True)
    action_id = attr.ib(type=str, init=True)
    object_class = attr.ib(type=str, init=True)

    scope_attr_key = attr.ib(type=str, default="")
    scope_attr_value = attr.ib(type=str, default="")


@attr.s
class ResourceScope(object):
    """
    权限中心表达式对象
    """

    id = attr.ib(type=str)
    # 类型： instance：1, simple_path: project&1, complex_path: project&1#flow&2
    type = attr.ib(type=str)


@attr.s
class Resource(object):
    id = attr.ib(type=str)
    resource_type = attr.ib(type=str)
    display_name = attr.ib(type=str)

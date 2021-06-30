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

import requests
from command.constants import (
    ACTION_ID,
    AUTH_OBJECT_NOT_FOUND,
    BIZ_COMMON_ACCESS,
    CLUSTER_GROUP_ID,
    CODE,
    DATA,
    HEADERS,
    HTTP_STATUS_OK,
    OBJECT_ID,
    PROJECT_MANAGE_FLOW,
    RAW_DATA_QUERY,
    RESULT,
    RESULT_TABLE_QUERY,
    RESULT_TABLE_UPDATE,
)
from command.exceptions import AuthException
from command.settings import AUTH_API_ROOT


class AuthErrorMessage(object):
    AUTH_INTERFACE_ERR = "auth接口异常，请联系管理员进行排查"
    AUTH_OBJECT_NOT_FOUND_ERR = "权限校验失败：结果表 %s 不存在，请确定表名是否正确"
    RETRIEVE_CLUSTER_GROUP_ERR = "获取项目有权限的集群组失败，请联系数据平台小助手进行排查"
    ACTION_ERROR = {
        RESULT_TABLE_QUERY: "权限校验失败：没有结果表 %s 的查询权限，请先申请结果表查询权限",
        RESULT_TABLE_UPDATE: "权限校验失败：没有结果表 %s 的修改权限，请确定结果表是否为此项目生成",
        RAW_DATA_QUERY: "权限校验失败：没有原始数据源 %s 的使用权限，请先申请原始数据源使用权限",
        PROJECT_MANAGE_FLOW: "权限校验失败：权限不足，%s 项目观察员不能执行代码，只能查看笔记内容",
        BIZ_COMMON_ACCESS: "权限校验失败：权限不足，没有业务 %s 的权限，不能在相应业务下创建结果表",
    }


def check_project_object_auth(project_id, object_ids, action_id=RESULT_TABLE_QUERY):
    """
    判断项目是否有相应动作的权限

    :param project_id: 项目id
    :param object_ids: 鉴权对象id，比如 result_table_id, raw_data_id
    :param action_id: 动作方式，默认 result_table.query_data 查询结果数据，可选 raw_data.query_data 查询数据源
    :return: 校验结果
    """
    for object_id in object_ids:
        url = "{}/projects/{}/data/check/".format(AUTH_API_ROOT, project_id)
        data = json.dumps({ACTION_ID: action_id, OBJECT_ID: object_id})
        response = requests.post(url=url, headers=HEADERS, data=data)
        if response.status_code == HTTP_STATUS_OK:
            res_json = response.json()
            if res_json[CODE] == AUTH_OBJECT_NOT_FOUND:
                raise AuthException(AuthErrorMessage.AUTH_OBJECT_NOT_FOUND_ERR % object_id)
            if not res_json[DATA]:
                raise AuthException(AuthErrorMessage.ACTION_ERROR[action_id] % object_id)
        else:
            raise AuthException(AuthErrorMessage.AUTH_INTERFACE_ERR)


def check_user_object_auth(bk_username, object_ids, action_id=PROJECT_MANAGE_FLOW):
    """
    判断用户是否有相应动作的权限

    :param bk_username: 用户名
    :param object_ids: 鉴权对象id，比如 project_id, bk_biz_id
    :param action_id: 动作方式，默认 project.manage_flow 判断用户是否有数据开发权限，可选 biz.common_access 判断用户是否有相应业务权限
    :return: 校验结果
    """
    for object_id in object_ids:
        url = "{}/users/{}/check/".format(AUTH_API_ROOT, bk_username)
        data = json.dumps({ACTION_ID: action_id, OBJECT_ID: object_id})
        response = requests.post(url=url, headers=HEADERS, data=data)
        if response.status_code == HTTP_STATUS_OK:
            if not response.json()[DATA]:
                raise AuthException(AuthErrorMessage.ACTION_ERROR[action_id] % object_id)
        else:
            raise AuthException(AuthErrorMessage.AUTH_INTERFACE_ERR)


def retrieve_cluster_group(project_id):
    """
    列举项目有权限的集群组

    :param project_id: 项目id
    :return: 项目有权限的集群组
    """
    url = "{}/projects/{}/cluster_group/".format(AUTH_API_ROOT, project_id)
    response = requests.get(url=url)
    if response.status_code == HTTP_STATUS_OK:
        res_json = response.json()
        if not res_json[RESULT]:
            raise AuthException(AuthErrorMessage.RETRIEVE_CLUSTER_GROUP_ERR)
        cluster_groups = [cluster_group[CLUSTER_GROUP_ID] for cluster_group in res_json[DATA]]
        return cluster_groups
    else:
        raise AuthException(AuthErrorMessage.RETRIEVE_CLUSTER_GROUP_ERR)

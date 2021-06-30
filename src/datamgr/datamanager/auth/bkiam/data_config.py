# -- coding: utf-8 --
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
import functools
import json
import os

import requests
import yaml

from auth.models import ActionConfig, ObjectConfig, RoleConfig, RolePolicyInfo
from common.exceptions import ApiRequestError
from conf import settings

AUTH_CORE_CONFIG_URL = "{}core_config/".format(settings.AUTH_API_URL)


def pull_configs():
    response = requests.get(AUTH_CORE_CONFIG_URL)
    result = json.loads(response.text)
    if result["result"]:
        return result["data"]
    else:
        raise ApiRequestError("API请求异常，请管理员排查问题,url = {}".format(AUTH_CORE_CONFIG_URL))


# 缓存函数返回结果，防止重复处理，提升效率
@functools.lru_cache()
def load_action_map():
    action_map = {}
    path = os.path.join(settings.PROJECT_DIR, "auth/bkiam/core/maps.yaml")
    with open(path, encoding="utf8") as f:
        content = yaml.load(f.read())
    actions = content["action_map"]
    for action in actions:
        key = action["action_id"]
        value = action["iam_action_id"]
        action_map[key] = value
    return action_map


configs = pull_configs()
action_maps = load_action_map()


@functools.lru_cache()
def build_object_config():
    object_list = configs["ObjectConfig"]
    return [ObjectConfig(**obj) for obj in object_list]


@functools.lru_cache()
def build_action_config():
    action_list = configs["ActionConfig"]
    for action in action_list:
        # 处理数据，因为不需要维护 object_class 这样的外键关系
        action["object_class"] = action["object_class_id"]
        action.pop("object_class_id")
    return [ActionConfig(**action) for action in action_list]


@functools.lru_cache()
def build_role_config():
    role_list = configs["RoleConfig"]
    for role in role_list:
        role["object_class"] = role["object_class_id"]
        role.pop("object_class_id")
    return [RoleConfig(**role) for role in role_list]


@functools.lru_cache()
def build_role_policy():
    role_polices = configs["RolePolicyInfo"]
    for policy in role_polices:
        policy["object_class"] = policy["object_class_id"]
        policy["role_id"] = policy["role_id_id"]
        policy["action_id"] = policy["action_id_id"]
        [policy.pop(key) for key in ["object_class_id", "role_id_id", "action_id_id"]]
    return [RolePolicyInfo(**policy) for policy in role_polices]

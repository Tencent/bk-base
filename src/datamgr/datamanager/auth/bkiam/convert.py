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
import logging

from auth.bkiam import constant
from auth.models import ResourceScope

logger = logging.getLogger(__name__)


class Convert:
    @classmethod
    def parse_iam_path_expression(cls, value):
        """
        解析权限中心拓扑授权表达式，处理成本地表示的路径
        :param value: "/project,12511/"
        :return: project&12511
        """
        # 解析路径，拼接成 resource&id的形式
        data = value.replace("/", "").split(",")
        return cls.to_local_path(data[0], data[1])

    @classmethod
    def to_local_path(cls, resource_type, object_id):
        """
        拼接本地路径
        :param resource_type: 资源类型
        :param separator: 分隔符
        :param object_id: 资源 id
        :return: example: project&1
        """
        return "{}{}{}".format(resource_type, constant.SEPARATOR, object_id)

    @classmethod
    def get_user_action_key(cls, user_id, action_id):
        """
        拼接权限中心用户的key
        :param user_id: 用户id
        :param action_id: 操作id
        :return: user_id$action_id
        """
        return "{}{}{}".format(user_id, constant.KEY_SEPARATOR, action_id)

    @classmethod
    def get_object_type(cls, object_id):
        """
        判断资源授权类型
        """
        if "#" in object_id:
            return constant.COMPLEX_PATH
        elif constant.SEPARATOR in object_id:
            return constant.SIMPLE_PATH
        else:
            return constant.INSTANCE

    @classmethod
    def build_resource_scopes(cls, resources):
        """
        构造list [auth.models.ResourceScope,auth.models.ResourceScope]
        """
        if not isinstance(resources, list):
            # 如果返回的是单个资源，转换成列表
            resources = [resources]
        resource_scopes = []
        for resource in resources:
            resource_scopes.append(
                ResourceScope(id=resource, type=cls.get_object_type(resource))
            )
        return resource_scopes

    @classmethod
    def to_local_action(self, iam_action_id):
        """
        将权限中心格式的action 转换成本地格式的 action
        :param iam_action_id: 权限中心权限操作id： project-delete
        :return: 本地 project.delete
        """
        return iam_action_id.replace("-", ".")

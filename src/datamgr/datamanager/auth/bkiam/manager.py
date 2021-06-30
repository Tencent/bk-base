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
from auth.bkiam import data_config


class Manager:
    def __init__(self, config_type):
        self.instances = self._get_instances(config_type)

    def _get_instances(self, config_type):
        """
        获取实例
        """
        if config_type == "objects":
            return data_config.build_object_config()
        elif config_type == "polices":
            return data_config.build_role_policy()
        elif config_type == "roles":
            return data_config.build_role_config()
        elif config_type == "actions":
            return data_config.build_action_config()

    def get(self, **kwargs):
        """
        获取指定实例
        """
        instances = self.filter(**kwargs)
        if len(instances) == 0:
            raise Exception("{} instacne not exist".format(kwargs))

        return instances[0]

    def all(self):
        return self.instances

    def filter(self, **kwargs):
        """
        支持实例过滤查询
        """
        return list(
            filter(
                lambda inst: all([getattr(inst, k) == v for k, v in kwargs.items()]),
                self.instances,
            )
        )

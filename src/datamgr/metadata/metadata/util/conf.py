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

from metadata.util.common import StrictABCMeta, camel_to_snake


class ConfigsCollection(object):
    """
    配置集，对配置进行统一管理，对放入集合的配置自动setup。
    """

    def __init__(self):
        self.registered_configs = {}

    def add(self, config_cls, name=None, *args, **kwargs):
        name = camel_to_snake(config_cls.__name__) if not name else name
        config = config_cls(*args, **kwargs)

        # 反向注册，将 ConfigsCollection 实例绑定到 Config 实例上
        config_cls.config_collection = property(lambda inner_self: self)

        # 安装 Config 实例
        if hasattr(config_cls, 'setup') and callable(getattr(config, 'setup')):
            config.setup()

        # 注册 Config 实例
        self.registered_configs[name] = config
        setattr(self, name, config)


class BaseConfig(object, metaclass=StrictABCMeta):
    """
    配置基类。
    """

    __abstract__ = True
    config_collection = None


def basic_configs_collection_gen(*configs_info, **kwargs):
    """
    快捷配置生成函数。
    :param configs_info: 配置和配置参数。
    :param kwargs: 函数配置参数。
    :rtype ConfigsCollection
    """
    cc_cls = kwargs.get(str('cc_cls'))
    cc_cls = cc_cls if cc_cls else ConfigsCollection
    cc = cc_cls()
    for config_info in configs_info:
        cc.add(**config_info)
    return cc

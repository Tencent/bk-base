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
from common.log import logger
from datahub.databus.exceptions import TaskStorageNotSupport
from datahub.databus.settings import DATABUS_PULLERS_DICT
from django.utils.module_loading import import_string


class PullerFactory(object):
    """
    Puller工厂类
    """

    puller_map = {}
    puller_factory = None

    @property
    def registered_pullers(self):
        return self.puller_map

    @property
    def support_pullers(self):
        return self.puller_map.keys()

    @classmethod
    def register_puller_class(cls, puller_scenario, puller_class):
        cls.puller_map[puller_scenario] = puller_class

    @classmethod
    def get_puller(cls, puller_scenario):
        return cls.puller_factory.registered_pullers[puller_scenario]

    @classmethod
    def new_api_module(cls, class_path):
        return import_string(class_path)

    @classmethod
    def check_storage_type(cls, storage):
        """
        校验存储类型
        :param storage:
        """
        if storage not in cls.support_pullers:
            logger.error("storage %s is not supported in databus puller!" % storage)
            raise TaskStorageNotSupport(message_kv={"storage": storage})

    @classmethod
    def init_factory(cls):
        cls.puller_factory = PullerFactory()
        for puller_scenario in DATABUS_PULLERS_DICT.keys():
            puller_path = DATABUS_PULLERS_DICT[puller_scenario]
            _puller = PullerFactory.new_api_module(puller_path)
            logger.info("PullerFactory load scenario:{} and puller_class:{}".format(puller_scenario, puller_path))
            PullerFactory.register_puller_class(puller_scenario, _puller)

    @classmethod
    def get_puller_factory(cls):
        if not cls.puller_factory:
            cls.init_factory()

        return cls.puller_factory

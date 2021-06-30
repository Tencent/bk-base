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
from datahub.databus.settings import DATABUS_SHIPPERS_DICT
from django.utils.module_loading import import_string


class ShipperFactory(object):
    """
    Shipper工厂类
    """

    shipper_map = {}
    shipper_factory = None

    def __init__(self):
        pass

    @property
    def registered_shippers(self):
        return self.shipper_map

    @property
    def support_shippers(self):
        return self.shipper_map.keys()

    @classmethod
    def register_shipper_class(cls, shipper_scenario, shipper_class):
        cls.shipper_map[shipper_scenario] = shipper_class

    @classmethod
    def get_shipper(cls, shipper_scenario):
        return ShipperFactory.registered_shippers[shipper_scenario]

    @classmethod
    def new_api_module(cls, class_path):
        return import_string(class_path)

    @classmethod
    def check_storage_type(cls, storage):
        """
        校验存储类型
        :param storage:
        """
        if storage not in cls.support_shippers:
            logger.error("storage %s is not supported in databus shipper!" % storage)
            raise TaskStorageNotSupport(message_kv={"storage": storage})

    @classmethod
    def init_factory(cls):
        cls.shipper_factory = ShipperFactory()
        for shipper_scenario in DATABUS_SHIPPERS_DICT.keys():
            shipper_path = DATABUS_SHIPPERS_DICT[shipper_scenario]
            _shipper = ShipperFactory.new_api_module(shipper_path)
            logger.info("ShipperFactory load scenario:{} and shipper_class:{}".format(shipper_scenario, shipper_path))
            cls.shipper_factory.register_shipper_class(shipper_scenario, _shipper)

    @classmethod
    def get_shipper_factory(cls):
        if not cls.shipper_factory:
            cls.init_factory()

        return cls.shipper_factory

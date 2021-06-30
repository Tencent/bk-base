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
from datahub.access.exceptions import CollectorError, CollerctorCode
from datahub.access.settings import ACCESS_SCENARIO_DICT
from django.utils.module_loading import import_string


class CollectorFactory(object):
    """
    采集器工厂类
    """

    Collector_MAP = {}
    collector_factory = None
    task_factory = None

    def __init__(self):
        pass

    @classmethod
    def register_collector_class(cls, data_scenario, collector_class):
        cls.Collector_MAP[data_scenario] = collector_class

    @classmethod
    def get_collector_by_data_scenario(cls, data_scenario):
        # 根据接入场景获取采集器
        try:
            return cls.Collector_MAP[data_scenario]
        except KeyError:
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_NO_REGISTER_COLLECT)

    @classmethod
    def get_collector_by_data_id(cls, raw_data_id):
        # 根据dataid获取采集器
        from datahub.access.handlers.raw_data import RawDataHandler

        raw_data = RawDataHandler(raw_data_id=raw_data_id)
        return cls.get_collector_by_data_scenario(raw_data.data["data_scenario"])

    @classmethod
    def new_api_module(cls, class_path):
        return import_string(class_path)

    @property
    def registered_collectors(self):
        return self.Collector_MAP

    @classmethod
    def init_factory(cls):
        cls.task_factory = TaskFactory()
        cls.collector_factory = CollectorFactory()

        for scenario in ACCESS_SCENARIO_DICT.keys():
            collector_path = ACCESS_SCENARIO_DICT[scenario]
            _access = CollectorFactory.new_api_module(collector_path)

            logger.info("COLLECTOR_FACTORY load scenario:{} and collector_class:{}".format(scenario, collector_path))
            cls.collector_factory.register_collector_class(scenario, _access)
            cls.task_factory.register_task_class(scenario, _access.access_task)

    @classmethod
    def get_collector_factory(cls):
        if not cls.collector_factory:
            cls.init_factory()

        return cls.collector_factory

    @classmethod
    def get_task_factory(cls):
        if not cls.collector_factory:
            cls.init_factory()

        return cls.task_factory


class TaskFactory(object):
    """
    任务工厂类
    """

    Task_MAP = {}

    def __init__(self):
        pass

    @classmethod
    def register_task_class(cls, data_scenario, task_class):
        cls.Task_MAP[data_scenario] = task_class

    @classmethod
    def get_task_by_data_scenario(cls, data_scenario):
        # 根据接入场景获取任务
        try:
            return cls.Task_MAP[data_scenario]
        except KeyError:
            raise CollectorError(error_code=CollerctorCode.COLLECTOR_NO_REGISTER_TASK)

    @property
    def registered_tasks(self):
        return self.Task_MAP

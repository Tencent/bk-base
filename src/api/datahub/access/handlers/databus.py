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

from datahub.databus.task.task_utils import get_task_component

from ...databus.clean import get_clean_list_using_data_id
from ...databus.task.storage_task import create_storage_task
from ...databus.task.task import delete_task


class DataBusHandler(object):
    """
    DataBus处理器
    """

    def __init__(self, raw_data_id=None):
        self.raw_data_id = raw_data_id

    def clean_list(self):
        res = get_clean_list_using_data_id(self.raw_data_id)
        return res.data

    @classmethod
    def task_list(cls, result_table_ids):
        result = get_task_component(result_table_ids)
        return result

    @classmethod
    def start_task(cls, result_table_id, storages=None):
        create_storage_task(result_table_id, storages if storages else [])
        return "任务(result_table_id:%s)启动成功!" % result_table_id

    @classmethod
    def stop_task(cls, result_table_id, storages=[]):
        delete_task(result_table_id, storages)
        return "OK"

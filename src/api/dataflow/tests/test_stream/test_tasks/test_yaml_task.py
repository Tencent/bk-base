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

from rest_framework.test import APITestCase

from dataflow.stream.models import DataFlowYamlExecuteLog, DataFlowYamlInfo


class TestYamlExecuteTask(APITestCase):
    def test_set_yaml_status(self):
        DataFlowYamlInfo.objects.create(yaml_id=1, yaml_name="yaml_1", project_id=1)
        task = DataFlowYamlExecuteLog.objects.create(yaml_id=1, action="start", context="{}")
        from dataflow.stream.extend.tasks import YamlExecuteTask

        yaml_task = YamlExecuteTask(task.id)
        yaml_task.set_yaml_status(DataFlowYamlInfo.STATUS.STARTING)
        yaml_info = DataFlowYamlInfo.objects.get(yaml_id=1)
        assert yaml_task.task.start_time is not None
        assert yaml_task.task.end_time is None
        assert yaml_info.status == DataFlowYamlInfo.STATUS.STARTING

        yaml_task.set_yaml_status(DataFlowYamlInfo.STATUS.RUNNING)
        yaml_info = DataFlowYamlInfo.objects.get(yaml_id=1)
        assert yaml_task.task.end_time is not None
        assert yaml_info.status == DataFlowYamlInfo.STATUS.RUNNING

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


from datamanage.pro.datamodel.application.jobs.orchestrator import achieved_job_system


class Console(object):
    job_system = None

    def __init__(self, data_scope, job_type='AssembleSql'):
        """
        任务面板初始化

        :param data_scope: 数据域
        {
            "node_type": (string) dim/fact/indicator,
            "node_instance":
        }
        :param job_type: 任务类型 AssembleSql - 构建sql任务
        """
        if job_type in achieved_job_system:
            self.job_system = achieved_job_system[job_type](data_scope=data_scope)
        if self.job_system is None:
            # Todo: XXX任务系统还未实现
            raise

    def build(self, *args, **kwargs):
        """
        构建任务

        :return: mixed None - 无执行内容; 非None - 执行结果
        """
        plan = self.job_system.gen_plan(command='build')
        return plan.execute(*args, **kwargs)

    def destroy(self):
        """
        销毁任务

        :return: mixed None - 无执行内容; 非None - 执行结果
        """
        plan = self.job_system.gen_plan(command='destroy')
        return plan.execute()

    def start(self):
        """
        启动任务

        :return: mixed None - 无执行内容; 非None - 执行结果
        """
        plan = self.job_system.gen_plan(command='start')
        return plan.execute()

    def stop(self):
        """
        停止任务

        :return: mixed None - 无执行内容; 非None - 执行结果
        """
        plan = self.job_system.gen_plan(command='stop')
        return plan.execute()

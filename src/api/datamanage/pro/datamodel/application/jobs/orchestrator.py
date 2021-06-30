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


from datamanage.pro.datamodel.application.jobs.prototype import Orchestrator
from datamanage.pro.datamodel.application.jobs.node import achieved_dm_node
from datamanage.pro.datamodel.application.jobs.operator import achieved_job_operator

achieved_job_system = dict()
"""
已注册编排者字典
"""


def orchestrator_register(cls):
    """
    注册数据任务编排类

    :param cls: 装饰类
    :return: boolean True
    """
    class_name = cls.__name__
    orchestrator_type = class_name.replace('Orchestrator', '')
    if orchestrator_type not in achieved_job_system:
        achieved_job_system[orchestrator_type] = cls
    return cls


@orchestrator_register
class AssembleSqlOrchestrator(Orchestrator):

    operator = None
    data_scope = None
    node_instance = None

    orchestrator_type = 'AssembleSql'

    def __init__(self, data_scope, *args, **kwargs):
        super(AssembleSqlOrchestrator, self).__init__(*args, **kwargs)
        self.data_scope = data_scope

    def gen_dm_node(self):
        """
        构建数据模型节点

        :return:
        """
        node_type = self.data_scope['node_type']
        node_conf = self.data_scope['node_conf']
        self.node_instance = achieved_dm_node[node_type](node_conf)

    def deploy_operator(self, command):
        self.operator = achieved_job_operator[self.orchestrator_type](command, self.node_instance)

    def gen_plan(self, command):
        """
        创建执行计划
        :return: Operator 配置好任务的执行者实例
        """
        if command == 'build':
            self.gen_dm_node()
        else:
            # Todo: XXX指令还未实现
            raise
        self.deploy_operator(command)
        return self.operator

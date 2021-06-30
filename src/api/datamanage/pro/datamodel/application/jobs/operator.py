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


from datamanage.pro.datamodel.application.jobs.prototype import Operator

achieved_job_operator = dict()
"""
已注册任务执行者
"""


def operator_register(cls):
    """
    注册数据任务执行类

    :param cls: 装饰类
    :return: boolean True
    """
    class_name = cls.__name__
    operator_type = class_name.replace('Operator', '')
    if operator_type not in achieved_job_operator:
        achieved_job_operator[operator_type] = cls
    return cls


@operator_register
class AssembleSqlOperator(Operator):

    command = None
    node_instance = None
    sql_engine = 'flink_sql'

    def __init__(self, command, node_instance):
        super(AssembleSqlOperator, self).__init__()
        self.command = command
        self.node_instance = node_instance

    def execute(self, *args, **kwargs):
        """
        执行入口
        :return:
        """
        return getattr(self, '_{}'.format(self.command), self._default)(*args, **kwargs)

    def _build(self, engine='flink_sql'):
        """
        执行sql构建任务
        :param engine: string 生成sql所需要运行的sql引擎环境 flink_sql/spark_sql
        :return: string 构建好的sql表达式
        """
        return self.node_instance.gen_sql_expression(engine)

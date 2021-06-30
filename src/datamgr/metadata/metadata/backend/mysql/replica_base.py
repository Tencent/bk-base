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
# 用于生成映射表的ReplicaBase。会自动合入MixIn信息。

from collections import Sequence

from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base

from metadata.db_models.meta_service.replica_conf import replica_mixins


class ReplicaMixIn(object):
    """
    映射表补充字段MixIn父类。
    """

    pass


replica_mixin_classes_info = {}
for module in replica_mixins:
    for attr in dir(module):
        item = getattr(module, attr)
        if isinstance(item, type) and issubclass(item, ReplicaMixIn) and item is not ReplicaMixIn:
            replica_mixin_classes_info[attr.split('MixIn')[0]] = item


class ReplicaMeta(DeclarativeMeta):
    """自动生成映射表Model的元类。"""

    def __new__(mcs, name, bases, namespace):
        # 自动添加mixin
        if name == 'Base':
            return super(ReplicaMeta, mcs).__new__(mcs, name, bases, namespace)
        else:
            namespace[str('__abstract__')] = True
            table_args = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8', 'mysql_collate': 'utf8_general_ci'}
            if namespace.get('__table_args__'):
                if isinstance(namespace['__table_args__'], Sequence):
                    table_args_lst = list(namespace['__table_args__'])
                    if isinstance(table_args_lst[-1], dict):
                        table_args_lst[-1].update(table_args)
                    else:
                        table_args_lst.append(table_args)
                else:
                    namespace['__table_args__'].update(table_args)
            namespace['__table_args__'] = table_args

            cls = super(ReplicaMeta, mcs).__new__(mcs, name, tuple(bases), namespace)
            mix_bases = [cls]
            if name in replica_mixin_classes_info:
                mix_bases.insert(0, replica_mixin_classes_info[name])
            mixed_cls = super(ReplicaMeta, mcs).__new__(mcs, str('Replica') + name, tuple(mix_bases), {})
            return mixed_cls


ReplicaBase = declarative_base(metaclass=ReplicaMeta)
metadata = ReplicaBase.metadata
ReplicaBase.db_name = ReplicaBase._db_name = 'bkdata_meta'

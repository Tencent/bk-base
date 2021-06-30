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

import json

from sqlalchemy import text

from metadata.backend.interface import RawBackendType
from metadata.interactor.core import Operator
from metadata.util.orm import get_pk
from metadata_biz.interactor.operate_filters import TagTargetRedundancyFilter


class CommonDgraphOperator(Operator):
    """
    公共Dgraph变更操作者类
    """

    batch_support = True

    def __init__(self, *args, **kwargs):
        super(CommonDgraphOperator, self).__init__(*args, **kwargs)
        # 设置TagTarget过滤器,dgraph不对标签和实体的继承关系做处理
        self.operate_filters.append(TagTargetRedundancyFilter)
        # 兼容:获取后端操作的版本列表，区分不同的backend行为
        self.dgraph_ver = getattr(self.backend_session.backend, str('dgraph_ver'), None)
        self.backend_type = RawBackendType.DGRAPH.value
        self.backend_id = getattr(self.backend_session.backend, str('id'), None)

    def create(self):
        if not self.batch:
            return self.backend_session.upsert(self.item, 'set')
        else:
            return self.backend_session.create(self.items)

    def update(self):
        if not self.batch:
            return self.backend_session.upsert(self.item, 'set', is_exists=True)
        else:
            return self.backend_session.update(self.items)

    def delete(self):
        if not self.batch:
            return self.backend_session.upsert(self.item, 'delete', is_exists=True)
        else:
            return self.backend_session.delete(self.items)

    @property
    def in_scope(self):
        return True


class CommonMySQLReplicaOperator(Operator):
    """
    公共MySQL变更操作者类
    """

    batch_support = True

    def __init__(self, *args, **kwargs):
        super(CommonMySQLReplicaOperator, self).__init__(*args, **kwargs)
        self.backend_type = RawBackendType.MYSQL.value

    def create(self):
        if not self.batch:
            self.backend_session.merge(self.item)
        else:
            self.backend_session.bulk_save_objects(self.items)

    def update(self):
        if not self.batch:
            replica_cls = self.action.item.__class__
            self.backend_session.query(replica_cls).filter(
                getattr(replica_cls, get_pk(replica_cls)) == self.action.linked_record.identifier_value
            ).update(json.loads(self.action.linked_record.changed_data))
        else:
            replica_cls = self.action.items[0].__class__
            pk_name = get_pk(replica_cls)
            instances = self.backend_session.query(replica_cls).filter(
                getattr(replica_cls, pk_name).in_(
                    list({record.identifier_value for record in self.action.linked_records})
                )
            )
            instance_mapping = {str(getattr(instance, pk_name)): instance for instance in instances}
            changed_data_mapping = {
                record.identifier_value: json.loads(record.changed_data) for record in self.action.linked_records
            }
            for pk_value, instance in list(instance_mapping.items()):
                changed_data = changed_data_mapping[pk_value]
                for changed_key, changed_val in list(changed_data.items()):
                    setattr(instance, changed_key, changed_val)
            self.backend_session.bulk_save_objects(instances)

    def delete(self):
        if not self.batch:
            replica_cls = self.action.item.__class__
            self.backend_session.query(replica_cls).filter(
                getattr(replica_cls, get_pk(replica_cls)) == self.action.linked_record.identifier_value
            ).delete()
        else:
            replica_cls = self.action.items[0].__class__
            self.backend_session.query(replica_cls).filter(
                getattr(replica_cls, get_pk(replica_cls)).in_(
                    [record.identifier_value for record in self.action.linked_records]
                )
            ).delete(synchronize_session='fetch')

    def conditional_update(self):
        replica_cls = self.action.item.__class__
        self.backend_session.query(replica_cls).filter(text(self.action.linked_record.conditional_filter)).update(
            json.loads(self.action.linked_record.changed_data), synchronize_session='fetch'
        )

    def conditional_delete(self):
        replica_cls = self.action.item.__class__
        self.backend_session.query(replica_cls).filter(text(self.action.linked_record.conditional_filter)).delete(
            synchronize_session='fetch'
        )

    @property
    def in_scope(self):
        return True

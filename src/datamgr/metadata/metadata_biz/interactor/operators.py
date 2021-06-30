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

from metadata.exc import DgraphNodeNotExistedError, InteractorOperatorError
from metadata.interactor.operators import (
    CommonDgraphOperator,
    CommonMySQLReplicaOperator,
)
from metadata.runtime import rt_context
from metadata.util.i18n import lazy_selfish as _
from metadata_biz.type_system.converters import (
    dataset_related_md_types,
    tag_related_md_types,
)
from metadata_biz.types.tags import Tag


class TagTargetDgraphOperator(CommonDgraphOperator):
    """
    附带Tag.targets边创建的Dgraph变更操作者,监控TagTarget类型的变更
    """

    batch_support = True

    target_md_type_mappings = {}
    # target_md和target_type关系映射
    for k, v in list(tag_related_md_types.items()):
        if isinstance(v['target_type'], set):
            for i in v['target_type']:
                target_md_type_mappings[i] = k
        else:
            target_md_type_mappings[v['target_type']] = k

    def __init__(self, *args, **kwargs):
        super(TagTargetDgraphOperator, self).__init__(*args, **kwargs)
        self.tag_targets = self.items

    def create(self):
        """
        创建TagTarget, 然后补齐Tag和TagTarget之间的关联

        :return:
        """
        super(TagTargetDgraphOperator, self).create()
        tag_target_edge_lst = self.gen_tag_direct_target_obj()
        if tag_target_edge_lst:
            if not self.batch:
                for tag_target_edge in tag_target_edge_lst:
                    self.backend_session.upsert(tag_target_edge, 'set', is_exists=False)
            else:
                self.backend_session.create(tag_target_edge_lst)

    def delete(self):
        """
        删除Tag到TagTarget的关联, 然后删除TagTarget

        :return:
        """
        tag_target_edge_lst = self.gen_tag_direct_target_obj()
        if tag_target_edge_lst:
            if not self.batch:
                for tag_target_edge in tag_target_edge_lst:
                    self.backend_session.upsert(tag_target_edge, 'delete', partial=True)
            else:
                self.backend_session.delete(tag_target_edge_lst, partial=True)
        super(TagTargetDgraphOperator, self).delete()

    def gen_tag_direct_target_obj(self):
        """
        生成需要附加的TagTarget边列表

        :return: list
        """
        tag_target_edge_lst = []
        for md_obj in self.tag_targets:
            # 筛选出实际上的标签关系(而非衍生的标签关系)
            if md_obj.tag_code != md_obj.source_tag_code:
                continue
            md_type = rt_context.md_types_registry[self.target_md_type_mappings[md_obj.target_type]]
            identifier_attr = md_type.metadata['identifier']
            tag = Tag.referred_cls(
                code=md_obj.tag_code,
                targets=[md_type.referred_cls(**{identifier_attr.name: identifier_attr.type(md_obj.target_id)})],
            )
            tag_target_edge_lst.append(tag)
        return tag_target_edge_lst

    @property
    def in_scope(self):
        """
        监听 TagTarget 来处理Tag和Target之间的关系补全

        :return:
        """
        if self.md_type_name in ('TagTarget', 'ReferredTagTarget'):
            return True
        else:
            return False


class LineageDgraphOperator(CommonDgraphOperator):
    """
    附带血缘lineage.descend边的Dgraph变更操作者,监控DataProcessingRelation的变更
    """

    batch_support = True

    dp_ds_query = """
    {dp(func: eq(DataProcessing.processing_id,"%s")) {uid}
    ds(func: eq(%s,"%s")) {uid}
    }"""

    dataset_md_type_mappings = {v['data_set_type']: k for k, v in list(dataset_related_md_types.items())}

    data_set_predicate_name = {'result_table': 'ResultTable.result_table_id', 'raw_data': 'AccessRawData.id'}

    def __init__(self, *args, **kwargs):
        super(LineageDgraphOperator, self).__init__(*args, **kwargs)
        self.dp_relations = self.items

    def create(self):
        super(LineageDgraphOperator, self).create()
        self.maintain_lineage('set')

    def delete(self):
        super(LineageDgraphOperator, self).delete()
        self.maintain_lineage('delete')

    def maintain_lineage(self, action):
        """
        Todo: 迁移到类型系统中统一管理
        """
        modify_content_lst = []
        for dp_relation in self.dp_relations:
            dp, ds = self.query_referred_nodes(dp_relation)
            if dp_relation.data_directing == "input":
                modify_content = ' '.join(['<{}>'.format(ds), '<lineage.descend>', '<{}>'.format(dp), '.'])
            elif dp_relation.data_directing == "output":
                modify_content = ' '.join(['<{}>'.format(dp), '<lineage.descend>', '<{}>'.format(ds), '.'])
            else:
                raise InteractorOperatorError(_('Invalid lineage data directing.'))
            if not self.batch:
                self.backend_session.common_operate([modify_content], action, data_format='rdf', commit_now=False)
            else:
                modify_content_lst.append(modify_content)
        if modify_content_lst:
            self.backend_session.common_operate(modify_content_lst, action, data_format='rdf', commit_now=False)

    def query_referred_nodes(self, md_obj):
        ret = self.backend_session.query(
            self.dp_ds_query
            % (md_obj.processing_id, self.data_set_predicate_name[md_obj.data_set_type], md_obj.data_set_id)
        )
        if not ret['data']['ds']:
            raise DgraphNodeNotExistedError(
                'DataSet {}/{} is not existed.'.format(md_obj.data_set_type, md_obj.data_set_id)
            )
        if not ret['data']['dp']:
            raise DgraphNodeNotExistedError('DP {} is not existed.'.format(md_obj.processing_id))
        dp = ret['data']['dp'][0]['uid']
        ds = ret['data']['ds'][0]['uid']
        return dp, ds

    @property
    def in_scope(self):
        """
        监听 DataProcessingRelation 来补全DataSet之间的血缘关系
        :return:
        """
        if self.md_type_name in ('DataProcessingRelation', 'ReferredDataProcessingRelation'):
            return True
        else:
            return False


class JsonFieldMySQLReplicaOperator(CommonMySQLReplicaOperator):
    batch_support = False

    def update(self):
        if not self.batch:
            changed_info = json.loads(self.action.linked_record.changed_data)
            if 'associated_lz_id' in changed_info:
                changed_info['associated_lz_id'] = json.dumps(changed_info['associated_lz_id'])
            elif 'part_values' in changed_info:
                changed_info['part_values'] = json.dumps(changed_info['part_values'])
            elif 'roles' in changed_info:
                changed_info['roles'] = json.dumps(changed_info['roles'])
            self.action.linked_record.changed_data = json.dumps(changed_info)

        # TODO：是否需要对batch状态下的数据进行操作。
        super(JsonFieldMySQLReplicaOperator, self).update()

    @property
    def in_scope(self):
        if self.batch:
            item = self.action.items[0]
        else:
            item = self.action.item
        if item.__class__.__tablename__ in ('tdw_table', 'tdw_column', 'result_table_field'):
            return True
        else:
            return False

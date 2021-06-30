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
from copy import deepcopy
from functools import partial

from metadata.exc import DgraphNodeNotExistedError, MetaDataTypeConvertError
from metadata.runtime import rt_context
from metadata.type_system.basic_type import Asset
from metadata.type_system.converter import (
    CommonInstanceConverter,
    CustomCommonAttrConverter,
    CustomReferredAttrConverter,
    ReferredAttrFromIdAttrConverter,
)
from metadata.util.common import camel_to_snake
from metadata.util.i18n import lazy_selfish as _
from metadata_biz.types import default_registry
from metadata_biz.types.entities.management import (
    Biz,
    BizCloud,
    BizV1,
    BizV3,
    BKBiz,
    Staff,
)
from metadata_biz.types.tags import Empty, Tag

tag_related_md_types = {
    'ResultTable': {'target_type': 'result_table'},
    'AccessRawData': {'target_type': 'raw_data'},
    'DataProcessing': {
        'target_type': 'data_processing',
    },
    'DataTransferring': {'target_type': 'data_transferring'},
    'ProjectInfo': {'target_type': 'project'},
    'ClusterGroupConfig': {
        'target_type': 'cluster_group',
    },
    'StorageClusterConfig': {
        'target_type': 'storage_cluster',
    },
    'DatabusChannelClusterConfig': {
        'target_type': 'channel_cluster',
    },
    'DatabusConnectorClusterConfig': {
        'target_type': 'connector_cluster',
    },
    'ProcessingClusterConfig': {
        'target_type': 'processing_cluster',
    },
    'DmStandardConfig': {
        'target_type': 'standard',
    },
    'DatamonitorAlertConfig': {
        'target_type': 'alert_config',
    },
    'DmStandardContentConfig': {'target_type': {'detail_data', 'indicator'}},
    'DmmModelInfo': {'target_type': 'data_model'},
}

for name in default_registry:
    if name not in tag_related_md_types:
        tag_related_md_types[name] = {'target_type': camel_to_snake(name)}
    elif isinstance(tag_related_md_types[name]['target_type'], str):
        info = {tag_related_md_types[name]['target_type']}
        info.add(camel_to_snake(name))
        tag_related_md_types[name] = {'target_type': info}
    elif isinstance(tag_related_md_types[name]['target_type'], set):
        tag_related_md_types[name]['target_type'].add(camel_to_snake(name))

dataset_related_md_types = {
    'ResultTable': {'data_set_type': 'result_table'},
    'AccessRawData': {
        'data_set_type': 'raw_data',
    },
}

cluster_related_attr_names = {
    'channel_cluster_config_id': {'storage_type': 'channel'},
    'storage_cluster_config_id': {
        'storage_type': 'storage',
    },
}


class TargetReffedAttrConverter(CustomReferredAttrConverter):
    __abstract__ = True
    target_md_type_mappings = {}
    for k, v in list(tag_related_md_types.items()):
        if isinstance(v['target_type'], set):
            for i in v['target_type']:
                target_md_type_mappings[i] = k
        else:
            target_md_type_mappings[v['target_type']] = k

    def __init__(self, *args, **kwargs):
        super(TargetReffedAttrConverter, self).__init__(*args, **kwargs)
        self.rules = {'target': self.target_attr_process}

    def target_attr_process(
        self,
    ):
        target_type = getattr(self.instance, 'target_type', Empty)
        target_id = getattr(self.instance, 'target_id', Empty)
        if target_type and target_id:
            md_type = rt_context.md_types_registry.get(self.target_md_type_mappings.get(target_type), None)
            if not md_type:
                raise MetaDataTypeConvertError(_('Not supported md type.'))
            identifier_attr = md_type.metadata['identifier']
            if md_type and target_id:
                return md_type.referred_cls, identifier_attr.type(target_id)
            else:
                raise MetaDataTypeConvertError(
                    _('Fail to access target info from target_type {} and target_id {}'.format(target_type, target_id))
                )
        else:
            return Asset.referred_cls, Empty


class TagTargetReffedAttrConverter(TargetReffedAttrConverter):
    pass


class DefaultTargetReffedAttrConverter(TargetReffedAttrConverter):
    target_md_type_mappings = {camel_to_snake(name): name for name in default_registry}


class TagTargetInstanceConverter(CommonInstanceConverter):
    def __init__(self, *args, **kwargs):
        super(TagTargetInstanceConverter, self).__init__(*args, **kwargs)
        self.attr_converters = deepcopy(self.attr_converters)
        self.attr_converters.insert(0, TagTargetReffedAttrConverter)


class DefaultTargetInstanceConverter(CommonInstanceConverter):
    def __init__(self, *args, **kwargs):
        super(DefaultTargetInstanceConverter, self).__init__(*args, **kwargs)
        self.attr_converters = deepcopy(self.attr_converters)
        self.attr_converters.insert(0, DefaultTargetReffedAttrConverter)


class TagReffedAttrConverter(CustomReferredAttrConverter):
    tag_query = """
        {tag(func: eq(Tag.id,"%s")) {code:Tag.code}
        }"""

    def __init__(self, *args, **kwargs):
        super(TagReffedAttrConverter, self).__init__(*args, **kwargs)
        self.rules = {'parent_tag': self.parent_code_attr_process}
        self.backend_session = rt_context.dgraph_session_now

    def parent_code_attr_process(
        self,
    ):
        parent_id = getattr(self.instance, 'parent_id', Empty)
        if parent_id is Empty:
            return Tag.referred_cls, Empty
        parent_code = self.query_referred_tag(parent_id)

        if parent_id is not None and parent_code is not None:
            return Tag.referred_cls, parent_code
        else:
            raise MetaDataTypeConvertError(_('Fail to access tag info from parent_id {}.'.format(parent_id)))

    def query_referred_tag(self, parent_id):
        ret = self.backend_session.query(self.tag_query % parent_id)
        if not ret['data']['tag']:
            raise DgraphNodeNotExistedError('Parent tag with id {} is not existed.'.format(parent_id))
        tag_code = ret['data']['tag'][0]['code']
        return tag_code


class AutoGenMDReffedAttrConverter(CustomReferredAttrConverter):
    biz_query = """
    {biz(func: eq(BKBiz.id,"%s")) {id:BKBiz.id}
        }
    """
    staff_query = """
    {login_name(func: eq(Staff.login_name,"%s")) {login_name:Staff.login_name}
        }
    """

    def __init__(self, *args, **kwargs):
        super(AutoGenMDReffedAttrConverter, self).__init__(*args, **kwargs)
        self.rules = {
            'bk_biz': self.ensure_biz,
            'created_by_person': partial(self.ensure_staff, 'created_by'),
            'updated_by_person': partial(self.ensure_staff, 'updated_by'),
            'request_user': partial(self.ensure_staff, 'request_user_id'),
        }
        self.backend_session = rt_context.dgraph_session_now

    def ensure_biz(self):
        bk_biz_id = getattr(self.instance, 'bk_biz_id', Empty)
        if bk_biz_id is Empty:
            return BKBiz.referred_cls, Empty
        # ret = self.backend_session.query(self.biz_query % bk_biz_id)
        # if not ret['data']['biz']:
        #     self.backend_session.create(BKBiz.referred_cls(id=bk_biz_id))
        return BKBiz.referred_cls, bk_biz_id

    def ensure_staff(self, attr_name):
        login_name = getattr(self.instance, attr_name, Empty)
        if not login_name:
            return Staff.referred_cls, Empty
        # ret = self.backend_session.query(self.staff_query % login_name)
        # if not ret['data']['login_name']:
        #     self.backend_session.create(Staff.referred_cls(login_name=login_name))
        return Staff.referred_cls, login_name


CommonInstanceConverter.attr_converters.insert(0, AutoGenMDReffedAttrConverter)


class BKBizActualBizConverter(CustomReferredAttrConverter):
    def __init__(self, *args, **kwargs):
        super(BKBizActualBizConverter, self).__init__(*args, **kwargs)
        self.rules = {'biz': self.actual_biz_process}

    def actual_biz_process(self):
        actual_biz_type = getattr(self.instance, 'biz_type', Empty)
        actual_biz_id = getattr(self.instance, 'id', Empty)
        if actual_biz_type and actual_biz_id:
            if actual_biz_type == 'v1':
                return BizV1.referred_cls, int(actual_biz_id)
            elif actual_biz_type == 'cloud':
                return BizCloud.referred_cls, int(actual_biz_id)
            elif actual_biz_type == 'v3':
                return BizV3.referred_cls, int(actual_biz_id)
            else:
                raise MetaDataTypeConvertError('Invalid actual biz type.')
        else:
            return Biz, Empty


class BKBizInstanceConverter(CommonInstanceConverter):
    def __init__(self, *args, **kwargs):
        super(BKBizInstanceConverter, self).__init__(*args, **kwargs)
        self.attr_converters = deepcopy(self.attr_converters)
        self.attr_converters.insert(0, BKBizActualBizConverter)


class TagInstanceConverter(CommonInstanceConverter):
    def __init__(self, *args, **kwargs):
        super(TagInstanceConverter, self).__init__(*args, **kwargs)
        self.attr_converters = deepcopy(self.attr_converters)
        self.attr_converters.insert(0, TagReffedAttrConverter)


class DatasetReffedAttrConverter(CustomReferredAttrConverter):
    dataset_md_type_mappings = {v['data_set_type']: k for k, v in list(dataset_related_md_types.items())}

    def __init__(self, *args, **kwargs):
        super(DatasetReffedAttrConverter, self).__init__(*args, **kwargs)
        self.rules = {'data_set': self.dataset_attr_process}

    def dataset_attr_process(self):
        data_set_type = getattr(self.instance, 'data_set_type')
        data_set_id = getattr(self.instance, 'data_set_id')
        md_type = rt_context.md_types_registry.get(self.dataset_md_type_mappings.get(data_set_type), None)
        identifier_attr = md_type.metadata['identifier']
        if md_type and data_set_id:
            return md_type.referred_cls, identifier_attr.type(data_set_id)
        else:
            raise MetaDataTypeConvertError(
                _(
                    'Fail to access target info from data_set_type {} and data_set_id {}'.format(
                        data_set_type, data_set_id
                    )
                )
            )


class DataProcedureRelationInstanceConverter(CommonInstanceConverter):
    def __init__(self, *args, **kwargs):
        super(DataProcedureRelationInstanceConverter, self).__init__(*args, **kwargs)
        self.attr_converters = deepcopy(self.attr_converters)
        self.attr_converters.insert(0, DatasetReffedAttrConverter)
        self.attr_converters.insert(0, StorageReffedAttrConverter)


class StorageReffedAttrConverter(CustomReferredAttrConverter):
    storage_attr_mappings = {v['storage_type']: k for k, v in list(cluster_related_attr_names.items())}

    def __init__(self, *args, **kwargs):
        super(StorageReffedAttrConverter, self).__init__(*args, **kwargs)
        self.rules = {
            'channel_cluster': self.storage_attr_process,
            'storage_cluster': self.storage_attr_process,
        }

    def storage_attr_process(self):
        storage_type = getattr(self.instance, 'storage_type', None)
        if storage_type and self.storage_attr_mappings[storage_type] == self.attr_def.name + '_config_id':
            return self.attr_def.type, getattr(self.instance, self.storage_attr_mappings[storage_type])
        else:
            return None, None


class ReferredAttrFromIdAttrForModelingConverter(ReferredAttrFromIdAttrConverter):
    attr_name_with_id_attr_name = dict(
        (
            ('component', 'component_name'),
            ('step', 'step_name'),
            ('scene', 'scene_name'),
            ('scene_info', 'scene_name'),
            ('action', 'action_name'),
            ('parent_scene', 'parent_scene_name'),
            ('parent_step', 'parent_step_name'),
            ('parent_feature_type', 'parent_feature_type_name'),
            ('algorithm', 'algorithm_name'),
            ('basic_model_generator', 'basic_model_generator_name'),
            ('visualization', 'visualization_name'),
        )
    )

    def __init__(self, *args, **kwargs):
        super(ReferredAttrFromIdAttrConverter, self).__init__(*args, **kwargs)
        self.rules = deepcopy(self.rules)
        if self.attr_def.name in self.attr_name_with_id_attr_name:
            if hasattr(self.instance, self.attr_name_with_id_attr_name[self.attr_def.name]):
                self.rules[self.attr_def.name] = self.from_id

    def from_id(self):
        obj_id = getattr(self.instance, self.attr_name_with_id_attr_name[self.attr_def.name], Empty)
        return self.attr_def.type, obj_id


class ModelSpecialConverter(CommonInstanceConverter):
    def __init__(self, *args, **kwargs):
        super(ModelSpecialConverter, self).__init__(*args, **kwargs)
        orgi_attr_converters = deepcopy(self.attr_converters)
        self.attr_converters = [ReferredAttrFromIdAttrForModelingConverter, DefaultTargetReffedAttrConverter]
        self.attr_converters.extend(orgi_attr_converters)


class ReferredAttrFromIdAttrForDataModelConverter(ReferredAttrFromIdAttrConverter):
    attr_name_with_id_attr_name = dict((('calculation_atom', 'calculation_atom_name'),))

    def __init__(self, *args, **kwargs):
        super(ReferredAttrFromIdAttrConverter, self).__init__(*args, **kwargs)
        self.rules = deepcopy(self.rules)
        if self.attr_def.name in self.attr_name_with_id_attr_name:
            if hasattr(self.instance, self.attr_name_with_id_attr_name[self.attr_def.name]):
                self.rules[self.attr_def.name] = self.from_id

    def from_id(self):
        obj_id = getattr(self.instance, self.attr_name_with_id_attr_name[self.attr_def.name], Empty)
        return self.attr_def.type, obj_id


class DataModelConverter(CommonInstanceConverter):
    def __init__(self, *args, **kwargs):
        super(DataModelConverter, self).__init__(*args, **kwargs)
        orgi_attr_converters = deepcopy(self.attr_converters)
        self.attr_converters = [ReferredAttrFromIdAttrForDataModelConverter, DefaultTargetReffedAttrConverter]
        self.attr_converters.extend(orgi_attr_converters)


class JsonAttrConverter(CustomCommonAttrConverter):
    __abstract__ = True

    def __init__(self, *args, **kwargs):
        super(JsonAttrConverter, self).__init__(*args, **kwargs)

    def json_attr_process(self):
        return json.dumps(getattr(self.instance, self.attr_def.name))


class CommonJsonAttrConverter(JsonAttrConverter):
    def __init__(self, *args, **kwargs):
        super(CommonJsonAttrConverter, self).__init__(*args, **kwargs)
        self.rules = {
            'associated_lz_id': self.json_attr_process,
            'part_values': self.json_attr_process,
            'roles': self.json_attr_process,
        }


class CommonJsonInstanceConverter(CommonInstanceConverter):
    def __init__(self, *args, **kwargs):
        super(CommonJsonInstanceConverter, self).__init__(*args, **kwargs)
        self.attr_converters = deepcopy(self.attr_converters)
        self.attr_converters.insert(0, CommonJsonAttrConverter)

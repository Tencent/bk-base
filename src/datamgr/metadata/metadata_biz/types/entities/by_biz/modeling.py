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
"""
元数据建模样例

所有既有数据源接入元数据都需要建立元模型，以确保元数据的准确性和可用性。具体元模型的构建可以参照样例即。

建模方法：
· 元模型的设计思路类似Django Model，每个实体由数据有若干属性定义，定义元数据所含属性的类型，默认值，校验规则，后端参数等。
· 当前元数据属性可支持的类型包括：unicode，bool，int，long，float，datetime。也可以引用其他元模型。
· 每个元模型都有一个属性被定义为identifier属性，为定义元数据的唯一资源标识符（类似MySql主键）。
"""

import enum
from datetime import datetime

import attr

from metadata.type_system.basic_type import AddOn, Asset, Entity, Relation
from metadata.type_system.core import StringDeclaredType, as_metadata
from metadata.util.common import Empty
from metadata_biz.types.entities.by_biz.data_flow import DataflowInfo, DataflowNodeInfo
from metadata_biz.types.entities.data_set import DataSet, ResultTable
from metadata_biz.types.entities.infrastructure import StorageClusterConfig
from metadata_biz.types.entities.management import BKBiz, ProjectInfo
from metadata_biz.types.entities.preference import Preference
from metadata_biz.types.entities.procedure import DataProcessing, Procedure
from metadata_biz.types.entities.storage import Storage

python_string_type = str
default_json = '{}'


class SampleFeatureGenerateType(enum.Enum):
    ORIGIN = 'origin'
    GENERATE = 'generate'


class PublicType(enum.Enum):
    PUBLIC = 'public'
    PRIVATE = 'private'


class SampleType(enum.Enum):
    TIMESERIES = 'timeseries'


class StorageGenerateType(enum.Enum):
    SYSTEM = 'system'


class TimeseriesSampleFragmentGenerateType(enum.Enum):
    MANUAL = 'manual'
    SAMPLE_FEEDBACK = 'sample_feedback'


class TimeseriesSampleFragmentLabelType(enum.Enum):
    TIMESERIES = 'timeseries'


class RunStatus(enum.Enum):
    NOT_RUN = 'not_run'
    PENDING = 'pending'
    RUNNING = 'running'
    SUCCESS = 'success'
    FAILED = 'failed'
    INIT = 'init'


class OperateStatus(enum.Enum):
    INITED = 'init'
    UPDATED = 'update'
    FINESHED = 'finished'


class OriginFeatureRequireType(enum.Enum):
    REQUIRED = 'required'
    NOT_REQUIRED = 'not_required'
    INNER_USED = 'inner_used'


class SceneStatus(enum.Enum):
    DEVELOPING = 'developing'
    FINISHED = 'finished'


@as_metadata
@attr.s
class SceneInfo(Preference):
    scene_name = attr.ib(type=python_string_type, metadata={'identifier': True})
    scene_alias = attr.ib(type=python_string_type)
    scene_type = attr.ib(type=python_string_type)
    parent_scene_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    description = attr.ib(type=python_string_type, default='')
    scene_index = attr.ib(type=int, default=1)
    scene_level = attr.ib(type=int, default=1)
    status = attr.ib(type=python_string_type, default='developing')
    brief_info = attr.ib(type=python_string_type, default='')
    properties = attr.ib(type=python_string_type, default='')
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    parent_scene = attr.ib(type=StringDeclaredType.typed('SceneInfo'), default=Empty)


@as_metadata
@attr.s
class SampleSet(DataSet):
    __tablename__ = 'sample_set'

    id = attr.ib(type=int, metadata={'identifier': True})
    sample_set_name = attr.ib(type=python_string_type)
    sample_set_alias = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    project_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    project = attr.ib(type=ProjectInfo)
    scene_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    sample_feedback = attr.ib(type=bool)
    sample_feedback_config = attr.ib(type=python_string_type)
    active = attr.ib(type=bool, metadata={'dgraph': {'index': ['bool']}})
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    sample_size = attr.ib(type=int)
    processing_cluster_id = attr.ib(type=int)
    storage_cluster_id = attr.ib(type=int)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    properties = attr.ib(type=python_string_type, default=default_json)
    description = attr.ib(type=python_string_type, default='', metadata={'dgraph': {'index': ['trigram']}})
    sensitivity = attr.ib(
        type=python_string_type, default=PublicType.PRIVATE.value, metadata={'dgraph': {'index': ['exact']}}
    )
    sample_type = attr.ib(
        type=python_string_type, default=SampleType.TIMESERIES.value, metadata={'dgraph': {'index': ['exact']}}
    )
    sample_latest_time = attr.ib(type=datetime, default=None)
    commit_status = attr.ib(type=str, default='init')
    scene_info = attr.ib(type=SceneInfo, default=Empty)
    modeling_type = attr.ib(type=python_string_type, default='aiops')


@as_metadata
@attr.s
class SampleSetStoreDataset(Relation):
    id = attr.ib(type=int, metadata={'identifier': True})
    sample_set_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    sample_set = attr.ib(type=SampleSet)
    data_set_id = attr.ib(type=str, metadata={'dgraph': {'index': ['exact']}})
    data_set_type = attr.ib(type=str, metadata={'dgraph': {'index': ['exact']}})
    data_set = attr.ib(type=DataSet)
    role = attr.ib(type=str, metadata={'dgraph': {'index': ['exact']}})
    created_by = attr.ib(type=str)
    updated_by = attr.ib(type=str)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class TimeseriesSampleConfig(AddOn):
    id = attr.ib(type=int, metadata={'identifier': True})
    sample_set_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    sample_set = attr.ib(type=SampleSet)
    start_time = attr.ib(type=datetime)
    end_time = attr.ib(type=datetime)
    ts_depend_config = attr.ib(type=python_string_type)
    ts_freq_config = attr.ib(type=python_string_type)
    properties = attr.ib(type=python_string_type)
    description = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    ts_depend = attr.ib(type=python_string_type, default='0d')
    ts_freq = attr.ib(type=python_string_type, default='0')


@as_metadata
@attr.s
class TimeseriesSampleFragments(AddOn):
    id = attr.ib(type=python_string_type, metadata={'identifier': True})
    start_time = attr.ib(type=datetime)
    end_time = attr.ib(type=datetime)
    labels = attr.ib(type=python_string_type)
    result_table_id = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    result_table = attr.ib(type=ResultTable)
    sample_set_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    sample_set = attr.ib(type=SampleSet)
    group_fields = attr.ib(type=python_string_type)
    group_dimension = attr.ib(type=python_string_type)
    line_id = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    properties = attr.ib(type=python_string_type)
    sample_size = attr.ib(type=int)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    generate_type = attr.ib(type=python_string_type, default=TimeseriesSampleFragmentGenerateType.MANUAL.value)
    labels_type = attr.ib(
        type=python_string_type,
        default=TimeseriesSampleFragmentLabelType.TIMESERIES.value,
        metadata={'dgraph': {'index': ['exact']}},
    )
    status = attr.ib(type=python_string_type, default='developing', metadata={'dgraph': {'index': ['exact']}})


@as_metadata
@attr.s
class TimeseriesSampleLabel(AddOn):
    id = attr.ib(type=python_string_type, metadata={'identifier': True})
    start = attr.ib(type=datetime)
    end = attr.ib(type=datetime)
    remark = attr.ib(type=python_string_type)
    result_table_id = attr.ib(type=python_string_type)
    sample_set_id = attr.ib(type=int)
    line_id = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    anomaly_label = attr.ib(type=python_string_type, default="1")
    anomaly_label_type = attr.ib(type=python_string_type, default='manual')
    status = attr.ib(type=str, default='init')
    label_size = attr.ib(type=int, default=0)


@as_metadata
@attr.s
class SampleResultTable(Relation):
    id = attr.ib(type=python_string_type, metadata={'identifier': True})
    result_table_id = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    result_table = attr.ib(type=ResultTable)
    sample_set_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    sample_set = attr.ib(type=SampleSet)
    fields_mapping = attr.ib(type=python_string_type)
    bk_biz_id = attr.ib(type=int)
    bk_biz = attr.ib(type=BKBiz)
    properties = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    group_config = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    status = attr.ib(type=python_string_type, default='init', metadata={'dgraph': {'index': ['exact']}})
    curve_type = attr.ib(type=python_string_type, default='single')
    ts_freq = attr.ib(type=int, default=0)
    protocol_version = attr.ib(type=python_string_type, default='1.3.2', metadata={'dgraph': {'index': ['exact']}})


@as_metadata
@attr.s
class SampleFeatures(Entity):
    id = attr.ib(type=python_string_type, metadata={'identifier': True})
    sample_set_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    sample_set = attr.ib(type=SampleSet)
    field_name = attr.ib(type=python_string_type)
    field_alias = attr.ib(type=python_string_type)
    description = attr.ib(type=python_string_type)
    field_index = attr.ib(type=int)
    field_type = attr.ib(type=python_string_type)
    origin = attr.ib(type=python_string_type)
    attr_type = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    properties = attr.ib(type=python_string_type)
    field_evaluation = attr.ib(type=python_string_type)
    parents = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    feature_transform_node_id = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    generate_type = attr.ib(
        type=python_string_type,
        default=SampleFeatureGenerateType.ORIGIN.value,
        metadata={'dgraph': {'index': ['exact']}},
    )
    active = attr.ib(type=bool, default=True, metadata={'dgraph': {'index': ['bool']}})
    is_dimension = attr.ib(type=bool, default=False)
    require_type = attr.ib(type=python_string_type, default=OriginFeatureRequireType.REQUIRED.value)
    status = attr.ib(type=python_string_type, default='init', metadata={'dgraph': {'index': ['exact']}})


@as_metadata
@attr.s
class FeatureTemplate(Relation):
    id = attr.ib(type=int, metadata={'identifier': True})
    template_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    template_alias = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    properties = attr.ib(type=python_string_type)
    scene_name = attr.ib(
        type=python_string_type,
    )
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    active = attr.ib(type=bool, default=True, metadata={'dgraph': {'index': ['bool']}})
    status = attr.ib(type=python_string_type, default='developing', metadata={'dgraph': {'index': ['exact']}})
    scene_info = attr.ib(type=SceneInfo, default=Empty)


@as_metadata
@attr.s
class FeatureTransformInfo(Relation):
    id = attr.ib(type=int, metadata={'identifier': True})
    sample_set_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    sample_set = attr.ib(type=SampleSet)
    description = attr.ib(type=python_string_type)
    status = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    feature_template_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    feature_template = attr.ib(type=FeatureTemplate)
    scene_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    active = attr.ib(type=bool, metadata={'dgraph': {'index': ['bool']}})
    properties = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    run_status = attr.ib(
        type=python_string_type, default=RunStatus.NOT_RUN.value, metadata={'dgraph': {'index': ['exact']}}
    )
    scene_info = attr.ib(type=SceneInfo, default=Empty)


@as_metadata
@attr.s
class FeatureTransformInfoStage(Relation):
    id = attr.ib(type=int, metadata={'identifier': True})
    sample_set_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    sample_set = attr.ib(type=SampleSet)
    description = attr.ib(type=python_string_type)
    status = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    feature_template_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    feature_template = attr.ib(type=FeatureTemplate)
    scene_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    active = attr.ib(type=bool, metadata={'dgraph': {'index': ['bool']}})
    properties = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    run_status = attr.ib(
        type=python_string_type, default=RunStatus.NOT_RUN.value, metadata={'dgraph': {'index': ['exact']}}
    )
    scene_info = attr.ib(type=SceneInfo, default=Empty)


@as_metadata
@attr.s
class ActionType(AddOn):
    action_type = attr.ib(type=python_string_type, metadata={'identifier': True})
    action_type_alias = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    parent_action_type = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    icon = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    action_type_index = attr.ib(type=int, default=1)
    parent_action_type_obj = attr.ib(type=StringDeclaredType.typed('ActionType'), default=Empty)


@as_metadata
@attr.s
class ActionInfo(Procedure):
    action_name = attr.ib(type=python_string_type, metadata={'identifier': True})
    action_alias = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    description = attr.ib(type=python_string_type)
    status = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    logic_config = attr.ib(type=python_string_type)
    target_id = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    target_type = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    input_config = attr.ib(type=python_string_type, default='{}')
    output_config = attr.ib(type=python_string_type, default='{}')
    target = attr.ib(type=Asset, default=Empty)
    generate_type = attr.ib(type=python_string_type, default='system')


@as_metadata
@attr.s
class ActionInputArg(AddOn):
    id = attr.ib(type=int, metadata={'identifier': True})
    arg_name = attr.ib(type=python_string_type)
    action_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    arg_alias = attr.ib(type=python_string_type)
    data_type = attr.ib(type=python_string_type)
    properties = attr.ib(type=python_string_type)
    description = attr.ib(type=python_string_type)
    default_value = attr.ib(type=python_string_type)
    advance_config = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    arg_index = attr.ib(type=python_string_type, default=1)
    action = attr.ib(type=ActionInfo, default=Empty)


@as_metadata
@attr.s
class ActionOutputField(AddOn):
    id = attr.ib(type=int, metadata={'identifier': True})
    action_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    field_name = attr.ib(type=python_string_type)
    field_alias = attr.ib(type=python_string_type)
    data_type = attr.ib(type=python_string_type)
    properties = attr.ib(type=python_string_type)
    description = attr.ib(type=python_string_type)
    default_value = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    field_index = attr.ib(type=int, default=1)
    action = attr.ib(type=ActionInfo, default=Empty)
    is_dimension = attr.ib(type=bool, default=False)


@as_metadata
@attr.s
class ModelingStep(Procedure):
    step_name = attr.ib(type=python_string_type, metadata={'identifier': True})
    step_alias = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    description = attr.ib(type=python_string_type)
    brief_info = attr.ib(type=python_string_type)
    step_type = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    step_icon = attr.ib(type=python_string_type)
    properties = attr.ib(type=python_string_type)
    parent_step_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    step_level = attr.ib(type=int, default=1)
    step_index = attr.ib(type=int, default=1)
    active = attr.ib(type=bool, default=True, metadata={'dgraph': {'index': ['bool']}})
    parent_step = attr.ib(type=StringDeclaredType.typed('ModelingStep'), default=Empty)


@as_metadata
@attr.s
class FeatureTransformNode(Procedure):
    id = attr.ib(type=python_string_type, metadata={'identifier': True})
    feature_info_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    action_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    input_config = attr.ib(type=python_string_type)
    output_config = attr.ib(type=python_string_type)
    execute_config = attr.ib(type=python_string_type)
    node_index = attr.ib(type=int)
    step_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    properties = attr.ib(type=python_string_type)
    node_config = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    run_status = attr.ib(
        type=python_string_type, default=RunStatus.NOT_RUN.value, metadata={'dgraph': {'index': ['exact']}}
    )
    operate_status = attr.ib(
        type=python_string_type, default=OperateStatus.INITED.value, metadata={'dgraph': {'index': ['exact']}}
    )
    active = attr.ib(type=bool, default=True, metadata={'dgraph': {'index': ['bool']}})
    select = attr.ib(type=bool, default=True, metadata={'dgraph': {'index': ['bool']}})
    step = attr.ib(type=ModelingStep, default=Empty)
    action = attr.ib(type=ActionInfo, default=Empty)
    status = attr.ib(type=python_string_type, default='init', metadata={'dgraph': {'index': ['exact']}})


@as_metadata
@attr.s
class FeatureType(AddOn):
    feature_type_name = attr.ib(type=python_string_type, metadata={'identifier': True})
    feature_type_alias = attr.ib(type=python_string_type)
    type_index = attr.ib(type=int)
    parent_feature_type_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    description = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    parent_feature_type = attr.ib(type=StringDeclaredType.typed('FeatureType'), default=Empty)


@as_metadata
@attr.s
class Feature(AddOn):
    feature_name = attr.ib(type=python_string_type, metadata={'identifier': True})
    feature_alias = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    feature_type = attr.ib(type=python_string_type)
    feature_type_obj = attr.ib(type=FeatureType)
    description = attr.ib(type=python_string_type)
    class_name = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class FeatureTemplateNode(Relation):
    id = attr.ib(type=int, metadata={'identifier': True})
    node_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    node_alias = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    node_index = attr.ib(type=int)
    default_node_config = attr.ib(type=python_string_type)
    properties = attr.ib(type=python_string_type)
    step_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    action_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    feature_template_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    feature_template = attr.ib(type=FeatureTemplate)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    active = attr.ib(type=bool, default=True, metadata={'dgraph': {'index': ['bool']}})
    step = attr.ib(type=ModelingStep, default=Empty)
    action = attr.ib(type=ActionInfo, default=Empty)


@as_metadata
@attr.s
class SceneSampleOriginFeature(Relation):
    id = attr.ib(type=int, metadata={'identifier': True})
    scene_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    field_name = attr.ib(type=python_string_type)
    field_alias = attr.ib(type=python_string_type)
    description = attr.ib(type=python_string_type)
    field_index = attr.ib(type=int)
    is_dimension = attr.ib(type=bool)
    field_type = attr.ib(type=python_string_type)
    active = attr.ib(type=bool)
    attr_type = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    properties = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    require_type = attr.ib(
        type=python_string_type,
        default=OriginFeatureRequireType.REQUIRED.value,
        metadata={'dgraph': {'index': ['exact']}},
    )
    scene = attr.ib(type=SceneInfo, default=Empty)


@as_metadata
@attr.s
class ModelInfo(DataSet):
    model_id = attr.ib(type=python_string_type, metadata={'identifier': True})
    model_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact', 'trigram']}})
    model_alias = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    sample_set_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    sample_set = attr.ib(type=SampleSet)
    created_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    updated_by = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    project_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    project = attr.ib(type=ProjectInfo)
    train_mode = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    scene = attr.ib(type=SceneInfo, default=Empty)
    description = attr.ib(type=python_string_type, default='')
    model_type = attr.ib(type=python_string_type, default='process')
    sensitivity = attr.ib(type=python_string_type, default='private', metadata={'dgraph': {'index': ['exact']}})
    active = attr.ib(type=bool, default=True, metadata={'dgraph': {'index': ['bool']}})
    properties = attr.ib(type=python_string_type, default='{}')
    scene_name = attr.ib(type=python_string_type, default='', metadata={'dgraph': {'index': ['exact']}})
    status = attr.ib(type=python_string_type, default='developing', metadata={'dgraph': {'index': ['exact']}})
    run_env = attr.ib(type=python_string_type, default='supervised')
    input_standard_config = attr.ib(type=python_string_type, default='{}')
    output_standard_config = attr.ib(type=python_string_type, default='{}')
    execute_standard_config = attr.ib(type=python_string_type, default='{}')
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    protocol_version = attr.ib(type=python_string_type, default='1.0')
    sample_type = attr.ib(type=python_string_type, default='timeseries')
    modeling_type = attr.ib(type=python_string_type, default='aiops')


@as_metadata
@attr.s
class ModelTemplate(Preference):
    id = attr.ib(type=int, metadata={'identifier': True})
    template_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    scene_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    template_alias = attr.ib(
        type=python_string_type,
    )
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    description = attr.ib(type=python_string_type, default='')
    properties = attr.ib(type=python_string_type, default='{}')
    status = attr.ib(type=python_string_type, default='developing', metadata={'dgraph': {'index': ['exact']}})
    active = attr.ib(type=bool, default=True, metadata={'dgraph': {'index': ['bool']}})
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    scene = attr.ib(type=SceneInfo, default=Empty)


@as_metadata
@attr.s
class AlgorithmType(AddOn):
    algorithm_type = attr.ib(type=python_string_type, metadata={'identifier': True})
    algorithm_type_alias = attr.ib(
        type=python_string_type,
    )
    algorithm_type_index = attr.ib(
        type=int,
    )
    parent_algorithm_type = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    parent_algorithm_type_obj = attr.ib(type=StringDeclaredType.typed('AlgorithmType'), default=Empty)


@as_metadata
@attr.s
class Algorithm(DataSet):
    algorithm_name = attr.ib(type=python_string_type, metadata={'identifier': True})
    algorithm_alias = attr.ib(type=python_string_type)
    description = attr.ib(type=python_string_type)
    algorithm_original_name = attr.ib(type=python_string_type)
    algorithm_type = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    category_type = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    project_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    run_env = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    framework = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    algorithm_type_obj = attr.ib(type=StringDeclaredType.typed('AlgorithmType'), default=Empty)
    generate_type = attr.ib(type=python_string_type, default='system')
    sensitivity = attr.ib(type=python_string_type, default='private', metadata={'dgraph': {'index': ['exact']}})
    protocol_version = attr.ib(type=python_string_type, default='1.0', metadata={'dgraph': {'index': ['exact']}})
    scene_name = attr.ib(type=python_string_type, default='custom')


@as_metadata
@attr.s
class ModelExperiment(Procedure):
    experiment_id = attr.ib(type=int, metadata={'identifier': True})
    experiment_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    experiment_alias = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    description = attr.ib(
        type=python_string_type,
    )
    project_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    project = attr.ib(
        type=ProjectInfo,
    )
    model_id = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    model = attr.ib(type=ModelInfo)
    template_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    template = attr.ib(type=ModelTemplate)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    sample_latest_time = attr.ib(type=datetime, default=None)
    status = attr.ib(type=python_string_type, default='developing')
    pass_type = attr.ib(type=python_string_type, default='not_confirmed', metadata={'dgraph': {'index': ['exact']}})
    active = attr.ib(type=bool, default=True, metadata={'dgraph': {'index': ['bool']}})
    experiment_config = attr.ib(type=python_string_type, default='{}')
    model_file_config = attr.ib(type=python_string_type, default='{}')
    execute_config = attr.ib(type=python_string_type, default='{}')
    properties = attr.ib(type=python_string_type, default='{}')
    experiment_index = attr.ib(type=int, default=1)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    protocol_version = attr.ib(type=python_string_type, default='1.0')
    experiment_training = attr.ib(type=bool, default=False, metadata={'dgraph': {'index': ['bool']}})
    continuous_training = attr.ib(type=bool, default=False, metadata={'dgraph': {'index': ['bool']}})


@as_metadata
@attr.s
class ModelExperimentNode(Procedure):
    node_id = attr.ib(type=python_string_type, metadata={'identifier': True})
    model_id = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    model_experiment_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    model_experiment = attr.ib(type=ModelExperiment)
    node_name = attr.ib(
        type=python_string_type,
    )
    node_alias = attr.ib(
        type=python_string_type,
    )
    step_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    action_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    node_role = attr.ib(
        type=python_string_type,
    )
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    node_index = attr.ib(type=int, default=1)
    node_config = attr.ib(type=python_string_type, default='{}')
    properties = attr.ib(type=python_string_type, default='{}')
    active = attr.ib(type=bool, default=True, metadata={'dgraph': {'index': ['bool']}})
    input_config = attr.ib(type=python_string_type, default='{}')
    output_config = attr.ib(type=python_string_type, default='{}')
    execute_config = attr.ib(type=python_string_type, default='{}')
    algorithm_config = attr.ib(type=python_string_type, default='{}')
    run_status = attr.ib(type=python_string_type, default='not_run', metadata={'dgraph': {'index': ['exact']}})
    operate_status = attr.ib(type=python_string_type, default='init')
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    step = attr.ib(type=ModelingStep, default=Empty)
    action = attr.ib(type=ActionInfo, default=Empty)


@as_metadata
@attr.s
class ModelExperimentNodeUpstream(Procedure):
    id = attr.ib(type=int, metadata={'identifier': True})
    parent_node_id = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    node_id = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    node = attr.ib(type=ModelExperimentNode)
    model_id = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    model_experiment_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    model_experiment = attr.ib(type=ModelExperiment)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    parent_node = attr.ib(type=ModelExperimentNode, default=Empty)
    properties = attr.ib(type=python_string_type, default='')
    complete_type = attr.ib(type=python_string_type, default='success')
    concurrent_type = attr.ib(type=python_string_type, default='sequence')
    execute_index = attr.ib(type=int, default=1)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class ModelRelease(AddOn):
    id = attr.ib(type=int, metadata={'identifier': True})
    model_id = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    model = attr.ib(type=ModelInfo)
    model_experiment_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    model_experiment = attr.ib(type=ModelExperiment)
    model_config_template = attr.ib(type=python_string_type)
    basic_model_id = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    active = attr.ib(type=bool, default=True, metadata={'dgraph': {'index': ['bool']}})
    description = attr.ib(type=python_string_type, default='')
    publish_status = attr.ib(type=python_string_type, default='latest', metadata={'dgraph': {'index': ['exact']}})
    version_index = attr.ib(type=int, default=1)
    protocol_version = attr.ib(type=python_string_type, default='1.0')


@as_metadata
@attr.s
class ModelInstance(Procedure):
    id = attr.ib(type=python_string_type, metadata={'identifier': True})
    model_release_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    model_release = attr.ib(type=ModelRelease)
    model_id = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    model_experiment_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    project_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    flow_id = attr.ib(type=int)
    flow = attr.ib(type=DataflowInfo)
    flow_node_id = attr.ib(type=int)
    flow_node = attr.ib(type=DataflowNodeInfo)
    instance_config = attr.ib(type=python_string_type)
    data_processing_id = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    data_processing = attr.ib(type=DataProcessing)
    basic_model_id = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    active = attr.ib(type=bool, default=True, metadata={'dgraph': {'index': ['bool']}})
    upgrade_config = attr.ib(type=python_string_type, default='')
    sample_feedback_config = attr.ib(type=python_string_type, default='')
    serving_mode = attr.ib(type=python_string_type, default='realtime')
    instance_status = attr.ib(type=python_string_type, default='', metadata={'dgraph': {'index': ['exact']}})
    execute_config = attr.ib(type=python_string_type, default='')
    input_result_table_ids = attr.ib(type=python_string_type, default='')
    output_result_table_ids = attr.ib(type=python_string_type, default='')
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    protocol_version = attr.ib(type=python_string_type, default='1.0')


@as_metadata
@attr.s
class ModelTemplateNode(Procedure):
    id = attr.ib(type=int, metadata={'identifier': True})
    template_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    template = attr.ib(
        type=ModelTemplate,
    )
    step_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    action_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    node_name = attr.ib(type=python_string_type, default='')
    node_alias = attr.ib(type=python_string_type, default='')
    node_index = attr.ib(type=int, default=1)
    default_node_config = attr.ib(type=python_string_type, default='{}')
    properties = attr.ib(type=python_string_type, default='{}')
    active = attr.ib(type=bool, default=True, metadata={'dgraph': {'index': ['bool']}})
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    step = attr.ib(type=ModelingStep, default=Empty)
    action = attr.ib(type=ActionInfo, default=Empty)


@as_metadata
@attr.s
class ModelTemplateNodeUpstream(Procedure):
    id = attr.ib(type=int, metadata={'identifier': True})
    node_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    node = attr.ib(type=ModelTemplateNode)
    parent_node_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    parent_node = attr.ib(type=ModelTemplateNode, default=Empty)
    properties = attr.ib(type=python_string_type, default='{}')
    active = attr.ib(type=bool, default=True, metadata={'dgraph': {'index': ['bool']}})
    complete_type = attr.ib(type=python_string_type, default='success')
    concurrent_type = attr.ib(type=python_string_type, default='sequence')
    execute_index = attr.ib(type=int, default=1)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class StorageModel(Storage):
    id = attr.ib(type=int, metadata={'identifier': True})
    model_id = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    model = attr.ib(type=ModelInfo)
    physical_table_name = attr.ib(type=python_string_type)
    expires = attr.ib(type=python_string_type)
    storage_config = attr.ib(type=python_string_type)
    storage_cluster_config_id = attr.ib(type=int)
    storage_cluster_config = attr.ib(type=StorageClusterConfig)
    storage_cluster_name = attr.ib(type=python_string_type)
    storage_cluster_type = attr.ib(type=python_string_type)
    storage_cluster = attr.ib(type=StorageClusterConfig)
    active = attr.ib(type=bool, metadata={'dgraph': {'index': ['bool']}})
    content_type = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    generate_type = attr.ib(type=python_string_type, default='system')
    priority = attr.ib(type=int, default=0)


@as_metadata
@attr.s
class SceneStepAction(Relation):
    id = attr.ib(type=int, metadata={'identifier': True})
    action_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    step_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    scene_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    active = attr.ib(type=bool, default=True)
    scene = attr.ib(type=SceneInfo, default=Empty)
    step = attr.ib(type=ModelingStep, default=Empty)
    action = attr.ib(type=ActionInfo, default=Empty)


@as_metadata
@attr.s
class ActionVisual(Entity):
    action_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    id = attr.ib(type=int, metadata={'identifier': True})
    visual_name = attr.ib(type=python_string_type)
    visual_index = attr.ib(type=int)
    brief_info = attr.ib(type=python_string_type)
    scene_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    description = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    scene = attr.ib(type=SceneInfo, default=Empty)


@as_metadata
@attr.s
class ActionVisualComponent(Entity):
    component_name = attr.ib(type=python_string_type, metadata={'identifier': True})
    component_alias = attr.ib(type=python_string_type)
    component_type = attr.ib(type=python_string_type)
    description = attr.ib(type=python_string_type)
    remark = attr.ib(type=python_string_type)
    properties = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class ActionVisualComponentRelation(Relation):
    id = attr.ib(type=int, metadata={'identifier': True})
    action_visual_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    component_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    component = attr.ib(type=ActionVisualComponent)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    action_visual = attr.ib(type=ActionVisual, default=Empty)


@as_metadata
@attr.s
class FeatureTemplateNodeUpstream(Relation):
    id = attr.ib(type=int, metadata={'identifier': True})
    node_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    node = attr.ib(type=ModelTemplateNode)
    parent_node_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    parent_node = attr.ib(type=ModelTemplateNode, default=Empty)
    properties = attr.ib(type=python_string_type, default='{}')
    active = attr.ib(type=bool, default=True, metadata={'dgraph': {'index': ['bool']}})
    complete_type = attr.ib(type=python_string_type, default='success')
    concurrent_type = attr.ib(type=python_string_type, default='sequence')
    execute_index = attr.ib(type=int, default=1)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


# 2020.09.04新增
@as_metadata
@attr.s
class AlgorithmDemo(Entity):
    algorithm_demo_name = attr.ib(type=python_string_type, metadata={'identifier': True})
    algorithm_demo_alias = attr.ib(type=python_string_type)
    description = attr.ib(type=python_string_type)
    algorithm_type = attr.ib(type=python_string_type)
    logic = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    config = attr.ib(type=python_string_type, default='{}')
    execute_config = attr.ib(type=python_string_type, default='{}')
    properties = attr.ib(type=python_string_type, default='{}')
    demo_type = attr.ib(type=python_string_type, default='')


@as_metadata
@attr.s
class AlgorithmVersion(Entity):
    id = attr.ib(type=int, metadata={'identifier': True})
    algorithm_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    algorithm = attr.ib(type=Algorithm)
    logic = attr.ib(type=python_string_type)
    config = attr.ib(type=python_string_type)
    execute_config = attr.ib(type=python_string_type)
    properties = attr.ib(type=python_string_type)
    description = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    version = attr.ib(type=int, default=1, metadata={'dgraph': {'index': ['int']}})
    status = attr.ib(type=python_string_type, default='developing')


@as_metadata
@attr.s
class BasicModel(Entity):
    basic_model_id = attr.ib(type=python_string_type, metadata={'identifier': True})
    basic_model_name = attr.ib(type=python_string_type)
    basic_model_alias = attr.ib(type=python_string_type)
    algorithm_name = attr.ib(type=python_string_type)
    algorithm_version = attr.ib(type=int)
    description = attr.ib(type=python_string_type)
    experiment_id = attr.ib(type=int)
    experiment_instance_id = attr.ib(type=int)
    node = attr.ib(type=ModelExperimentNode)
    model_id = attr.ib(type=python_string_type)
    source = attr.ib(type=python_string_type)
    sample_latest_time = attr.ib(type=datetime)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    config = attr.ib(type=python_string_type, default='{}')
    execute_config = attr.ib(type=python_string_type, default='{}')
    properties = attr.ib(type=python_string_type, default='{}')
    status = attr.ib(type=python_string_type, default='developing')
    active = attr.ib(type=bool, default=True, metadata={'dgraph': {'index': ['bool']}})


@as_metadata
@attr.s
class BasicModelGenerator(Entity):
    generator_name = attr.ib(type=python_string_type, metadata={'identifier': True})
    generator_alias = attr.ib(type=python_string_type)
    generator_type = attr.ib(type=python_string_type)
    description = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    config = attr.ib(type=python_string_type, default='{}')
    execute_config = attr.ib(type=python_string_type, default='{}')
    properties = attr.ib(type=python_string_type, default='{}')
    active = attr.ib(type=bool, default=True, metadata={'dgraph': {'index': ['bool']}})


@as_metadata
@attr.s
class CommonEnum(Entity):
    id = attr.ib(type=int, metadata={'identifier': True})
    enum_name = attr.ib(type=python_string_type)
    enum_alias = attr.ib(type=python_string_type)
    enum_category = attr.ib(type=python_string_type)
    properties = attr.ib(type=python_string_type)
    description = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class ExperimentInstance(Entity):
    id = attr.ib(type=int, metadata={'identifier': True})
    experiment_id = attr.ib(type=int)
    experiment = attr.ib(type=ModelExperiment)
    model_id = attr.ib(type=python_string_type)
    description = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    runtime_info = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    generate_type = attr.ib(type=python_string_type, default='system')
    training_method = attr.ib(type=python_string_type, default='manual')
    status = attr.ib(type=python_string_type, default='developing')
    run_status = attr.ib(type=python_string_type, default='init')
    config = attr.ib(type=python_string_type, default='{}')
    execute_config = attr.ib(type=python_string_type, default='{}')
    properties = attr.ib(type=python_string_type, default='{}')
    active = attr.ib(type=bool, default=True, metadata={'dgraph': {'index': ['bool']}})
    protocol_version = attr.ib(type=python_string_type, default='1.3.2', metadata={'dgraph': {'index': ['hash']}})


@as_metadata
@attr.s
class ProcessingModel(Entity):
    id = attr.ib(type=int, metadata={'identifier': True})
    model_id = attr.ib(type=python_string_type)
    description = attr.ib(type=python_string_type)
    processing_cluster_config_id = attr.ib(type=int)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    processing_config = attr.ib(type=python_string_type, default='{}')
    active = attr.ib(type=bool, default=True, metadata={'dgraph': {'index': ['bool']}})


@as_metadata
@attr.s
class TrainingStrategy(Entity):
    id = attr.ib(type=int, metadata={'identifier': True})
    experiment_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    experiment = attr.ib(type=ModelExperiment)
    node_id = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    node = attr.ib(type=ModelExperimentNode)
    basic_model_generator_name = attr.ib(type=str, metadata={'dgraph': {'index': ['exact']}})
    basic_model_generator = attr.ib(type=BasicModelGenerator)
    description = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    config = attr.ib(type=python_string_type, default='{}')
    execute_config = attr.ib(type=python_string_type, default='{}')
    properties = attr.ib(type=python_string_type, default='{}')
    active = attr.ib(type=bool, default=True, metadata={'dgraph': {'index': ['bool']}})


@as_metadata
@attr.s
class TrainingStrategyTemplate(Entity):
    training_strategy_template_name = attr.ib(type=python_string_type, metadata={'identifier': True})
    description = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    config = attr.ib(type=python_string_type, default='{}')
    execute_config = attr.ib(type=python_string_type, default='{}')
    properties = attr.ib(type=python_string_type, default='{}')
    active = attr.ib(type=bool, default=True, metadata={'dgraph': {'index': ['bool']}})


# ai_ops v1.2 added at 2020.12.30
@as_metadata
@attr.s
class SampleCurve(Entity):
    """样本曲线表"""

    id = attr.ib(type=python_string_type, metadata={'identifier': True})
    line_id = attr.ib(type=python_string_type)
    line_alias = attr.ib(type=python_string_type)
    sample_set_id = attr.ib(type=int)
    sample_result_table_id = attr.ib(type=python_string_type)
    sample_result_table = attr.ib(type=SampleResultTable)
    result_table_id = attr.ib(type=python_string_type)
    group_dimension = attr.ib(type=python_string_type)
    properties = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class SampleCollectConfig(Entity):
    """样本修改配置表"""

    id = attr.ib(type=python_string_type, metadata={'identifier': True})
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    config = attr.ib(type=python_string_type)
    auto_append = attr.ib(type=bool, default=False)
    auto_remove = attr.ib(type=bool, default=False)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class SampleCollectRelation(Relation):
    """样本修改配置关联表"""

    id = attr.ib(type=python_string_type, metadata={'identifier': True})
    sample_set_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    result_table_id = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    line_id = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    config_id = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class LabelMappingConfig(Entity):
    """标注映射策略表"""

    id = attr.ib(type=python_string_type, metadata={'identifier': True})
    mapping_type = attr.ib(type=python_string_type)
    config = attr.ib(type=python_string_type)
    logic = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class LabelMappingRelation(Relation):
    """标注映射关联表"""

    id = attr.ib(type=python_string_type, metadata={'identifier': True})
    sample_set_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    result_table_id = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    line_id = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    config_id = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class SampleSetInstance(Entity):
    """样本集执行配置实例"""

    id = attr.ib(type=int, metadata={'identifier': True})
    sample_set_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    config = attr.ib(type=python_string_type)
    execute_config = attr.ib(type=python_string_type)
    properties = attr.ib(type=python_string_type)
    runtime_info = attr.ib(type=python_string_type)
    description = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    status = attr.ib(type=python_string_type, default='standby')
    run_status = attr.ib(type=python_string_type, default='init')
    active = attr.ib(type=bool, default=True)


@as_metadata
@attr.s
class SampleSetTask(Entity):
    """样本集执行任务"""

    id = attr.ib(type=int, metadata={'identifier': True})
    sample_set_instance_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    sample_set_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    execute_config = attr.ib(type=python_string_type)
    task_config = attr.ib(type=python_string_type)
    task_result = attr.ib(type=python_string_type)
    status = attr.ib(type=python_string_type)
    active = attr.ib(type=bool)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class SampleSetNode(Entity):
    """样本集任务节点"""

    id = attr.ib(type=python_string_type, metadata={'identifier': True})
    sample_set_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    action_name = attr.ib(type=python_string_type)
    input_config = attr.ib(type=python_string_type)
    output_config = attr.ib(type=python_string_type)
    execute_config = attr.ib(type=python_string_type)
    node_index = attr.ib(type=int)
    step_name = attr.ib(type=python_string_type)
    properties = attr.ib(type=python_string_type)
    node_config = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    status = attr.ib(type=python_string_type, default='standby')
    active = attr.ib(type=bool, default=True)
    select = attr.ib(type=bool, default=True)


@as_metadata
@attr.s
class SampleSetNodeUpstream(Relation):
    """样本集任务节点上下游关系"""

    id = attr.ib(type=python_string_type, metadata={'identifier': True})
    sample_set_id = attr.ib(type=int, metadata={'dgraph': {'index': ['int']}})
    sample_set_node_id = attr.ib(type=python_string_type)
    # 若没有父节点则为null
    parent_sample_set_node_id = attr.ib(type=python_string_type)
    properties = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    status = attr.ib(type=python_string_type, default='standby')
    active = attr.ib(type=bool, default=True)
    execute_index = attr.ib(type=int, default=1)


@as_metadata
@attr.s
class SampleSetTemplate(Entity):
    """样本集任务模板"""

    id = attr.ib(type=int, metadata={'identifier': True})
    template_name = attr.ib(type=python_string_type)
    template_alias = attr.ib(type=python_string_type)
    status = attr.ib(type=python_string_type)
    properties = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    active = attr.ib(type=bool, default=True)
    scene_name = attr.ib(type=python_string_type, default='custom')


@as_metadata
@attr.s
class SampleSetTemplateNode(Entity):
    """样本集任务节点模版"""

    id = attr.ib(type=python_string_type, metadata={'identifier': True})
    action_name = attr.ib(type=python_string_type)
    input_config = attr.ib(type=python_string_type)
    output_config = attr.ib(type=python_string_type)
    execute_config = attr.ib(type=python_string_type)
    node_index = attr.ib(type=int)
    step_name = attr.ib(type=python_string_type)
    properties = attr.ib(type=python_string_type)
    node_config = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    active = attr.ib(type=bool, default=True)
    status = attr.ib(type=python_string_type, default='standby')


@as_metadata
@attr.s
class SampleSetTemplateNodeUpstream(Relation):
    """样本集任务节点上下游关系模版"""

    id = attr.ib(type=int, metadata={'identifier': True})
    node_id = attr.ib(type=python_string_type)
    parent_node_id = attr.ib(type=python_string_type)
    properties = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    active = attr.ib(type=bool, default=True)
    status = attr.ib(type=python_string_type, default='standby')
    execute_index = attr.ib(type=int, default=1)


# AIOps v1.3
@as_metadata
@attr.s
class AggregationMethod(Entity):
    __tablename__ = 'aggregation_method'

    agg_name = attr.ib(type=python_string_type, metadata={'identifier': True})
    agg_name_alias = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    properties = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    created_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class Visualization(Entity):

    visualization_name = attr.ib(type=python_string_type, metadata={'identifier': True})
    visualization_alias = attr.ib(type=python_string_type)
    description = attr.ib(type=python_string_type)
    properties = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    created_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class VisualizationComponent(Entity):
    component_name = attr.ib(type=python_string_type, metadata={'identifier': True})
    component_alias = attr.ib(type=python_string_type)
    component_type = attr.ib(type=python_string_type)
    description = attr.ib(type=python_string_type)
    logic = attr.ib(type=python_string_type)
    logic_type = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    config = attr.ib(type=python_string_type)
    properties = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    created_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class VisualizationComponentRelation(Entity):
    id = attr.ib(type=int, metadata={'identifier': True})
    component_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    component = attr.ib(type=VisualizationComponent)
    visualization_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    visualization = attr.ib(type=Visualization)
    description = attr.ib(type=python_string_type)
    properties = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    created_at = attr.ib(type=datetime, factory=datetime.now)


@as_metadata
@attr.s
class TargetVisualizationRelation(Entity):
    id = attr.ib(type=int, metadata={'identifier': True})
    target_name = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    target_type = attr.ib(type=python_string_type, metadata={'dgraph': {'index': ['exact']}})
    visualization_name = attr.ib(type=python_string_type)
    visualization = attr.ib(type=Visualization)
    description = attr.ib(type=python_string_type)
    properties = attr.ib(type=python_string_type)
    created_by = attr.ib(type=python_string_type)
    updated_by = attr.ib(type=python_string_type)
    updated_at = attr.ib(type=datetime, factory=datetime.now)
    created_at = attr.ib(type=datetime, factory=datetime.now)
    target = attr.ib(type=Asset, default=Empty)

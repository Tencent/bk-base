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
import enum

from django.utils.translation import ugettext_lazy as _


class ListValueMixin(object):
    @classmethod
    def get_enum_value_list(cls, excludes=None):
        if excludes is None:
            excludes = []
        return [m.value for m in list(cls.__members__.values()) if m.value not in excludes]

    @classmethod
    def get_enum_key_list(cls):
        return list(cls.__members__.keys())

    @classmethod
    def get_enum_alias_map_list(cls, excludes=None):
        if excludes is None:
            excludes = []
        return [
            (e.value, e.alias) if hasattr(e, 'alias') else (e.value, e.value)
            for e in cls.__members__.values() if e.value not in excludes
        ]


class DataModelPublishStatus(ListValueMixin, enum.Enum):
    # 数据模型发布状态字典表
    DEVELOPING = 'developing'  # 编辑中，未发布
    PUBLISHED = 'published'  # 已发布
    REDEVELOPING = 're-developing'  # 发布后再次编辑


class DataModelStep(ListValueMixin, enum.Enum):
    # 数据模型设计和完成步骤
    INIT = 0  # 均未完成
    MODEL_BASIC_INFO_DONE = 1  # 初始状态，模型信息已保存
    MASTER_TABLE_DONE = 2  # 主表信息已保存
    INDICATOR_DONE = 3  # 统计口经和指标已保存
    MODLE_OVERVIEW = 4  # 数据模型预览
    MODLE_RELEASE = 5  # 数据模型发布


class DataModelType(ListValueMixin, enum.Enum):
    # 数据模型类型
    FACT_TABLE = 'fact_table'  # 事实表
    DIMENSION_TABLE = 'dimension_table'  # 维度表


ModelTypeConfigs = {
    DataModelType.FACT_TABLE.value: {
        'model_type': DataModelType.FACT_TABLE.value,
        'model_type_alias': _('事实表数据模型'),
        'description': _('描述业务活动的细节，如：登录流水'),
        'order': 0,
    },
    DataModelType.DIMENSION_TABLE.value: {
        'model_type': DataModelType.DIMENSION_TABLE.value,
        'model_type_alias': _('维度表数据模型'),
        'description': _('定义事实表数据模型中的维度，如：定义登录流水表中的用户维度（性别、年龄等字段）'),
        'order': 1,
    },
}


class FieldCategory(ListValueMixin, enum.Enum):
    # 数据模型类型
    MEASURE = 'measure'  # 度量
    DIMENSION = 'dimension'  # 维度


class DataModelActiveStatus(ListValueMixin, enum.Enum):
    # 模型可用状态
    ACTIVE = 'active'  # 有效
    DISABLED = 'disabled'  # 无效
    CONFLICTING = 'conflicting'  # 冲突


class ConstraintType(ListValueMixin, enum.Enum):
    # 约束类型
    GENERAL = 'general'  # 通用约束
    SPECIFIC = 'specific'  # 特定场景约束


class SpecificConstraint(object):
    SPECIFIC_CONSTRAINT_NAME = _('常用规则')
    SPECIFIC_CONSTRAINT_ID = 'specific_constraint'
    SPECIFIC_CONSTRAINT_GROUP_TYPE = '1'
    SPECIFIC_CONSTRAINT_FIELD_TYPE = 'string'


class RelatedMethod(ListValueMixin, enum.Enum):
    # 主表关联方式
    LEFT_JOIN = 'left-join'  # 左连接
    RIGHT_JOIN = 'right-join'  # 右连接
    INNER_JOIN = 'inner-join'  # 自连接


class AggregateFunction(ListValueMixin, enum.Enum):
    # 聚合函数
    SUM = 'sum'
    COUNT = 'count'
    COUNT_DISTINCT = 'count_distinct'
    AVG = 'avg'
    MAX = 'max'
    MIN = 'min'

    @classmethod
    def get_calculation_formula(cls, calculation_function, calculation_field):
        if calculation_function != cls.COUNT_DISTINCT.value:
            calculation_formula = '{}(`{}`)'.format(calculation_function, calculation_field)
        else:
            calculation_formula = 'count(distinct `{}`)'.format(calculation_field)
        return calculation_formula


class CalculationContentType(ListValueMixin, enum.Enum):
    # 统计口径加工内容提交类型
    SQL = 'SQL'
    TABLE = 'TABLE'


class SchedulingType(ListValueMixin, enum.Enum):
    # 计算类型
    STREAM = 'stream'
    BATCH = 'batch'


class BatchWindowType(ListValueMixin, enum.Enum):
    # 离线窗口类型
    FIXED = 'fixed'  # 固定窗口
    ACCUMULATE_BY_HOUR = 'accumulate_by_hour'  # 按小时累加


class CountFreq(object):
    # 离线统计频率
    batch_hour_count_freq_list = [1, 2, 3, 4, 6, 12]  # 离线统计频率单位为小时时的统计频率列表


class StreamWindowType(ListValueMixin, enum.Enum):
    # 实时窗口类型
    SCROLL = 'scroll'  # 滚动窗口
    SLIDE = 'slide'  # 滑动窗口
    ACCUMULATE = 'accumulate'  # 累加窗口


class BatchSchedulePeriod(ListValueMixin, enum.Enum):
    # 离线调度单位
    HOUR = 'hour'
    DAY = 'day'
    WEEK = 'week'
    MONTH = 'month'


class ScheduleContentUnit(object):
    # 用于前端展示的格式化单位
    HOUR = 'h'
    DAY = 'd'
    WEEK = 'w'
    MONTH = 'm'
    MINUTE = 'min'
    SECOND = 's'
    FORMAT_UNIT_DICT = {
        BatchSchedulePeriod.HOUR.value: HOUR,
        BatchSchedulePeriod.DAY.value: DAY,
        BatchSchedulePeriod.WEEK.value: WEEK,
        BatchSchedulePeriod.MONTH.value: MONTH,
    }


class DependencyRule(ListValueMixin, enum.Enum):
    # 依赖策略
    ALL_FINISHED = 'all_finished'
    AT_LEAST_ONE_FINISHED = 'at_least_one_finished'
    NO_FAILED = 'no_failed'


class InnerField(object):
    # 内部预留字段
    INNER_FIELD_LIST = [
        'timestamp',
        'offset',
        'bkdata_par_offset',
        'dtEventTime',
        'dtEventTimeStamp',
        'localTime',
        'thedate',
        'rowtime',
        '_startTime_',
        '_endTime_',
        'desc',
        'asc',
    ]


class TimeField(object):
    # 主表吸底时间字段
    TIME_FIELD_NAME = '__time__'
    TIME_FIELD_TYPE = 'timestamp'
    TIME_FIELD_ALIAS = _('时间字段')
    TIME_FIELD_DICT = {
        'field_name': TIME_FIELD_NAME,
        'field_alias': TIME_FIELD_ALIAS,
        'field_type': TIME_FIELD_TYPE,
        'field_category': FieldCategory.DIMENSION.value,
        'description': _('平台内置时间字段，数据入库后将转换为可查询字段，比如 dtEventTime、dtEventTimeStamp、localtime'),
    }


class ExcludeFieldType(ListValueMixin, enum.Enum):
    # 从元数据数据类型中排除的字段
    TEXT_FIELD_TYPE = 'text'
    TIMESTAMP_FIELD_TYPE = 'timestamp'


class CalculationAtomType(object):
    # 统计口径在当前模型中创建/引用
    QUOTE = 'quote'
    CREATE = 'create'


class RecoveryConfig(object):
    # 失败重试配置
    RECOVERY_TIMES_LIST = [1, 2, 3]
    RECOVERY_INTERVAL_LIST = ['5m', '10m', '15m', '30m', '60m']


class DataModelObjectType(ListValueMixin, enum.Enum):
    # 数据模型实体类型
    MODEL = 'model'  # 模型基本信息
    MASTER_TABLE = 'master_table'
    CALCULATION_ATOM = 'calculation_atom'
    INDICATOR = 'indicator'
    FIELD = 'field'
    MODEL_RELATION = 'model_relation'


DATA_MODEL_OBJECT_TYPE_ALIAS_MAPPINGS = {
    DataModelObjectType.MODEL.value: _('模型'),
    DataModelObjectType.MASTER_TABLE.value: _('主表'),
    DataModelObjectType.CALCULATION_ATOM.value: _('统计口径'),
    DataModelObjectType.INDICATOR.value: _('指标'),
    DataModelObjectType.FIELD.value: _('字段'),
    DataModelObjectType.MODEL_RELATION.value: _('模型关联'),
}


class DependencyConfigType(ListValueMixin, enum.Enum):
    # 离线计算固定窗口依赖配置
    UNIFIED = 'unified'
    CUSTOM = 'custom'


class DataModelObjectOperationType(ListValueMixin, enum.Enum):
    # 操作类型
    CREATE = 'create'
    UPDATE = 'update'
    DELETE = 'delete'
    RELEASE = 'release'


DATA_MODEL_OBJECT_OPERATION_TYPE_ALIAS_MAPPINGS = {
    DataModelObjectOperationType.CREATE.value: _('新增'),
    DataModelObjectOperationType.UPDATE.value: _('修改'),
    DataModelObjectOperationType.DELETE.value: _('删除'),
}


class SchedulingEngineType(ListValueMixin, enum.Enum):
    # 调度引擎类型
    FLINK = 'flink_sql'
    SPARK = 'spark_sql'

    SCHEDULING_MAPPINGS = {
        SchedulingType.STREAM: FLINK,
        SchedulingType.BATCH: SPARK,
    }


SCHEDULING_ENGINE_MAPPINGS = {
    SchedulingType.STREAM.value: SchedulingEngineType.FLINK,
    SchedulingType.BATCH.value: SchedulingEngineType.SPARK,
}


class DataModelUsedType(ListValueMixin, enum.Enum):
    # 数据模型被使用类型：被引用 & 被关联 & 被实例化
    IS_QUOTED = 'is_quoted'
    IS_RELATED = 'is_related'
    IS_INSTANTIATED = 'is_instantiated'


DataModelUsedInfoDict = {
    DataModelUsedType.IS_QUOTED.value: _('引用'),
    DataModelUsedType.IS_RELATED.value: _('关联'),
    DataModelUsedType.IS_INSTANTIATED.value: _('实例化'),
}


class ModelInputNodeType(ListValueMixin, enum.Enum):
    # 模型输入节点类型
    STREAM_SOURCE = 'stream_source'
    KV_SOURCE = 'kv_source'
    BATCH_SOURCE = 'batch_source'
    UNIFIED_KV_SOURCE = 'unified_kv_source'


class InstanceInputType(ListValueMixin, enum.Enum):
    # 模型实例输入表类型
    MAIN_TABLE = 'main_table'
    DIM_TABLE = 'dimension_table'


# 对于离线关联输入节点，如果只有一个输入表，则将作为主表汇总到实例输入表中
NODE_TYPE_INSTANCE_INPUT_MAPPINGS = {
    ModelInputNodeType.STREAM_SOURCE.value: InstanceInputType.MAIN_TABLE,
    ModelInputNodeType.KV_SOURCE.value: InstanceInputType.DIM_TABLE,
    ModelInputNodeType.UNIFIED_KV_SOURCE.value: InstanceInputType.DIM_TABLE,
}


class SqlVerifierType(ListValueMixin, enum.Enum):
    # flink/spark
    FLINK_SQL = 'flink_sql'
    SPARK_SQL = 'spark_sql'


# 模型应用阶段字段映射需要排除的内置字段
APPLICATION_EXCLUDE_FIELDS = ['time', '__time__']


class DataModelNodeType(ListValueMixin, enum.Enum):
    DATA_MODEL = 'data_model_app'
    STREAM_INDICATOR = 'data_model_stream_indicator'
    BATCH_INDICATOR = 'data_model_batch_indicator'


SCHEDULING_INDICATOR_NODE_TYPE_MAPPINGS = {
    SchedulingType.STREAM.value: DataModelNodeType.STREAM_INDICATOR.value,
    SchedulingType.BATCH.value: DataModelNodeType.BATCH_INDICATOR.value,
}


BATCH_CHANNEL_CLUSTER_TYPE = 'hdfs'
BATCH_CHANNEL_NODE_TYPE = 'hdfs_storage'
BATCH_CHANNEL_NODE_DEFAULT_CONFIG = {
    'node_type': BATCH_CHANNEL_NODE_TYPE,
    'config': {
        'bk_biz_id': None,
        'name': '',
        'cluster': '',
        'expires': 3,
        'result_table_id': '',
        'from_result_table_ids': [],
    },
}

RELATION_STORAGE_CLUSTER_TYPE = 'ignite'
RELATION_STORAGE_NODE_TYPE = 'ignite'
RELATION_STORAGE_NODE_DEFAULT_CONFIG = {
    'node_type': RELATION_STORAGE_NODE_TYPE,
    'config': {
        'bk_biz_id': None,
        'name': '',
        'cluster': '',
        'expires': 3,
        'expires_unit': 'd',
        'max_records': 1000000,
        'storage_type': 'join',
        'storage_keys': [],
        'result_table_id': '',
        'indexed_fields': [],
        'from_result_table_ids': [],
    },
}


class CleanOptionType(ListValueMixin, enum.Enum):
    # 应用清洗类型
    SQL = 'SQL'
    Constant = 'constant'


class OperationLogQueryConditionKeys(ListValueMixin, enum.Enum):
    OBJECT_OPERATION = 'object_operation'
    OBJECT_TYPE = 'object_type'
    OBJECT = 'object'
    CREATED_BY = 'created_by'
    QUERY = 'query'


class DeepDiffType(ListValueMixin, enum.Enum):
    VALUES_CHANGED = 'values_changed'
    TYPE_CHANGES = 'type_changes'
    DICTIONARY_ITEM_ADDED = 'dictionary_item_added'
    DICTIONARY_ITEM_REMOVED = 'dictionary_item_removed'
    ITERABLE_ITEM_ADDED = 'iterable_item_added'
    ITERABLE_ITEM_REMOVED = 'iterable_item_removed'


DIFF_MAPPINGS = {
    DeepDiffType.ITERABLE_ITEM_ADDED.value: DataModelObjectOperationType.CREATE.value,
    DeepDiffType.VALUES_CHANGED.value: DataModelObjectOperationType.UPDATE.value,
    DeepDiffType.ITERABLE_ITEM_REMOVED.value: DataModelObjectOperationType.DELETE.value,
    DeepDiffType.DICTIONARY_ITEM_ADDED.value: DataModelObjectOperationType.UPDATE.value,
    DeepDiffType.DICTIONARY_ITEM_REMOVED.value: DataModelObjectOperationType.UPDATE.value,
    DeepDiffType.TYPE_CHANGES.value: DataModelObjectOperationType.UPDATE.value,
}

OPERATOR_AND_TIME_KEYS = ['created_by', 'created_at', 'updated_by', 'updated_at']

NO_SYNC_ATTRS = ['id']

IGNORE_KEYS = [
    'latest_version',
    'is_applied_by_indicators',
    'is_quoted_by_other_models',
    'deletable',
    'editable',
    'indicator_count',
    'publish_status',
    'step_id',
    'aggregation_fields_alias',
    'origin_fields',
    'related_model_version_id',
    'has_sub_indicators',
    'key_params_editable',
    'indicator_id',
    'hash',
    'id',
]
IGNORE_KEYS.extend(OPERATOR_AND_TIME_KEYS)

FILED_IGNORE_KEYS = ['origin_fields', 'id', 'related_field_exist', 'source_field_exist']

MODEL_RELATION_IGNORE_KEYS = ['id', 'related_model_active_status', 'related_model_version_id']

SCHEDULING_CONTENT_IGNORE_KEYS = ['format_window_size_unit', 'format_window_size']


class SchedulingContentValidateRange(object):
    # 调度内容校验范围
    CURRENT = 'current'  # 当前节点
    OUTPUT = 'output'  # 当前节点 & 下游
    ALL = 'all'  # 全局


DATA_MODEL_UNIQUE_KEY_MAPPINGS = {
    DataModelObjectType.MASTER_TABLE.value: ['model_id', 'field_name', 'related_model_id', 'related_field_name']
}

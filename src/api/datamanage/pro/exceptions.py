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


from django.utils.translation import ugettext_lazy as _

from datamanage.exceptions import DatamanageError, DatamanageErrorCode


class DatamanageProErrorCode(DatamanageErrorCode):
    PRO_ERR = '300'  # 增值包通用异常
    AUDIT_RULE_NOT_EXIST_ERR = '301'  # 规则不存在
    DQ_EVENT_NOT_EXIST_ERR = '302'  # 质量事件不存在
    AUDIT_RULE_EVENT_NOT_FOUND_ERR = '303'  # 无法找到规则关联的事件
    START_AUDIT_TASK_ERR = '304'  # 启动审核任务失败
    STOP_AUDIT_TASK_ERR = '305'  # 停止审核任务失败
    AUDIT_TASK_ALREADY_STARTED_ERR = '306'  # 审核任务已经启动
    AUDIT_TASK_ALREADY_STOPPED_ERR = '307'  # 审核任务已经停止
    RUNNING_TASK_CANNOT_EDIT_ERR = '308'  # 运行中的审核任务不能修改或删除
    DQ_CORRECT_CONFIG_NOT_EXIST_ERR = '309'  # 修正配置不存在
    FETCH_DEBUG_SAMPLE_DATA_ERR = '310'  # 获取调试样本数据异常
    SESSION_SERVER_NOT_EXIST_ERR = '311'  # Session server不存在异常
    SET_CONSUMER_OFFSET_TIMEOUT_ERR = '312'  # Consume设置offset超时

    DATA_MODEL_NOT_EXIST_ERR = '330'  # 数据模型不存在异常
    TAG_TARGET_ERR = '331'  # 打标签失败
    UNTAG_TARGET_ERR = '332'  # 删除给实体打的标签失败
    GHANGE_TAG_TARGET_ERR = '333'  # 删除给实体打的标签失败
    TOP_DATA_MODEL_ERR = '334'  # 模型置顶失败
    CANCEL_TOP_DATA_MODEL_ERR = '335'  # 模型取消置顶失败
    DATA_MODEL_NOT_TOP_ERR = '336'  # 模型尚未置顶
    DATA_MODEL_ALREADY_TOP_ERR = '337'  # 模型已经置顶
    DATA_MODEL_ALREADY_DELETED_ERR = '338'  # 模型已经被删除
    DATA_MODEL_NAME_ALREADY_EXIST_ERR = '339'  # 模型名称已存在
    DIMENSION_TABLE_HAVE_MEASURE_ERR = '340'  # 维度表有度量错误
    DIMENSION_TABLE_NOT_HAVE_PRIMARY_KEY_ERR = '341'  # 维度表没有主键错误
    FIELD_CONSTRAINT_FORMAT_ERR = '342'  # 主表字段约束格式错误
    SQL_VALIDATE_ERR = '343'  # SQL校验失败
    FIELD_NOT_IN_MASTER_TABLE_ERR = '344'  # 关联字段非主表字段
    FIELD_NOT_EXIST_ERR = '345'  # 字段不存在异常
    SET_CACHE_ERROR = '346'  # 缓存异常
    CALCULATION_ATOM_NOT_EXIST_ERR = '347'  # 统计口径不存在异常
    CALCULATION_ATOM_NAME_ALREADY_EXIST_ERR = '348'  # 统计口径名称已经存在异常
    INDICATOR_NOT_EXIST_ERR = '349'  # 指标不存在异常
    CALCULATION_ATOM_CAN_NOT_BE_DELETED_ERR = '350'  # 统计口径不能被删除异常
    AGGREGATION_FIELDS_NOT_DIMENSION_ERR = '351'  # 聚合字段不是主表维度字段异常
    CALCULATION_ATOM_CAN_NOT_BE_QUOTED_ERR = '352'  # 统计口径不能被引用异常
    ALIAS_SET_ERR = '353'
    CALCULATION_FORMULA_ALREADY_EXIST_ERR = '354'  # 当前模型下聚合逻辑已经存在异常
    TAG_NUMBER_LARGER_THAN_MAX_ERR = '355'  # 模型标签数超过最大标签数量限制
    CALCULATION_ATOM_NOT_SAME_ERR = '356'  # 指标统计口径和父指标统计口径不一致
    MODEL_RELEASE_CONTENT_VALIDATION_ERR = '357'  # 模型发布内容校验不通过
    INDICATOR_AGGR_FIELDS_NOT_SUBSET_ERR = '358'  # 指标聚合字段不是父指标维度字段子集异常
    MODEL_INSTANCE_NOT_EXIST_ERR = '359'  # 模型应用实例不存在
    MODEL_VERSION_NOT_LATEST_ERR = '360'  # 当前模型版本ID并非最新版本
    INSTANCE_HAS_NO_TABLE_ERR = '361'  # 当前实例没有任何主表
    RELATION_FIELD_NOT_EXIST_ERR = '362'  # 模型应用实例主表关联关系字段在主表字段表中无法找到
    INSTANCE_TABLE_BEEN_USED_ERR = '363'  # 模型实例主表ID已经被使用
    MODEL_RELATION_NOT_EXIST_ERR = '364'  # 模型构建中不存在关联关系
    MODEL_NOT_THE_SAME_ERR = '365'  # 模型应用实例所选模型ID与字段映射中所用模型ID不一致
    OTHER_FIELDS_NOT_PERMITTED_ERR = '366'  # 模型字段映射的自定义SQL中不允许使用非选中字段
    INS_INDICATOR_NOT_EXIST_ERR = '367'  # 数据模型应用实例指标表不存在
    INS_STILL_HAS_INDICATORS_ERR = '368'  # 数据模型应用实例尚有指标未删除，请先清空所有指标后再删除当前数据模型实例
    NODE_TYPE_NOT_SUPPORTED_ERR = '369'  # 暂不支持用当前类型节点作为数据模型节点的输入
    MULTI_MAIN_SOURCE_TABLE_ERR = '370'  # 不允许数据模型应用输入表超过一个主表
    NO_MAIN_SOURCE_TABLE_ERR = '371'  # 数据模型应用输入需要有一个主表作为输入表
    MODEL_CONTENT_HAS_NO_CHANGE_ERR = '372'  # 版本内容没有修改异常
    MODEL_VERSION_NOT_EXIST_ERR = '373'  # 无法找到模型关于某版本的信息
    ALIAS_NOT_MAPPING_FIELD_ERR = '374'  # 当前alias名称与模型输出字段名称不一致
    DIM_FIELD_CANNOT_CLEAN_ERR = '375'  # 仅来自维度表的字段不允许进行清洗加工
    MAIN_FIELD_MUST_FROM_MAIN_TABLE_ERR = '376'  # 主表字段（非维度关联字段）必须输入来源于主表
    DIM_FIELD_MUST_FROM_DIM_TABLE_ERR = '377'  # 关联字段关联的输入表必须来源于维度表
    EXTENDED_FIELD_INPUT_ERR = '378'  # 维度扩展字段来源输入表只允许从输入主表和关联字段输入表中进行选择
    MODEL_STRUCTURE_NOT_SAME_AS_RELEASED_ERR = '379'  # 应用模型结构和该模型版本发布时结构不一致
    FIELD_CAN_NOT_BE_DELETED_ERR = '380'  # 字段不能被删除
    FIELD_CAN_NOT_BE_EDITED_ERR = '381'  # 字段不能被修改
    DATA_MODEL_CAN_NOT_BE_DELETED_ERR = '382'  # 模型不能被删除
    RELEASED_MODEL_INDICATORS_ERR = '383'  # 已发布的模型指标配置异常
    DATA_MODEL_CAN_NOT_BE_RELEASED_ERR = '384'  # 模型不能被发布
    INDICATOR_TABLE_BEEN_USED_ERR = '385'  # 模型实例指标结果表ID已经被使用
    RT_NOT_GENERATE_BY_INS_ERR = '386'  # 结果表不属于任何数据模型实例的主表或指标表
    CREATE_DMM_DATAFLOW_NODE_ERR = '387'  # 创建数据模型数据开发任务节点失败
    NO_OUTPUT_RT_FROM_NODE_CREATION = '388'  # 创建节点失败，没有返回任何输出结果表信息
    BATCH_CHANNEL_IS_NECESSARY_ERR = '389'  # 离线channel存储是必须的
    INPUT_FIELD_NOT_USED_IN_CLEAN_ERR = '390'  # 字段加工逻辑中没有使用所选的来源表输入字段
    DIM_FIELD_MUST_EQUAL_FIELD_NAME_ERR = '391'  # 来源于维度表的维度关联字段名称必须与模型输出字段名称相同
    FLOW_HAS_NO_INSTANCE_ERR = '392'  # 数据开发任务没有任何模型实例
    INDICATOR_MUST_BE_QUERYABLE_ERR = '393'  # 数据模型实例末端指标节点必须有可查询的存储
    DIMENSION_TABLE_HAS_NO_RELATION_ERR = '394'  # 维度模型主表缺少关联存储
    SAME_INDICATOR_NOT_PERMITTED_ERR = '395'  # 不允许创建关键配置相同的指标
    INDICATOR_HAS_NO_CORRESPONDING_PARAM_ERR = '396'  # 指标没有传对应参数
    DIM_FIELD_REQUIRED_ERR = '397'  # 当扩展字段来源于非主表字段时，维度关联字段必选
    INDICATOR_KEY_PARAMS_CAN_NOT_BE_UPDATED_ERR = '398'  # 指标一旦创建则关键参数不允许修改
    INDICATOR_WITH_SUB_INDICATORS_CAN_NOT_BE_DELETED_ERR = '399'  # 父指标不允许删除
    CALCULATION_ATOM_KEY_PARAMS_CAN_NOT_BE_UPDATED_ERR = '400'  # 统计口径被指标使用/被模型引用，不可以修改关键参数
    DATA_MODEL_HAS_NOT_BEEN_RELEASED_ERR = '401'  # 模型尚未被发布
    DATA_MODEL_RELEASE_HAS_NO_MODEL_CONTENT_ERR = '402'  # 模型发布时未成功记录模型内容，请联系管理员
    CALCULATION_ATOM_IMAGE_NOT_EXIST_ERR = '403'  # 统计口径引用不存在异常
    INDICATOR_NAME_ALREADY_EXIST_ERR = '404'  # 指标名称已存在
    DATA_REPORT_ES_ERR = '405'  # 数据上报es失败
    GET_DATA_SET_CREATE_INFO_ERR = '406'  # 数据集创建相关信息获取失败,请联系元数据管理员
    ES_SEARCH_ERR = '407'  # es数据查询失败


class AuditRuleNotExistError(DatamanageError):
    MESSAGE = _('规则不存在异常')
    CODE = DatamanageProErrorCode.AUDIT_RULE_NOT_EXIST_ERR


class DataQualityEventNotExistError(DatamanageError):
    MESSAGE = _('数据质量事件不存在')
    CODE = DatamanageProErrorCode.DQ_EVENT_NOT_EXIST_ERR


class AuditRuleEventNotFoundError(DatamanageError):
    MESSAGE = _('无法找到规则关联的事件')
    CODE = DatamanageProErrorCode.AUDIT_RULE_EVENT_NOT_FOUND_ERR


class StartAuditTaskError(DatamanageError):
    MESSAGE = _('启动审核任务失败')
    CODE = DatamanageProErrorCode.START_AUDIT_TASK_ERR


class StopAuditTaskError(DatamanageError):
    MESSAGE = _('停止审核任务失败')
    CODE = DatamanageProErrorCode.STOP_AUDIT_TASK_ERR


class AuditTaskAlreadyStartedError(DatamanageError):
    MESSAGE = _('审核任务已经启动')
    CODE = DatamanageProErrorCode.AUDIT_TASK_ALREADY_STARTED_ERR


class AuditTaskAlreadyStoppedError(DatamanageError):
    MESSAGE = _('审核任务已经停止')
    CODE = DatamanageProErrorCode.AUDIT_TASK_ALREADY_STOPPED_ERR


class RunningAuditTaskCannotEditError(DatamanageError):
    MESSAGE = _('运行中的审核任务不能修改或删除')
    CODE = DatamanageProErrorCode.RUNNING_TASK_CANNOT_EDIT_ERR


class CorrectConfigNotExistError(DatamanageError):
    MESSAGE = _('修正配置不存在')
    CODE = DatamanageProErrorCode.DQ_CORRECT_CONFIG_NOT_EXIST_ERR


class FetchDebugSampleDataError(DatamanageError):
    MESSAGE = _('获取调试样例数据异常')
    CODE = DatamanageProErrorCode.FETCH_DEBUG_SAMPLE_DATA_ERR


class SessionServerNotExistError(DatamanageError):
    MESSAGE = _('Session server({session_key})不存在')
    CODE = DatamanageProErrorCode.SESSION_SERVER_NOT_EXIST_ERR


class SetConsumerOffsetTimeoutError(DatamanageError):
    MESSAGE = _('Consume设置offset超时')
    CODE = DatamanageProErrorCode.SET_CONSUMER_OFFSET_TIMEOUT_ERR


class DataModelNotExistError(DatamanageError):
    MESSAGE = _('数据模型不存在')
    CODE = DatamanageProErrorCode.DATA_MODEL_NOT_EXIST_ERR


class DataModelTAGTARGETError(DatamanageError):
    MESSAGE = _('数据模型打标签失败')
    CODE = DatamanageProErrorCode.TAG_TARGET_ERR


class DataModelUNTAGTARGETError(DatamanageError):
    MESSAGE = _('删除给数据模型打的标签失败')
    CODE = DatamanageProErrorCode.UNTAG_TARGET_ERR


class DataModelUPDATETAGTARGETError(DatamanageError):
    MESSAGE = _('修改给数据模型打的标签失败')
    CODE = DatamanageProErrorCode.GHANGE_TAG_TARGET_ERR


class TopDataModelError(DatamanageError):
    MESSAGE = _('模型置顶失败')
    CODE = DatamanageProErrorCode.TOP_DATA_MODEL_ERR


class CancelTopDataModelError(DatamanageError):
    MESSAGE = _('模型取消置顶失败')
    CODE = DatamanageProErrorCode.CANCEL_TOP_DATA_MODEL_ERR


class DataModelNotTopError(DatamanageError):
    MESSAGE = _('模型尚未置顶')
    CODE = DatamanageProErrorCode.DATA_MODEL_NOT_TOP_ERR


class DataModelAlreadyTopError(DatamanageError):
    MESSAGE = _('模型已经置顶')
    CODE = DatamanageProErrorCode.DATA_MODEL_ALREADY_TOP_ERR


class DataModelAlreadyDELETEDError(DatamanageError):
    MESSAGE = _('模型已经被删除')
    CODE = DatamanageProErrorCode.DATA_MODEL_ALREADY_DELETED_ERR


class DataModelNameAlreadyExistError(DatamanageError):
    MESSAGE = _('数据模型名称已存在，请修改数据模型名称')
    CODE = DatamanageProErrorCode.DATA_MODEL_NAME_ALREADY_EXIST_ERR


class DimensionTableHaveMeasureError(DatamanageError):
    MESSAGE = _('维度表不能有度量')
    CODE = DatamanageProErrorCode.DIMENSION_TABLE_HAVE_MEASURE_ERR


class DimensionTableNotHaveMetricError(DatamanageError):
    MESSAGE = _('维度表没有主键错误')
    CODE = DatamanageProErrorCode.DIMENSION_TABLE_NOT_HAVE_PRIMARY_KEY_ERR


class FieldConstraintFormatError(DatamanageError):
    MESSAGE = _('主表字段约束格式错误: {format_error_type}')
    CODE = DatamanageProErrorCode.FIELD_CONSTRAINT_FORMAT_ERR


class SqlValidateError(DatamanageError):
    MESSAGE = _('SQL校验不通过: {error_info}')
    CODE = DatamanageProErrorCode.SQL_VALIDATE_ERR


class FieldNotInMasterTableError(DatamanageError):
    MESSAGE = _('关联字段: {field_name}非主表字段')
    CODE = DatamanageProErrorCode.FIELD_NOT_IN_MASTER_TABLE_ERR


class FieldNotExistError(DatamanageError):
    MESSAGE = _('字段不存在')
    CODE = DatamanageProErrorCode.FIELD_NOT_EXIST_ERR


class SetCacheError(DatamanageError):
    MESSAGE = _('设置cache [{cache_key}] 异常: {error}')
    CODE = DatamanageProErrorCode.SET_CACHE_ERROR


class CalculationAtomNotExistError(DatamanageError):
    MESSAGE = _('统计口径不存在')
    CODE = DatamanageProErrorCode.CALCULATION_ATOM_NOT_EXIST_ERR


class CalculationAtomNameAlreadyExistError(DatamanageError):
    MESSAGE = _('统计口径名称已存在，请修改统计口径名称')
    CODE = DatamanageProErrorCode.CALCULATION_ATOM_NAME_ALREADY_EXIST_ERR


class IndicatorNotExistError(DatamanageError):
    MESSAGE = _('指标不存在')
    CODE = DatamanageProErrorCode.INDICATOR_NOT_EXIST_ERR


class CalculationAtomCanNotBeDeletedError(DatamanageError):
    MESSAGE = _('统计口径不能被删除异常')
    CODE = DatamanageProErrorCode.CALCULATION_ATOM_CAN_NOT_BE_DELETED_ERR


class AggregationFieldsNotDimensionError(DatamanageError):
    MESSAGE = _('聚合字段不是主表维度字段异常')
    CODE = DatamanageProErrorCode.AGGREGATION_FIELDS_NOT_DIMENSION_ERR


class CalculationAtomCanNotBeQuotedError(DatamanageError):
    MESSAGE = _('不能引用当前模型下创建 & 已经引用 & 加工字段不在主表中的统计口径')
    CODE = DatamanageProErrorCode.CALCULATION_ATOM_CAN_NOT_BE_QUOTED_ERR


class AliasSetError(DatamanageError):
    MESSAGE = _('校验 [{alias}] 字段的别名发生异常, (当前设置别名为 [{err_alias}], 正确别名应为 [{alias}]), 请检查该字段的SQL加工逻辑')
    CODE = DatamanageProErrorCode.ALIAS_SET_ERR


class ModelInstanceNotExistError(DatamanageError):
    MESSAGE = _('模型应用实例({model_instance_id})不存在')
    CODE = DatamanageProErrorCode.MODEL_INSTANCE_NOT_EXIST_ERR


class ModelVersionNotLatestError(DatamanageError):
    MESSAGE = _('当前模型版本ID({version_id})并非最新版本({latest_version_id})，请同步最新版本后再提交')
    CODE = DatamanageProErrorCode.MODEL_VERSION_NOT_LATEST_ERR


class ModelInstanceHasNoTableError(DatamanageError):
    MESSAGE = _('当前模型实例({model_instance_id})没有任何主表信息，请联系管理员处理')
    CODE = DatamanageProErrorCode.INSTANCE_HAS_NO_TABLE_ERR


class RelationFieldNotExistError(DatamanageError):
    MESSAGE = _('模型应用实例主表关联关系字段{field_name}在主表字段表中无法找到')
    CODE = DatamanageProErrorCode.RELATION_FIELD_NOT_EXIST_ERR


class ModelInstanceTableBeenUsedError(DatamanageError):
    MESSAGE = _('模型实例主表ID({result_table_id})已经被使用')
    CODE = DatamanageProErrorCode.INSTANCE_TABLE_BEEN_USED_ERR


class ModelRelationNotExistError(DatamanageError):
    MESSAGE = _('模型构建中基于字段({field_name})从模型({model_id})关联到({related_model_id})的关联关系不存在')
    CODE = DatamanageProErrorCode.MODEL_RELATION_NOT_EXIST_ERR


class ModelNotTheSameError(DatamanageError):
    MESSAGE = _('模型应用实例所选模型ID与字段映射中所用模型ID不一致')
    CODE = DatamanageProErrorCode.MODEL_NOT_THE_SAME_ERR


class OtherFieldsNotPermittedError(DatamanageError):
    MESSAGE = _('模型字段映射的自定义SQL中不允许使用非选中字段({fields})')
    CODE = DatamanageProErrorCode.OTHER_FIELDS_NOT_PERMITTED_ERR


class InstanceIndicatorNotExistError(DatamanageError):
    MESSAGE = _('数据模型应用实例指标表({result_table_id})不存在')
    CODE = DatamanageProErrorCode.INS_INDICATOR_NOT_EXIST_ERR


class InstanceStillHasIndicatorsError(DatamanageError):
    MESSAGE = _('数据模型应用实例尚有指标未删除，请先清空所有指标后再删除当前数据模型实例')
    CODE = DatamanageProErrorCode.INS_STILL_HAS_INDICATORS_ERR


class NodeTypeNotSupportedError(DatamanageError):
    MESSAGE = _('暂不支持用当前类型({node_type})节点作为数据模型节点的输入')
    CODE = DatamanageProErrorCode.NODE_TYPE_NOT_SUPPORTED_ERR


class MultiMainSourceTableError(DatamanageError):
    MESSAGE = _('不允许数据模型应用输入表超过一个主表')
    CODE = DatamanageProErrorCode.MULTI_MAIN_SOURCE_TABLE_ERR


class NoMainSourceTableError(DatamanageError):
    MESSAGE = _('数据模型应用输入需要有一个主表作为输入表')
    CODE = DatamanageProErrorCode.NO_MAIN_SOURCE_TABLE_ERR


class CalculationFormulaAlreadyExistError(DatamanageError):
    MESSAGE = _('当前模型下聚合逻辑已经存在')
    CODE = DatamanageProErrorCode.CALCULATION_FORMULA_ALREADY_EXIST_ERR


class TagNumberLargerThanMaxError(DatamanageError):
    MESSAGE = _('模型标签数超过最大标签数量限制:{max_tag_num}')
    CODE = DatamanageProErrorCode.TAG_NUMBER_LARGER_THAN_MAX_ERR


class CalculationAtomNotSameError(DatamanageError):
    MESSAGE = _('指标统计口径和父指标统计口径不一致')
    CODE = DatamanageProErrorCode.CALCULATION_ATOM_NOT_SAME_ERR


class ModelReleaseContentValidationError(DatamanageError):
    MESSAGE = _('模型发布内容校验不通过:{error_info}')
    CODE = DatamanageProErrorCode.MODEL_RELEASE_CONTENT_VALIDATION_ERR


class IndicatorAggrFieldsNotSubsetError(DatamanageError):
    MESSAGE = _('指标聚合字段不是父指标维度字段子集异常:{error_info}')
    CODE = DatamanageProErrorCode.INDICATOR_AGGR_FIELDS_NOT_SUBSET_ERR


class ModelContentHasNoChangeError(DatamanageError):
    MESSAGE = _('当前没有模型关键内容变更，无需重复发布')
    CODE = DatamanageProErrorCode.MODEL_CONTENT_HAS_NO_CHANGE_ERR


class ModelVersionNotExistError(DatamanageError):
    MESSAGE = _('无法找到模型({model_id})关于版本({version_id})的信息')
    CODE = DatamanageProErrorCode.MODEL_VERSION_NOT_LATEST_ERR


class AliasNotMappingFieldError(DatamanageError):
    MESSAGE = _('当前alias名称({alias_name})与模型输出字段名称不一致')
    CODE = DatamanageProErrorCode.ALIAS_NOT_MAPPING_FIELD_ERR


class DimFieldCannotCleanError(DatamanageError):
    MESSAGE = _('不允许使用维度表的字段({field_name})进行清洗加工')
    CODE = DatamanageProErrorCode.DIM_FIELD_CANNOT_CLEAN_ERR


class MainFieldMustFromMainTableError(DatamanageError):
    MESSAGE = _('主表字段({field_name})（非维度关联字段）必须输入来源于主表')
    CODE = DatamanageProErrorCode.MAIN_FIELD_MUST_FROM_MAIN_TABLE_ERR


class DimFieldMustFromDimTableError(DatamanageError):
    MESSAGE = _('关联字段({field_name})关联的输入表必须来源于维度表')
    CODE = DatamanageProErrorCode.DIM_FIELD_MUST_FROM_DIM_TABLE_ERR


class DimFieldMustEqualFieldNameError(DatamanageError):
    MESSAGE = _(
        '来源于维度表({related_input_result_table_id})的维度关联字段名称({related_input_field_name})必须与维度模型输出字段名称({source_field})相同'
    )
    CODE = DatamanageProErrorCode.DIM_FIELD_MUST_EQUAL_FIELD_NAME_ERR


class ExtendedFieldInputError(DatamanageError):
    MESSAGE = _('维度扩展字段来源输入表只允许从输入主表({input_result_table_id})和关联字段输入表({related_result_table_id})中进行选择')
    CODE = DatamanageProErrorCode.EXTENDED_FIELD_INPUT_ERR


class ModelInstanceStructureNotSameError(DatamanageError):
    MESSAGE = _('应用模型结构和该模型版本发布时结构不一致')
    CODE = DatamanageProErrorCode.MODEL_STRUCTURE_NOT_SAME_AS_RELEASED_ERR


class ReleasedModelIndicatorError(DatamanageError):
    MESSAGE = _('已发布的模型指标配置异常')
    CODE = DatamanageProErrorCode.RELEASED_MODEL_INDICATORS_ERR


class InstanceIndicatorTableBeenUsedError(DatamanageError):
    MESSAGE = _('模型实例指标结果表ID({result_table_id})已经被使用')
    CODE = DatamanageProErrorCode.INDICATOR_TABLE_BEEN_USED_ERR


class ResultTableNotGenerateByAnyInstanceError(DatamanageError):
    MESSAGE = _('结果表({result_table_id})不属于任何数据模型实例的主表或指标表')
    CODE = DatamanageProErrorCode.RT_NOT_GENERATE_BY_INS_ERR


class CreateDataModelDataflowNodeError(DatamanageError):
    MESSAGE = _('创建数据模型数据开发任务节点({node_name})失败')
    CODE = DatamanageProErrorCode.CREATE_DMM_DATAFLOW_NODE_ERR


class NoOutputRtFromNodeCreationError(DatamanageError):
    MESSAGE = _('创建节点失败，没有返回任何输出结果表信息')
    CODE = DatamanageProErrorCode.NO_OUTPUT_RT_FROM_NODE_CREATION


class BatchChannelIsNecessaryError(DatamanageError):
    MESSAGE = _('离线channel存储是必须的')
    CODE = DatamanageProErrorCode.BATCH_CHANNEL_IS_NECESSARY_ERR


class FieldCanNotBeDeletedError(DatamanageError):
    MESSAGE = _('字段{field_name}不能被删除')
    CODE = DatamanageProErrorCode.FIELD_CAN_NOT_BE_DELETED_ERR


class FieldCanNotBeEditedError(DatamanageError):
    MESSAGE = _('字段{field_name}不能被修改')
    CODE = DatamanageProErrorCode.FIELD_CAN_NOT_BE_EDITED_ERR


class DataModelCanNotBeDeletedError(DatamanageError):
    MESSAGE = _('模型{model_id}不能被删除:{error_info}')
    CODE = DatamanageProErrorCode.DATA_MODEL_CAN_NOT_BE_DELETED_ERR


class DataModelCanNotBeReleasedError(DatamanageError):
    MESSAGE = _('模型不能发布:{error_info}')
    CODE = DatamanageProErrorCode.DATA_MODEL_CAN_NOT_BE_RELEASED_ERR


class InputFieldNotUsedInCleanError(DatamanageError):
    MESSAGE = _('字段加工逻辑中没有使用所选的来源表输入字段({input_field_name})')
    CODE = DatamanageProErrorCode.INPUT_FIELD_NOT_USED_IN_CLEAN_ERR


class FlowHasNoInstanceError(DatamanageError):
    MESSAGE = _('数据开发任务({flow_id})没有任何模型实例')
    CODE = DatamanageProErrorCode.FLOW_HAS_NO_INSTANCE_ERR


class IndicatorMustBeQueryableError(DatamanageError):
    MESSAGE = _('数据模型实例末端指标节点({result_table_id})必须有可查询的存储')
    CODE = DatamanageProErrorCode.INDICATOR_MUST_BE_QUERYABLE_ERR


class DimensionTableHasNoRelationError(DatamanageError):
    MESSAGE = _('维度模型主表({result_table_id})缺少关联存储')
    CODE = DatamanageProErrorCode.DIMENSION_TABLE_HAS_NO_RELATION_ERR


class SameIndicatorNotPermittedError(DatamanageError):
    MESSAGE = _('不允许创建/修改为关键配置：统计口径、聚合字段、过滤条件、计算类型、窗口类型、统计频率、窗口长度均相同的指标')
    CODE = DatamanageProErrorCode.SAME_INDICATOR_NOT_PERMITTED_ERR


class DimFieldRequiredError(DatamanageError):
    MESSAGE = _('如果维度扩展字段({extended_field})不来源于主表，则必须在字段({join_field})中选择关联的维度字段({related_field})')
    CODE = DatamanageProErrorCode.DIM_FIELD_REQUIRED_ERR


class IndicatorHasNoCorrespondingParamError(DatamanageError):
    MESSAGE = _('指标中没有{param_name}参数')
    CODE = DatamanageProErrorCode.INDICATOR_HAS_NO_CORRESPONDING_PARAM_ERR


class IndicatorKeyParamsCanNotBeUpdatedError(DatamanageError):
    MESSAGE = _(
        """指标一旦创建则不允许修改关键配置：统计口径、父指标；
    父指标一旦创建则不允许修改配置：指标名称、统计口径、父指标、聚合字段、计算类型、窗口类型、统计频率、窗口长度"""
    )
    CODE = DatamanageProErrorCode.INDICATOR_KEY_PARAMS_CAN_NOT_BE_UPDATED_ERR


class IndicatorWithSubIndicatorsCanNotBeDeletedError(DatamanageError):
    MESSAGE = _('父指标不允许删除')
    CODE = DatamanageProErrorCode.INDICATOR_WITH_SUB_INDICATORS_CAN_NOT_BE_DELETED_ERR


class CalculationAtomKeyParamsCanNotBeUpdatedError(DatamanageError):
    MESSAGE = _('统计口径被指标使用/被模型引用，不可以修改关键参数')
    CODE = DatamanageProErrorCode.CALCULATION_ATOM_KEY_PARAMS_CAN_NOT_BE_UPDATED_ERR


class DataModelHasNotBeenReleasedError(DatamanageError):
    MESSAGE = _('模型尚未被发布')
    CODE = DatamanageProErrorCode.DATA_MODEL_HAS_NOT_BEEN_RELEASED_ERR


class DataModelReleaseHasNoModelContentError(DatamanageError):
    MESSAGE = _('模型发布时未成功记录模型内容，请联系管理员')
    CODE = DatamanageProErrorCode.DATA_MODEL_RELEASE_HAS_NO_MODEL_CONTENT_ERR


class CalculationAtomImageNotExistError(DatamanageError):
    MESSAGE = _('统计口径引用不存在')
    CODE = DatamanageProErrorCode.CALCULATION_ATOM_IMAGE_NOT_EXIST_ERR


class IndicatorNameAlreadyExistError(DatamanageError):
    MESSAGE = _('指标名称已存在，请修改指标名称')
    CODE = DatamanageProErrorCode.INDICATOR_NAME_ALREADY_EXIST_ERR


class DataReportEsError(DatamanageError):
    MESSAGE = _('数据上报es错误:{error_info}')
    CODE = DatamanageProErrorCode.DATA_REPORT_ES_ERR


class GetDataSetCreateInfoError(DatamanageError):
    MESSAGE = _('{dataset_id}数据集创建相关信息获取失败,请联系元数据管理员')
    CODE = DatamanageProErrorCode.GET_DATA_SET_CREATE_INFO_ERR


class EsSearchError(DatamanageError):
    MESSAGE = _('es查询错误:{error_info}')
    CODE = DatamanageProErrorCode.ES_SEARCH_ERR

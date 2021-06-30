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


from common.errorcodes import ErrorCode
from common.exceptions import CommonAPIError
from django.utils.translation import ugettext_lazy as _


class MetaErrorCode(ErrorCode):
    DEFAULT_ERR = "000"  # MetaApi默认异常
    API_CALL_ERR = "001"  # 其他模块API调用异常
    ALTAS_CALL_ERR = "002"  # ALTAS组件API调用异常
    DATABASE_ERR = "003"  # 数据库操作异常
    TRANSLATE_ERR = "004"  # 无法找到对应语言的翻译
    PARAMS_ERR = "005"

    NOT_SUPPORT_OPERATE_OBJECT = "010"  # 不支持该操作对象（业务失败）
    NOT_SUPPORT_OPERATE_TYPE = "011"  # 不支持该操作类型（业务失败）

    DEPTH_ERROR = "012"  # 不支持的查询深度（业务失败）
    NOT_SUPPORT_QUERY = "013"  # 不支持的查询（业务失败）

    RESULT_TABLE_NOT_EXIST = "020"  # 结果表不存在（业务失败）
    RESULT_TABLE_FIELD_CONFLICT = "021"  # 结果表字段冲突（业务失败）
    RESULT_TABLE_STORAGE_DELETE_ERR = "022"  # 删除结果表存储失败（业务失败）
    RESULT_TABLE_TYPE_NOT_EXIST = "023"  # 结果表类型不存在（业务失败）
    RESULT_TABLE_HAS_RELATION_ERR = "024"  # 该结果表的数据处理尚未删除（业务失败）
    RESULT_TABLE_FIELD_TYPE_ILLEGAL = "025"  # 结果表字段类型不合法（业务失败）
    RESULT_TABLE_HAS_EXISTED = "026"  # 结果表已存在（业务失败）
    COMPUTE_RESULT_TABLE_NO_SQL = "027"  # 结果表类型不存在（业务失败）
    RESULT_TABLE_FIELD_TYPE_CHANGED = "028"  # 结果表字段类型变更（业务失败）
    TOO_MANY_RESULT_TABLE_FIELD = "029"  # 结果表字段数目超过限制 (业务失败)

    DATA_PROCESSING_NOT_EXIST = "040"  # 数据处理不存在（业务失败）
    DATA_PROCESSING_OUTPUT_LIMIT = "041"  # 数据处理最多允许输出到一个结果表（业务失败）
    DATA_PROCESSING_OUTPUT_CONFLICT = "042"  # 数据处理输出结果表冲突（业务失败）
    DATA_PROCESSING_TYPE_NOT_EXIST = "043"  # 数据处理类型不存在（业务失败）
    DATA_PROCESSING_RELATION_NOT_EXIST = "044"  # 数据处理关联不存在（业务失败）
    DATA_PROCESSING_INPUT_ILLEGALITY = "045"  # 数据处理输入不合法(业务失败)
    DATA_TRANSFERRING_NOT_EXIST = "050"  # 数据传输不存在（业务失败）
    DATA_TRANSFERRING_TYPE_NOT_EXIST = "051"  # 数据传输类型不存在（业务失败）

    PROJECT_NOT_EXIST = "060"  # （业务失败）
    PROJECT_CAN_NOT_BE_DELETED = "061"  # 默认项目不能被删除（业务失败）

    SYNC_ERR = "070"  # （业务失败）
    SYNC_IN_REMOTE_METADATA_ERR = "071"  # （业务失败）

    METADATA_CALL_ERROR = "080"  # 元数据系统请求异常（系统失败）
    METADATA_COMPLIANCE_ERROR = "091"  # 元数据数据合规检查失败（业务失败）

    ATLAS_DIRECT_CALL_ERROR = "081"  # Atlas直接请求异常
    ATLAS_DIRECT_COMPLIANCE_ERROR = "092"  # Atlas数据合规检查失败

    TDW_SYNC_ERROR = "280"  # （系统失败）
    TDW_SYNC_AUTH_ERROR = "281"  # （业务失败）
    TDW_SYNC_INCONSISTENCY_ERROR = "282"  # （业务失败）
    TDW_UNDEFINED_SYNC_SCENE_ERROR = "283"  # （系统失败）
    TDW_COUNT_FREQ_ERROR = "284"  # （业务失败）
    TDW_FIELD_CONVERT_ERROR = "285"  # （业务失败）
    TDW_CLIENT_ERROR = "286"  # （系统失败）

    TDW_CREDENTIAL_GET_FAILED = "290"  # （业务失败）
    TDW_USABILITY_ERROR = "291"  # （业务失败）
    TDW_TABLE_NOT_EXIST = "300"  # （业务失败）
    TDW_TABLE_EXISTED = "301"  # （业务失败）
    NO_ASSOCIATED_TDW_TABLE = "302"  # （业务失败）
    ASSOCIATED_TDW_TABLE = "303"  # （业务失败）
    TDW_APP_GROUP_NOT_EXIST = "304"  # （业务失败）
    TDW_APP_GROUP_CONF_NOT_EXIST = "305"  # （业务失败）
    TDW_TABLE_FIELD_NOT_EXIST = "306"  # （业务失败）

    TAG_NOT_EXIST = "350"  # （业务失败）
    TAG_TARGET_NOT_EXIST = "351"  # （业务失败）
    TAG_BE_DEPENDED = "352"  # （业务失败）
    TAG_HIBERARCHY_ERROR = "353"  # （业务失败）
    TAG_TARGET_EXIST = "354"  # （业务失败）
    TARGET_NOT_EXIST = "355"  # （业务失败）
    TARGET_TYPE_NOT_ALLOWED = "356"  # （业务失败）
    BUILT_IN_TAG_CREATED_WITHOUT_ID = "357"  # 内建标签创建缺少tag_id

    ERP_CRITERION_ERROR = "500"  # （业务失败）


class MetaError(CommonAPIError):
    MODULE_CODE = ErrorCode.BKDATA_META
    SOLUTION = None

    def __init__(self, message=None, message_kv=None, code=None, errors=None, solution=None):
        super(MetaError, self).__init__(message, message_kv, code, errors)
        self._solution = solution or self.SOLUTION

    @property
    def solution(self):
        return self._solution


class DatabaseError(MetaError):
    MESSAGE = _("数据库操作异常{err_msg}")
    CODE = MetaErrorCode.DATABASE_ERR


class TranslateError(MetaError):
    MESSAGE = _("无法找到key({content_key})对应语言({language})的翻译")
    CODE = MetaErrorCode.TRANSLATE_ERR


class ParamsError(MetaError):
    MESSAGE = _("参数错误：{error_detail}")
    CODE = MetaErrorCode.PARAMS_ERR
    SOLUTION = _("请查看API请求文档")


class ResultTableNotExistError(MetaError):
    MESSAGE = _("结果表({result_table_id})不存在")
    CODE = MetaErrorCode.RESULT_TABLE_NOT_EXIST


class ResultTableFieldConflictError(MetaError):
    MESSAGE = _("结果表字段({field})冲突")
    CODE = MetaErrorCode.RESULT_TABLE_FIELD_CONFLICT


class ResultTableStorageDeleteError(MetaError):
    MESSAGE = _("结果表存储删除失败")
    CODE = MetaErrorCode.RESULT_TABLE_STORAGE_DELETE_ERR


class ResultTableTypeNotExistError(MetaError):
    MESSAGE = _("结果表类型({result_table_type})不存在")
    CODE = MetaErrorCode.RESULT_TABLE_TYPE_NOT_EXIST


class ComputeResultTableNoUserSQLInfoError(MetaError):
    MESSAGE = _("创建计算结果表未同时提交用户SQL")
    CODE = MetaErrorCode.COMPUTE_RESULT_TABLE_NO_SQL


class ResultTableHasRelationError(MetaError):
    MESSAGE = _("该结果表的数据处理尚未删除，请先删除相关数据处理")
    CODE = MetaErrorCode.RESULT_TABLE_TYPE_NOT_EXIST


class ResultTableFieldTypeIllegalError(MetaError):
    MESSAGE = _("结果表字段类型({field_type})不合法")
    CODE = MetaErrorCode.RESULT_TABLE_FIELD_TYPE_ILLEGAL


class ResultTableHasExistedError(MetaError):
    MESSAGE = _("结果表({result_table_id})已存在")
    CODE = MetaErrorCode.RESULT_TABLE_HAS_EXISTED


class TooManyResultTableFieldError(MetaError):
    MESSAGE = _("结果表字段数目超过限制(最大字段数: {fields_limit})")
    CODE = MetaErrorCode.TOO_MANY_RESULT_TABLE_FIELD


class ProjectNotExistError(MetaError):
    MESSAGE = _("项目({project_id})不存在")
    CODE = MetaErrorCode.PROJECT_NOT_EXIST


class ProjectCanNotBeDeleted(MetaError):
    MESSAGE = _("默认项目不能被删除")
    CODE = MetaErrorCode.PROJECT_CAN_NOT_BE_DELETED


class DataProcessingNotExistError(MetaError):
    MESSAGE = _("数据处理({processing_id})不存在")
    CODE = MetaErrorCode.DATA_PROCESSING_NOT_EXIST


class DataProcessingOutputLimit(MetaError):
    MESSAGE = _("数据处理最多允许输出一个结果表，当前参数输出有{}个")
    CODE = MetaErrorCode.DATA_PROCESSING_OUTPUT_LIMIT


class DataProcessingOutputConflictError(MetaError):
    MESSAGE = _("数据处理输出结果表冲突")
    CODE = MetaErrorCode.DATA_PROCESSING_OUTPUT_CONFLICT


class DataProcessingTypeNotExistError(MetaError):
    MESSAGE = _("数据处理类型{processing_type}不存在")
    CODE = MetaErrorCode.DATA_PROCESSING_TYPE_NOT_EXIST


class DataProcessingRelationNotExistError(MetaError):
    MESSAGE = _("数据处理({data_processing_relation.id})不存在")
    CODE = MetaErrorCode.DATA_PROCESSING_RELATION_NOT_EXIST


class DataProcessingRelationInputIllegalityError(MetaError):
    MESSAGE = _("数据处理输入不合法")
    CODE = MetaErrorCode.DATA_PROCESSING_INPUT_ILLEGALITY


class DataTransferringNotExistError(MetaError):
    MESSAGE = _("数据传输({transferring_id})不存在")
    CODE = MetaErrorCode.DATA_TRANSFERRING_NOT_EXIST


class DataTransferringTypeNotExistError(MetaError):
    MESSAGE = _("数据传输类型{transferring_type}不存在")
    CODE = MetaErrorCode.DATA_TRANSFERRING_TYPE_NOT_EXIST


class SyncError(MetaError):
    MESSAGE = _("同步出错：操作元数据系统失败。错误信息：{inner_error}")
    CODE = MetaErrorCode.SYNC_ERR


class RemoteSyncError(MetaError):
    MESSAGE = _("同步出错：元数据不合规。错误：{inner_error}")
    CODE = MetaErrorCode.SYNC_IN_REMOTE_METADATA_ERR


class AtlasDirectCallError(MetaError):
    MESSAGE = _("无法操作元数据系统(Direct)。错误信息：{inner_error}")
    CODE = MetaErrorCode.ATLAS_DIRECT_COMPLIANCE_ERROR


class AtlasDirectComplianceError(MetaError):
    MESSAGE = _("元数据录入出错：数据不合规(Direct)。错误信息：{inner_error}")
    CODE = MetaErrorCode.ATLAS_DIRECT_COMPLIANCE_ERROR


class MetaDataCallError(MetaError):
    MESSAGE = _("操作元数据系统失败。错误：{inner_error}")
    CODE = MetaErrorCode.METADATA_CALL_ERROR


class MetaDataComplianceError(MetaError):
    MESSAGE = _("请求的数据不合规。错误：{inner_error}")
    CODE = MetaErrorCode.METADATA_COMPLIANCE_ERROR


class NotSupportOperateObject(MetaError):
    MESSAGE = _("不支持该操作对象: {operate_object}")
    CODE = MetaErrorCode.NOT_SUPPORT_OPERATE_OBJECT


class NotSupportOperateType(MetaError):
    MESSAGE = _("不支持操作对象({operate_type})的该操作类型: {operate_type}")
    CODE = MetaErrorCode.NOT_SUPPORT_OPERATE_TYPE


class TdwTableNotExistError(MetaError):
    MESSAGE = _("TDW表({table_id})不存在")
    CODE = MetaErrorCode.TDW_TABLE_NOT_EXIST


class TdwTableFieldNotExistError(MetaError):
    MESSAGE = _("TDW表字段({col_name})不存在")
    CODE = MetaErrorCode.TDW_TABLE_FIELD_NOT_EXIST


class TdwTableExistedError(MetaError):
    MESSAGE = _("TDW表({table_id})已存在。无法新建。")
    CODE = MetaErrorCode.TDW_TABLE_EXISTED


class NoAssociatedTdwTableError(MetaError):
    MESSAGE = _("结果表({result_table_id})没有关联的TDW Table。")
    CODE = MetaErrorCode.NO_ASSOCIATED_TDW_TABLE


class AssociatedTdwTableError(MetaError):
    MESSAGE = _("结果表({table_id})已有关联的TDW Table。")
    CODE = MetaErrorCode.ASSOCIATED_TDW_TABLE


class TdwSyncInconsistencyError(MetaError):
    MESSAGE = _("本地元数据与TDW侧不一致。错误信息:{error_msg}。")
    CODE = MetaErrorCode.TDW_SYNC_INCONSISTENCY_ERROR


class TdwSyncAuthError(MetaError):
    MESSAGE = _("本地元数据创建者已失去TDW侧权限。")
    CODE = MetaErrorCode.TDW_SYNC_AUTH_ERROR


class TdwCountFreqError(MetaError):
    MESSAGE = _("统计频率空白或不一致。")
    CODE = MetaErrorCode.TDW_COUNT_FREQ_ERROR


class TdwUndefinedSyncSceneError(MetaError):
    MESSAGE = _("未知同步场景。")
    CODE = MetaErrorCode.TDW_UNDEFINED_SYNC_SCENE_ERROR


class TdwSyncError(MetaError):
    MESSAGE = _("TDW数据同步异常。错误信息:{error_msg}。")
    CODE = MetaErrorCode.TDW_SYNC_ERROR


class TdwClientError(MetaError):
    MESSAGE = _("TDW客户端异常。错误信息:{error_msg}。")
    CODE = MetaErrorCode.TDW_CLIENT_ERROR


class TdwCredentialGetFailed(MetaError):
    MESSAGE = _("无法获取用户{user}的TDW鉴权信息。")
    CODE = MetaErrorCode.TDW_SYNC_AUTH_ERROR


class TdwUsabilityError(MetaError):
    MESSAGE = _("所提交的Usability无法在当前场景下使用。")
    CODE = MetaErrorCode.TDW_USABILITY_ERROR


class TdwAppGroupAccessError(MetaError):
    MESSAGE = _("无法使用当前用户获取TDW应用组({app_group_name})信息。")
    CODE = MetaErrorCode.TDW_APP_GROUP_NOT_EXIST


class TdwAppGroupConfNotExistedError(MetaError):
    MESSAGE = _("当前TDW应用组({app_group_name})配置未存储。")
    CODE = MetaErrorCode.TDW_APP_GROUP_CONF_NOT_EXIST


class TdwFieldConvertError(MetaError):
    MESSAGE = _("无法转换tdw字段类型({field_type})。")
    CODE = MetaErrorCode.TDW_FIELD_CONVERT_ERROR


class TagNotExistError(MetaError):
    MESSAGE = _("标签({code})不存在")
    CODE = MetaErrorCode.TAG_NOT_EXIST


class TargetNotExistError(MetaError):
    MESSAGE = _("目标实体({type}:{id})不存在")
    CODE = MetaErrorCode.TARGET_NOT_EXIST


class TargetTypeNotAllowedError(MetaError):
    MESSAGE = _("实体类型({type})不支持标签")
    CODE = MetaErrorCode.TARGET_TYPE_NOT_ALLOWED


class TagTargetNotExistError(MetaError):
    MESSAGE = _("标签关联关系({tag_target})不存在")
    CODE = MetaErrorCode.TAG_TARGET_NOT_EXIST


class TagTargetExistError(MetaError):
    MESSAGE = _("标签关联关系({tag_target})存在")
    CODE = MetaErrorCode.TAG_TARGET_EXIST


class TagBeDependedError(MetaError):
    MESSAGE = _("标签定义({code})仍被依赖")
    CODE = MetaErrorCode.TAG_BE_DEPENDED


class TagHiberarchyError(MetaError):
    MESSAGE = _("标签({code})的层级解析失败")
    CODE = MetaErrorCode.TAG_HIBERARCHY_ERROR


class BuiltInTagCreateError(MetaError):
    MESSAGE = _("创建标签({code})失败, 未指定tag_id")
    CODE = MetaErrorCode.BUILT_IN_TAG_CREATED_WITHOUT_ID


class DepthError(MetaError):
    MESSAGE = _("层级{depth}不在允许的范围")
    CODE = MetaErrorCode.DEPTH_ERROR


class NotSupportQuery(MetaError):
    MESSAGE = _("不支持的查询")
    CODE = MetaErrorCode.NOT_SUPPORT_QUERY


class ResultTableFieldTypeChangedError(MetaError):
    MESSAGE = _("字段({field_name})类型发生变化。系统禁止变更字段类型，若数据需要使用新的类型请新建字段。")
    CODE = MetaErrorCode.RESULT_TABLE_FIELD_TYPE_CHANGED


class ERPCriterionError(MetaError):
    MESSAGE = _("ERP表达式出错。")
    CODE = MetaErrorCode.ERP_CRITERION_ERROR
    SOLUTION = _("请参照ERP文档检查错误")

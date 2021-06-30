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
from django.utils.translation import ugettext_lazy as _

from dataflow.modeling.exceptions.modeling_exceptions import ModelingException

# BKDATA_DATAAPI_MLSQL = ErrorCode.BKDATA_DATAAPI_MLSQL
BKDATA_DATAAPI_MLSQL = "86"


class ModelingCode(object):
    UNEXPECTED_ERR = ("999", _("非预期异常"))
    DEFAULT_ERR = ("500", _("未知异常"), _("联系管理员"))

    CREATE_PROCESSING_ERR = ("501", _("创建Processing失败"), _("联系管理员"))
    MODEL_CREATE_ERR = ("502", _("模型创建失败"), _("联系管理员"))
    RT_CREATE_ERR = ("503", _("RT创建失败"), _("联系管理员"))
    SQL_SUBMIT_ERR = ("504", _("sql提交失败"), _("联系管理员"))
    GENERATE_CONFIG_ERR = ("505", _("获取依赖关系异常"), _("联系管理员"))
    JOB_CREATE_ERROR_ERR = ("506", _("Job创建失败"), _("联系管理员"))
    JOB_START_ERROR_ERR = ("507", _("Job启动失败"))
    MODEL_UPDATE_ERR = ("508", _("模型更新失败"))
    TASK_KILL_ERR = ("509", _("任务终止失败"))
    TASK_CHECK_ERR = ("510", _("任务状态获取失败"), _("请稍后重试"))
    DDL_SQL_ERR = ("511", _("接口调用失败"), _("接口调用失败，请重试"))
    CLEAR_JOB_ERR = ("512", _("任务清理失败"))
    MODEL_RELEASE_INSPECTION_ERR_CODE = ("513", _("模型发布检查失败."), _("请根据提示检查原因或重试"))

    ALREADY_EXISTS_ERR = ("001", _("表或模型已经存在"), _("请求变更实体名称"))
    SQL_PARSE_ERR = ("003", _("sql解析失败"), _("请检查MLSQL语句是否合法"))
    COMPONENT_TYPE_NOT_SUPPORTED_ERR = ("006", _("组件类型不支持"))
    SQL_NOT_SUPPORTED_ERR = (
        "007",
        _("drop、show及truncate与其它语句混用时，请将它们置于其它语句前面"),
        _("请调整MLSQL语句"),
    )
    MODEL_NOT_EXISTS_ERR = ("010", _("模型不存在"), _("请检查模型是否存在"))
    TABLE_NOT_EXISTS_ERR = ("012", _("表不存在或没有权限"), _("请检查表是否存在"))
    SQL_FORBIDDEN_ERR = ("017", _("表已存在或暂不支持创建并insert同一张表"), _("此用法暂不支持"))
    DROP_MODEL_ERR = ("020", _("删除模型失败"), _("请确认有删除模型的权限"))
    TABLE_NOT_QUERYSET_ERR = (
        "021",
        _("仅支持从查询结果集计算，而当前为普通RT"),
        _("将普通RT导出为查询结果集后再使用"),
    )
    HDFS_NOT_EXISTS_ERR = ("022", _("指定hdfs集群不存在"), _("请检查指定集群是否存在"))
    DATA_PROCESSING_INPUT_ERR = ("023", _("依赖其它数据源"))
    MODEL_EXISTS_ERR_CODE = ("024", _("模型已经存在"), _("请重新命名"))

    DROP_MODEL_NOT_EXISTS_ERR = ("026", _("模型不存在"))
    DROP_TABLE_NOT_EXISTS_ERR = ("027", _("查询结果集不存在"))
    MODEL_EXISTS_ERR = ("028", _("模型已存在"))
    TABLE_EXISTS_ERR = ("029", _("表已经存在"))
    DP_EXISTS_ERR = ("030", _("数据处理已存在"))
    MULTI_SQL_PARSE_ERR = ("031", _("sql解析失败"), _("请检查MLSQL语句是否合法"))
    HDFS_STORAGE_NOT_EXISTS_ERROR = ("032", _("HDFS存储不存在"), _("请检查HDFS存储是否存在"))
    NOT_SUPPORT_JOIN_MODEL_PUBLISH_ERROR = (
        "033",
        _("不支持含有Join子查询的模型进行发布"),
        _("请将子查询转换为查询结果集再尝试"),
    )
    GRPC_SERVICE_NOT_FOUND_ERROR = ("034", _("未找到模型评估服务"), _("请联系管理员"))
    MODEL_EVALUATE_ERROR = ("035", _("模型评估失败"), _("请联系管理员"))

    SUCCESS_CODE = ("200", _("操作成功"))


DEFAULT_MODELING_ERR = "{}{}{}".format(ErrorCode.BKDATA_PLAT_CODE, BKDATA_DATAAPI_MLSQL, ModelingCode.DEFAULT_ERR[0])
DEFAULT_SUCCESS_CODE = "{}{}{}".format(ErrorCode.BKDATA_PLAT_CODE, BKDATA_DATAAPI_MLSQL, ModelingCode.SUCCESS_CODE[0])

EVALUATE_RUNNING_CODE = "{}{}{}".format(ErrorCode.BKDATA_PLAT_CODE, BKDATA_DATAAPI_MLSQL, "300")
EVALUATE_SUCCESS_CODE = "{}{}{}".format(ErrorCode.BKDATA_PLAT_CODE, BKDATA_DATAAPI_MLSQL, "301")
EVALUATE_FAILED_CODE = "{}{}{}".format(ErrorCode.BKDATA_PLAT_CODE, BKDATA_DATAAPI_MLSQL, "302")


class ModelingExpectedException(ModelingException):
    """
    可预期的异常
    """

    CODE = ModelingCode.DEFAULT_ERR[0]
    MESSAGE = _("未知异常")

    def __init__(self, **args):
        super(ModelingExpectedException, self).__init__(**args)


class ModelingUnexpectedException(ModelingException):
    """
    未知异常
    """

    CODE = ModelingCode.UNEXPECTED_ERR[0]
    MESSAGE = "mlsql unexpected error"

    def __init__(self, **args):
        super(ModelingUnexpectedException, self).__init__(**args)


class EntityAlreadyExistsError(ModelingExpectedException):
    def __init__(self, **args):
        super(EntityAlreadyExistsError, self).__init__(**args)

    CODE = ModelingCode.ALREADY_EXISTS_ERR[0]
    MESSAGE = _("{entity}已经存在:{name}")


class ModelAlreadyExistsError(EntityAlreadyExistsError):
    def __init__(self, **args):
        super(ModelAlreadyExistsError, self).__init__(**args)

    CODE = ModelingCode.MODEL_EXISTS_ERR[0]
    MESSAGE = _("模型已存在:{name}")


class TableAlreadyExistsError(EntityAlreadyExistsError):
    def __init__(self, **args):
        super(TableAlreadyExistsError, self).__init__(**args)

    CODE = ModelingCode.TABLE_EXISTS_ERR[0]
    MESSAGE = _("表已经存在:{name}")


class DPAlreadyExistsError(EntityAlreadyExistsError):
    def __init__(self, **args):
        super(DPAlreadyExistsError, self).__init__(**args)

    CODE = ModelingCode.DP_EXISTS_ERR[0]
    MESSAGE = _("数据处理已存在:{name}")


class CreateModelProcessingException(ModelingExpectedException):
    def __init__(self, **args):
        super(CreateModelProcessingException, self).__init__(**args)

    CODE = ModelingCode.CREATE_PROCESSING_ERR[0]
    MESSAGE = _("创建Processing失败")


class SqlParseError(ModelingExpectedException):
    def __init__(self, **args):
        super(SqlParseError, self).__init__(**args)

    CODE = ModelingCode.MULTI_SQL_PARSE_ERR[0]
    MESSAGE = _("第{number}个sql解析失败:{content}")


class SingleSqlParseError(ModelingExpectedException):
    def __init__(self, **args):
        super(SingleSqlParseError, self).__init__(**args)

    CODE = ModelingCode.SQL_PARSE_ERR[0]
    MESSAGE = _("sql解析失败:{content}")


class ModelCreateError(ModelingExpectedException):
    def __init__(self, **args):
        super(ModelCreateError, self).__init__(**args)

    CODE = ModelingCode.MODEL_CREATE_ERR[0]
    MESSAGE = _("模型{name}创建失败:{content}")


class RTCreateError(ModelingUnexpectedException):
    def __init__(self, **args):
        super(RTCreateError, self).__init__(**args)

    CODE = ModelingCode.RT_CREATE_ERR[0]
    MESSAGE = _("RT创建失败:{content}")


class CompoentTypeNotSupportedError(ModelingExpectedException):
    def __init__(self, **args):
        super(CompoentTypeNotSupportedError, self).__init__(**args)

    CODE = ModelingCode.COMPONENT_TYPE_NOT_SUPPORTED_ERR[0]
    MESSAGE = _("组件类型不支持:{content}")


class SQLNotSupportedError(ModelingExpectedException):
    CODE = ModelingCode.SQL_NOT_SUPPORTED_ERR[0]
    MESSAGE = _("drop、show及truncate与其它语句混用时，请将它们置于其它语句前面")


class SQLForbiddenError(ModelingExpectedException):
    CODE = ModelingCode.SQL_FORBIDDEN_ERR[0]
    MESSAGE = _("表{table}已存在或暂不支持创建并insert同一张表")


class SQLSubmitError(ModelingUnexpectedException):
    CODE = ModelingCode.SQL_SUBMIT_ERR[0]
    MESSAGE = _("sql提交失败")


class GenerateConfigError(ModelingUnexpectedException):
    CODE = ModelingCode.GENERATE_CONFIG_ERR[0]
    MESSAGE = _("获取依赖关系异常")


class ModelNotExistsError(ModelingExpectedException):
    def __init__(self, **args):
        super(ModelNotExistsError, self).__init__(**args)

    CODE = ModelingCode.MODEL_NOT_EXISTS_ERR[0]
    MESSAGE = _("模型不存在:{name}")


class TableNotExistsError(ModelingExpectedException):
    def __init__(self, **args):
        super(TableNotExistsError, self).__init__(**args)

    CODE = ModelingCode.TABLE_NOT_EXISTS_ERR[0]
    MESSAGE = _("表不存在或没有权限:{name}")


class CreateJobError(ModelingUnexpectedException):
    def __init__(self, **args):
        super(CreateJobError, self).__init__(**args)

    CODE = ModelingCode.JOB_CREATE_ERROR_ERR[0]
    MESSAGE = _("Job创建失败:{content}")


class StartJobError(ModelingUnexpectedException):
    def __init__(self, **args):
        super(StartJobError, self).__init__(**args)

    CODE = ModelingCode.JOB_START_ERROR_ERR[0]
    MESSAGE = _("Job启动失败:{content}")


class UpdateModelError(ModelingUnexpectedException):
    def __init__(self, **args):
        super(UpdateModelError, self).__init__(**args)

    CODE = ModelingCode.MODEL_UPDATE_ERR[0]
    MESSAGE = _("模型更新失败:{content}")


class KillTaskError(ModelingUnexpectedException):
    def __init__(self, **args):
        super(KillTaskError, self).__init__(**args)

    CODE = ModelingCode.TASK_KILL_ERR[0]
    MESSAGE = _("任务终止失败:{content}")


class CheckTaskError(ModelingUnexpectedException):
    def __init__(self, **args):
        super(CheckTaskError, self).__init__(**args)

    CODE = ModelingCode.TASK_CHECK_ERR[0]
    MESSAGE = _("任务状态获取失败:{content}")


class ClearJobError(ModelingUnexpectedException):
    def __init__(self, **args):
        super(ClearJobError, self).__init__(**args)

    CODE = ModelingCode.CLEAR_JOB_ERR[0]
    MESSAGE = _("任务清理失败:{content}")


class DDLSqlExecuteError(ModelingUnexpectedException):
    def __init__(self, **args):
        super(DDLSqlExecuteError, self).__init__(**args)

    CODE = ModelingCode.DDL_SQL_ERR[0]
    MESSAGE = _("{content}")


class DropModelError(ModelingUnexpectedException):
    def __init__(self, **args):
        super(DropModelError, self).__init__(**args)

    CODE = ModelingCode.DROP_MODEL_ERR[0]
    MESSAGE = _("删除模型失败:{content}")


class DropModelNotExistsError(ModelingExpectedException):
    def __init__(self, **args):
        super(DropModelNotExistsError, self).__init__(**args)

    CODE = ModelingCode.DROP_MODEL_NOT_EXISTS_ERR[0]
    MESSAGE = _("模型不存在:{content}")


class DropTableNotExistsError(ModelingExpectedException):
    def __init__(self, **args):
        super(DropTableNotExistsError, self).__init__(**args)

    CODE = ModelingCode.DROP_TABLE_NOT_EXISTS_ERR[0]
    MESSAGE = _("查询结果集不存在:{content}")


class TableNotQuerySetError(ModelingExpectedException):
    def __init__(self, **args):
        super(TableNotQuerySetError, self).__init__(**args)

    CODE = ModelingCode.TABLE_NOT_QUERYSET_ERR[0]
    MESSAGE = _("仅支持从查询结果集计算，而表{name}为普通RT")


class HDFSNotExistsError(ModelingUnexpectedException):
    def __init__(self, **args):
        super(HDFSNotExistsError, self).__init__(**args)

    CODE = ModelingCode.HDFS_NOT_EXISTS_ERR[0]
    MESSAGE = _("指定hdfs集群不存在:{content}")


class DataProcessingInputError(ModelingExpectedException):
    def __init__(self, **args):
        super(DataProcessingInputError, self).__init__(**args)

    CODE = ModelingCode.DATA_PROCESSING_INPUT_ERR[0]
    MESSAGE = _("依赖其它数据源:{content}")


class ModelExistsError(ModelingExpectedException):
    CODE = ModelingCode.MODEL_EXISTS_ERR_CODE[0]
    MESSAGE = _("模型已经存在")

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = "模型 %s 已经存在, 请重新命名." % args[0]
        super(ModelExistsError, self).__init__(message=self.MESSAGE)


class ModelReleaseInspectionError(ModelingExpectedException):
    CODE = ModelingCode.MODEL_RELEASE_INSPECTION_ERR_CODE[0]
    MESSAGE = _("模型发布检查失败.")

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = args[0]
        super(ModelReleaseInspectionError, self).__init__(message=self.MESSAGE)


class HDFSStorageNotExistsError(ModelingExpectedException):
    def __init__(self, **args):
        super(HDFSStorageNotExistsError, self).__init__(**args)

    CODE = ModelingCode.HDFS_STORAGE_NOT_EXISTS_ERROR[0]
    MESSAGE = _("表{name}未关联hdfs存储")


class NotSupportJoinModelPublishError(ModelingExpectedException):
    def __init__(self, **args):
        super(NotSupportJoinModelPublishError, self).__init__(**args)

    CODE = ModelingCode.NOT_SUPPORT_JOIN_MODEL_PUBLISH_ERROR[0]
    MESSAGE = _("不支持含有Join子查询的模型进行发布:{processing_id}")


class GrpcServiceNotFoundError(ModelingExpectedException):
    def __init__(self, **args):
        super(GrpcServiceNotFoundError, self).__init__(**args)

    CODE = ModelingCode.GRPC_SERVICE_NOT_FOUND_ERROR[0]
    MESSAGE = _("未找到模型评估服务")


class ModelEvaluateError(ModelingExpectedException):
    def __init__(self, **args):
        super(ModelEvaluateError, self).__init__(**args)

    CODE = ModelingCode.MODEL_EVALUATE_ERROR[0]
    MESSAGE = _("模型评估失败：{message}")


class EvaluateFunctionNotFoundError(ModelEvaluateError):
    def __init__(self, **args):
        super(EvaluateFunctionNotFoundError, self).__init__(**args)

    MESSAGE = _("评估函数不存在：{message}")


class EvaluateLabelNotFoundError(ModelEvaluateError):
    def __init__(self, **args):
        super(EvaluateLabelNotFoundError, self).__init__(**args)

    MESSAGE = _("评估列不存在：{message}")

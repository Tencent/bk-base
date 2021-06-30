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
from common.exceptions import BaseAPIError
from django.utils.translation import ugettext_lazy as _


class AuthAPIError(BaseAPIError):
    MODULE_CODE = ErrorCode.BKDATA_AUTH


class AuthCode:
    # 错误码映射表，目前以三元组定义，包含了错误码、描述和处理方式（可选）

    SDK_PERMISSION_DENIED_ERR = ("001", _("资源访问权限不足"))
    SDK_PARAM_MISS_ERR = ("002", _("认证参数缺失"), _("请查看API请求文档"))
    SDK_AUTHENTICATION_ERR = ("003", _("认证不通过，请提供合法的 BKData 认证信息"), _("请查看API请求文档"))
    SDK_INVALID_SECRET_ERR = (
        "004",
        _("内部模块调用请传递准确的 bk_app_code 和 bk_app_secret"),
        _("传递给的变量需要与 dataapi_settings.py 保持一致"),
    )
    SDK_INVALID_TOKEN_ERR = ("005", _("数据平台授权码不正确"), _("前往平台授权码页面查看授权码"))
    SDK_NO_INDENTITY_ERR = ("007", _("未检测到有效的认证信息"), _("请查看API请求文档"))
    SDK_WRONG_INDENTIRY_ERR = ("008", _("错误的认证方式"), _("请查看API请求文档"))
    SDK_JWT_VERIFY_ERR = ("009", _("ESB 传递的 JWT 字符串解析失败"))

    # 单据异常
    NO_PERMISSION_ERR = ("101", _("当前用户无单据权限"))
    TICKET_STATE_HAS_BEEN_OPERATED_ERR = ("102", _("当前单据状态已审批"))
    APPROVAL_RANGE_ERR = ("103", _("审批范围错误"))
    NOT_IN_APPROVAL_PROCESS_ERR = ("104", _("未在可审批阶段"))
    NO_UPDATE_PERMISSION_ERR = ("105", _("无更新权限"))
    OBJECT_CLASS_ERR = ("106", _("object类型错误"))
    SUBJECT_CHECK_ERR = ("107", _("主体校验失败"))
    SCOPE_CHECK_ERR = ("108", _("申请范围校验失败"))
    HAS_ALREADY_EXISTS_ERR = ("109", _("已提交过此权限申请，请勿重复提交"))
    OBJECT_NOT_EXIST_ERR = ("110", _("object不存在"))
    ACTION_CHECK_ERR = ("111", _("action校验错误"))
    NO_PROCESSOR_ERR = ("112", _("没有审批人"))
    UNEXPECTED_TICKET_TYPE = ("113", _("单据类型校验错误"))
    REPEAT_PERMISSION_INFO = ("114", _("重复的权限信息"))
    ROLE_AT_LEAST_ONE_ERR = ("115", _("该角色成员不能为空"))
    PERMISSION_OBJECT_DOSE_NOT_EXIST_ERR = ("116", _("申请的权限对象不存在"))
    PROJECT_DATA_VALID_ERR = ("117", _("项目所申请的业务数据的区域标签不合法"))
    NO_MATCHED_TICKET_TYPE_PROCESS_ERR = ("118", _("没有匹配的单据类型的审批流程"))
    TICKET_CALLBACK_ERR = ("119", _("通用单据回调失败"))
    NOTICE_APPROVE_CALLBACK_ERR = ("120", _("单据通知审核回调异常"))
    NOT_EXIST_ERR = ("120", _("实例不存在"))
    NOT_OBJECT_CLASS_SAME_WITH_META = ("121", _("不存在与 Meta 映射的对象类型"))

    # data token 异常
    TOKEN_NOT_EXIST_ERR = ("201", _("授权码不存在"))
    TOKEN_NOT_AUTHORIZED = ("202", _("授权码未完全通过审批"))
    TOKEN_EXPIRED_ERR = ("203", _("授权码已过期"))
    DATA_SCOPE_FORMAT_ERR = ("204", _("数据范围格式错误"))
    DATA_SCOPE_VALID_ERR = ("205", _("不合法的对象范围"))
    TOKEN_DISABLED_ERR = ("206", _("授权码已被禁止使用"))

    # 资源关系异常
    QUERY_TOO_MANY_RESOURCE_ERR = ("401", _("查询太多资源，拒绝返回内容"))
    INVALID_PARENT_RESOURCE_TYPE_ERR = ("402", _("非法的父级资源类型"))
    INVALID_RESOURCE_ATTR_ERR = ("403", _("非法的资源属性"))
    FILTER_NOT_TO_SCOPE_ERR = ("404", _("资源过滤器无法装换为属性范围"))

    # IAM 关联异常
    BKIAM_NOT_SUPPOR_ATTR_AUTHO = ("501", _("BKIAM 不支持通过接口进行属性授权"))
    BKIAM_CONFIGURE_RESTRICTION = ("502", _("BKIAM 配置限制"))
    BKIAM_SYNC_AUTHO_ERR = ("503", _("BKIAM 同步失败"))
    BKIAM_POLICIES_COUNT_LIMIT_ERR = ("504", _("BKIAM 策略数上限溢出"))

    # BaseModel 异常
    CORE_BASE_MODEL_NO_PK = ("601", _("基础模型配置不存在PK"))
    CORE_BASE_MODEL_INSTANCE_NOT_EXIT = ("602", _("基础模型实例不存在"))

    # Others
    PERMISSION_DENIED_ERR = ("705", _("角色访问权限不足"))
    NO_FUNCTION_ERR = ("706", _("功能还未实现"))
    APP_NOT_MATCH_ERR = ("707", _("非法APP发起访问，请检查 APP_CODE"))
    OUTER_MODEL_ATTR_ERR = ("708", _("存在不符合预期的 Model 数据"))
    PARAM_ERR = ("709", _("参数校验错误"))
    REDIS_CONNECT_ERR = ("710", _("无法连接REDIS服务"), _("检查AuthAPI依赖redis服务是否正常"))
    OBJECT_SERIALIZER_NO_PK_ERR = ("711", _("ObjectClass 类型定义缺少对主键定义"))
    UPDATE_ROLE_ERR = ("712", _("更新角色成员列表错误"))

    # ITSM
    TICKET_CREATE_ERROR = ("801", _("当前有未完成的单据"))
    CALL_BACK_ERROR = ("802", _("回调第三方模块发生错误"))
    ITSM_CATALOGS_NOT_EXIST = ("803", _("未在itsm找到相关的目录"))


class PermissionDeniedError(AuthAPIError):
    CODE = AuthCode.PERMISSION_DENIED_ERR[0]
    MESSAGE = AuthCode.PERMISSION_DENIED_ERR[1]


class NoPermissionError(AuthAPIError):
    CODE = AuthCode.NO_PERMISSION_ERR[0]
    MESSAGE = AuthCode.NO_PERMISSION_ERR[1]


class TicketStateHasBeenOperatedError(AuthAPIError):
    CODE = AuthCode.TICKET_STATE_HAS_BEEN_OPERATED_ERR[0]
    MESSAGE = AuthCode.TICKET_STATE_HAS_BEEN_OPERATED_ERR[1]


class ApprovalRangeError(AuthAPIError):
    CODE = AuthCode.APPROVAL_RANGE_ERR[0]
    MESSAGE = AuthCode.APPROVAL_RANGE_ERR[1]


class NotInApprovalProcessError(AuthAPIError):
    CODE = AuthCode.NOT_IN_APPROVAL_PROCESS_ERR[0]
    MESSAGE = AuthCode.NOT_IN_APPROVAL_PROCESS_ERR[1]


class NoUpdatePermissionError(AuthAPIError):
    CODE = AuthCode.NO_UPDATE_PERMISSION_ERR[0]
    MESSAGE = AuthCode.NO_UPDATE_PERMISSION_ERR[1]


class ObjectClassError(AuthAPIError):
    CODE = AuthCode.OBJECT_CLASS_ERR[0]
    MESSAGE = AuthCode.OBJECT_CLASS_ERR[1]


class SubjectCheckErr(AuthAPIError):
    CODE = AuthCode.SUBJECT_CHECK_ERR[0]
    MESSAGE = AuthCode.SUBJECT_CHECK_ERR[1]


class UnexpectedTicketTypeErr(AuthAPIError):
    CODE = AuthCode.UNEXPECTED_TICKET_TYPE[0]
    MESSAGE = AuthCode.UNEXPECTED_TICKET_TYPE[1]


class ScopeCheckErr(AuthAPIError):
    CODE = AuthCode.SCOPE_CHECK_ERR[0]
    MESSAGE = AuthCode.SCOPE_CHECK_ERR[1]


class NoFunctionErr(AuthAPIError):
    CODE = AuthCode.NO_FUNCTION_ERR[0]
    MESSAGE = AuthCode.NO_FUNCTION_ERR[1]


class HasAlreadyExistsErr(AuthAPIError):
    CODE = AuthCode.HAS_ALREADY_EXISTS_ERR[0]
    MESSAGE = AuthCode.HAS_ALREADY_EXISTS_ERR[1]


class ObjectNotExistsErr(AuthAPIError):
    CODE = AuthCode.OBJECT_NOT_EXIST_ERR[0]
    MESSAGE = AuthCode.OBJECT_NOT_EXIST_ERR[1]


class ActionCheckErr(AuthAPIError):
    CODE = AuthCode.ACTION_CHECK_ERR[0]
    MESSAGE = AuthCode.ACTION_CHECK_ERR[1]


class NoProcessorErr(AuthAPIError):
    CODE = AuthCode.NO_PROCESSOR_ERR[0]
    MESSAGE = AuthCode.NO_PROCESSOR_ERR[1]


class RepeatPermissionErr(AuthAPIError):
    CODE = AuthCode.REPEAT_PERMISSION_INFO[0]
    MESSAGE = AuthCode.REPEAT_PERMISSION_INFO[1]


class RoleAtLeastOneErr(AuthAPIError):
    CODE = AuthCode.ROLE_AT_LEAST_ONE_ERR[0]
    MESSAGE = AuthCode.ROLE_AT_LEAST_ONE_ERR[1]


class TokenNotExistErr(AuthAPIError):
    CODE = AuthCode.TOKEN_NOT_EXIST_ERR[0]
    MESSAGE = AuthCode.TOKEN_NOT_EXIST_ERR[1]


class TokenNotAuthorizedErr(AuthAPIError):
    CODE = AuthCode.TOKEN_NOT_AUTHORIZED[0]
    MESSAGE = AuthCode.TOKEN_NOT_AUTHORIZED[1]


class TokenExpiredErr(AuthAPIError):
    CODE = AuthCode.TOKEN_EXPIRED_ERR[0]
    MESSAGE = AuthCode.TOKEN_EXPIRED_ERR[1]


class DataScopeFormatErr(AuthAPIError):
    CODE = AuthCode.DATA_SCOPE_FORMAT_ERR[0]
    MESSAGE = AuthCode.DATA_SCOPE_FORMAT_ERR[1]


class DataScopeValidErr(AuthAPIError):
    CODE = AuthCode.DATA_SCOPE_VALID_ERR[0]
    MESSAGE = AuthCode.DATA_SCOPE_VALID_ERR[1]


class AppNotMatchErr(AuthAPIError):
    CODE = AuthCode.APP_NOT_MATCH_ERR[0]
    MESSAGE = AuthCode.APP_NOT_MATCH_ERR[1]


class PermissionObjectDoseNotExistError(AuthAPIError):
    CODE = AuthCode.PERMISSION_OBJECT_DOSE_NOT_EXIST_ERR[0]
    MESSAGE = AuthCode.PERMISSION_OBJECT_DOSE_NOT_EXIST_ERR[1]


class TokenDisabledErr(AuthAPIError):
    CODE = AuthCode.TOKEN_DISABLED_ERR[0]
    MESSAGE = AuthCode.TOKEN_DISABLED_ERR[1]


class OuterModelAttrErr(AuthAPIError):
    CODE = AuthCode.OUTER_MODEL_ATTR_ERR[0]
    MESSAGE = AuthCode.OUTER_MODEL_ATTR_ERR[1]


class ProjectDataTagValidErr(AuthAPIError):
    CODE = AuthCode.PROJECT_DATA_VALID_ERR[0]
    MESSAGE = AuthCode.PROJECT_DATA_VALID_ERR[1]


class NoMatchedTicketTypeProcessErr(AuthAPIError):
    CODE = AuthCode.NO_MATCHED_TICKET_TYPE_PROCESS_ERR[0]
    MESSAGE = AuthCode.NO_MATCHED_TICKET_TYPE_PROCESS_ERR[1]


class TicketCallbackErr(AuthAPIError):
    CODE = AuthCode.TICKET_CALLBACK_ERR[0]
    MESSAGE = AuthCode.TICKET_CALLBACK_ERR[1]


class NoticeApproveCallbackErr(AuthAPIError):
    CODE = AuthCode.NOTICE_APPROVE_CALLBACK_ERR[0]
    MESSAGE = AuthCode.NOTICE_APPROVE_CALLBACK_ERR[1]


class NotExistErr(AuthAPIError):
    CODE = AuthCode.NOT_EXIST_ERR[0]
    MESSAGE = AuthCode.NOT_EXIST_ERR[1]


class NotObjectClassSameWithMeta(AuthAPIError):
    CODE = AuthCode.NOT_OBJECT_CLASS_SAME_WITH_META[0]
    MESSAGE = AuthCode.NOT_OBJECT_CLASS_SAME_WITH_META[1]


class ParameterErr(AuthAPIError):
    CODE = AuthCode.PARAM_ERR[0]
    MESSAGE = AuthCode.PARAM_ERR[1]


class RedisConnectError(AuthAPIError):
    CODE = AuthCode.REDIS_CONNECT_ERR[0]
    MESSAGE = AuthCode.REDIS_CONNECT_ERR[1]


class ObjectSerilizerNoPKErr(AuthAPIError):
    CODE = AuthCode.OBJECT_SERIALIZER_NO_PK_ERR[0]
    MESSAGE = AuthCode.OBJECT_SERIALIZER_NO_PK_ERR[1]


class UpdateRoleErr(AuthAPIError):
    CODE = AuthCode.UPDATE_ROLE_ERR[0]
    MESSAGE = AuthCode.UPDATE_ROLE_ERR[1]


class QueryTooManyResourceErr(AuthAPIError):
    CODE = AuthCode.QUERY_TOO_MANY_RESOURCE_ERR[0]
    MESSAGE = AuthCode.QUERY_TOO_MANY_RESOURCE_ERR[1]


class InvalidParentResourceTypeErr(AuthAPIError):
    CODE = AuthCode.INVALID_PARENT_RESOURCE_TYPE_ERR[0]
    MESSAGE = AuthCode.INVALID_PARENT_RESOURCE_TYPE_ERR[1]


class InvalidResourceAttrErr(AuthAPIError):
    CODE = AuthCode.INVALID_RESOURCE_ATTR_ERR[0]
    MESSAGE = AuthCode.INVALID_RESOURCE_ATTR_ERR[1]


class FilterNotToScopeErr(AuthAPIError):
    CODE = AuthCode.FILTER_NOT_TO_SCOPE_ERR[0]
    MESSAGE = AuthCode.FILTER_NOT_TO_SCOPE_ERR[1]


class BKIAMNotSupportAttrAutho(AuthAPIError):
    CODE = AuthCode.BKIAM_NOT_SUPPOR_ATTR_AUTHO[0]
    MESSAGE = AuthCode.BKIAM_NOT_SUPPOR_ATTR_AUTHO[1]


class BKIAMConfigureRestrictionErr(AuthAPIError):
    CODE = AuthCode.BKIAM_CONFIGURE_RESTRICTION[0]
    MESSAGE = AuthCode.BKIAM_CONFIGURE_RESTRICTION[1]


class BKIAMSyncAuthoErr(AuthAPIError):
    CODE = AuthCode.BKIAM_SYNC_AUTHO_ERR[0]
    MESSAGE = AuthCode.BKIAM_SYNC_AUTHO_ERR[1]


class BKIAMPolicesCountLimitErr(AuthAPIError):
    CODE = AuthCode.BKIAM_POLICIES_COUNT_LIMIT_ERR[0]
    MESSAGE = AuthCode.BKIAM_POLICIES_COUNT_LIMIT_ERR[1]


class CoreBaseModelNoPK(AuthAPIError):
    CODE = AuthCode.CORE_BASE_MODEL_NO_PK[0]
    MESSAGE = AuthCode.CORE_BASE_MODEL_NO_PK[1]


class CoreBaseModelInstanceNotExist(AuthAPIError):
    CODE = AuthCode.CORE_BASE_MODEL_INSTANCE_NOT_EXIT[0]
    MESSAGE = AuthCode.CORE_BASE_MODEL_INSTANCE_NOT_EXIT[1]


class TicketCreateError(AuthAPIError):
    CODE = AuthCode.TICKET_CREATE_ERROR[0]
    MESSAGE = AuthCode.TICKET_CREATE_ERROR[1]


class CallBackError(AuthAPIError):
    CODE = AuthCode.CALL_BACK_ERROR[0]
    MESSAGE = AuthCode.CALL_BACK_ERROR[1]


class ItsmCatalogsNotExist(AuthAPIError):
    CODE = AuthCode.ITSM_CATALOGS_NOT_EXIST[0]
    MESSAGE = AuthCode.ITSM_CATALOGS_NOT_EXIST[1]

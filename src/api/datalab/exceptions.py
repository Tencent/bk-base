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


class DatalabError(BaseAPIError):
    MODULE_CODE = ErrorCode.BKDATA_DATAAPI_DATALAB


class DatalabCode(object):
    # 错误码映射表，目前以三元组定义，包含了错误码、描述和处理方式
    PARAM_MISSING_ERR = ("001", _("请求参数必须包含{param}"), _("查看相关参数是否存在"))
    PARAM_FORMAT_ERR = ("002", _("请求参数格式不正确：{param}"), _("检查请求参数"))
    NOTEBOOK_ERR = ("003", _("笔记服务异常，请联系管理员进行排查"), _("联系管理员进行排查"))
    NOTEBOOK_TASK_NOT_FOUND = ("004", _("笔记任务不存在"), _("检查笔记任务是否存在"))
    NOTEBOOK_USER_NOT_FOUND = ("005", _("笔记用户不存在"), _("检查笔记用户是否存在"))
    EXECUTE_CELL_ERR = ("006", _("cell执行失败"), _("cell执行失败，检查执行失败详情"))
    DOWNLOAD_ERR = ("007", _("下载失败，无可下载数据"), _("查看查询任务是否有结果"))
    STORAGE_NOT_SUPPORT = ("008", _("不支持关联的存储类型：{param}"), _("请确认此存储类型是否支持关联"))
    OUTPUT_TYPE_NOT_SUPPORTED = ("009", _("不支持的产出物类型：{param}"), _("请确认此产出物类型是否支持关联"))
    OUTPUT_ALREADY_EXISTS = ("010", _("产出物已存在"), _("不能再次关联已存在的产出物"))
    OUTPUT_NOT_FOUND = ("011", _("产出物不存在"), _("不能取消关联不存在的产出物"))
    REPORT_SECRET_NOT_FOUND = ("012", _("报告秘钥不存在"), _("请确认此报告秘钥是否正确"))
    QUERY_TASK_NOT_FOUND = ("013", _("查询任务不存在"), _("检查查询任务是否存在"))
    # 第三方接口错误: 100 ~ 199
    META_TRANSACT_ERR = ("100", _("结果表创建失败：{message}"), _("根据message信息做检查"))
    CREATE_STORAGE_ERR = ("101", _("关联存储失败：{message}"), _("根据message信息做检查"))
    STORAGE_PREPARE_ERR = ("102", _("存储prepare失败：{message}"), _("根据message信息做检查"))
    DELETE_STORAGE_ERR = ("103", _("删除存储失败：{message}"), _("根据message信息做检查"))
    CLEAR_DATA_ERR = ("104", _("清理数据失败：{message}"), _("根据message信息做检查"))
    # 笔记编辑权相关错误码: 200 ~ 299
    EDITOR_PERMISSION_INVALID = ("200", _("您没有笔记编辑的权限，如需编辑笔记，请申请成为项目数据开发员"), _("申请相关权限"))
    NOTEBOOK_OCCUPIED_ERR = ("201", _("{bk_username}"), _("获取编辑权"))
    TUTORIAL_NOT_SUPPORT_EDITOR = ("202", _("《笔记功能介绍》不支持编辑，如需编辑，请使用笔记克隆功能克隆成可执行笔记"), _("克隆笔记"))


class ParamMissingError(DatalabError):
    CODE = DatalabCode.PARAM_MISSING_ERR[0]
    MESSAGE = DatalabCode.PARAM_MISSING_ERR[1]


class NotebookError(DatalabError):
    CODE = DatalabCode.NOTEBOOK_ERR[0]
    MESSAGE = DatalabCode.NOTEBOOK_ERR[1]


class DownloadError(DatalabError):
    CODE = DatalabCode.DOWNLOAD_ERR[0]
    MESSAGE = DatalabCode.DOWNLOAD_ERR[1]


class ExecuteCellError(DatalabError):
    CODE = DatalabCode.EXECUTE_CELL_ERR[0]
    MESSAGE = DatalabCode.EXECUTE_CELL_ERR[1]


class ParamFormatError(DatalabError):
    CODE = DatalabCode.PARAM_FORMAT_ERR[0]
    MESSAGE = DatalabCode.PARAM_FORMAT_ERR[1]


class NotebookTaskNotFoundError(DatalabError):
    CODE = DatalabCode.NOTEBOOK_TASK_NOT_FOUND[0]
    MESSAGE = DatalabCode.NOTEBOOK_TASK_NOT_FOUND[1]


class NotebookUserNotFoundError(DatalabError):
    CODE = DatalabCode.NOTEBOOK_USER_NOT_FOUND[0]
    MESSAGE = DatalabCode.NOTEBOOK_USER_NOT_FOUND[1]


class StorageNotSupportError(DatalabError):
    CODE = DatalabCode.STORAGE_NOT_SUPPORT[0]
    MESSAGE = DatalabCode.STORAGE_NOT_SUPPORT[1]


class OutputTypeNotSupportedError(DatalabError):
    CODE = DatalabCode.OUTPUT_TYPE_NOT_SUPPORTED[0]
    MESSAGE = DatalabCode.OUTPUT_TYPE_NOT_SUPPORTED[1]


class OutputAlreadyExistsError(DatalabError):
    CODE = DatalabCode.OUTPUT_ALREADY_EXISTS[0]
    MESSAGE = DatalabCode.OUTPUT_ALREADY_EXISTS[1]


class OutputNotFoundError(DatalabError):
    CODE = DatalabCode.OUTPUT_NOT_FOUND[0]
    MESSAGE = DatalabCode.OUTPUT_NOT_FOUND[1]


class ReportSecretNotFoundError(DatalabError):
    CODE = DatalabCode.REPORT_SECRET_NOT_FOUND[0]
    MESSAGE = DatalabCode.REPORT_SECRET_NOT_FOUND[1]


class QueryTaskNotFoundError(DatalabError):
    CODE = DatalabCode.QUERY_TASK_NOT_FOUND[0]
    MESSAGE = DatalabCode.QUERY_TASK_NOT_FOUND[1]


class MetaTransactError(DatalabError):
    CODE = DatalabCode.META_TRANSACT_ERR[0]
    MESSAGE = DatalabCode.META_TRANSACT_ERR[1]


class CreateStorageError(DatalabError):
    CODE = DatalabCode.CREATE_STORAGE_ERR[0]
    MESSAGE = DatalabCode.CREATE_STORAGE_ERR[1]


class StoragePrepareError(DatalabError):
    CODE = DatalabCode.STORAGE_PREPARE_ERR[0]
    MESSAGE = DatalabCode.STORAGE_PREPARE_ERR[1]


class DeleteStorageError(DatalabError):
    CODE = DatalabCode.STORAGE_PREPARE_ERR[0]
    MESSAGE = DatalabCode.STORAGE_PREPARE_ERR[1]


class ClearDataError(DatalabError):
    CODE = DatalabCode.CLEAR_DATA_ERR[0]
    MESSAGE = DatalabCode.CLEAR_DATA_ERR[1]


class EditorPermissionInvalid(DatalabError):
    CODE = DatalabCode.EDITOR_PERMISSION_INVALID[0]
    MESSAGE = DatalabCode.EDITOR_PERMISSION_INVALID[1]


class NotebookOccupied(DatalabError):
    CODE = DatalabCode.NOTEBOOK_OCCUPIED_ERR[0]
    MESSAGE = DatalabCode.NOTEBOOK_OCCUPIED_ERR[1]


class NotebookTutorialNotSupportEditor(DatalabError):
    CODE = DatalabCode.TUTORIAL_NOT_SUPPORT_EDITOR[0]
    MESSAGE = DatalabCode.TUTORIAL_NOT_SUPPORT_EDITOR[1]

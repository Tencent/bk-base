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

from dataflow.udf.exceptions.udf_exception import UdfException


class UdfCode(object):
    UNEXPECTED_ERR = ("999", _("非预期异常"))
    DEV_LOCK_ERR = ("001", _("函数开发界面被锁定"))
    DEBUG_MAVEN_ERR = ("002", _("函数调试中 maven 编译异常"))
    DEBUG_SQL_ERR = ("003", _("调试 SQL 必须包含且只能包含开发的UDF函数"))
    PACKAGE_ERR = ("004", _("UDF 函数代码打包失败"))
    SECURITY_CHECK_ERR = ("005", _("未通过代码安全检测"))
    LIST_FUNCTIONS_ERR = ("006", _("展示 UDF 列表失败"))
    PARSE_SQL_ERR = ("007", _("调试 SQL 解析失败"))
    FUNC_NAME_REGEX_ERR = ("008", _("函数名称必须以小写字母开头，并且只能包含小写字母、数字或下划线"))
    FUNC_EXISTS_ERR = ("009", _("函数名称已存在"))
    FUNC_LANGUAGE_NOT_SUPPORT_ERR = ("010", _("不支持此开发语言"))
    YARN_RESOURCES_LACK_ERR = ("011", _("计算资源不足"), _("请联系数据平台管理员"))
    FUNC_NOT_EXISTS_ERR = ("012", _("函数不存在"))
    EXAMPLE_SQL_ERR = ("013", _("请规范使用样例SQL"))
    FUNCTION_DELETE_ERR = ("014", _("有计算节点使用该函数，禁止删除"))
    FUNCTION_DELETE_ERR1 = ("015", _("此函数不是用户自定义函数，不允许删除"))
    FTP_SERVER_INFO_ERR = ("017", _("ftp服务器信息异常"))
    DEBUG_JOB_SUBMIT_ERR = ("018", _("函数调试任务提交异常"))
    DEBUG_JOB_RUN_ERR = ("019", _("函数调试任务运行时异常"))


class UdfExpectedException(UdfException):
    """
    Expected exception.
    """

    def __init__(self, *args):
        raw_message = args[0] if len(args) > 0 else self.MESSAGE
        self.MESSAGE = raw_message
        super(UdfExpectedException, self).__init__(raw_message)


class UdfUnexpectedException(UdfException):
    """
    Unexpected exception.
    """

    CODE = UdfCode.UNEXPECTED_ERR[0]
    MESSAGE = UdfCode.UNEXPECTED_ERR[1]


class FunctionDevLockError(UdfExpectedException):
    CODE = UdfCode.DEV_LOCK_ERR[0]
    MESSAGE = UdfCode.DEV_LOCK_ERR[1]

    def __init__(self, *args):
        if len(args) == 2:
            self.MESSAGE = _(
                "%(basic_message)s, 锁定人: %(operator)s, 锁定时间: %(lock_time)s"
                % {
                    "basic_message": UdfCode.DEV_LOCK_ERR[1],
                    "operator": args[0],
                    "lock_time": args[1],
                }
            )
        super(FunctionDevLockError, self).__init__(self.MESSAGE)


class DebugMavenRunError(UdfExpectedException):
    CODE = UdfCode.DEBUG_MAVEN_ERR[0]
    MESSAGE = UdfCode.DEBUG_MAVEN_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = args[0]
        super(DebugMavenRunError, self).__init__(self.MESSAGE)


class DebugSqlError(UdfExpectedException):
    CODE = UdfCode.DEBUG_SQL_ERR[0]
    MESSAGE = UdfCode.DEBUG_SQL_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = "{} {}".format(UdfCode.DEBUG_SQL_ERR[1], args[0])
        super(DebugSqlError, self).__init__(self.MESSAGE)


class PackageError(UdfExpectedException):
    CODE = UdfCode.PACKAGE_ERR[0]
    MESSAGE = UdfCode.PACKAGE_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = "{} {}".format(UdfCode.PACKAGE_ERR[1], args[0])
        super(PackageError, self).__init__(self.MESSAGE)


class SecurityCheckError(UdfExpectedException):
    CODE = UdfCode.SECURITY_CHECK_ERR[0]
    MESSAGE = UdfCode.SECURITY_CHECK_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = args[0]
        super(SecurityCheckError, self).__init__(self.MESSAGE)


class ListFunctionsError(UdfExpectedException):
    CODE = UdfCode.LIST_FUNCTIONS_ERR[0]
    MESSAGE = UdfCode.LIST_FUNCTIONS_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = args[0]
        super(ListFunctionsError, self).__init__(self.MESSAGE)


class ParseSqlError(UdfExpectedException):
    CODE = UdfCode.PARSE_SQL_ERR[0]
    MESSAGE = UdfCode.PARSE_SQL_ERR[1]


class FunctionNameRegexError(UdfExpectedException):
    CODE = UdfCode.FUNC_NAME_REGEX_ERR[0]
    MESSAGE = UdfCode.FUNC_NAME_REGEX_ERR[1]


class FunctionExistsError(UdfExpectedException):
    CODE = UdfCode.FUNC_EXISTS_ERR[0]
    MESSAGE = UdfCode.FUNC_EXISTS_ERR[1]


class FuncLanguageNotSupportError(UdfExpectedException):
    CODE = UdfCode.FUNC_LANGUAGE_NOT_SUPPORT_ERR[0]
    MESSAGE = UdfCode.FUNC_LANGUAGE_NOT_SUPPORT_ERR[1]


class YarnResourcesLackError(UdfExpectedException):
    CODE = UdfCode.YARN_RESOURCES_LACK_ERR[0]
    MESSAGE = UdfCode.YARN_RESOURCES_LACK_ERR[1]


class FunctionNotExistsError(UdfExpectedException):
    CODE = UdfCode.FUNC_NOT_EXISTS_ERR[0]
    MESSAGE = UdfCode.FUNC_NOT_EXISTS_ERR[1]


class ExampleSqlError(UdfExpectedException):
    CODE = UdfCode.EXAMPLE_SQL_ERR[0]
    MESSAGE = UdfCode.EXAMPLE_SQL_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = _(
                "%(basic_message)s，且必须包含此函数： %(function_name)s"
                % {
                    "basic_message": UdfCode.EXAMPLE_SQL_ERR[1],
                    "function_name": args[0],
                }
            )
        super(ExampleSqlError, self).__init__(self.MESSAGE)


class FunctionDeleteError(UdfExpectedException):
    CODE = UdfCode.FUNCTION_DELETE_ERR[0]
    MESSAGE = UdfCode.FUNCTION_DELETE_ERR[1]


class FunctionIllegalDeleteError(UdfExpectedException):
    CODE = UdfCode.FUNCTION_DELETE_ERR1[0]
    MESSAGE = UdfCode.FUNCTION_DELETE_ERR1[1]


class FtpServerInfoError(UdfExpectedException):

    CODE = UdfCode.FTP_SERVER_INFO_ERR[0]
    MESSAGE = UdfCode.FTP_SERVER_INFO_ERR[1]


class DebugJobSumitError(UdfExpectedException):
    CODE = UdfCode.DEBUG_JOB_SUBMIT_ERR[0]
    MESSAGE = UdfCode.DEBUG_JOB_SUBMIT_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = args[0]
        super(DebugJobSumitError, self).__init__(self.MESSAGE)


class DebugJobRunError(UdfExpectedException):
    CODE = UdfCode.DEBUG_JOB_RUN_ERR[0]
    MESSAGE = UdfCode.DEBUG_JOB_RUN_ERR[1]

    def __init__(self, *args):
        if len(args) == 1:
            self.MESSAGE = args[0]
        super(DebugJobRunError, self).__init__(self.MESSAGE)

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

from dataflow.batch.error_code.errorcodes import DataapiBatchCode

from .base_exception import BatchException


class BatchUnexpectedException(BatchException):
    """
    未知异常
    """

    custom_code = DataapiBatchCode.UNEXPECT_EX[0]
    custom_message = "batch unexpected error"

    def __init__(self, message=None, errors=None):
        raw_message = self.custom_message
        if message is not None:
            raw_message = message
        super(BatchUnexpectedException, self).__init__(raw_message, self.custom_code, errors=errors)


class BatchExpectedException(BatchException):
    """
    可预期的异常
    """

    custom_code = DataapiBatchCode.DEFAULT_EXPECT_ERR[0]
    custom_message = DataapiBatchCode.DEFAULT_EXPECT_ERR[1]

    def __init__(self, message=None, errors=None):
        raw_message = self.custom_message
        if message is not None:
            raw_message = message
        super(BatchExpectedException, self).__init__(raw_message, self.custom_code, errors=errors)


class BatchIllegalArgumentError(BatchExpectedException):
    custom_code = DataapiBatchCode.ILLEGAL_ARGUMENT_EX[0]
    custom_message = DataapiBatchCode.ILLEGAL_ARGUMENT_EX[1]


class BatchNotImplementError(BatchExpectedException):
    custom_code = DataapiBatchCode.NOT_IMPLEMENT_EX[0]
    custom_message = DataapiBatchCode.NOT_IMPLEMENT_EX[1]


class BatchUnsupportedOperationError(BatchExpectedException):
    custom_code = DataapiBatchCode.UNSUPPORT_OPERATION_EX[0]
    custom_message = DataapiBatchCode.UNSUPPORT_OPERATION_EX[1]


class BatchIllegalStatusError(BatchExpectedException):
    custom_code = DataapiBatchCode.ILLEGAL_STATUS_EX[0]
    custom_message = DataapiBatchCode.ILLEGAL_STATUS_EX[1]


class BatchTimeCompareError(BatchExpectedException):
    custom_code = DataapiBatchCode.BATCH_TIME_COMPARE_ERROR[0]
    custom_message = DataapiBatchCode.BATCH_TIME_COMPARE_ERROR[1]


class CodeCheckSyntaxError(BatchExpectedException):
    custom_code = DataapiBatchCode.CODECHECK_SYNTAX_ERR[0]
    custom_message = DataapiBatchCode.CODECHECK_SYNTAX_ERR[1]


class CodeCheckBannedCodeError(BatchExpectedException):
    custom_code = DataapiBatchCode.CODECHECK_BANNEDCODE_ERR[0]
    custom_message = DataapiBatchCode.CODECHECK_BANNEDCODE_ERR[1]


class JobNaviNoValidScheduleError(BatchExpectedException):
    custom_code = DataapiBatchCode.NO_RUNNING_JOBNAVI_INSTANCE_ERR[0]
    custom_message = DataapiBatchCode.NO_RUNNING_JOBNAVI_INSTANCE_ERR[1]


class JobNaviAlreadyExitsScheduleError(BatchExpectedException):
    custom_code = DataapiBatchCode.JOBNAVI_SCHEDULE_ALREADY_EXISTS_ERR[0]
    custom_message = DataapiBatchCode.JOBNAVI_SCHEDULE_ALREADY_EXISTS_ERR[1]


class SparkSQLCheckException(BatchExpectedException):
    custom_code = DataapiBatchCode.SPARK_SQL_CHECK_ERR[0]
    custom_message = DataapiBatchCode.SPARK_SQL_CHECK_ERR[1]


class OneTimeJobDataAlreadyExistsException(BatchExpectedException):
    custom_code = DataapiBatchCode.ONE_TIME_JOB_DATA_ALREADY_EXISTS_ERR[0]
    custom_message = DataapiBatchCode.ONE_TIME_JOB_DATA_ALREADY_EXISTS_ERR[1]


class OneTimeJobDataNotFoundException(BatchExpectedException):
    custom_code = DataapiBatchCode.ONE_TIME_JOB_DATA_NOT_FOUND_ERR[0]
    custom_message = DataapiBatchCode.ONE_TIME_JOB_DATA_NOT_FOUND_ERR[1]


class JobInfoNotFoundException(BatchExpectedException):
    custom_code = DataapiBatchCode.JOB_INFO_NOT_FOUND_ERR[0]
    custom_message = DataapiBatchCode.JOB_INFO_NOT_FOUND_ERR[1]


class BatchInfoNotFoundException(BatchExpectedException):
    custom_code = DataapiBatchCode.BATCH_INFO_NOT_FOUND_ERR[0]
    custom_message = DataapiBatchCode.BATCH_INFO_NOT_FOUND_ERR[1]


class BatchInfoAlreadyExistsException(BatchExpectedException):
    custom_code = DataapiBatchCode.BATCH_INFO_ALREADY_EXISTS_ERR[0]
    custom_message = DataapiBatchCode.BATCH_INFO_ALREADY_EXISTS_ERR[1]


class BatchNoInputDataException(BatchExpectedException):
    custom_code = DataapiBatchCode.NO_INPUT_DATA_EX[0]
    custom_message = DataapiBatchCode.NO_INPUT_DATA_EX[1]


class BatchSQLNodeCheckException(BatchExpectedException):
    custom_code = DataapiBatchCode.BATCH_SQL_NODE_CHECK_INVALID_ERR[0]
    custom_message = DataapiBatchCode.BATCH_SQL_NODE_CHECK_INVALID_ERR[1]


class BatchInteractiveServerTimeoutException(BatchExpectedException):
    custom_code = DataapiBatchCode.CREATE_SESSION_SERVER_TIMEOUT_ERR[0]
    custom_message = DataapiBatchCode.CREATE_SESSION_SERVER_TIMEOUT_ERR[1]


class BatchResourceNotFoundException(BatchExpectedException):
    custom_code = DataapiBatchCode.RESOURCE_NOT_FOUND_ERR[0]
    custom_message = DataapiBatchCode.RESOURCE_NOT_FOUND_ERR[1]


class ExternalApiReturnException(BatchExpectedException):
    custom_code = DataapiBatchCode.EXTERNAL_API_RETURN_ERR[0]
    custom_message = DataapiBatchCode.EXTERNAL_API_RETURN_ERR[1]


class InnerHDFSServerException(BatchExpectedException):
    custom_code = DataapiBatchCode.HDFS_SERVER_EX[0]
    custom_message = DataapiBatchCode.HDFS_SERVER_EX[1]


class InnerYarnServerException(BatchExpectedException):
    custom_code = DataapiBatchCode.YARN_SERVER_EX[0]
    custom_message = DataapiBatchCode.YARN_SERVER_EX[1]


class TdwEnvJarInvalidException(BatchExpectedException):
    custom_code = DataapiBatchCode.TDW_ENV_JAR_INVALID_ERR[0]
    custom_message = DataapiBatchCode.TDW_ENV_JAR_INVALID_ERR[1]


class HiveScheduleInvalidException(BatchExpectedException):
    custom_code = DataapiBatchCode.HIVE_SCHEDULE_INVALID_ERR[0]
    custom_message = DataapiBatchCode.HIVE_SCHEDULE_INVALID_ERR[1]


class JobSysException(BatchExpectedException):
    """
    作业平台 异常
    """

    custom_code = DataapiBatchCode.JOB_SYS_ERR[0]
    custom_message = DataapiBatchCode.JOB_SYS_ERR[1]

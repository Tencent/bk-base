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


class DataapiBatchCode(object):

    # 期望内异常
    DEFAULT_EXPECT_ERR = ("000", _("Batch可预期异常"))

    # 参数异常
    ILLEGAL_ARGUMENT_EX = ("001", _("Batch非法参数异常"))

    # 状态异常
    ILLEGAL_STATUS_EX = ("002", _("Batch非法状态异常"))
    NOT_IMPLEMENT_EX = ("004", _("Batch方法没有实现异常"))
    UNSUPPORT_OPERATION_EX = ("005", _("Batch非支持的操作异常"))

    # 期望外异常
    UNEXPECT_EX = ("777", _("Batch非预期异常"))

    # 数据异常
    NO_INPUT_DATA_EX = ("101", _("Batch没有输入数据异常"))
    NO_OUTPUT_DATA_EX = ("111", _("Batch没有输出数据异常"))

    # code check异常
    CODECHECK_SYNTAX_ERR = ("130", _("代码检查语法异常"))
    CODECHECK_BANNEDCODE_ERR = ("131", _("代码检查发现禁用方法异常"))

    # jobnavi异常
    NO_RUNNING_JOBNAVI_INSTANCE_ERR = ("150", _("不能找到Jobnavi在运行的实例"))
    JOBNAVI_SCHEDULE_ALREADY_EXISTS_ERR = ("151", _("Jobnavi调度信息已经存在"))

    # Node valid异常
    SPARK_SQL_CHECK_ERR = ("180", _("SparkSQL校验异常"))
    BATCH_SQL_NODE_CHECK_INVALID_ERR = ("181", _("节点校验异常"))
    BATCH_TIME_COMPARE_ERROR = ("182", _("该时间无法比较错误"))

    # one time job异常
    ONE_TIME_JOB_DATA_ALREADY_EXISTS_ERR = ("210", _("一次性任务已存在"))
    ONE_TIME_JOB_DATA_NOT_FOUND_ERR = ("211", _("找不到一次性任务异常"))

    # 交互式session server
    CREATE_SESSION_SERVER_TIMEOUT_ERR = ("230", _("Session server创建超时"))

    # resource center
    RESOURCE_NOT_FOUND_ERR = ("250", _("无法找到有效的资源选项"))

    # DB操作异常
    BATCH_INFO_NOT_FOUND_ERR = ("401", _("Batch Info库中找不到数据"))
    BATCH_INFO_ALREADY_EXISTS_ERR = ("402", _("Batch Info库中数据已存在"))
    JOB_INFO_NOT_FOUND_ERR = ("403", _("Job info库中找不到数据"))

    # API异常
    EXTERNAL_API_RETURN_ERR = ("420", _("api调用返回异常"))

    # 服务异常
    HDFS_SERVER_EX = ("500", _("HDFS调用异常"))
    YARN_SERVER_EX = ("501", _("Yarn调用异常"))
    JOB_SYS_ERR = ("502", _("作业平台调用异常"))  # 作业平台错误

    # ADHOC Query
    ADHOC_QUERY_ERR = ("601", _("Adhoc query调用异常"))

    # TDW
    TDW_ENV_JAR_INVALID_ERR = ("620", _("运行环境jar包没有找到"))

    # HIVE
    HIVE_SCHEDULE_INVALID_ERR = ("640", _("Hive任务非法调度异常"))

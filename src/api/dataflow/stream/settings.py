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

# flake8: noqa
import os
from enum import Enum

from dataflow.pizza_settings import (
    BASE_BKSQL_URL,
    BASE_DATABUS_URL,
    BASE_STREAM_URL,
    FLINK_CODE_VERSION,
    FLINK_VERSION,
    JOBNAVI_STREAM_CLUSTER_ID,
    KAFKA_OP_ROLE_NAME,
    METRIC_KAFKA_TOPIC,
    REDIS_INFO,
    RELEASE_ENV,
    RELEASE_ENV_TGDP,
    RUN_VERSION,
    SPARK_STRUCTURED_STREAMING_CHECKPOINT_FS_CLEAN_PROXY_USER,
    SPARK_STRUCTURED_STREAMING_CHECKPOINT_FS_CLUSTER_TAG,
    STORM_RESOURCE_THRESHOLD,
    STREAM_DEBUG_ERROR_DATA_REST_API_URL,
    STREAM_DEBUG_NODE_METRIC_REST_API_URL,
    STREAM_DEBUG_RESULT_DATA_REST_API_URL,
    STREAM_YARN_CLUSTER_DEFAULT_RESOURCE,
    UC_TIME_ZONE,
)

# 查询yarn session 轮询时长
QUERY_APPLICATION_TIMEOUT = 180
STREAM_API_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__)))
PIZZA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
STREAM_WORK_DIR = os.path.abspath(os.path.join(STREAM_API_DIR, "extend/support"))
# 任务json配置目录
STREAM_JOB_CONFIG_DIR = os.path.abspath(os.path.join(STREAM_API_DIR, "extend/support/tmp"))

# storm 1.0.3版本集群
STORM2_CLUSTERS = ["huanle2", "msdk"]

# bksql api url
BKSQL_API_URL = BASE_BKSQL_URL

DATABUS_API_URL = BASE_DATABUS_URL

BASE_STREAM_URL = BASE_STREAM_URL

STREAM_USED_API_URL = {
    "get_result_table_chain": BASE_STREAM_URL + "result_tables/get_chain/",
    "submit_job": BASE_STREAM_URL + "jobs/{job_id}/submit/",
    "cancel_job": BASE_STREAM_URL + "jobs/{job_id}/cancel/",
    "sync_status": BASE_STREAM_URL + "jobs/{job_id}/sync_status/",
    "get_code_version": BASE_STREAM_URL + "jobs/{job_id}/code_version/",
    "set_error_data": BASE_STREAM_URL + "debugs/{debug_id}/error_data/",
    "get_udf_info": BASE_STREAM_URL + "processings/{processing_id}/udf_info/",
    "get_job_info": BASE_STREAM_URL + "jobs/{job_id}/",
}

# type
CLEAN_TYPE = "clean"
STREAM_TYPE = "stream"

# user 代表用户生成的即真正的实时表 system代表系统生成的，即虚拟表
USER_GENERATE_TYPE = "user"
SYSTEM_GENERATE_TYPE = "system"

# 企业版还是内部版

RELEASE_ENV = RELEASE_ENV

REDIS_INFO = REDIS_INFO

FLINK_YARN_SESSION_QUEUE = "root.dataflow.stream.{cluster_group}.session"
FLINK_YARN_CLUSTER_QUEUE = "root.dataflow.stream.{cluster_group}.cluster"


class DeployMode(Enum):
    YARN_SESSION = "yarn-session"
    YARN_CLUSTER = "yarn-cluster"
    K8S = "k8s"


class ImplementType(Enum):
    SQL = "sql"
    CODE = "code"
    YAML = "yaml"


class ComponentType(Enum):
    STORM = "storm"
    FLINK = "flink"


class ProgrammingLanguage(Enum):
    JAVA = "java"
    PYTHON = "python"


class ComponentType(Enum):
    STORM = "storm"
    FLINK = "flink"
    SPARK_STRUCTURED_STREAMING = "spark_structured_streaming"


# 支持的窗口类型
NONE = "none"
TUMBLING = "tumbling"
ACCUMULATE = "accumulate"
SLIDING = "sliding"
SESSION = "session"
WINDOW_TYPE = [TUMBLING, ACCUMULATE, SLIDING, SESSION]

# 适配层 type_id 和 FLINK 版本的映射
FLINK_VERSIONS = {
    "stream": FLINK_VERSION,
    "stream-code-flink-%s" % FLINK_CODE_VERSION: FLINK_CODE_VERSION,
}

# 调试 implement 和 FLINK 版本的映射
DEBUG_CLUSTER_VERSIONS = {
    ImplementType.SQL.value: "debug_standard",
    ImplementType.CODE.value: "debug_standard_{version_tag}".format(version_tag=FLINK_CODE_VERSION),
}

DEFAULT_GEOG_AREA_CODE = "inland"

# redis记录flink checkpoint的key前缀
FLINK_CHECKPOINT_PREFIX = "bkdata|flink|"

COMMON_MAX_COROUTINE_NUM = 5

RECOVER_SESSION_MAX_COROUTINE_NUM = 20

RECOVER_JOB_MAX_COROUTINE_NUM = 20

API_ERR_RETRY_TIMES = 20

ONE_CODE_USER_MAIN_CLASS = "CodeTransform"

RESOURCES_ALLOCATE_MEMORY_DIVIDE_SLOT_NUM = 5

SPARK_STRUCTURED_STREAMING_CHECKPOINT_FS_CLUSTER_TAG = SPARK_STRUCTURED_STREAMING_CHECKPOINT_FS_CLUSTER_TAG

SPARK_STRUCTURED_STREAMING_CHECKPOINT_FS_CLEAN_PROXY_USER = SPARK_STRUCTURED_STREAMING_CHECKPOINT_FS_CLEAN_PROXY_USER

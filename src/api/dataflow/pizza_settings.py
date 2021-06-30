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
import re

from conf.dataapi_settings import *
from pizza.settings_default import (
    INSTALLED_APPS,
    LOG_BACKUP_COUNT,
    LOG_CLASS,
    LOG_DIR,
    LOG_MAX_BYTES,
    LOGGING,
    MIDDLEWARE,
    REST_FRAMEWORK,
)

DATA_FLOW_APPS = ["flow", "stream", "batch", "component", "udf"]
# 系统保留字段
SYSTEM_RESERVE_FIELD = ["timestamp", "_startTime_", "_endTime_"]

if RUN_MODE in ["LOCAL"]:
    # 开发时候注意改成开发环境自定义接入地址
    BASE_ESB_URL = "http://127.0.0.1:8000"
    BASE_LOCAL_URL = "http://127.0.0.1:8000"
    try:
        from dataflow.dev.flow_settings import BASE_ESB_URL, BASE_LOCAL_URL
    except ImportError:
        pass

    DATA_FLOW_APPS = ["stream", "batch", "flow", "component", "udf", "modeling", "uc"]
else:
    DATA_FLOW_APPS = ["flow", "stream", "batch", "component", "udf", "modeling", "uc"]

    BASE_ESB_URL = BK_PAAS_HOST + "/api/c/compapi"
    BASE_LOCAL_URL = "http://{host}:{port}".format(host=BIND_IP_ADDRESS, port=pizza_port)

DATABASE_ROUTERS = ["dataflow.db_router.DBRouter"]

BASE_DATAFLOW_URL = BASE_LOCAL_URL + "/v3/dataflow/"
BASE_STREAM_URL = BASE_LOCAL_URL + "/v3/dataflow/stream/"
BASE_FLOW_URL = BASE_LOCAL_URL + "/v3/dataflow/flow/"
BASE_BATCH_URL = BASE_LOCAL_URL + "/v3/dataflow/batch/"
BASE_MODEL_APP_URL = BASE_LOCAL_URL + "/v3/dataflow/modelapp/"
BASE_COMPONENT_URL = BASE_LOCAL_URL + "/v3/dataflow/component/"

BASE_META_URL = "http://{host}:{port}/v3/meta/".format(host=META_API_HOST, port=META_API_PORT)

BASE_JOBNAVI_URL = "http://{host}:{port}/v3/jobnavi/".format(host=JOBNAVI_API_HOST, port=JOBNAVI_API_PORT)

BASE_DATABUS_URL = "http://{host}:{port}/v3/databus/".format(host=DATABUS_API_HOST, port=DATABUS_API_PORT)

BASE_STOREKIT_URL = "http://{host}:{port}/v3/storekit/".format(host=STOREKIT_API_HOST, port=STOREKIT_API_PORT)

BASE_DATAMANAGE_URL = "http://{host}:{port}/v3/datamanage/".format(host=DATAMANAGE_API_HOST, port=DATAMANAGE_API_PORT)

BASE_ACCESS_URL = "http://{host}:{port}/v3/access/".format(host=ACCESS_API_HOST, port=ACCESS_API_PORT)

BASE_MODEL_URL = "http://{host}:{port}/v3/algorithm/".format(host=MODELFLOW_API_HOST, port=MODELFLOW_API_PORT)

BASE_PROCESS_MODEL_URL = "http://{host}:{port}/v3/aiops/".format(host=MODEL_API_HOST, port=MODEL_API_PORT)

BASE_AUTH_URL = "http://{host}:{port}/v3/auth/".format(host=AUTH_API_HOST, port=AUTH_API_PORT)

BASE_CODECHECK_URL = "http://{host}:{port}/v3/codecheck/".format(host=CODECHECK_API_HOST, port=CODECHECK_API_PORT)

BASE_RESOURCECENTER_URL = "http://{host}:{port}/v3/resourcecenter/".format(
    host=RESOURCECENTER_API_HOST, port=RESOURCECENTER_API_PORT
)

BASE_DATALAB_URL = "http://{host}:{port}/v3/datalab/".format(
    host=BASE_DATALAB_URL_API_HOST, port=BASE_DATALAB_URL_API_PORT
)

BASE_MODELING_URL = "http://{host}:{port}/v3/dataflow/modeling/".format(host=DATAFLOW_API_HOST, port=DATAFLOW_API_PORT)

BASE_TOOL_URL = BASE_ESB_URL + "/data/tool/"
BASE_CMSI_URL = BASE_ESB_URL + "/v3/cmsi/"
BASE_BKSQL_URL = "http://{host}:{port}/v3/".format(host=BKSQL_HOST, port=BKSQL_PORT)

BASE_BKSQL_SPARKSQL_URL = "http://{host}:{port}/v3/".format(host=BKSQL_SPARKSQL_HOST, port=BKSQL_SPARKSQL_PORT)

BASE_BKSQL_FLINKSQL_URL = "http://{host}:{port}/v3/".format(host=BKSQL_FLINKSQL_HOST, port=BKSQL_FLINKSQL_PORT)

BASE_BKSQL_STORMSQL_URL = "http://{host}:{port}/v3/".format(host=BKSQL_STORMSQL_HOST, port=BKSQL_STORMSQL_PORT)

BASE_BKSQL_MLSQL_URL = "http://{host}:{port}/v3/".format(host=BKSQL_MLSQL_HOST, port=BKSQL_MLSQL_PORT)

BASE_BKSQL_EXTEND_URL = "http://{host}:{port}/v3/".format(host=BKSQL_EXTEND_HOST, port=BKSQL_EXTEND_PORT)

BASE_AIOPS_URL = "http://{host}:{port}/v3/aiops/".format(host=AIOPS_API_HOST, port=AIOPS_API_PORT)

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": "bkdata_flow",
        "USER": DATAFLOW_DB_USER,
        "PASSWORD": DATAFLOW_DB_PASSWORD,
        "HOST": DATAFLOW_DB_HOST,
        "PORT": DATAFLOW_DB_PORT,
        "ENCRYPTED": False,
        "TEST": {"NAME": "test_bkdata_flow"},
    },
    "bkdata_basic": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": "bkdata_basic",
        "USER": CONFIG_DB_USER,
        "PASSWORD": CONFIG_DB_PASSWORD,
        "HOST": CONFIG_DB_HOST,
        "PORT": CONFIG_DB_PORT,
        "ENCRYPTED": False,
        "TEST": {"NAME": "test_bkdata_basic"},
    },
    "bkdata_modeling": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": "bkdata_modeling",
        "USER": MODELING_DB_USER,
        "PASSWORD": MODELING_DB_PASSWORD,
        "HOST": MODELING_DB_HOST,
        "PORT": MODELING_DB_PORT,
        "ENCRYPTED": False,
        "TEST": {"NAME": "test_bkdata_modeling"},
    },
}

CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_ENABLE_UTC = False
CELERY_BROKER_URL = "amqp://{user}:{password}@{host}:{port}/{vhost}".format(
    user=RABBITMQ_USER,
    password=RABBITMQ_PASS,
    host=RABBITMQ_HOST,
    port=RABBITMQ_PORT,
    vhost=RABBITMQ_VHOST,
)

INSTALLED_APPS += ("django.contrib.auth", "django.contrib.contenttypes", "dataflow")

# 标识部署环境
# ee:企业版
RELEASE_ENV = "ee"
RELEASE_ENV_TGDP = "tgdp"

TENCENT_ENVS = ["tencent", RELEASE_ENV_TGDP]

LOGGER_LEVEL = "INFO"
LOGGER_FORMATTER = "verbose"


# 自定义日志过滤器：过滤正常日志
def skip_normal_logs(record):
    if record.msg and record.levelname:
        pattern = r"&&response_result:(.+?)&&"
        response_result = re.findall(pattern, str(record.msg))
        try:
            # 收集日志类型：
            # 1. 返回result为false
            # 2. 打点为error
            if (
                not response_result
                or not eval(response_result[0].strip().capitalize())
                or record.levelname.lower() == "error"
            ):
                return True
            # 权限校验比较特殊，校验结果是写在data中的
            # 收集日志类型：
            # 1. 权限校验不通过的访问日志
            module = re.findall(r"&&module:(.+?)&&", str(record.msg))
            response_data = re.findall(r"&&response_data:(.+?)&&", str(record.msg))
            if not module or module[0].strip().lower() == "auth":
                # 访问auth模块的日志
                if not response_data or not eval(response_data[0].strip().capitalize()):
                    return True
        except BaseException:
            return True
    return False


log_filters = LOGGING.setdefault("filters", {})
log_filters["skip_normal_logs"] = {
    "()": "django.utils.log.CallbackFilter",
    "callback": skip_normal_logs,
}

# 自定义日志
LOGGING["handlers"].update(
    {
        "stream": {
            "class": LOG_CLASS,
            "formatter": LOGGER_FORMATTER,
            "filename": os.path.join(LOG_DIR, "sys_stream.log"),
            "maxBytes": LOG_MAX_BYTES,
            "backupCount": LOG_BACKUP_COUNT,
        },
        "batch": {
            "class": LOG_CLASS,
            "formatter": LOGGER_FORMATTER,
            "filename": os.path.join(LOG_DIR, "sys_batch.log"),
            "maxBytes": LOG_MAX_BYTES,
            "backupCount": LOG_BACKUP_COUNT,
        },
        "flow_filter": {
            "class": LOG_CLASS,
            "formatter": LOGGER_FORMATTER,
            "filename": os.path.join(LOG_DIR, "sys_flow_error.log"),
            "maxBytes": LOG_MAX_BYTES,
            "filters": ["skip_normal_logs"],
            "backupCount": LOG_BACKUP_COUNT,
        },
        "flow": {
            "class": LOG_CLASS,
            "formatter": LOGGER_FORMATTER,
            "filename": os.path.join(LOG_DIR, "sys_flow.log"),
            "maxBytes": LOG_MAX_BYTES,
            "backupCount": LOG_BACKUP_COUNT,
        },
        "component": {
            "class": LOG_CLASS,
            "formatter": LOGGER_FORMATTER,
            "filename": os.path.join(LOG_DIR, "sys_component.log"),
            "maxBytes": LOG_MAX_BYTES,
            "backupCount": LOG_BACKUP_COUNT,
        },
        "udf": {
            "class": LOG_CLASS,
            "formatter": LOGGER_FORMATTER,
            "filename": os.path.join(LOG_DIR, "sys_udf.log"),
            "maxBytes": LOG_MAX_BYTES,
            "backupCount": LOG_BACKUP_COUNT,
        },
        "modeling": {
            "class": LOG_CLASS,
            "formatter": LOGGER_FORMATTER,
            "filename": os.path.join(LOG_DIR, "sys_modeling.log"),
            "maxBytes": LOG_MAX_BYTES,
            "backupCount": LOG_BACKUP_COUNT,
        },
        "uc": {
            "class": LOG_CLASS,
            "formatter": LOGGER_FORMATTER,
            "filename": os.path.join(LOG_DIR, "sys_uc.log"),
            "maxBytes": LOG_MAX_BYTES,
            "backupCount": LOG_BACKUP_COUNT,
        },
    }
)

LOGGING["loggers"].update(
    {
        "stream": {
            "handlers": ["stream"],
            "level": LOGGER_LEVEL,
            "propagate": False,
        },
        "batch": {
            "handlers": ["batch"],
            "level": LOGGER_LEVEL,
            "propagate": False,
        },
        "flow": {
            "handlers": ["flow", "flow_filter"],
            "level": LOGGER_LEVEL,
            "propagate": False,
        },
        "component": {
            "handlers": ["component"],
            "level": LOGGER_LEVEL,
            "propagate": False,
        },
        "udf": {
            "handlers": ["udf"],
            "level": LOGGER_LEVEL,
            "propagate": False,
        },
        "modeling": {
            "handlers": ["modeling"],
            "level": LOGGER_LEVEL,
            "propagate": False,
        },
        "uc": {"handlers": ["uc"], "level": LOGGER_LEVEL, "propagate": False},
    }
)

# 自定义异常处理
REST_FRAMEWORK["EXCEPTION_HANDLER"] = "dataflow.shared.log.custom_exception_handler"
MIDDLEWARE += ("dataflow.shared.middlewares.PatchLogger",)

# redis info
REDIS_INFO = {"host": REDIS_DNS, "password": REDIS_AUTH, "port": REDIS_PORT}

# 监控打点module值
STREAM_MODULE_NAME = "stream"
BATCH_MODULE_NAME = "batch"
MODEL_APP_MODULE_NAME = "model_app"

# 为不同的发布版本指定消息通知方式
CMSI_CONF = {
    "ee": ["mail", "weixin"],
    "tgdp": ["mail", "weixin"],
    "tencent": ["mail", "rtx"],
}
CMSI_CHOICES = CMSI_CONF.get(RUN_VERSION, [])

# 默认小于 FILE_UPLOAD_MAX_MEMORY_SIZE 的文件会自动转为 InMemoryUploadedFile，并且不支持分片
# 而若上传文件大于 200000，会触发 hdfs 超过jetty上传文件的限制
FILE_UPLOAD_MAX_MEMORY_SIZE = 200000

DATA_UPLOAD_MAX_MEMORY_SIZE = 300 * 1024 * 1024

# TDW 上传 jar 包集群组ID
HDFS_UPLOADED_FILE_CLUSTER = "default"
# TDW 上传 jar 包临时目录
HDFS_UPLOADED_FILE_TMP_DIR = "/app/api/tmp/upload/"
# TDW 上传 jar 包最终目录
HDFS_UPLOADED_FILE_DIR = "/app/api/upload/batch/tdw/"
# SDK 上传包目录
HDFS_UPLOADED_SDK_FILE_DIR = "/app/api/upload/sdk/"

# 支持标签应用
TAG_RELATED_MODELS = {
    "ProcessingClusterConfig": {
        "target_type": "processing_cluster",
        "table_primary_key": "processing_cluster_config.id",
    }
}

# storage tag
DEFAULT_HDFS_TAG = "batch_cluster"
COMPUTE_TAG = "compute"

# stream相关
STREAM_JOB_NODE_LABEL = "stream"
JOBNAVI_STREAM_CLUSTER_ID = "default"
JOBNAVI_DEFAULT_CLUSTER_ID = "default"
STREAM_YARN_CLUSTER_DEFAULT_RESOURCE = 2048

# Flink相关配置
FLINK_STATE_CHECKPOINT_DIR = "hdfs:///app/flink/checkpoints/"
FLINK_VERSION = "1.7.2"
# flink code version
FLINK_CODE_VERSION = "1.10.1"
# flink 集群选举过程超时时长
ELECTION_FLINK_CLUSTER_TIMEOUT = 300

# flink 标配
# Number of YARN container to allocate (=Number of Task Managers)
FLINK_STANDARD_CONTAINERS = 1
# Memory for JobManager Container
FLINK_STANDARD_JOB_MANAGER_MEMORY = 1024
# Memory per TaskManager Container
FLINK_STANDARD_TASK_MANAGER_MEMORY = 1024
# Number of slots per TaskManager
FLINK_STANDARD_SLOTS = 1
# flink default slots per taskmanager in yarn cluster mode
FLINK_DEFAULT_YARN_CLUSTER_SLOTS = 10
# flink udf slots per taskmanager in yarn cluster mode
FLINK_UDF_YARN_CLUSTER_SLOTS = 1

# flink 高配
FLINK_ADVANCED_CONTAINERS = 1
FLINK_ADVANCED_JOB_MANAGER_MEMORY = 1024
FLINK_ADVANCED_TASK_MANAGER_MEMORY = 2048
FLINK_ADVANCED_SLOTS = 1
MAX_FLINK_YARN_SESSION_SLOTS = 40

STORM_RESOURCE_THRESHOLD = 5

# spark streaming 相关
# spark structured streaming checkpoint fs cluster tag
SPARK_STRUCTURED_STREAMING_CHECKPOINT_FS_CLUSTER_TAG = "system_cluster"
# spark structured streaming checkpoint clean by hdfs user
SPARK_STRUCTURED_STREAMING_CHECKPOINT_FS_CLEAN_PROXY_USER = "dataflow"

# 扩分区相关
# 扩分区操作下游节点类型，现在默认支持以下三种类型，以后可以扩展
EXPAND_PARTITION_DOWNSTREAM_NODE_TYPES = (
    "realtime,model,process_model,data_model_app,"
    "data_model_stream_indicator,flink_streaming,spark_structured_streaming"
)

# tasklog日志相关
# node manager addr
NODE_MANAGER_ADDRESS = "0.0.0.0:45454"
NODE_MANAGER_WEBAPP_ADDRESS = "0.0.0.0:23999"
# log regex: must capture (log_time, log_level, log_source, log_content)
FLINK_LOG_REGEX_STR = r"^(\d{4}\S\d{1,2}\S\d{1,2}\s\d{1,2}\S\d{1,2}\S\d{1,2}\S+)\s+(\S+)\s+(\S+)\s+(.+)"
SPARK_LOG_REGEX_STR = (
    r"^(\d{4}\S\d{1,2}\S\d{1,2}\s\d{1,2}\S\d{1,2}\S\d{1,2}\S+)\s+(\S+)\s+" r"(\[\S*\s*[\S\s\[\]]*\])\s+(.+)"
)
JOB_NAVI_LOG_REGEX_STR = (
    r"^(\d{4}\S\d{1,2}\S\d{1,2}\s\d{1,2}\S\d{1,2}\S\d{1,2}\S+\s+[+\-]\d+)\s+(\S+)\s+" r"(\[\S*\s*[\S\s\[\]]*\])\s+(.+)"
)

SUBMIT_STREAM_JOB_POLLING_TIMES = 72

# monitor相关
BK_MONITOR_METRIC_TOPIC = "bkdata_monitor_metrics591"
# 数据平台stream任务异常告警指标上报
BK_MONITOR_RAW_ALERT_TOPIC = "dmonitor_raw_alert"
# UC metric 配置
METRIC_KAFKA_TOPIC = "bkdata_data_monitor_metrics591"

# batch相关
# custom_calculate
CHECK_CUSTOM_CALCULATE_TIMEOUT = 60

# spark相关
SPARK_SQL_DEFAULT_CLUSTER_GROUP = "default"
SPARK_SQL_CLUSTER_NAME_PREFIX = "root.dataflow.batch"
SPARK_SQL_TEXT_MAX_LENGTH = 65530
SPARK_SQL_UDF_ENGINE_CONF = {
    "spark.executor.cores": "1",
    "spark.executor.memory": "1g",
    "spark.dynamicAllocation.maxExecutors": "150",
    "spark.executor.memoryOverhead": "512m",
}

# TDW相关
TDW_PREFIX = "BKDATA"
TDW_SPARK_TYPE = 128
TDW_DESCRIPTION = "CREATE BY BLUEKING."
TDW_BASE_FILE_NAME = "batch-tdw-0.1.0-jar-with-dependencies"
TDW_BASE_FILE_MD5 = "batch-tdw.md5"
TDW_JAR_FILE_NAME = "tdw-spark-jar-0.1.0.jar"
TDW_CLASS_NAME = "com.tencent.bk.base.dataflow.spark.sql.tdw.UCTDWSQL"
TDW_JAR_CLASS_NAME = "com.tencent.bk.base.dataflow.spark.sql.tdw.UCTDWJar"
TDW_DRIVER_MEMORY = "1g"
TDW_EXECUTOR_CORES = "4"
TDW_NUM_EXECUTORS = "30"
TDW_EXECUTOR_MEMORY = "4g"

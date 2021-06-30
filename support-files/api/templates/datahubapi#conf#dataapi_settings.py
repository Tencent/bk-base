# -*- coding: utf-8 -*
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
import pathlib

"""
模板注释说明

[公共] PIZZA框架依赖的配置，并且是各项目模块共用的配置
[可选] 平台提供的公共服务相关的链接信息，按需引入
[专属] 项目模块自身的配置，根据自身模块的情况进行修改和增减
"""

# [公共] API 启动环境
RUN_MODE = "PRODUCT"  # PRODUCT/DEVELOP
RUN_VERSION = "oss"

# [专属] 项目部署相关信息
pizza_port = __BKDATA_DATAHUBAPI_PORT__
BIND_IP_ADDRESS = "__BKDATA_DATAHUBAPI_HOST__"
APP_NAME = "datahub"

# [公共] 其他模块 API 参数
AUTH_API_HOST = "__BKDATA_AUTHAPI_HOST__"
AUTH_API_PORT = __BKDATA_AUTHAPI_PORT__
ACCESS_API_HOST = "__BKDATA_DATAHUBAPI_HOST__"
ACCESS_API_PORT = __BKDATA_DATAHUBAPI_PORT__
DATABUS_API_HOST = "__BKDATA_DATAHUBAPI_HOST__"
DATABUS_API_PORT = __BKDATA_DATAHUBAPI_PORT__
DATAFLOW_API_HOST = "__BKDATA_DATAFLOWAPI_HOST__"
DATAFLOW_API_PORT = __BKDATA_DATAFLOWAPI_PORT__
DATAMANAGE_API_HOST = "__BKDATA_DATAMANAGEAPI_HOST__"
DATAMANAGE_API_PORT = __BKDATA_DATAMANAGEAPI_PORT__
DATAQUERY_API_HOST = "__BKDATA_QUERYENGINEAPI_HOST__"
DATAQUERY_API_PORT = __BKDATA_QUERYENGINEAPI_PORT__
JOBNAVI_API_HOST = "__BKDATA_JOBNAVIAPI_HOST__"
JOBNAVI_API_PORT = __BKDATA_JOBNAVIAPI_PORT__
META_API_HOST = "__BKDATA_METAAPI_HOST__"
META_API_PORT = __BKDATA_METAAPI_PORT__
STOREKIT_API_HOST = "__BKDATA_DATAHUBAPI_HOST__"
STOREKIT_API_PORT = __BKDATA_DATAHUBAPI_PORT__
DATAHUB_API_HOST = "__BKDATA_DATAHUBAPI_HOST__"
DATAHUB_API_PORT = __BKDATA_DATAHUBAPI_PORT__

# [公共] 第三方系统 URL
CC_API_URL = "__CC_API_URL__"
CC_V3_API_URL = "http://__PAAS_HOST__:__PAAS_HTTP_PORT__/api/c/compapi/v2/cc/"
JOB_API_URL = "http://__PAAS_HOST__/component/compapi/job/"
GSE_API_URL = "http://__PAAS_HOST__/api/c/compapi/v2/gse/"
JOB_V3_API_URL = "http://__PAAS_HOST__"
CMSI_API_URL = "http://__PAAS_HOST__/component/compapi/cmsi/"
SMCS_API_URL = "http://__PAAS_HOST__/component/compapi/smcs/"
GSE_DATA_API_URL = "http://__GSE_DATA_HOST_URL__:__GSE_DATA_API_PORT__/"
PAAS_API_URL = "http://__PAAS_HOST__:__PAAS_HTTP_PORT__/api/c/compapi/v2/bk_paas/"

# [公共] 运行时区设置
TIME_ZONE = "__BK_TIMEZONE__"

# [公共] PaaS 注册之后生成的 APP_ID, APP_TOKEN, BK_PAAS_HOST
APP_ID = "__APP_CODE__"
APP_TOKEN = "__APP_TOKEN__"
BK_PAAS_HOST = "http://__PAAS_HOST__:__PAAS_HTTP_PORT__"
SECRET_KEY = "__DJANGO_SECRET_KEY__"

# [公共] 加解密配置
CRYPT_INSTANCE_KEY = "__CRYPT_INSTANCE_KEY__"
CRYPT_ROOT_KEY = "__BKDATA_ROOT_KEY__"
CRYPT_ROOT_IV = "__BKDATA_ROOT_IV__"

# [可选] MYSQL01 配置，提供给数据集成「基础版」使用
CONFIG_DB_HOST = "__MYSQL_DATAHUB_IP0__"
CONFIG_DB_PORT = __MYSQL_DATAHUB_PORT__
CONFIG_DB_USER = "__MYSQL_DATAHUB_USER__"
CONFIG_DB_PASSWORD = "__MYSQL_DATAHUB_PASS__"

# [可选] MYSQL02 配置，提供给数据开发「计算版」使用
CONFIG_DB_HOST_2 = "__MYSQL_DATAFLOW_IP0__"
CONFIG_DB_PORT_2 = __MYSQL_DATAFLOW_PORT__
CONFIG_DB_USER_2 = "__MYSQL_DATAFLOW_USER__"
CONFIG_DB_PASSWORD_2 = "__MYSQL_DATAFLOW_PASS__"

# 支持场景以及所加载的类
ACCESS_SCENARIO_DICT = {
    "custom": "datahub.access.collectors.custom_collector.access.CustomAccess",
    "db": "datahub.access.collectors.db_collector.access.DBAccess",
    "file": "datahub.access.collectors.file_collector.access.FileAccess",
    "http": "datahub.access.collectors.http_collector.access.HttpAccess",
    "log": "datahub.access.collectors.log.access.LogAccess",
    "offlinefile": "datahub.access.collectors.offlinefile_collector.access.OfflineFileAccess",
    "queue": "datahub.access.collectors.queue_collector.access.QueueAccess",
}

DATABUS_SHIPPERS_DICT = {
    "clean": "datahub.databus.shippers.clean.shipper.CleanShipper",
    "clickhouse": "datahub.databus.shippers.clickhouse.shipper.ClickHouseShipper",
    "druid": "datahub.databus.shippers.druid.shipper.DruidShipper",
    "es": "datahub.databus.shippers.es.shipper.EsShipper",
    "eslog": "datahub.databus.shippers.eslog.shipper.EslogShipper",
    "hdfs": "datahub.databus.shippers.hdfs.shipper.HdfsShipper",
    "ignite": "datahub.databus.shippers.ignite.shipper.IgniteShipper",
    "mysql": "datahub.databus.shippers.mysql.shipper.MysqlShipper",
    "postgresql": "datahub.databus.shippers.postgresql.shipper.PostgresqlShipper",
    "queue": "datahub.databus.shippers.queue.shipper.QueueShipper",
    "queue_pulsar": "datahub.databus.shippers.queue_pulsar.shipper.QueuePulsarShipper",
}

DATABUS_PULLERS_DICT = {
    "jdbc": "datahub.databus.pullers.db.puller.DbPuller",
    "http": "datahub.databus.pullers.http.puller.HttpPuller",
    "queue": "datahub.databus.pullers.queue.puller.QueuePuller",
}

# queue相关的数据库配置
CONFIG_QUEUE_DB_NAME = "bkdata_queue"
CONFIG_QUEUE_DB_USER = "__MYSQL_DATAHUB_USER__"
CONFIG_QUEUE_DB_PASSWORD = "__MYSQL_DATAHUB_PASS__"
CONFIG_QUEUE_DB_HOST = "__MYSQL_DATAHUB_IP0__"
CONFIG_QUEUE_DB_PORT = __MYSQL_DATAHUB_PORT__

# RabbitMQ
RABBITMQ_HOST = "__RABBITMQ_HOST__"
RABBITMQ_PORT = "__RABBITMQ_PORT__"
RABBITMQ_USER = "__APP_CODE__"
RABBITMQ_PASS = "__APP_TOKEN__"
RABBITMQ_VHOST = "__APP_CODE__"

# 连接hive server的一些配置
HIVE_SERVER = {
    "__DEFAULT_GEOG_AREA_TAG__": {
        "HIVE_SERVER_HOST": "__DEFAULT_HIVE_SERVER_HOST__",
        "HIVE_SERVER_PORT": __DEFAULT_HIVE_HIVESERVER2_PORT__,
        "HIVE_SERVER_USER": "__DEFAULT_HIVE_SERVER_USER__",
        "HIVE_SERVER_PASS": "__DEFAULT_HIVE_SERVER_PASS__",
    }
}

# 连接hive metastore的一些配置，iceberg需要使用
HIVE_METASTORE_SERVER = {
    "__DEFAULT_GEOG_AREA_TAG__": {
        "HIVE_METASTORE_HOSTS": "__HIVE_METASTORE_HOSTS__",
        "HIVE_METASTORE_PORT": __DEFAULT_HIVE_METASTORE_PORT__,
    }
}

WEBHDFS_USER = "root"

# 告警相关
RTX_RECEIVER = "__RTX_RECEIVER__"

STORAGE_QUERY_ORDER = __STORAGE_QUERY_ORDER__
BKDATA_STORAGE_EXCLUDE_QUERY = __BKDATA_STORAGE_EXCLUDE_QUERY__

DEFAULT_CLUSTER_ROLE_TAG = "usr"
DEFAULT_GEOG_AREA_TAG = "__DEFAULT_GEOG_AREA_TAG__"
MULTI_GEOG_AREA = __MULTI_GEOG_AREA__


RESOURCE_CENTER_API_HOST = "__RESOURCE_CENTER_API_HOST__"
RESOURCE_CENTER_API_PORT = 80

HUB_MANAGER_API_HOST = "__BKDATA_HUBMGRAPI_HOST__"
HUB_MANAGER_API_PORT = __BKDATA_HUBMGRAPI_PORT__
HUB_MANAGER_API_URL = "http://%s:%d" % (HUB_MANAGER_API_HOST, HUB_MANAGER_API_PORT)

SUPPORT_STORAGE_TYPE = __SUPPORT_STORAGE_TYPE__
BKDATA_VERSION_TYPE = "__BKDATA_VERSION_TYPE__"

SUPPORT_HIVE_VERSION = __BKDATA_SUPPORT_HIVE_VERSION__


# 老的dataid注册信息使用zk同步
# 支持第多个zk环境
V3_DATA_ZK_HOST = None
V3_PREFIX_DATA_NODE_PATH = "/gse/config/etc/dataserver/data/"
PREFIX_DATA_NODE_PATH = "/gse/config/etc/dataserver/data/"

# [专属] 文件上传目录
UPLOAD_MEDIA_ROOT = "__BK_HOME__/public/bkdata/nfs/media"

# [专属] 采集器资源消耗
COLLECTOR_DEFAULT_CPU_LIMIT = 30
COLLECTOR_DEFAULT_MEM_LIMIT = 10


# 节点管理相关配置
BK_NODE_API_URL = "http://__PAAS_HOST__:__PAAS_HTTP_PORT__/api/c/compapi/v2/nodeman/backend/api/"
BKUNIFYLOGBEAT_PLUGIN_NAME = "bkunifylogbeat"
BKUNIFYLOGBEAT_PLUGIN_VERSION = "latest"
BKUNIFYLOGBEAT_PLUGIN_TPL_NAME = "bkunifylogbeat.conf"
BKUNIFYLOGBEAT_PLUGIN_TPL_VERSION = "1"

# [专属] kafka、zk的一些配置项
# 专用的Kafka配置集群对应的ZK的链接地址
CONFIG_ZK_ADDR = __BKDATA_CONFIG_ZK_ADDR__
KAFKA_CONFIG_HOST = "__KAFKA_HOST__:__KAFKA_PORT__"
# 专用的Kafka配置集群的地址
KAFKA_OP_HOST = __BKDATA_KAFKA_OP_CONFIG_HOST__
# 专用的队列服务（queue）的管理员账号密码，dev环境默认不启用队列服务鉴权
QUEUE_SASL_ENABLE = True
QUEUE_SASL_USER = "__KAFKA_QUEUE_ADMIN_USERNAME__"  # 默认用户名为 bkdata_admin
QUEUE_SASL_PASS = "__KAFKA_QUEUE_ADMIN_PASSWORD__"

# puller-bkhdfs-M 集群中任务并发数
PULLER_BKHDFS_WORKERS = 5
# 脚本采集配置
GSE_AGENT_HOME = "/usr/local/gse_global"
SCRIPT_COLLECTOR_SETUP_PATH = GSE_AGENT_HOME + "/scripts/"

SYNC_GSE_ROUTE_DATA_USE_API = True
LOG_COLLECTOR_FORCE_USE_NODE_MAN = True

HDFS_DEFAULT_KAFKA_CONF_DIR = "/data/bkee/bkdata/databus/conf/hdfs/tdwhdfs"
QUEUE_API_URL = "http://__BKDATA_DATABUS_QUEUE_MGR_HOST__:__BKDATA_DATABUS_QUEUE_MGR_PORT__/databus/queue/console/"
DATANODE_CLUSTER_NAME = __BKDATA_DATANODE_CLUSTER_NAME__

TRACING_SAMPLE_RATE = 0

BKHDFS_CLUSTER_NAME = __BKDATA_BKHDFS_CLUSTER_NAME__

CHANNEL_VERSION = {"default": (0, 10, 2)}
DATA_UPLOAD_MAX_MEMORY_SIZE = 10485760

INSTALL_PATH = "__INSTALL_PATH__"
NEED_AUTH_RT = []

# 尝试加载extend配置文件，并覆盖同名配置
if pathlib.Path("conf/dataapi_settings_extend.py").exists():
    from conf.dataapi_settings_extend import *  # noqa

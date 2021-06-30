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

# [公共] API 启动环境
RUN_MODE = "__BKDATA_RUN_MODE__"  # PRODUCT/DEVELOP
RUN_VERSION = "__BKDATA_RUN_VERSION__"

# [专属] 项目部署相关信息
pizza_port = __BKDATA_RESOURCECENTERAPI_PORT__
BIND_IP_ADDRESS = "__BKDATA_RESOURCECENTERAPI_HOST__"
APP_NAME = 'resourcecenter'

# [公共] 其他模块 API 参数
AUTH_API_HOST = "__BKDATA_AUTHAPI_HOST__"
AUTH_API_PORT = __BKDATA_AUTHAPI_PORT__
ACCESS_API_HOST = "__BKDATA_ACCESSAPI_HOST__"
ACCESS_API_PORT = __BKDATA_ACCESSAPI_PORT__
DATABUS_API_HOST = "__BKDATA_DATABUSAPI_HOST__"
DATABUS_API_PORT = __BKDATA_DATABUSAPI_PORT__
DATAFLOW_API_HOST = "__BKDATA_DATAFLOWAPI_HOST__"
DATAFLOW_API_PORT = __BKDATA_DATAFLOWAPI_PORT__
DATAMANAGE_API_HOST = "__BKDATA_DATAMANAGEAPI_HOST__"
DATAMANAGE_API_PORT = __BKDATA_DATAMANAGEAPI_PORT__
DATAQUERY_API_HOST = "__BKDATA_DATAQUERYAPI_HOST__"
DATAQUERY_API_PORT = __BKDATA_DATAQUERYAPI_PORT__
JOBNAVI_API_HOST = "__BKDATA_JOBNAVIAPI_HOST__"
JOBNAVI_API_PORT = __BKDATA_JOBNAVIAPI_PORT__
META_API_HOST = "__BKDATA_METAAPI_HOST__"
META_API_PORT = __BKDATA_METAAPI_PORT__
STOREKIT_API_HOST = "__BKDATA_STOREKITAPI_HOST__"
STOREKIT_API_PORT = __BKDATA_STOREKITAPI_PORT__
TDW_API_HOST = "__BKDATA_TDWAPI_HOST__"
TDW_API_PORT = __BKDATA_TDWAPI_PORT__
RESOURCECENTER_API_HOST = "__BKDATA_RESOURCECENTERAPI_HOST__"
RESOURCECENTER_API_PORT = __BKDATA_RESOURCECENTERAPI_PORT__

# [公共] 第三方系统 URL
CC_API_URL = "http://__PAAS_FQDN__/component/compapi/cc/"
JOB_API_URL = "http://__PAAS_FQDN__/component/compapi/job/"
TOF_API_URL = "http://__PAAS_FQDN__/component/compapi/tof/"
SMCS_API_URL = "http://__PAAS_FQDN__/component/compapi/smcs/"
GSE_API_URL = "http://__PAAS_FQDN__/component/compapi/gse/"
CMSI_API_URL = "http://__PAAS_FQDN__/component/compapi/cmsi/"
BK_LOGIN_API_URL = "http://__PAAS_HOST__:__PAAS_HTTP_PORT__/api/c/compapi/v2/bk_login/"
ES_LOG_API_URL = "http://__LOG_SEARCH__"

# [公共] 运行时区设置
TIME_ZONE = '__BK_TIMEZONE__'

# PaaS注册之后生成的APP_ID, APP_TOKEN, BK_PAAS_HOST
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

# [专属] resourcecenter api 配置
# 资源系统运营数据存储的类型
RESOURCE_CENTER_METRIC_STORAGE_TYPE = "mysql"
# 存储集群可以采集的类型
RESOURCE_CENTER_COLLECT_STORAGE_TYPES = ["hdfs", "tspider", "es", "druid", "hermes", "clickhouse"]
# 存储集群的运营数据结果RT
STORAGE_METRIC_DATA_RT = '591_resource_storage_metrics_1h_batch'
# 计算资源可以采集的集群类型
RESOURCE_CENTER_COLLECT_PROCESSING_CLUSTER_TYPES = ['flink-yarn-session', 'flink-yarn-cluster', 'spark']
# 计算集群队列名名规则模板
PROCESSING_CLUSTER_QUEUE_TEMPLATE = {
    'flink-yarn-session': "root.dataflow.stream.{cluster_group_id}.session",
    'flink-yarn-cluster': "root.dataflow.stream.{cluster_group_id}.cluster",
    'spark': "root.dataflow.batch.{cluster_group_id}",
}
# 计算集群队列名类型
YARN_QUEUE_TYPE = "yarn"
K8S_QUEUE_TYPE = "k8s"
PROCESSING_CLUSTER_QUEUE_TYPE = {
    'flink-yarn-session': YARN_QUEUE_TYPE,
    'flink-yarn-cluster': YARN_QUEUE_TYPE,
    'spark': YARN_QUEUE_TYPE,
    'k8s-cluster': K8S_QUEUE_TYPE,
}
SPARK_JOB_CLUSTER_TYPES = ['spark']
SPARK_APPLICATION_TYPE = 'SPARK'
FLINK_JOB_CLUSTER_TYPES = ['flink-yarn-session', 'flink-yarn-cluster']
FLINK_APPLICATION_TYPE = "Apache Flink"
YARN_APPLICATION_TYPES = [SPARK_APPLICATION_TYPE, FLINK_APPLICATION_TYPE]
# k8s api url
K8S_RESOURCE_QUOTA_URL = "{cluster_domain}/api/v1/namespaces/{cluster_name}/resourcequotas/{cluster_name}"
K8S_METRICS_URL = "{cluster_domain}/apis/metrics.k8s.io/v1beta1/namespaces/{cluster_name}/pods"
# 计算集群统计任务的窗口长度
PROCESSING_METRIC_CALCULATION_WINDOW_LENGTH = 3600
# 计算集群的运营数据结果RT
PROCESSING_METRIC_DATA_RT = "591_resource_processing_capacity_clean"
# 默认查询结果集大小限制
DEFAULT_RESULT_SET_LIMIT = 100
# 任务提交记录最小保存时间（单位：天）
MINIMUM_JOB_SUBMIT_RECORD_KEEP_TIME_DAY = 15

DEFAULT_RESOURCE_GROUP_ID = 'default'

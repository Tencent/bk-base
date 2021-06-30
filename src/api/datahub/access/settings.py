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

import sys

from conf import dataapi_settings

CREATE_RAW_DATA_SCENARIO = "sys_res"
CREATE_RAW_DATA_MSG_SYS_REDIS = 4  # redis normal: 4, sentinal:3
CREATE_RAW_DATA_MSG_SYS_SENTINAL = 3
CREATE_RAW_DATA_MSG_SYS_KAFKA = 1
CREATE_RAW_DATA_MSG_SYS_PULSAR = 7  # pulsar type
CREATE_RAW_DATA_ZK_DEFAULT_PART = 1
CREATE_RAW_DATA_ZK_CLUSTER_ID = 1000

CREATE_RAW_DATA_DEFUALT_PART = 0
CREATE_RAW_DATA_SERVER_ID = -1  # not used
CREATE_RAW_DATA_DEFUALT_CLUSTER_INDEX = 1

# 参数校验规则
SENSITIVITY_PRIVATE_TYPE = "private"  # 私有    2
SENSITIVITY_PUBLIC_TYPE = "public"  # 公开    1
SENSITIVITY_TOP_SECRET_TYPE = "topsecret"  # 绝密    4 最高安全等级
SENSITIVITY_CONFIDENTIAL_TYPE = "confidential"  # 机密    3

LIMIT_RAW_DATA_NAME_LEN = 50  # 数据源名称长度限制
LIMIT_RAW_DATA_ALIAS_LEN = 50  # 数据源中文名称长度限制
LIMIT_RAW_DATA_NAME_REG = "^[_a-zA-Z0-9]*$"  # "^[a-zA-Z][a-zA-Z0-9_]*$"
LIMIT_DATA_SCENARIO_LEN = 128  # 数据场景长度限制
LIMIT_DATA_SOURCE_LEN = 32
LIMIT_DATA_ENCODING_LEN = 32
LIMIT_DATA_CATEGORY_LEN = 32
LIMIT_BK_APP_CODE_LEN = 128
LIMIT_SENSITIVITY_LEN = 32
LIMIT_CREATE_BY_LEN = 128
LIMIT_UPDATE_BY_LEN = 128
LIMIT_MAINTAINER_BY_LEN = 255

# 最小长度限制
MIN_RAW_DATA_NAME_LEN = 1

# 日期正则
TIME_FORMAT_REG = {
    "yyyyMMddHHmmss": r"\d{14}",
    "yyyyMMddHHmm": r"\d{12}",
    "yyyyMMdd HH:mm:ss.SSSSSS": r"(\d{8}\s\d{1,2}:\d{1,2}:\d{1,2}\.\d{6})",
    "yyyyMMdd HH:mm:ss": r"(\d{8}\s\d{1,2}:\d{1,2}:\d{1,2})",
    "yyyyMMdd": r"\d{8}",
    "yyyy-MM-dd+HH:mm:ss": r"(\d{4}-\d{1,2}-\d{1,2}\+\d{1,2}:\d{1,2}:\d{1,2})",
    "yyyy-MM-dd'T'HH:mm:ssXXX": r"(\d{4}-\d{1,2}-\d{1,2}T\d{1,2}:\d{1,2}:\d{1,2}\+\d{1,2}:\d{1,2})",
    "yyyy-MM-dd HH:mm:ss.SSSSSS": r"(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2}.\d{6})",
    "yyyy-MM-dd HH:mm:ss": r"(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2})",
    "yyyy-MM-dd": r"(\d{4}-\d{1,2}-\d{1,2})",
    "yy-MM-dd HH:mm:ss": r"(\d{2}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2})",
    "Unix Time Stamp(seconds)": r"\d{10}",
    "Unix Time Stamp(mins)": r"\d{8}",
    "Unix Time Stamp(seconDs)": r"\d{10}",
    "Unix Time Stamp(milliseconds)": r"\d{13}",
    "MM/dd/yyyy HH:mm:ss": r"(\d{2}\/\d{1,2}\/\d{4}\s\d{1,2}:\d{1,2}:\d{1,2})",
    "dd/MMM/yyyy:HH:mm:ss": r"(\d{2}\/[A-Za-z]{3}\/\d{4}:\d{1,2}:\d{1,2}:\d{1,2})",
}

UNIX_TIME_FORMAT_PREFIX = "Unix Time Stamp"

DATA_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

TAG_TARGET_PAGE_SIZE = 50
TAG_TARGET_PAGE = 1
TAG_TARGET_LIMIT = 200

TARGET_TYPE_ACCESS_RAW_DATA = "raw_data"
TARGET_TYPE_DATABUS_CHANNEL = "channel_cluster"
GEOG_AREA_MAINLAND = "mainland"
GEOG_AREA_OVERSEAS = "overseas"
GEOG_AREA = "geog_area"
CLUSTER_ROLE = "cluster_role"

KAFKA_ACL_SCRIPT = ""
KAFKA_CONFIG_SCRIPT = ""

PERMISSION_ALL = "all"
PERMISSION_RO = "read_only"
PERMISSION_AO = "access_only"

# 需要校验的权限常量名
JOB_ACCESS = "biz.job_access"  # log tlog script 三类场景的权限校验
COMMON_ACCESS = "biz.common_access"  # 除去上面三类，其他的接入场景校验
RAW_DATA_MANAGE_AUTH = "raw_data.manage_auth"  # 修改数据的敏感度时，需要请求的权限

# 是否使用data_route 从gse获取data_id
SYNC_GSE_ROUTE_DATA_USE_API = getattr(dataapi_settings, "SYNC_GSE_ROUTE_DATA_USE_API", False)

# 默认使用节点管理下发新创建的数据源，给所有新创建数据源打上enable标签
LOG_COLLECTOR_FORCE_USE_NODE_MAN = getattr(dataapi_settings, "LOG_COLLECTOR_FORCE_USE_NODE_MAN", False)
LOG_COLLECTOR_BIZ_LIST = getattr(dataapi_settings, "LOG_COLLECTOR_BIZ_LIST", [])
UPLOAD_MEDIA_ROOT = getattr(dataapi_settings, "UPLOAD_MEDIA_ROOT", ["."])
ACCESS_SCENARIO_DICT = getattr(dataapi_settings, "ACCESS_SCENARIO_DICT", {})

# 创建raw_data的topic时，默认初始的分区数
DEFAULT_KAFKA_PARTITION_NUM = getattr(dataapi_settings, "DEFAULT_KAFKA_PARTITION_NUM", 1)
DEFAULT_PULSAR_PARTITION_NUM = getattr(dataapi_settings, "DEFAULT_PULSAR_PARTITION_NUM", 1)
STREAM_TO_TGDP_TEMPLATE = "stream_to_tgdp_%s_%s%d"
QUEUE_STREAM_TO_TGDP_TEMPLATE = "queue_stream_to_tgdp_%s_%s%d"

RAW_DATA_MANAGER = "raw_data.manager"
USE_V2_UNIFYTLOGC_TAG = "use_v2_unifytlogc"

DEFAULT_CC_V1_CODE = 1
DEFAULT_CC_V3_CODE = 0
CASCADE_STOP_CLEAN_SHIPPER = getattr(dataapi_settings, "CASCADE_STOP_CLEAN_SHIPPER", False)
IGNORE_FILE_STRING_SPLIT = ";"

# TGLOG_USE_GSE为True时，全部业务切换到tglog gse的接口，为False时 只有TGLOG_USE_GSE_ODM_LIST内的生效
TGLOG_USE_GSE = getattr(dataapi_settings, "TGLOG_USE_GSE", False)
# biz_list配置的是odm的缩写
TGLOG_USE_GSE_ODM_LIST = getattr(dataapi_settings, "TGLOG_USE_GSE_ODM_LIST", [])
MULTI_GEOG_AREA = getattr(dataapi_settings, "MULTI_GEOG_AREA", False)
DEFAULT_GEOG_AREA_TAG = getattr(dataapi_settings, "DEFAULT_GEOG_AREA_TAG", "inland")

INVALID_DATA_ID = -sys.maxsize
TGLOG_USE_GSE_BIZ_LIST = getattr(dataapi_settings, "TGLOG_USE_GSE_BIZ_LIST", [])
LOGC_COLLECTOR_SETUP_PATH = getattr(dataapi_settings, "LOGC_COLLECTOR_SETUP_PATH", "")
LOGC_COLLECTOR_PID_PATH = getattr(dataapi_settings, "LOGC_COLLECTOR_PID_PATH", "")
LOGC_COLLECTOR_PROC_NAME = getattr(dataapi_settings, "LOGC_COLLECTOR_PROC_NAME", "")
LOGC_COMMON_COLLECTOR_SETUP_PATH = getattr(
    dataapi_settings,
    "LOGC_COMMON_COLLECTOR_SETUP_PATH",
    "/data/MapleLeaf/unifyTlogc/sbin/",
)
LOGC_COMMON_COLLECTOR_PID_PATH = getattr(
    dataapi_settings,
    "LOGC_COMMON_COLLECTOR_PID_PATH",
    "/data/MapleLeaf/unifyTlogc/sbin/unifyTlogc_common_log.pid",
)
LOGC_COMMON_COLLECTOR_PROC_NAME = getattr(dataapi_settings, "LOGC_COMMON_COLLECTOR_PROC_NAME", "unifyTlogc_common_log")

TDM_CLUSTER_PULBIC_CLUSTER_CODE = "0"

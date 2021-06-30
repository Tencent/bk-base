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

from collections import OrderedDict

from conf.dataapi_settings import JUPYTERHUBWEB_FQDN
from django.utils.translation import ugettext_lazy as _

# notebook url的域名
NOTEBOOK_HOST = JUPYTERHUBWEB_FQDN

# query_x 形式的查询任务名
QUERY_PATTERN = r"query_(\d+)$"

# notebook_x 形式的笔记任务名
NOTEBOOK_PATTERN = r"notebook_(\d+)$"

# 查询阶段：提交查询，连接存储，执行查询，写入缓存
SUBMIT_QUERY = "submitQuery"
CONNECT_DB = "connectDb"
QUERY_DB = "queryDb"
WRITE_CACHE = "writeCache"

STAGES = {SUBMIT_QUERY: 1, CONNECT_DB: 2, QUERY_DB: 3, WRITE_CACHE: 4}

# 查询阶段描述
STAGES_DESCRIPTION = {SUBMIT_QUERY: _("提交查询"), CONNECT_DB: _("连接存储"), QUERY_DB: _("执行查询"), WRITE_CACHE: _("写入缓存")}

# 提交查询中包含的各子阶段
SUBMIT_QUERY_LIST = [
    "checkQuerySyntax",
    "checkPermission",
    "pickValidStorage",
    "matchQueryForbiddenConfig",
    "convertQueryStatement",
    "checkQuerySemantic",
    "matchQueryRoutingRule",
    "getQueryDriver",
]

# 状态信息
STATUS = "status"
FINISHED = "finished"
FAILED = "failed"
RUNNING = "running"
PENDING = "pending"
CANCELED = "canceled"

# 项目类型
PERSONAL = "personal"
COMMON = "common"

# 用户相关属性
BK_USERNAME = "bk_username"
USER = "user"

# http请求相关
HTTP = "http"
WS = "ws"
POST = "post"
GET = "get"
DELETE = "delete"
PARAM = "param"
API = "api"
DATA = "data"
MESSAGE = "message"
CODE = "code"
SOLUTION = "solution"
AUTH_INFO = "auth_info"
BK_TICKET = "bk_ticket"

COPY = "copy"
COPY_FROM = "copy_from"

# route相关
CANCEL = "cancel"
MINE = "mine"

# 查询相关属性
BKDATA_AUTHENTICATION_METHOD = "bkdata_authentication_method"
BK_APP_CODE = "bk_app_code"
BK_APP_SECRET = "bk_app_secret"
QUERY_ID = "query_id"
QUERY_NAME = "query_name"
LIMIT = "limit"
CHART_CONFIG = "chart_config"
STATEMENT_TYPE = "statement_type"
DML_SELECT = "DML_SELECT"
TIME = "time"
TIME_TAKEN = "time_taken"
TOTAL = "total"
KEYWORD = "keyword"
START_TIME = "start_time"
END_TIME = "end_time"
SEARCH_RANGE_START_TIME = "search_range_start_time"
SEARCH_RANGE_END_TIME = "search_range_end_time"
TOTALRECORDS = "totalRecords"
INFO = "info"
ERROR = "error"
ERROR_MESSAGE = "error_message"
_STAGES = "stages"
SELECT_FIELDS_ORDER = "select_fields_order"
SCHEMA_INFO = "schema_info"
EXECUTE_TIME = "execute_time"
STAGE_TYPE = "stage_type"
STAGE_STATUS = "stage_status"
STAGE_COST_TIME = "stage_cost_time"
TOOK = "took"
PREFER_STORAGE = "prefer_storage"
LIST = "list"
PAGE = "page"
PAGE_SIZE = "page_size"
TOTAL_PAGE = "total_page"
COUNT = "count"
INTERVAL = "interval"
RESULTS = "results"

# es查询相关属性
ES_QUERY = "es_query"
DATALAB_ES_QUERY_HISTORY = "datalab_es_query_history"
INDEX = "index"
BODY = "body"
HITS = "hits"
_SOURCE = "_source"
HIGHLIGHT = "highlight"
MUST = "must"
QUERY_STRING = "query_string"
QUERY = "query"
LENIENT = "lenient"
ANALYZE_WILDCARD = "analyze_wildcard"
FILTER = "filter"
RANGE = "range"
DTEVENTTIME = "dtEventTime"
DTEVENTTIMESTAMP = "dtEventTimeStamp"
FROM = "from"
TO = "to"
SIZE = "size"
SORT = "sort"
ORDER = "order"
DESC = "desc"
BOOL = "bool"
AGGREGATIONS = "aggregations"
AGGS = "aggs"
TIME_AGGS = "time_aggs"
BUCKETS = "buckets"
KEY = "key"
DOC_COUNT = "doc_count"
CNT = "cnt"
DATE_HISTOGRAM = "date_histogram"
FIELD = "field"
PRE_TAGS = "pre_tags"
POST_TAGS = "post_tags"
NUMBER_OF_FRAGMENTS = "number_of_fragments"
REQUIRE_FIELD_MATCH = "require_field_match"
GT = "gt"
LTE = "lte"
ASTERISK = "*"
EM_START = "<em>"
EM_END = "</em>"

# 结果表相关属性
RESULT_TABLES = "result_tables"
RESULT_TABLE = "result_table"
RESULT_TABLE_ID = "result_table_id"
RESULT_TABLE_IDS = "result_table_ids"
RESULT_TABLE_NAME = "result_table_name"
RESULT_TABLE_NAME_ALIAS = "result_table_name_alias"
FIELDS = "fields"
FIELD_NAME = "field_name"
SENSITIVITY = "sensitivity"
PRIVATE = "private"
CREATED_BY_NOTEBOOK = "created by notebook"
CREATED_AT = "created_at"
CREATED_BY = "created_by"
PLACEHOLDER_S = "%ss"
RELATED = "related"

# 数据处理相关属性
PROCESSING_TYPE = "processing_type"
QUERYSET = "queryset"
SNAPSHOT = "snapshot"
DATA_PROCESSINGS = "data_processings"
DATA_PROCESSING = "data_processing"
DATA_PROCESSING_ID = "data_processing_id"
PROCESSING_ID = "processing_id"
PROCESSING_ALIAS = "processing_alias"
DATA_SET_TYPE = "data_set_type"
DATA_SET_ID = "data_set_id"
OPERATE_OBJECT = "operate_object"
OPERATE_TYPE = "operate_type"
OPERATE_PARAMS = "operate_params"
CREATE = "create"
INPUTS = "inputs"
OUTPUTS = "outputs"
API_OPERATE_LIST = "api_operate_list"

# 平台相关属性
BKDATA = "bkdata"
PLATFORM = "platform"

# 业务相关属性
BK_BIZ_ID = "bk_biz_id"

# 项目相关属性
PROJECT_ID = "project_id"
PROJECT_TYPE = "project_type"

# 权限相关属性
RESULT_TABLE_UPDATE_DATA = "result_table.update_data"
PROJECT_RETRIEVE = "project.retrieve"
PROJECT_MANAGE_FLOW = "project.manage_flow"

# 产出物相关属性
OUTPUT_TYPE = "output_type"
OUTPUT_NAME = "output_name"
SQL = "sql"
NOTEBOOK_ID = "notebook_id"
CELL_ID = "cell_id"
MODEL = "model"
OUTPUT_FOREIGN_ID = "output_foreign_id"
NAME = "name"
SQL_LIST = "sql_list"
TYPE = "type"
DROP_MODEL = "drop model if exists %s"

# 存储类型
HDFS = "hdfs"
CLICKHOUSE = "clickhouse"
ELASTICSEARCH = "es"

# 集群配置
STORAGES = "storages"
STORAGE = "storage"
CLUSTER_TYPE = "cluster_type"
CLUSTER_NAME = "cluster_name"
STORAGE_CLUSTER_CONFIG_ID = "storage_cluster_config_id"
CHANNEL_CLUSTER_CONFIG_ID = "channel_cluster_config_id"
STORAGE_TYPE = "storage_type"
DATA_TYPE = "data_type"
DELTA_DAY = "delta_day"
EXPIRES = "expires"
STORAGE_CONFIG = "storage_config"
DESCRIPTION = "description"
PRIORITY = "priority"
GENERATE_TYPE = "generate_type"
ID = "id"
ENABLE = "enable"
ORDER_BY = "order_by"

# 数据清理相关属性
DESTROY = "destroy"
WITH_DATA = "with_data"
WITH_HIVE_META = "with_hive_meta"

# 标签
TAGS = "tags"
DATALAB = "datalab"

# 函数相关
FUNC_GROUP = "func_group"
FUNC_NAME = "func_name"
FUNC_ALIAS = "func_alias"
USAGE = "usage"
EXAMPLE = "example"
EXAMPLE_RETURN_VALUE = "example_return_value"
SUPPORT_STORAGE = "support_storage"
EXPLAIN = "explain"
FUNC_INFO = "func_info"
FUNCTION_GROUP_INFO = OrderedDict(
    [
        ("string-functions", _("字符串函数")),
        ("math-functions", _("数学函数")),
        ("agg-functions", _("聚合函数")),
        ("time-functions", _("时间函数")),
    ]
)

# 时间格式相关
DATE_TIME_FORMATTER = "%Y-%m-%d %H:%M:%S"
DEFAULT_MULTIPLICATOR = 1
# dtEventTimeStamp时间戳乘数
DTEVENTTIMESTAMP_MULTIPLICATOR = 1000

# 笔记模板相关
MLSQL_TEMPLATE = "mlsql.ipynb"
TEMPLATE_NAMES = {
    "example.ipynb": _("笔记快速入门"),
    "mlsql.ipynb": _("MLSQL 快速入门"),
}

# 笔记内容相关属性
NOTEBOOKS = "notebooks"
NOTEBOOK = "notebook"
CONTENTS = "contents"
CONTENT = "content"
NOTEBOOK_CONTENT = "notebook_content"
NOTEBOOK_NAME = "notebook_name"
CONTENT_NAME = "content_name"
KERNEL = "kernel"
CELLS = "cells"
TOKEN = "token"
PATH = "path"
REPORT = "report"
REPORT_SECRET = "report_secret"
REPORT_CONFIG = "report_config"
GENERATE_REPORT = "generate_report"
EDIT_RIGHT = "edit_right"

# 新用户默认创建的SQL模板
SQL_EXAMPLE = """SELECT LEFT(dtEventTime, 10) as thedate, COUNT(*) as access_cnt
    FROM 591_demo_nginx_access_log.hdfs
    WHERE thedate>=DATE_SUB(CURDATE(), INTERVAL '7' DAY)
    GROUP BY thedate
    ORDER BY thedate
    LIMIT 10"""

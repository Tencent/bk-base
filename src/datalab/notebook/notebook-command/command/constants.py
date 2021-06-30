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

# http请求正常状态码
HTTP_STATUS_OK = 200

# http请求头
HEADERS = {"Content-Type": "application/json"}

# 数据探索对应的平台项目id
DATALAB_PROJECT = "datalab_project"

# 调用schedule失败(1575500) - schedule [server_id] already exist.
SCHEDULE_EXIST_CODE = "1575500"

# SyntaxError, invalid syntax
SYNTAX_ERROR_CODE = "1572130"
SYNTAX_ERROR_MESSAGE = "语法错误"

# 发现禁止加载的类: code
FORBIDDEN_CODE = "1572131"
FORBIDDEN_MESSAGE = "发现禁止加载的类"

# Failed to get code status response
STATUS_RES_FAILED = "1572777"

# 对象不存在
CLUSTER_OBJECT_NOT_FOUND = "1500004"

# 对象不存在
AUTH_OBJECT_NOT_FOUND = "1511110"

# mlsql执行异常错误码
MODEL_ALREADY_EXIST_CODE = "1586028"
MODEL_ALREADY_EXIST_MESSAGE = "模型已存在，执行 show train model %s 查看详情"
RESULT_TABLE_ALREADY_EXIST_CODE = "1586029"
RESULT_TABLE_ALREADY_EXIST_MESSAGE = "结果表已存在，执行 show create table %s 查看详情"
MLSQL_EXECUTE_ERROR = {
    MODEL_ALREADY_EXIST_CODE: MODEL_ALREADY_EXIST_MESSAGE,
    RESULT_TABLE_ALREADY_EXIST_CODE: RESULT_TABLE_ALREADY_EXIST_MESSAGE,
}

# 执行迁移任务等待的时间间隔
TRANSPORT_WAIT_INTERVAL = 3
# 获取迁移任务状态失败时的重试次数
TRANSPORT_RETRY_TIMES = 5
# 重试时的时间间隔
TRANSPORT_RETRY_INTERVAL = 3

# 执行mlsql等待的时间间隔
EXECUTE_MLSQL_WAIT_INTERVAL = 3

# 获取spark执行状态等待的时间间隔
SPARK_STATUS_WAIT_INTERVAL = 3

# 获取查询执行状态等待的时间间隔
QUERY_STATUS_WAIT_INTERVAL = 0.5

# mlsql相关
MLSQL_TYPE_NORMAL = "normal"
MLSQL_TYPE_DDL = "ddl"
MLSQL_TYPE_SHOW_MODELS = "show_models"
MLSQL_TYPE_SHOW_TABLES = "show_tables"
MLSQL_TYPE_SHOW_CREATE_TABLE = "show_create_table"
MLSQL_TYPE_SHOW_TRAIN_MODEL = "show_train_model"
TASK_ID = "task_id"
SQL_LIST = "sql_list"
BK_STATUS = "bk_status"
STATUS = "status"
STAGES = "stages"
BK_RELEASE = "bk_release"
RELEASE = "release"
READ = "read"
DELETE = "delete"
ADD = "add"
REMOVE = "remove"
DISABLE = "disable"
DISABLE_MESSAGE = "disable_message"

# spark相关
SERVER_ID = "server_id"
ID = "id"
BK_USER = "bk_user"
CONTEXT = "context"
GEOG_AREA_CODE = "geog_area_code"
ERROR_LINE = "error_line"
IDLE = "idle"
STATE = "state"
OUTPUT = "output"
EVALUE = "evalue"
AVAILABLE = "available"
TEXT_PLAIN = "text/plain"

SUCCESS = "success"
STOPPED = "stopped"
FINISHED = "finished"
FAILED = "failed"

# 存储类型
CLICKHOUSE = "clickhouse"
HDFS = "hdfs"
IGNITE = "ignite"
SUPPORT_SAVE_STORAGES = [HDFS, CLICKHOUSE, IGNITE]
# 数据类型
ICEBERG = "iceberg"

# 数据迁移相关
SOURCE_RT_ID = "source_rt_id"
SOURCE_TYPE = "source_type"
SINK_RT_ID = "sink_rt_id"
SINK_TYPE = "sink_type"

# 结果表相关
INPUTS = "inputs"
INPUT_RT = "input_rt"
OUTPUT_RT = "output_rt"
SNAPSHOT = "snapshot"
QUERYSET = "queryset"
FIELDS = "fields"
STORAGES = "storages"
CLUSTER_TYPE = "cluster_type"
CLUSTER_NAME = "cluster_name"
CLUSTER_GROUP = "cluster_group"
CLUSTER_GROUP_ID = "cluster_group_id"
STORAGE_CONFIG = "storage_config"
DATA_PROCESSINGS = "data_processings"
PROCESSING_ID = "processing_id"
PROCESSING_TYPE = "processing_type"
DATA_TYPE = "data_type"
FIELD_TYPE = "field_type"
FIELD_ALIAS = "field_alias"
FIELD_NAME = "field_name"
FIELD_INDEX = "field_index"
PHYSICAL_TABLE_NAME = "physical_table_name"
COLUMN_NAME = "column_name"
ALL = "all"
SRC_RT = "src_rt"
DST_RT = "dst_rt"

# 产出物相关
OUTPUT_TYPE = "output_type"
OUTPUT_NAME = "output_name"
OUTPUTS = "outputs"
MODEL = "model"
MODELS = "models"
RESULT_TABLE = "result_table"
RESULT_TABLES = "result_tables"
RESULT_TABLE_INFO_DICT = OrderedDict(
    [
        ("name", "结果表id"),
        ("created_by", "创建人"),
        ("created_at", "创建时间"),
    ]
)
MODEL_INFO_DICT = OrderedDict(
    [
        ("name", "模型id"),
        ("created_by", "创建人"),
        ("created_at", "创建时间"),
    ]
)

RESULT_DICT = {True: "true", False: "false"}

# 源数据相关
BK_BIZ_ID = "bk_biz_id"
RAW_DATA_ID = "raw_data_id"
FILE_NAME = "file_name"
FILE_PATH = "file_path"
HDFS_HOST = "hdfs_host"
HDFS_PORT = "hdfs_port"
HDFS_PATH = "hdfs_path"
RAW_DATA = "raw_data"
RAW_DATA_INFO_DICT = OrderedDict(
    [
        (RAW_DATA_ID, "数据源id"),
        (FILE_NAME, "文件名"),
        (FILE_PATH, "文件路径"),
    ]
)
ITEM_MAPPING = {RESULT_TABLE: RESULT_TABLE_INFO_DICT, MODEL: MODEL_INFO_DICT, RAW_DATA: RAW_DATA_INFO_DICT}
DIR_PATH = "/files"

# 查询任务相关
SQL = "sql"
SQL_TEXT = "sql_text"
PREFER_STORAGE = "prefer_storage"
BK_APP_CODE = "bk_app_code"
BK_USERNAME = "bk_username"
BK_APP_SECRET = "bk_app_secret"
BKDATA_AUTHENTICATION_METHOD = "bkdata_authentication_method"
PROPERTIES = "properties"
USER = "user"
QUERY_ID = "query_id"
QUERY_NAME = "query_name"
QUERY_TASK_ID = "query_task_id"
STAGE_TYPE = "stage_type"
STAGE_STATUS = "stage_status"
WRITECACHE = "writeCache"
RESULT_TABLE_ID = "result_table_id"
RESULT_TABLE_IDS = "result_table_ids"
STATEMENT_TYPE = "statement_type"
ERROR_MESSAGE = "error_message"
# create table类型sql
DDL_CREATE = "ddl_create"
# create table as select类型sql
DDL_CTAS = "ddl_ctas"
SHOW_TABLES = "show_tables"
SHOW_SQL = "show_sql"
DML_SELECT = "dml_select"
DML_UPDATE = "dml_update"
DML_DELETE = "dml_delete"
CREATED_RESULT_TABLE_ID = "created_result_table_id"
TOTAL_RECORDS = "total_records"
TOTALRECORDS = "totalRecords"
SELECT_FIELDS_ORDER = "select_fields_order"
INFO = "info"
LIST = "list"
NAME = "name"
VALUE = "value"
VALUES = "values"
TYPE = "type"

COMMON = "common"
PERSONAL = "personal"
PROJECT_ID = "project_id"
NOTEBOOK_ID = "notebook_id"
NOTEBOOK = "notebook"
CELL_ID = "cell_id"
KERNEL_ID = "kernel_id"
CONTENT = "content"
CONTENT_TYPE = "content_type"
REQUESTED_AT = "requested_at"
EXEC_MESSAGE = "exec_message"

# cell相关
USERNAME = "username"
DISPATCH_SHELL = "dispatch_shell"
REQUEST = "request"
EXECUTE_REQUEST = "execute_request"
REPLY = "reply"
HEADER = "header"
MSG = "msg"

# 语句类型
BKSQL = "bksql"
MLSQL = "mlsql"
SPARK = "spark"
SAVE = "save"
DROP = "drop"
CREATE = "create"
UPDATE = "update"
INSERT = "insert"
SHOW_NOTEBOOK_FILE = "show_notebook_file"
DOWNLOAD_FILE = "download_file"
DATASETS = "datasets"

# 权限相关
ACTION_ID = "action_id"
OBJECT_ID = "object_id"
RESULT_TABLE_QUERY = "result_table.query_data"
RESULT_TABLE_UPDATE = "result_table.update_data"
RAW_DATA_QUERY = "raw_data.query_data"
PROJECT_MANAGE_FLOW = "project.manage_flow"
BIZ_COMMON_ACCESS = "biz.common_access"

# tag相关
TAGS = "tags"
DATALAB = "datalab"
INLAND = "inland"

# 接口返回结果相关
RESULT = "result"
DATA = "data"
MESSAGE = "message"
ERRORS = "errors"
ERROR = "error"
CODE = "code"
OK = "ok"

# 图表相关
BK_CHART = "bk_chart"
# 结果集图表展示
QUERY_TABLE = "query_table"
# 结果集只展示列表
QUERY_LIST = "query_list"
LIMIT = "limit"
QUERY_RESULT_LIMIT = 100

"""
int64 --> numpy.int64
float64 --> numpy.float64
object --> numpy.object0
bool --> numpy.bool8
"""
# dataframe字段类型和结果表字段类型的映射关系
PANDAS_TYPE_CONVERT = {"int64": "long", "float64": "double", "object": "string", "bool": "string"}

STRING = "string"
# iceberg数据分批写入，每批写入20万数据
SINGLE_WRITE_NUMS = 200000
COMMITTER = "committer"
OPERATOR = "operator"
CONDITION = "condition"
RIGHT_VALUE = "right_value"
SCHEMA = "schema"
AFFECTED_RECORDS = "affected.records"
AND = "and"
OR = "or"
NOTEBOOK_CREATE = "notebook_create"
NOTEBOOK_INSERT = "notebook_insert"

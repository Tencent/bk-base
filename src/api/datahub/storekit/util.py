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

import json
import re
import time
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps

from common.base_crypt import BaseCrypt
from common.exceptions import ValidationError
from common.http import post
from common.log import logger
from conf.dataapi_settings import DEFAULT_GEOG_AREA_TAG, RUN_MODE
from datahub.common.const import (
    ATTR_NAME,
    ATTR_VALUE,
    BK_APP_CODE,
    BK_APP_SECRET,
    CHANNEL_CLUSTER,
    CLICKHOUSE,
    CLUSTER_GROUP,
    CLUSTER_NAME,
    CLUSTER_TYPE,
    CODE,
    COLS_INFO,
    COMPONENT_TYPE,
    CONFIG,
    CONTENT,
    DATA,
    DATABASE,
    DATE,
    DEPTH,
    DESCRIPTION,
    DIRECTION,
    DRUID,
    DTEVENTTIME,
    DTEVENTTIMESTAMP,
    ERRORS,
    ES,
    EXTRA,
    EXTRA_RETRIEVE,
    FAILED,
    FALSE,
    FIELD_ALIAS,
    FIELD_INDEX,
    FIELD_NAME,
    FIELD_TYPE,
    FIELDS,
    GEOG_AREA,
    GEOG_AREA_ALIAS,
    GEOG_AREA_CODE,
    GTE,
    HDFS,
    HERMES,
    IGNITE,
    INDEXED_FIELDS,
    IS_DIMENSION,
    IS_TIME,
    KEY_SEPARATOR,
    LIMIT,
    LIST_EXPIRE,
    LOCALTIME,
    LONG,
    LTE,
    MANAGE,
    MATCH_POLICY,
    MAX_EXPIRE,
    MAX_RECORDS,
    MESSAGE,
    MIN_EXPIRE,
    MYSQL,
    NAME,
    NODES,
    NOTIFY_WAY,
    OPERATE_TYPE,
    ORDER,
    PAGE,
    PAGE_SIZE,
    PARTS_INFO,
    PASSWORD,
    PASSWORD_BACKEND,
    PHYSICAL_FIELD,
    PHYSICAL_FIELD_TYPE,
    POSTGRESQL,
    PRESTO,
    PROCESSING_TYPE,
    QUALIFIED_NAME,
    QUERYSET,
    RECEIVER,
    RELATED,
    RELATED_FILTER,
    RESOURCE_TYPE,
    RESULT,
    RESULT_TABLE,
    RESULT_TABLE_ID,
    RESULT_TABLE_IDS,
    RT_FIELDS,
    RT_ID_STORAGE,
    SASL_PASS,
    SERIES,
    SERVICE_TYPE,
    SNAPSHOT,
    SORTED_KEYS,
    SQL,
    SRC_CLUSTER_ID,
    STORAGE,
    STORAGE_CLUSTER,
    STORAGE_CONFIG,
    STORAGE_KEYS,
    STORAGES,
    STRING,
    SUCCESS,
    SYS_TYPE,
    TAG_FILTER,
    TAGS,
    TARGET_FILTER,
    TARGET_TYPE,
    TDW_RELATED,
    THEDATE,
    TIME,
    TIMESTAMP,
    TRUE,
    TSDB,
    TSPIDER,
    TYPE,
    U_RT_ID,
    UPDATE_TYPE,
    VALUE,
)
from datahub.storekit.api import DataManageApi, MetaApi, ResourceCenterApi
from datahub.storekit.exceptions import (
    ClusterSyncResourceException,
    ClusterTagException,
    GeogAreaError,
    IgniteConfigException,
    MaxRecordConfigException,
    MetaApiException,
    QueryGeogTagException,
    QueryTagTargetException,
    ResourceQueryException,
    RetryException,
    StorageClusterConfigException,
    StorageConfigMySQLException,
    StorageRTMySQLIncludeSystemTimeException,
)
from datahub.storekit.settings import (
    ALARM_TYPE_MAIL,
    ALARM_TYPE_PHONE,
    APP_ID,
    APP_TOKEN,
    CALL_MONITOR_DATABASE,
    CALL_MONITOR_MEASUREMENT,
    DATABUS_CHANNEL_TYPE,
    DATALAB_HDFS_SQL_TEMPLATE,
    DEF_KEY_SEPARATOR,
    DEFAULT_DRUID_SQL_TEMPLATE,
    DEFAULT_EXPIRE_DAYS,
    DEFAULT_IGNITE_SQL_TEMPLATE,
    DEFAULT_SQL_TEMPLATE,
    ES_DSL_TEMPLATE,
    EXPIRES_SUPPORT,
    HDFS_SQL_TEMPLATE,
    IGNITE_DEFAULT_MAX_RECORDS,
    INNER_TIME_FIELDS,
    K_STORAGE,
    MAX_RETRIES,
    MONITOR_TOPIC,
    MYSQL_TYPE_MAPPING,
    PAGE_SIZE_DEFAULT,
    SKIP_RT_FIELDS,
    STORAGE_EXCLUDE_QUERY,
    STORAGE_QUERY_ORDER,
)
from django.utils.translation import ugettext as _


def retry(retry_times, interval=1):
    def do_retry(func):
        @wraps(func)
        def do(*args, **kwargs):
            max_retry = MAX_RETRIES if retry_times < 1 else retry_times
            logger.debug(f"{func.__name__}: going to retry: {args}, {kwargs}, max_retry: {max_retry}")

            while max_retry > 0:
                try:
                    # 函数返回值，支持返回数据集合，bool类型。 对于非正常返回，函数入参必须抛出异常，否则没法重试
                    return func(*args, **kwargs)
                except Exception:
                    # 异常情况需要重试
                    logger.warning(f"{func.__name__}: retry remains {max_retry}", exc_info=True)
                    max_retry -= 1
                    time.sleep(interval)
            # 重试失败
            raise RetryException()

        return do

    return do_retry


def report_api_metric(call_time, sys_type, operate_type, result):
    """
    :param call_time: api的调用时间
    :param sys_type: 上报记录的一级分类
    :param operate_type: 上报记录的二级分类
    :param result: api的调用状态，1 or 0
    """
    messages = []
    status = SUCCESS if result == 1 else FAILED
    info = f"{call_time}: {sys_type} call {operate_type} {status}"
    entry = {
        DATABASE: CALL_MONITOR_DATABASE,
        TIME: call_time,
        CALL_MONITOR_MEASUREMENT: {
            RESULT: result,
            MESSAGE: info,
            TAGS: {
                SYS_TYPE: sys_type,
                OPERATE_TYPE: operate_type,
            },
        },
    }
    messages.append(entry)
    logger.info(f"report_api_metric entry: {entry}")
    report_metrics(MONITOR_TOPIC, messages)


def post_with_report(url, param, sys_type, operate_type):
    """
    :param url: http请求地址
    :param param: http请求中携带的数据
    :param sys_type: 上报数据的一级分类
    :param operate_type: 上报数据的二级分类
    :return: http请求的返回状态和数据
    """
    call_time = time.time()
    result, data = post(url, param)
    if result:
        report_api_metric(call_time, sys_type, operate_type, 1)
    else:
        report_api_metric(call_time, sys_type, operate_type, 0)
    return result, data


@retry(retry_times=3)
def get_rt_info_with_tdw(result_table_id):
    """
    根据result_table_id从meta中获取相关的字段、存储、tdw等配置信息
    :param result_table_id: 结果表id
    :return: 结果表相关的字段、存储、tdw等配置信息
    """
    res = MetaApi.result_tables.retrieve(
        {
            RESULT_TABLE_ID: result_table_id,
            RELATED: [FIELDS, STORAGES],
            TDW_RELATED: [COLS_INFO, PARTS_INFO],
            EXTRA: True,
        }
    )
    if res.is_success():
        return res.data
    else:
        logger.warning(f"get rt {result_table_id} from meta failed. {res.code} {res.message} {res.data}")
        raise MetaApiException(message_kv={MESSAGE: res.message})


@retry(retry_times=3)
def get_rt_info(result_table_id):
    """
    根据result_table_id从meta中获取相关的字段和存储配置信息
    :param result_table_id: 结果表id
    :return: 结果表相关的字段和存储配置信息
    """
    res = MetaApi.result_tables.retrieve({RESULT_TABLE_ID: result_table_id, RELATED: [FIELDS, STORAGES]})
    if res.is_success():
        return res.data
    else:
        logger.warning(f"get rt {result_table_id} from meta failed. {res.code} {res.message} {res.data}")
        raise MetaApiException(message_kv={MESSAGE: res.message})


@retry(retry_times=3)
def get_rts_by_processing_type(processing_type, page, page_size=PAGE_SIZE_DEFAULT):
    """
    processing_type, 按批查询rt列表
    :param processing_type: 结果表处理了下
    :param page: 当前页
    :param page_size: 每页条数
    :return: rt列表
    """

    param = {PROCESSING_TYPE: processing_type, PAGE: page, PAGE_SIZE: page_size}
    if processing_type == QUERYSET:
        param[RELATED_FILTER] = json.dumps({TYPE: PROCESSING_TYPE, ATTR_NAME: "show", ATTR_VALUE: QUERYSET})

    res = MetaApi.result_tables.list(param)
    if res.is_success():
        return res.data
    else:
        logger.error(
            f"get rt by processing_type {processing_type} from meta failed. {res.code} {res.message} {res.data}"
        )
        raise MetaApiException(message_kv={MESSAGE: res.message})


@retry(retry_times=3)
def get_rts_by_ids(result_table_ids, page, page_size=PAGE_SIZE_DEFAULT):
    """
    processing_type, 按批查询rt列表
    :param result_table_ids: 结果表列表
    :param page: 当前页
    :param page_size: 每页条数
    :return: rt列表
    """

    param = {RESULT_TABLE_IDS: result_table_ids, PAGE: page, PAGE_SIZE: page_size, RELATED: [FIELDS, STORAGES]}

    res = MetaApi.result_tables.list(param)
    if res.is_success():
        return res.data
    else:
        logger.error(
            f"get rt by result_table_ids {result_table_ids} from meta failed. {res.code} {res.message} {res.data}"
        )
        raise MetaApiException(message_kv={MESSAGE: res.message})


@retry(retry_times=3)
def update_meta_rt(result_table_id, data):
    """
    更新meta中获取rt的一些配置信息
    :param result_table_id: 结果表id
    :param data: rt需更新的配置
    :return: 结果表相关的字段和存储配置信息
    """
    res = MetaApi.result_tables.update(data)
    if res.is_success():
        return res.data
    else:
        logger.warning(f"{result_table_id}: update meta failed. {res.code} {res.message} {res.data}")
        raise MetaApiException(message_kv={MESSAGE: res.message})


def wechat_msg(receivers, message):
    """
    发送微信通知
    :param receivers: 接收人列表
    :param message: 消息内容
    :return: 发送结果
    """
    return _send_msg(receivers, f"({RUN_MODE}) {message}", "wechat")


def phone_msg(receivers, message):
    """
    发送电话通知
    :param receivers: 接收人列表
    :param message: 消息内容
    :return: 发送结果
    """
    return _send_msg(receivers, message, ALARM_TYPE_PHONE)


def mail_msg(receivers, message):
    """
    发送邮件通知
    :param receivers: 接收人列表
    :param message: 消息内容
    :return: 发送结果
    """
    return _send_msg(receivers, message, ALARM_TYPE_MAIL)


def _send_msg(receivers, message, notify_way):
    """
    发送通知
    :param receivers: 接收人列表
    :param message: 消息内容
    :param notify_way: 发送方式
    :return: 发送结果
    """
    try:
        res = DataManageApi.alerts.send(
            {
                BK_APP_CODE: APP_ID,
                BK_APP_SECRET: APP_TOKEN,
                RECEIVER: receivers,
                MESSAGE: f"{message}",
                NOTIFY_WAY: notify_way,
            }
        )
        if res.is_success():
            return res.data
        else:
            logger.warning(f"send {notify_way} alter to {receivers} failed. mesasge: {message}")
    except Exception:
        logger.error("send_msg encounter some exception", exc_info=True)


def translate_expires_day(expires):
    """
    将rt存储中的过期配置转换为天数
    :param expires: 过期配置，可能是年、月、周、日、小时等
    :return: 过期天数，-1代表永不过期
    """
    if expires.find("-1") >= 0:
        return -1  # 永久保存，直接返回-1

    try:
        expires_num, expires_unit = int(re.sub(r"\D", "", expires)), re.sub(r"\d", "", expires)
        if not expires_unit:
            return int(expires)
        elif expires_unit.lower() == "h":
            return expires_num / 24
        elif expires_unit.lower() == "d":
            return expires_num
        elif expires_unit.lower() == "w":
            return expires_num * 7
        elif expires_unit.lower() == "m":
            return expires_num * 31
        elif expires_unit.lower() == "y":
            return expires_num * 365
        else:
            logger.warning(f"bad expires conf {expires}")
    except Exception as e:
        logger.warning(f"failed to parse expires {expires} conf. {e}")

    return DEFAULT_EXPIRE_DAYS  # 返回默认保存天数


def get_date_by_diff(date_diff):
    """
    根据相对今天的日期偏移量，返回对应的日期字符串，格式yyyyMMdd
    :param date_diff: 日期偏移量，正负均可
    :return: 相对偏移量的日期字符串
    """
    # date_diff = -1: 20180706
    # date_diff = 0:  20180707, today
    # date_diff = 1:  20180708
    return (datetime.today() + timedelta(date_diff)).strftime("%Y%m%d")


def get_timestamp_diff(date_diff):
    """
    根据相对今天的日期的偏移量，返回对应日期的时间戳的值，单位是毫秒
    :param date_diff: 日期偏移量
    :return: 相对偏移量的日期对应的时间戳，毫秒。
    """
    tmp_str = (datetime.today() + timedelta(date_diff)).strftime("%Y-%m-%d %H:%M:%S")
    tmp_array = time.strptime(tmp_str, "%Y-%m-%d %H:%M:%S")
    return int(time.mktime(tmp_array)) * 1000


def is_near_tomorrow():
    """
    获取当前时间是否临近明天
    :return: True/False
    """
    hour = datetime.today().hour
    return hour == 23


def get_timestamp(time_str, format_str):
    """
    :param time_str 时间字符串
    :param format_str 格式
    :return: 格式化输出时间戳。
    """
    time_array = time.strptime(time_str, format_str)
    return time.mktime(time_array)


def valid_json_str(key, value):
    """
    校验输入的是否是json字符串
    :param key: 待校验的字段的名称
    :param value: 待校验的值
    :return: 校验通过的json字符串，校验不通过时，抛出异常
    """
    try:
        json.loads(value)
        return value
    except Exception:
        raise ValidationError(_(f"{key}非json格式的值：{value}"))


def valid_cluster_expires(key, value):
    """
    校验输入的是否是合法的存储集群的过期配置
    :param key: 待校验的字段的名称
    :param value: 待校验的值
    :return: 校验通过的存储集群的过期配置，如果校验不通过，抛出异常
    """
    try:
        dic = json.loads(value)
        if (
            MIN_EXPIRE in dic
            and isinstance(dic[MIN_EXPIRE], int)
            and MAX_EXPIRE in dic
            and isinstance(dic[MAX_EXPIRE], int)
        ):
            min_expire = dic.get(MIN_EXPIRE)
            max_expire = dic.get(MAX_EXPIRE)
            # -1 为永久保存，最小过期时间不能为-1，最大过期时间可以为-1，最小过期时间不能大于最大过期时间
            if min_expire > max_expire > 0:
                raise ValidationError(_("min_expire不能大于max_expire，且不能为-1"))
        else:
            raise ValidationError(_("expires配置中必须包含值为整数的属性min_expire和max_expire"))
        return value
    except ValueError:
        raise ValidationError(_(f"{key}非json格式的值：{value}"))


def generate_expires(expires, cluster_type):
    """
    根据min_expire, max_expire生成expires列表
    :param expires: 过期时间最小值和最大值
    :param cluster_type: 集群类型，如mysql
    :return: 最终的过期时间配置，如 {"min_expire": 3, "max_expire": 7, "list_expire":
            [{"name": "3天", "value": "3d"}, {"name": "7天", "value": "7d"}]}
    """
    # 根据min_expire和max_expire生成过期时间
    result = json.loads(expires)
    result[LIST_EXPIRE] = []
    if result[MIN_EXPIRE] != -1:
        for days in EXPIRES_SUPPORT:
            if days >= result[MIN_EXPIRE]:
                if result[MAX_EXPIRE] == -1 or days <= result[MAX_EXPIRE]:
                    result[LIST_EXPIRE].append({NAME: f"{days}天", VALUE: f"{days}d"})

    if result[MAX_EXPIRE] == -1 or result[MIN_EXPIRE] == -1:
        result[LIST_EXPIRE].append({NAME: "永久保存", VALUE: "-1"})

    return json.dumps(result)


def get_schema_and_sql(rt_info, all_flag, storage_hint=False):
    """
    获取rt的schema和查询sql语句
    :param rt_info: rt的配置信息
    :param storage_hint: 是否存储提示
    :param all_flag: 是否包含所有类型的存储
    :return: rt的schema和查询sql语句
    """
    storage_list = (
        list(STORAGE_QUERY_ORDER.keys()) + STORAGE_EXCLUDE_QUERY if all_flag else list(rt_info[STORAGES].keys())
    )
    data = {STORAGE: {}, STORAGES: storage_list}
    fields = rt_info[FIELDS]
    processing_type = rt_info[PROCESSING_TYPE]
    for storage in storage_list:
        data[STORAGE][storage] = {}
        # append_fields, 写入不同存储时，会额外添加的字段(用户时间字段 --> timestamp --> append_fields)
        rt_fields = get_basic_fields(storage, fields)

        if processing_type in [SNAPSHOT, QUERYSET]:
            data[STORAGE][storage][FIELDS] = rt_fields
        else:
            data[STORAGE][storage][FIELDS] = rt_fields + get_append_fields_by_storage(storage, len(fields))

        storage_config = rt_info[STORAGES][storage][STORAGE_CONFIG] if storage in rt_info[STORAGES] else "{}"
        # 存储的配置可能是空字符串，或者非法的json，这里兼容下这些脏数据
        try:
            data[STORAGE][storage][CONFIG] = json.loads(storage_config)
        except Exception:
            data[STORAGE][storage][CONFIG] = {}
        if storage in [MYSQL, HDFS, DRUID, TSDB, ES, IGNITE, CLICKHOUSE]:
            data[STORAGE][storage][SQL] = get_default_query(storage, rt_info, storage_hint)
        elif storage in STORAGE_EXCLUDE_QUERY and not all_flag:
            del data[STORAGE][storage]
        else:
            data[STORAGE][storage][SQL] = ""

    data[ORDER] = get_query_order(storage_list)
    return data


def get_basic_fields(storage, fields):
    """
    根据存储类型获取其基础字段列表，去掉timestamp/offset等内部字段
    :param storage: 存储类型
    :param fields: 字段列表
    :return: 基础字段列表
    """
    rt_fields = []
    for field in fields:
        if field[FIELD_NAME] in SKIP_RT_FIELDS:
            continue
        new_field = {
            FIELD_NAME: field[FIELD_NAME],
            FIELD_TYPE: field[FIELD_TYPE],
            FIELD_ALIAS: field[FIELD_ALIAS],
            IS_DIMENSION: field[IS_DIMENSION],
            FIELD_INDEX: field[FIELD_INDEX],
            PHYSICAL_FIELD: field[FIELD_NAME],
            DESCRIPTION: field[DESCRIPTION],
            PHYSICAL_FIELD_TYPE: MYSQL_TYPE_MAPPING.get(field[FIELD_TYPE]) if storage in [MYSQL] else field[FIELD_TYPE],
            IS_TIME: False,
        }
        rt_fields.append(new_field)
    return rt_fields


def get_append_fields_by_storage(storage, basic_fields_length):
    """
    获取平台追加的不同类型存储的字段列表
    :param storage: 存储类型
    :param basic_fields_length: 基础字段的数量
    :return: 平台追加的内部字段列表，主要是一些时间相关的字段
    """
    common_time_fields = [
        {
            FIELD_NAME: TIMESTAMP,
            FIELD_TYPE: TIMESTAMP,
            FIELD_ALIAS: _("数据时间，格式：YYYY-mm-dd HH:MM:SS"),
            IS_DIMENSION: False,
            IS_TIME: True,
            FIELD_INDEX: basic_fields_length + 1,
            PHYSICAL_FIELD: DTEVENTTIME,
            PHYSICAL_FIELD_TYPE: STRING,
            DESCRIPTION: "",
        },
        {
            FIELD_NAME: TIMESTAMP,
            FIELD_TYPE: TIMESTAMP,
            FIELD_ALIAS: _("数据时间戳，毫秒级别"),
            IS_DIMENSION: False,
            IS_TIME: True,
            FIELD_INDEX: basic_fields_length + 2,
            PHYSICAL_FIELD: DTEVENTTIMESTAMP,
            PHYSICAL_FIELD_TYPE: LONG,
            DESCRIPTION: "",
        },
        {
            FIELD_NAME: TIMESTAMP,
            FIELD_TYPE: TIMESTAMP,
            FIELD_ALIAS: _("本地时间，格式：YYYY-mm-dd HH:MM:SS"),
            IS_DIMENSION: False,
            IS_TIME: True,
            FIELD_INDEX: basic_fields_length + 3,
            PHYSICAL_FIELD: LOCALTIME,
            PHYSICAL_FIELD_TYPE: STRING,
            DESCRIPTION: "",
        },
    ]

    if storage in [TSPIDER, MYSQL, POSTGRESQL, HERMES, HDFS, PRESTO, CLICKHOUSE]:
        return common_time_fields + [
            {
                FIELD_NAME: TIMESTAMP,
                FIELD_TYPE: TIMESTAMP,
                FIELD_ALIAS: _("数据时间，格式：YYYYmmdd"),
                IS_DIMENSION: False,
                IS_TIME: True,
                FIELD_INDEX: basic_fields_length + 4,
                PHYSICAL_FIELD: THEDATE,
                PHYSICAL_FIELD_TYPE: STRING,
                DESCRIPTION: "",
            }
        ]
    elif storage in [DRUID, TSDB]:
        return [
            {
                FIELD_NAME: TIMESTAMP,
                FIELD_TYPE: TIMESTAMP,
                FIELD_ALIAS: _("数据时间戳，毫秒级别"),
                IS_DIMENSION: False,
                IS_TIME: True,
                FIELD_INDEX: basic_fields_length + 1,
                PHYSICAL_FIELD: TIME,
                PHYSICAL_FIELD_TYPE: LONG,
                DESCRIPTION: "",
            }
        ]
    elif storage in [ES]:
        return [
            {
                FIELD_NAME: TIMESTAMP,
                FIELD_TYPE: TIMESTAMP,
                FIELD_ALIAS: _("数据时间戳，毫秒级别"),
                IS_DIMENSION: False,
                IS_TIME: True,
                FIELD_INDEX: basic_fields_length + 1,
                PHYSICAL_FIELD: DTEVENTTIMESTAMP,
                PHYSICAL_FIELD_TYPE: DATE,
                DESCRIPTION: "",
            }
        ]
    else:
        return common_time_fields


def get_default_query(storage, rt_info, storage_hint=False):
    """
    获取指定存储类型的默认查询语句
    :param storage: 存储类型
    :param rt_info: rt的配置信息
    :param storage_hint: 是否提示存储
    :return: 指定存储的查询sql语句
    """
    fields_list = [field[FIELD_NAME] for field in rt_info[FIELDS] if field[FIELD_NAME] not in SKIP_RT_FIELDS]
    _dict = {
        U_RT_ID: rt_info[RESULT_TABLE_ID],
        RT_ID_STORAGE: f"{rt_info[RESULT_TABLE_ID]}.{storage}" if storage_hint else rt_info[RESULT_TABLE_ID],
        RT_FIELDS: ", ".join(fields_list),
    }

    if storage in [MYSQL, CLICKHOUSE]:
        _dict[THEDATE] = get_date_by_diff(0)
        return DEFAULT_SQL_TEMPLATE.format(**_dict)
    elif storage in [IGNITE]:
        _dict[DTEVENTTIMESTAMP] = get_timestamp_diff(0)
        return DEFAULT_IGNITE_SQL_TEMPLATE.format(**_dict)
    elif storage in [DRUID]:
        _dict[THEDATE] = get_date_by_diff(0)
        return DEFAULT_DRUID_SQL_TEMPLATE.format(**_dict)
    elif storage in [ES]:
        # es中查询语句的查询最近24小时的数据，其他存储是查询今天的数据
        _dict[GTE] = get_timestamp_diff(-1)
        _dict[LTE] = get_timestamp_diff(0)
        return ES_DSL_TEMPLATE.format(**_dict)
    elif storage in [HDFS]:
        if rt_info[PROCESSING_TYPE] in [QUERYSET, SNAPSHOT]:
            return DATALAB_HDFS_SQL_TEMPLATE.format(**_dict)
        else:
            _dict[THEDATE] = get_date_by_diff(0)
            return HDFS_SQL_TEMPLATE.format(**_dict)


def get_query_order(storage_list):
    """
    获取存储的查询顺序
    :param storage_list: 存储列表
    :return: 按照查询顺序排列的存储列表
    """
    storages = [_ for _ in storage_list if _ not in STORAGE_EXCLUDE_QUERY]
    _list = [(_, STORAGE_QUERY_ORDER[_]) for _ in storages]
    return [_[0] for _ in sorted(_list, key=lambda e: e[1], reverse=False)]


def query_metrics(database, sql):
    """
    查询metric数据，将结果返回
    :param database: 查询的metric数据库
    :param sql: 查询的sql语句
    :return: 查询结果
    """
    try:
        res = DataManageApi.metrics.query({DATABASE: database, SQL: sql, TAGS: [DEFAULT_GEOG_AREA_TAG]})
        if res.is_success():
            return res.data
        else:
            logger.warning(f"query metric failed. {sql} {res.code} {res.message} {res.data}")
            return {SERIES: None}
    except Exception:
        logger.error("query metric failed, encounter some exception", exc_info=True)
        return {SERIES: None}


def report_metrics(topic, message):
    """
    将metric数据通过datamanage上报到存储中
    :param topic: 需要上报的topic
    :param message: 需要上报的打点数据
    :return: 上报结果
    """
    try:
        res = DataManageApi.metrics.report({"kafka_topic": topic, MESSAGE: message, TAGS: [DEFAULT_GEOG_AREA_TAG]})
        logger.info(f"report capacity metric {json.dumps(message)}")
        if res.is_success():
            return True
        else:
            logger.warning(f"report metric failed. {json.dumps(message)} {res.message}")
            return False
    except Exception:
        logger.error("query metric failed, encounter some exception", exc_info=True)
        return False


def query_target_all_tag_by_id(target_id, target_type):
    """
    :param target_type: 实体类型
    :param target_id: 实体id
    :return: 实体所有标签
    """
    res = MetaApi.tag_target.list(
        {
            LIMIT: PAGE_SIZE_DEFAULT,
            TARGET_FILTER: json.dumps([{"k": K_STORAGE[target_type], "func": "eq", "v": target_id}]),
            TARGET_TYPE: target_type,
        }
    )
    if not res.is_success():
        logger.error(f"query tag of targets happend error, error message: {res.errors}")
        raise QueryTagTargetException(message_kv={MESSAGE: res.message})

    target_list = res.data[CONTENT]
    tag_code_dict = dict()
    for target in target_list:
        tag_code_dict[str(target[K_STORAGE[target_type]])] = target.get(TAGS, {})

    return tag_code_dict


def query_geog_tags():
    """
    :return: 所有区域标签
    """
    geog_area_map = dict()

    geog_area_alias = dict()
    geog_area = list()
    res = MetaApi.tag.list()
    if not res.is_success():
        logger.error(f"query all geog tags happend error, error message: {res.errors}")
        raise QueryGeogTagException(message_kv={MESSAGE: res.message})

    if res.data.get("supported_areas"):
        for key, val in list(res.data["supported_areas"].items()):
            geog_area_alias[key] = val["alias"]
            geog_area.append(key)
        geog_area_map[GEOG_AREA_ALIAS] = geog_area_alias

    geog_area_map = {
        GEOG_AREA_ALIAS: geog_area_alias,
        GEOG_AREA: geog_area,
    }

    return geog_area_map


def rt_storage_geog_area_check(cluster_id, cluster_type, result_table_id):
    geog_all_tags = query_geog_tags()

    geog_tags = geog_all_tags[GEOG_AREA]
    target_type = CHANNEL_CLUSTER if cluster_type in DATABUS_CHANNEL_TYPE else STORAGE_CLUSTER

    cluster_tags = query_target_all_tag_by_id(cluster_id, target_type)
    result_table_tags = query_target_all_tag_by_id(result_table_id, RESULT_TABLE)

    result_table_geog_tags = [
        i.get(CODE, "") for i in result_table_tags.get(str(result_table_id), {}).get(MANAGE, {}).get(GEOG_AREA, [])
    ]
    cluster_geog_tags = [
        i.get(CODE, "") for i in cluster_tags.get(str(cluster_id), {}).get(MANAGE, {}).get(GEOG_AREA, [])
    ]

    diff_tags = [i for i in result_table_geog_tags if i in cluster_geog_tags]
    has_geog_tag = True if [i for i in diff_tags if i in geog_tags] else False
    if not has_geog_tag:
        logger.error(f"geographical area error, supported area is {geog_tags}, diff tags is {diff_tags}")
        raise GeogAreaError(message_kv={"geog_areas": [cluster_tags, result_table_tags]})
    return True


def query_target_by_target_tag(tag_codes, target_type):
    """
    :param tag_codes: 标签列表
    :param target_type: 实体类型
    :return: 过滤符合条件的实体
    """
    res = MetaApi.tag_target.list(
        {
            TAG_FILTER: json.dumps(
                [{"criterion": [{"k": CODE, "func": "eq", "v": tag_code} for tag_code in tag_codes], "condition": "OR"}]
            ),
            TARGET_TYPE: target_type,
            MATCH_POLICY: "both",
            LIMIT: PAGE_SIZE_DEFAULT,
        }
    )
    if not res.is_success():
        logger.error(f"query tag of targets happend error, error message: {res.errors}")
        raise QueryTagTargetException(message_kv={MESSAGE: res.message})

    target_dict = dict()
    for target in res.data[CONTENT]:
        target_dict[str(target[K_STORAGE[target_type]])] = target

    return target_dict


def query_storage_geog_area(storage_cluster_id):
    """
    获取指定的存储集群的区域信息
    :param storage_cluster_id: 存储集群的id
    :return: 存储集群所在区域的code
    """
    data = query_target_all_tag_by_id(storage_cluster_id, STORAGE_CLUSTER)
    return data[str(storage_cluster_id)][MANAGE][GEOG_AREA][0][CODE]


def encrypt_password(conn_info):
    """
    :param conn_info: 连接信息
    :return:
    """
    connection_info = json.loads(conn_info)

    pass_key_list = [PASSWORD, PASSWORD_BACKEND, SASL_PASS]
    for pass_key in pass_key_list:
        key = connection_info.get(pass_key)
        if key:
            connection_info[pass_key] = BaseCrypt.bk_crypt().encrypt(key)

    return json.dumps(connection_info)


def cluster_register_or_update(cluster):
    """
    集群注册或者更新到资源系统
    :param cluster: 集群信息
    """
    # 获取集群区域tag信息
    cluster_tags = query_target_all_tag_by_id(cluster.id, STORAGE_CLUSTER)
    if not cluster_tags or not cluster_tags[str(cluster.id)].get(MANAGE, {}).get(GEOG_AREA):
        raise ClusterTagException()

    # 查询集群是否已经同步到资源关联系统
    res = ResourceCenterApi.cluster_register.get_cluster(
        {
            RESOURCE_TYPE: STORAGE,
            SERVICE_TYPE: cluster.cluster_type,
            CLUSTER_TYPE: cluster.cluster_type,
            SRC_CLUSTER_ID: cluster.id,
            GEOG_AREA_CODE: cluster_tags[str(cluster.id)][MANAGE][GEOG_AREA][0][CODE],
        }
    )

    if res.is_success():
        # 集群是否已经同步
        param = {
            RESOURCE_TYPE: STORAGE,
            SERVICE_TYPE: cluster.cluster_type,
            SRC_CLUSTER_ID: cluster.id,
            CLUSTER_TYPE: cluster.cluster_type,
            CLUSTER_NAME: cluster.cluster_name,
            COMPONENT_TYPE: cluster.cluster_type,
            GEOG_AREA_CODE: cluster_tags[str(cluster.id)][MANAGE][GEOG_AREA][0][CODE],
            CLUSTER_GROUP: cluster.cluster_group,
        }
        if not res.data:
            result = ResourceCenterApi.cluster_register.create_cluster(param)
        else:
            param[UPDATE_TYPE] = "full"
            result = ResourceCenterApi.cluster_register.update_cluster(param)

        if result.is_success():
            return res.data
        else:
            logger.error(
                f"{cluster.cluster_name}:cluster sync resource center failed. {res.code} {res.message} {res.data}"
            )
            raise ClusterSyncResourceException()

    else:
        logger.error(f"{cluster.cluster_name}: query resource center failed. {res.code} {res.message} {res.data}")
        raise ResourceQueryException()


def query_result_table_geog_area(result_table_id):
    """
    查询结果表id区域信息
    :param result_table_id: 结果表id
    :return: 实例区域信息
    """
    # 获取集群区域tag信息
    result_table_tags = query_target_all_tag_by_id(result_table_id, RESULT_TABLE)
    if not result_table_tags or not result_table_tags[str(result_table_id)].get(MANAGE, {}).get(GEOG_AREA):
        raise GeogAreaError()

    geog_area_code = result_table_tags[str(result_table_id)][MANAGE][GEOG_AREA][0][CODE]
    return geog_area_code


def get_lineage_rts_by_processing_type(result_table_id, depth, processing_type):
    """
    :param result_table_id: 结果表id
    :param depth: 深度
    :param processing_type: processing_type
    """
    res = MetaApi.lineage.list(
        {
            TYPE: RESULT_TABLE,
            QUALIFIED_NAME: result_table_id,
            DEPTH: depth,
            DIRECTION: "OUTPUT",
            EXTRA_RETRIEVE: json.dumps({"ResultTable": {PROCESSING_TYPE: True}}),
        }
    )

    if not res.is_success():
        logger.error(f"query lineage rts error, error message: {res.errors}")
        raise MetaApiException(message_kv={MESSAGE: res.message})
    ret = []
    nodes = res.data.get(NODES, {})
    for item in nodes.values():
        if item[TYPE] == RESULT_TABLE and item[EXTRA][PROCESSING_TYPE] == processing_type:
            ret.append(item[RESULT_TABLE_ID])

    return ret


def sort_keys(sorted_keys, keys):
    """
    :param sorted_keys: 标准排序字段
    :param keys: 排序字段，非sorted_keys的字段按照字典序排列并与按照sorted_keys排列好的字段组合起来
    """
    exclude_keys = set()
    index_keys = {}
    for _key in keys:
        if _key in sorted_keys:
            index_keys[_key] = sorted_keys.index(_key)
            continue
        # 不存在排序字段列表中的字段，按照字典序。对于存量rt，标准排序字段列表是有的，增量的则使用字典序
        exclude_keys.add(_key)

    # 排序规则为： sorted_keys + 字典序
    result_keys = sorted(list(index_keys.items()), key=lambda item: item[1])
    exclude_sort_keys = list(exclude_keys)
    exclude_sort_keys.sort()

    result = [key for key, index in result_keys]
    result.extend(exclude_sort_keys)
    logger.info(
        f"sorted_keys: {sorted_keys}, keys: {keys}, index_keys: {index_keys}, exclude_keys: {exclude_sort_keys}, "
        f"result: {result}"
    )
    return result


def transform_type(value):
    """
    转换类型，只有boolean/int/string三种
    :param value: 值
    """
    if value.lower() == FALSE:
        return False
    elif value.lower() == TRUE:
        return True
    elif value.isdigit():
        return int(value)
    else:
        return value


def response_to_str(rsp):
    """
    将json response转换为字符串
    :param rsp: json response对象
    :return: response转换为字符串的内容
    """
    return json.dumps({DATA: rsp.data, MESSAGE: rsp.message, ERRORS: rsp.errors, CODE: rsp.code})


class OrderedEnum(Enum):
    def __ge__(self, other):
        if self.__class__ is other.__class__:
            return self.value >= other.value
        return NotImplemented

    def __gt__(self, other):
        if self.__class__ is other.__class__:
            return self.value > other.value
        return NotImplemented

    def __le__(self, other):
        if self.__class__ is other.__class__:
            return self.value <= other.value
        return NotImplemented

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


class MigrateStatus(OrderedEnum):
    # MigrateStatus有四种：未迁移，data_type已更新，iceberg table已创建，connector已启动
    migrate_not_started = 1
    data_type_changed = 2
    iceberg_table_created = 3
    connector_started = 4


def valid_mysql_storage_conf(conf):
    """
    :param conf: 存储配置
    """
    if INDEXED_FIELDS not in conf:
        logger.error(_("存储没有索引字段配置项，请检查此配置项"))
        raise StorageConfigMySQLException()
    indexed_fields = conf.get(INDEXED_FIELDS, [])
    _common = [idx for idx in indexed_fields if idx in INNER_TIME_FIELDS]

    if _common:
        system_time_fields = ", ".join(INNER_TIME_FIELDS)
        logger.error(
            f"Please do not select the system time field. The system time field includes: " f"{system_time_fields}"
        )
        raise StorageRTMySQLIncludeSystemTimeException(message_kv={"system_time_fields": system_time_fields})


def valid_ignite_storage_conf(result_table_id, cluster_type, cluster_name, conf):
    """
    校验ignite存储配置信息
    :param result_table_id: 结果表id
    :param cluster_type: 存储类型
    :param cluster_name: 集群名称
    :param conf: 存储配置
    :return: 存储配置
    """
    key_fields = conf.get(STORAGE_KEYS, [])
    max_records = conf.get(MAX_RECORDS)
    # 在内部引用，避免循环依赖
    import datahub.storekit.model_manager as model_manager

    cluster = model_manager.get_cluster_obj_by_name_type(cluster_name, cluster_type)
    if not cluster:
        raise StorageClusterConfigException("cluster not found")

    connection_info = json.loads(cluster.connection_info)
    if max_records and max_records > connection_info.get("cache_records_limit", IGNITE_DEFAULT_MAX_RECORDS):
        raise MaxRecordConfigException()

    if not key_fields:
        logger.error(_("ignite存储没有主键字段配置项，请检查此配置项"))
        raise IgniteConfigException()

    objs = model_manager.get_storage_rt_objs_by_rt_type(result_table_id, cluster_type)
    # 排序规则字段默认为空，使用默认分隔符。对于已经存在的配置不变
    sorted_keys = []
    key_separator = DEF_KEY_SEPARATOR

    if objs:  # 对于存量rt的分隔符和排序字段配置不能修改
        relation_obj = objs[0]  # 数据按照id倒序排列，第一个即为最新的一条关联关系记录
        # 如果已经存在，则为修改存储关联配置。
        from datahub.storekit.serializers import ResultTableSerializer

        storage_rt_info = ResultTableSerializer(relation_obj).data
        storage_config = json.loads(storage_rt_info[STORAGE_CONFIG])
        # 获取排序规则字段和分隔符
        sorted_keys = storage_config.get(SORTED_KEYS, storage_config[STORAGE_KEYS])
        key_separator = storage_config.get(KEY_SEPARATOR, "")

    # 当不存在排序字段时，为字典序
    result_sort_keys = sort_keys(sorted_keys, key_fields)
    conf[KEY_SEPARATOR] = key_separator
    conf[STORAGE_KEYS] = result_sort_keys
    conf[SORTED_KEYS] = sorted_keys


def valid_storage_conf(result_table_id, cluster_type, cluster_name, conf):
    """
    校验存储配置信息
    :param result_table_id: 结果表id
    :param cluster_type: 存储类型
    :param cluster_name: 集群名称
    :param conf: 存储配置
    :return: 存储配置
    """
    if cluster_type in [MYSQL]:  # mysql
        valid_mysql_storage_conf(conf)
    elif cluster_type in [IGNITE]:  # ignite, key_fields 该配置项为必填项
        valid_ignite_storage_conf(result_table_id, cluster_type, cluster_name, conf)

    logger.info(f"{result_table_id}: finish validate {cluster_type}-{cluster_type} storage config, {conf}")
    return conf

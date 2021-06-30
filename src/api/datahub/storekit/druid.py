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
import random
import threading
import time
import uuid
from datetime import datetime, timedelta

import requests
from common.http import get, post
from common.log import logger
from datahub.common.const import (
    APPEND_FIELDS,
    BAD_FIELDS,
    BIGINT,
    CHECK_DIFF,
    CHECK_RESULT,
    CLUSTER_NAME,
    CONNECTION_INFO,
    COUNT,
    DATASOURCE,
    DRUID,
    EXPIRES,
    FAILED,
    FIELD_NAME,
    FIELD_TYPE,
    FIELDS,
    HOST,
    ID,
    INFO,
    INTERVAL,
    JSON_HEADERS,
    LOCATION,
    LONG,
    MESSAGE,
    MINTIME,
    NAME,
    PENDING,
    PERIOD,
    PHYSICAL_TABLE_NAME,
    PORT,
    REPORT_TIME,
    RESULT_TABLE_ID,
    RT_FIELDS,
    RUNNING,
    SAMPLE,
    SEGMENTS,
    SIZE,
    STATUS,
    STORAGE_CLUSTER,
    STORAGE_CONFIG,
    STORAGES,
    STRING,
    SUCCESS,
    TABLE,
    TABLE_RECORD_NUMS,
    TABLE_SIZE_MB,
    TASK,
    TASK_TYPE,
    TIMESTAMP,
    TYPE,
    UNKNOWN,
    VARCHAR,
    VERSION,
    WAITING,
    ZOOKEEPER_CONNECT,
)
from datahub.storekit import model_manager
from datahub.storekit.exceptions import (
    DruidCreateTaskErrorException,
    DruidDeleteDataException,
    DruidHttpRequestException,
    DruidQueryDataSourceException,
    DruidQueryExpiresException,
    DruidQueryHistoricalException,
    DruidQueryTaskErrorException,
    DruidQueryWorkersException,
    DruidShutDownTaskException,
    DruidUpdateExpiresException,
    DruidZkConfException,
    DruidZKPathException,
    NotSupportTaskTypeException,
)
from datahub.storekit.settings import (
    CLEAN_DELTA_DAY,
    COORDINATOR,
    DEFAULT_DRUID_EXPIRES,
    DEFAULT_EXPIRES_RULE,
    DEFAULT_MAX_IDLE_TIME,
    DEFAULT_SEGMENT_GRANULARITY,
    DEFAULT_TASK_MEMORY,
    DEFAULT_TIMESTAMP_COLUMN,
    DEFAULT_WINDOW_PERIOD,
    DRUID_CLEAN_DEEPSTORAGE_TASK_CONFIG_TEMPLATE,
    DRUID_COMPACT_SEGMENTS_TASK_CONFIG_TEMPLATE,
    DRUID_MAINTAIN_TIMEOUT,
    DRUID_VERSION_V1,
    DRUID_VERSION_V2,
    ENDPOINT_DATASOURCE_RULE,
    ENDPOINT_GET_ALL_DATASOURCES,
    ENDPOINT_GET_DATASOURCES,
    ENDPOINT_GET_PENDING_TASKS,
    ENDPOINT_GET_RUNNING_TASKS,
    ENDPOINT_GET_RUNNING_WORKERS,
    ENDPOINT_HISTORICAL_SIZES,
    ENDPOINT_PUSH_EVENTS,
    ENDPOINT_RUN_TASK,
    ENDPOINT_SHUTDOWN_TASK,
    EXCEPT_FIELDS,
    EXECUTE_TIMEOUT,
    HTTP_REQUEST_TIMEOUT,
    INT_MAX_VALUE,
    MAINTAIN_DELTA_DAY,
    MERGE_BYTES_LIMIT,
    MERGE_DAYS_DEFAULT,
    OVERLORD,
    TASK_CONFIG_TEMPLATE,
    TASK_TYPE_PENDING,
    TASK_TYPE_RUNNING,
    TIME_ZONE_DIFF,
    UTC_BEGIN_TIME,
    UTC_FORMAT,
    ZK_DRUID_PATH,
)
from datahub.storekit.util import translate_expires_day
from django.template import Context, Template
from kazoo.client import KazooClient


def initialize(rt_info):
    """
    初始化rt的druid存储
    :param rt_info: rt的字段和配置信息
    :return: 初始化操作结果
    """
    return prepare(rt_info)


def info(rt_info):
    """
    获取rt的druid存储相关信息
    :param rt_info: rt的字段和配置信息
    :return: rt的druid相关信息
    """
    druid, physical_tn, conn_info = _get_druid_storage_info(rt_info)
    zk_addr = conn_info[ZOOKEEPER_CONNECT]
    coordinator = _get_role_leader(zk_addr, COORDINATOR, druid[STORAGE_CLUSTER][VERSION])

    # 获取维度和指标信息
    broker_host, broker_port = conn_info[HOST], conn_info[PORT]
    schema_url = f"http://{broker_host}:{broker_port}/druid/v2/sql/"
    schema_sql = (
        '{"query": "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE '
        "TABLE_NAME = '%s'\"}" % physical_tn
    )
    ok, schema = post(schema_url, params=json.loads(schema_sql))
    table_schema = {}
    if ok and schema:
        for e in schema:
            table_schema[e["COLUMN_NAME"].lower()] = e["DATA_TYPE"].lower()
        logger.info(f"physical_tn: {physical_tn}, schema_url: {schema_url}, schema: {table_schema}")

    # 获取segments信息：curl -XGET http://{router_ip:port}/druid/coordinator/v1/datasources/{datasource}
    segments_url = f"http://{coordinator}/druid/coordinator/v1/datasources/{physical_tn}"
    ok, segments = get(segments_url)
    logger.info(f"physical_tn: {physical_tn}, segments_url: {segments_url}, segments: {segments}")

    # 获取样例数据
    sample_url = f"http://{broker_host}:{broker_port}/druid/v2/sql/"
    sample_sql = '{"query": "SELECT * FROM \\"%s\\" ORDER BY __time DESC LIMIT 10"}' % physical_tn
    ok, sample = post(sample_url, params=json.loads(sample_sql))
    logger.info(f"physical_tn: {physical_tn}, sample_url: {sample_url}, sample_sql: {sample_sql}, sample: {sample}")

    druid[INFO] = {TABLE: table_schema, SEGMENTS: segments, SAMPLE: sample}
    return druid


def get_task_status(overlord, task_id, druid_version):
    """
    获取指定task_id的任务状态
    :param druid_version: druid集群的版本
    :param overlord: overlord角色leader，形式ip:port
    :param task_id: index task的id
    :return: index task的状态
    """
    # 获取segments信息：curl -XGET http://{router_ip:port}/druid/coordinator/v1/datasources/{datasource}
    status_url = f"http://{overlord}/druid/indexer/v1/task/{task_id}/status"
    # 5种状态：RUNNING, PENDING, WAITING, SUCCESS, FAILED
    ok, status = get(status_url)
    if not ok:
        return UNKNOWN

    logger.info(f"task_id: {task_id}, status_url: {status_url}, status: {status}")
    runner_status = status[STATUS][STATUS]
    if druid_version == DRUID_VERSION_V1:
        return runner_status
    else:
        return runner_status if runner_status in [SUCCESS, FAILED] else status[STATUS]["runnerStatusCode"]


def shutdown_index_task(overlord, task_id):
    """
    强制关闭指定task_id的任务状态，会导致丢peon数据, 谨慎使用
    :param overlord: overlord角色，形式ip:port
    :param task_id: index task的id
    :return:  index task的状态
    """
    # 关闭任务：curl -XPOST http://{router_ip:port}/druid/overlord/v1/task/{task_id}/shutdown
    shutdown_url = f"http://{overlord}/druid/indexer/v1/task/{task_id}/shutdown"
    # 尽最大努力关闭druid index task, 重试3次
    for i in range(3):
        try:
            resp = requests.post(shutdown_url, headers=JSON_HEADERS, timeout=HTTP_REQUEST_TIMEOUT)
            if resp.status_code == 200:
                break
        except Exception:
            logger.error(
                f"{i} times, shutdown index task failed with task_id: {task_id}, shutdown_url: {shutdown_url}, "
                f"resp.text: {resp.text}"
            )


def merge_segments(zk_addr, datasource, begin_date, end_date, druid_version, timeout, merge_days):
    """
    按照天级合并指定数据源的指定时间范围的segments
    :param merge_days: 合并天数
    :param zk_addr: zk连接信息
    :param datasource: 合作操作的datasource
    :param begin_date: 合并操作的开始日期
    :param end_date: 合并操作的结束日期
    :param druid_version: druid集群版本
    :param timeout: merge任务执行超时时间，单位分钟
    """
    coordinator = _get_role_leader(zk_addr, COORDINATOR, druid_version)
    # 检查是否需要Merge
    if not should_merge(coordinator, datasource, begin_date, end_date, merge_days):
        return

    interval = f"{begin_date}/{end_date}"
    overlord = _get_role_leader(zk_addr, OVERLORD, druid_version)
    execute_task(DRUID_COMPACT_SEGMENTS_TASK_CONFIG_TEMPLATE, overlord, datasource, interval, druid_version, timeout)


def execute_task(task_template, overlord, datasource, interval, druid_version, timeout=60):
    """
    :param task_template: task config模板
    :param overlord: overlord leader进程 ip:port格式
    :param datasource: druid datasource名称
    :param interval: 时间区间
    :param druid_version: druid集群版本
    :param timeout: 任务执行超时时间，单位分钟
    """
    data = Template(task_template)
    context = Context({DATASOURCE: datasource, INTERVAL: interval})
    body = data.render(context)
    task_url = f"http://{overlord}/druid/indexer/v1/task"
    ok, task = post(task_url, params=json.loads(body))
    task_id = task["task"] if ok else ""
    logger.info(
        f"datasource: {datasource}, overlord: {overlord}, interval: {interval}, task config: {body}, task_id: {task_id}"
    )
    begin_time = datetime.now()
    time_delta = timedelta(minutes=timeout)
    while True:
        time.sleep(10)
        status = get_task_status(overlord, task_id, druid_version)
        if status == RUNNING:
            if datetime.now() - begin_time > time_delta:
                shutdown_index_task(overlord, task_id)
                logger.warning(f"datasource: {datasource}, task_id {task_id} timeout, has been shutdown")
                return
        elif status in [PENDING, WAITING]:
            shutdown_index_task(overlord, task_id)
            return
        else:
            return


def clean_unused_segments(cluster_name, druid_version, timeout=60):
    """
    清理的单个集群的
    :param cluster_name: 集群名
    :param druid_version: druid集群版本
    :param timeout: clean任务执行超时时间，单位分钟
    :return:
    """
    coordinator = get_leader(cluster_name, COORDINATOR)
    ok, datasources_all = get(f"http://{coordinator}{ENDPOINT_GET_ALL_DATASOURCES}")
    if not ok or not datasources_all:
        return False
    ok, datasources_used = get(f"http://{coordinator}{ENDPOINT_GET_DATASOURCES}")
    if not ok:
        return False

    logger.info(f"datasources_all: {datasources_all}, datasources_used: {datasources_used}")
    for datasource in datasources_all:
        try:
            begin_date, end_date = "1000-01-01", "3000-01-01"
            if datasource in datasources_used:
                coordinator = get_leader(cluster_name, COORDINATOR)
                ok, resp = get(f"http://{coordinator}/druid/coordinator/v1/datasources/{datasource}/")
                if not ok:
                    continue
                end_date = (
                    datetime.strptime(resp[SEGMENTS][MINTIME], "%Y-%m-%dT%H:%M:%S.000Z") - timedelta(CLEAN_DELTA_DAY)
                ).strftime("%Y-%m-%d")

            interval = f"{begin_date}/{end_date}"
            overlord = get_leader(cluster_name, OVERLORD)
            logger.info(f"datasource: {datasource}, overlord: {overlord}, interval: {interval}")
            execute_task(
                DRUID_CLEAN_DEEPSTORAGE_TASK_CONFIG_TEMPLATE, overlord, datasource, interval, druid_version, timeout
            )
        except Exception:
            logger.warning(f"clean unused segments failed for datasource {datasource}", exc_info=True)

    return True


def should_merge(coordinator, datasource, begin_date, end_date, merge_days=MERGE_DAYS_DEFAULT):
    """
    判断指定数据源的指定时间范围的segments是否需要合并，interval是一天, 下列条件下不需要merge，
        1) 平均segment size大于300MB
        2) 平均每天的segment文件数量小于2
    :param merge_days: 合并天数
    :param coordinator: coordinator角色leader节点
    :param datasource: druid数据源名称
    :param begin_date: merge时间区间的左边界
    :param end_date: merge时间区间的右边界
    :return:
    """
    segments_url = (
        f"http://{coordinator}/druid/coordinator/v1/datasources/{datasource}/intervals/"
        f"{begin_date}_{end_date}?simple"
    )
    ok, segments = get(segments_url)
    # segments是按天合并的，预期合并后每天至多一个segment
    if not ok or len(segments) <= merge_days:
        return False

    size = 0
    file_count = 0
    for value in segments.values():
        size += value[SIZE]
        file_count += value[COUNT]

    logger.info(
        f"datasource: {datasource}, segments_url: {segments_url}, segments: {segments}, size: {size}, "
        f"file_count: {file_count}, status: True"
    )
    if file_count <= 1 or size > MERGE_BYTES_LIMIT:
        return False

    return True


def alter(rt_info):
    """
    修改rt的druid存储相关信息
    :param rt_info: rt的字段和配置信息
    :return: rt的druid存储的变更结果
    """
    return prepare(rt_info)


def prepare(rt_info):
    """
    准备rt关联的druid存储（创建新库表或旧表新增字段）
    :param rt_info: rt的配置信息
    :return: True/False
    """
    return True


def maintain_merge_segments(zk_addr, physical_tn, expires_day, delta_day, druid_version, timeout, merge_days):
    """
    用于在maintain和maintain_all中执行的merge segment逻辑
    :param zk_addr: zk连接信息
    :param physical_tn: 物理表名
    :param expires_day: 数据保留天数
    :param delta_day: 跳过的天数
    :param druid_version : druid 集群版本
    :param timeout : druid 任务的执行超时时间
    """
    expires_date = (datetime.today() - timedelta(expires_day)).strftime("%Y-%m-%d")
    end_date = (datetime.today() - timedelta(delta_day)).strftime("%Y-%m-%d")
    begin_date = (datetime.today() - timedelta(delta_day + merge_days)).strftime("%Y-%m-%d")
    logger.info(
        f"physical_tn: {physical_tn}, expires_day: {expires_day}, begin_date: {begin_date}, end_date: {end_date}"
    )
    if end_date >= expires_date:
        merge_segments(zk_addr, physical_tn, begin_date, end_date, druid_version, timeout, merge_days)


def set_retain_rule(coordinator, cluster_name, physical_tn, expires_day, druid_version):
    """
    设置druid datasource的数据保留规则
    :param coordinator: coordinator角色leader, 格式hostname:port
    :param cluster_name: 集群名称
    :param physical_tn: 物理表名
    :param expires_day: 数据保留天数
    :param druid_version: druid集群版本
    :return: 数据保留规则是否设置成功，True or False
    """
    rules = build_retain_rule(druid_version, expires_day)
    url = f"http://{coordinator}/druid/coordinator/v1/rules/{physical_tn}"
    resp = requests.post(url, data=rules, headers=JSON_HEADERS)
    if resp.status_code != 200:
        logger.warning(
            f"{cluster_name}: failed to set retention rule for datasource {physical_tn}. "
            f"status_code: {resp.status_code}, response: {resp.text}"
        )
        return False

    return True


def build_retain_rule(druid_version, expires_day):
    """
    构建数据保留规则
    :param expires_day: 数据保留天数
    :param druid_version: druid集群版本
    :return: json字符串
    """
    load_rule = {
        PERIOD: f"P{expires_day}D",
        "includeFuture": True,
        "tieredReplicants": {"_default_tier": 2},
        TYPE: "loadByPeriod",
    }

    if druid_version == DRUID_VERSION_V1:
        load_rule["tieredReplicants"]["tier_hot"] = 2

    rules = [load_rule, {"type": "dropForever"}]
    return json.dumps(rules)


def kill_waiting_tasks(cluster_name):
    """
    kill druid集群的所有waiting状态的任务
    :param cluster_name: 集群名
    """
    try:
        overlord = get_leader(cluster_name, OVERLORD)
        waiting_tasks_url = "http://" + overlord + "/druid/indexer/v1/waitingTasks"
        res = requests.get(waiting_tasks_url, verify=False, timeout=HTTP_REQUEST_TIMEOUT)
        pending_tasks = json.loads(res.text, encoding="utf-8")
        for task_json in pending_tasks:
            kill_task_url = "http://" + overlord + "/druid/indexer/v1/task/" + task_json[ID] + "/shutdown"
            headers = JSON_HEADERS
            requests.post(kill_task_url, headers=headers, verify=False)
    except Exception:
        logger.warning("failed to kill waiting tasks", exc_info=True)


def kill_pending_tasks(cluster_name):
    """
    kill druid集群的所有pending状态的任务
    :param cluster_name: 集群名
    """
    try:
        overlord = get_leader(cluster_name, OVERLORD)
        pending_tasks_url = "http://" + overlord + "/druid/indexer/v1/pendingTasks"
        res = requests.get(pending_tasks_url, verify=False, timeout=HTTP_REQUEST_TIMEOUT)
        pending_tasks = json.loads(res.text, encoding="utf-8")
        for task_json in pending_tasks:
            kill_task_url = "http://" + overlord + "/druid/indexer/v1/task/" + task_json[ID] + "/shutdown"
            headers = JSON_HEADERS
            requests.post(kill_task_url, headers=headers, verify=False)
    except Exception:
        logger.warning("failed to kill pending tasks", exc_info=True)


def maintain(rt_info, delta_day=MAINTAIN_DELTA_DAY, timeout=EXECUTE_TIMEOUT, merge_days=MERGE_DAYS_DEFAULT):
    """
    根据用户设定的数据保留时间维护druid表数据保留规则
    :param merge_days: 合并天数
    :param rt_info: rt的配置信息
    :param delta_day: merge segments的日期偏移量
    :param timeout: druid index任务的执行超时时间
    """
    druid, physical_tn, conn_info = _get_druid_storage_info(rt_info)
    cluster_name, version = druid[STORAGE_CLUSTER][CLUSTER_NAME], druid[STORAGE_CLUSTER][VERSION]
    coordinator = get_leader(cluster_name, COORDINATOR)
    expires_day = translate_expires_day(druid[EXPIRES])
    # 设置数据保留规则
    set_retain_rule(coordinator, cluster_name, physical_tn, expires_day, version)

    # merge segments
    zk_addr = conn_info[ZOOKEEPER_CONNECT]
    maintain_merge_segments(zk_addr, physical_tn, expires_day, delta_day, version, timeout, merge_days)

    return True


def maintain_all(delta_day=MAINTAIN_DELTA_DAY):
    """
    根据用户设定的数据保留时间维护druid表数据保留规则
    """
    start = time.time()
    # rt维度的mantain, 主要是设置数据保存时间
    storage_rt_list = model_manager.get_storage_rt_objs_by_type(DRUID)
    for rt_storage in storage_rt_list:
        try:
            conn_info = json.loads(rt_storage.storage_cluster_config.connection_info)
            zk_addr = conn_info[ZOOKEEPER_CONNECT]
            coordinator = _get_role_leader(zk_addr, COORDINATOR, rt_storage.storage_cluster_config.version)
            expires_day = translate_expires_day(rt_storage.expires)
            physical_tn = rt_storage.physical_table_name
            cluster_name = rt_storage.storage_cluster_config.cluster_name

            # 设置数据保留规则
            set_retain_rule(
                coordinator, cluster_name, physical_tn, expires_day, rt_storage.storage_cluster_config.version
            )

        except Exception:
            logger.warning(
                f"{rt_storage.storage_cluster_config.cluster_name}: failed to maintain the retention rule of "
                f"datasource {rt_storage.physical_table_name}",
                exc_info=True,
            )

    set_rule_finish = time.time()
    # 集群维度的maintain, 功能是清理deepstorage和compact segments
    cluster_list = model_manager.get_storage_cluster_configs_by_type(DRUID)
    check_threads = []
    for cluster in cluster_list:
        cluster_name = cluster[CLUSTER_NAME]
        thread = threading.Thread(target=maintain_druid_cluster, name=cluster_name, args=(cluster_name,))
        # 设置线程为守护线程，主线程结束后，结束子线程
        thread.setDaemon(True)
        check_threads.append(thread)
        thread.start()

    # join所有线程，等待所有集群检查都执行完毕
    # 设置超时时间，防止集群出现问题，一直阻塞，导致后续集群维护任务等待
    for th in check_threads:
        th.join(timeout=DRUID_MAINTAIN_TIMEOUT)

    end = time.time()
    logger.info(
        f"druid maintain_all total time: {end - start}(s), set rule take {set_rule_finish - start}(s), "
        f"cluster maintain takes {end - set_rule_finish}(s)"
    )
    return True


def maintain_druid_cluster(cluster_name):
    """
    对单个集群串行maintain其rt, 清理rt在deepstorage上的无用数据和合并小segment
    :param cluster_name: 集群名称
    """
    cluster = model_manager.get_storage_cluster_config(cluster_name, DRUID)
    version = cluster[VERSION]
    clean_unused_segments(cluster_name, version, EXECUTE_TIMEOUT)
    # 对于0.11 druid版，无法执行compact操作
    if version == DRUID_VERSION_V2:
        segments_compaction(cluster_name, MAINTAIN_DELTA_DAY, MERGE_DAYS_DEFAULT, EXECUTE_TIMEOUT)
    logger.info(
        "{cluster_name}: maintain_druid_cluster total time: {end - start}(s), clean_unused_segments task "
        "{clean_finish - start}(s), compaction takes {end - clean_finish}(s)"
    )


def check_schema(rt_info):
    """
    校验RT的字段（名字、类型）的修改是否满足存储的限制
    :param rt_info: rt的配置信息
    :return: rt字段和存储字段的schema对比
    """
    result = {RT_FIELDS: {}, "druid_fields": {}, CHECK_RESULT: True, CHECK_DIFF: {}}
    for field in rt_info[FIELDS]:
        if field[FIELD_NAME].lower() in EXCEPT_FIELDS:
            continue
        result[RT_FIELDS][field[FIELD_NAME]] = field[FIELD_TYPE]

    _, physical_tn, conn_info = _get_druid_storage_info(rt_info)
    broker_host, broker_port = conn_info[HOST], conn_info[PORT]
    druid_schema_url = f"http://{broker_host}:{broker_port}/druid/v2/sql/"
    druid_schema_sql = (
        '{"query": "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS '
        "WHERE TABLE_NAME = '%s'\"}" % physical_tn
    )
    ok, druid_schema = post(druid_schema_url, params=json.loads(druid_schema_sql))
    if not ok or not druid_schema:
        return result

    logger.info(f"physical_tn: {physical_tn}, druid_schema_url: {druid_schema_url}, druid_schema: {druid_schema}")
    for e in druid_schema:
        result["druid_fields"][e["COLUMN_NAME"].lower()] = e["DATA_TYPE"].lower()

    append_fields, bad_fields = check_rt_druid_fields(result[RT_FIELDS], result["druid_fields"])
    result[CHECK_DIFF] = {APPEND_FIELDS: append_fields, BAD_FIELDS: bad_fields}
    if bad_fields:
        result[CHECK_RESULT] = False
    logger.info(f"diff result: {result}")

    return result


def check_rt_druid_fields(rt_table_columns, druid_columns):
    """
    对比rt的字段，和druid物理表字段的区别
    :param rt_table_columns: rt的字段转换为druid中字段后的字段信息
    :param druid_columns: druid物理表字段
    :return: (append_fields, bad_fields)，需变更增加的字段 和 有类型修改的字段
    """
    append_fields, bad_fields = [], []
    for key, value in rt_table_columns.items():
        col_name, col_type = key.lower(), value.lower()
        if druid_columns[col_name]:
            # 再对比类型
            druid_col_type = druid_columns[col_name]
            ok = (
                (col_type == druid_col_type)
                or (col_type == STRING and druid_col_type == VARCHAR)
                or (col_type == LONG and druid_col_type == BIGINT)
            )
            if not ok:
                bad_fields.append({col_name: f"difference between rt and druid({col_type} != {druid_col_type})"})
        else:
            append_fields.append({FIELD_NAME: col_name, FIELD_TYPE: col_type})

    return append_fields, bad_fields


def clusters():
    """
    获取druid存储集群列表
    :return: druid存储集群列表
    """
    result = model_manager.get_storage_cluster_configs_by_type(DRUID)
    return result


def create_task(rt_info):
    """
    创建任务
    :param rt_info: rt的配置信息
    :return: 创建task
    """
    druid, physical_tn, conn_info = _get_druid_storage_info(rt_info)
    zk_addr = conn_info.get(ZOOKEEPER_CONNECT)
    overlord = _get_role_leader(zk_addr, OVERLORD, druid[STORAGE_CLUSTER][VERSION])
    task_config = _get_task_config(rt_info)
    url = f"http://{overlord}{ENDPOINT_RUN_TASK}"
    result, resp = post(url=url, params=json.loads(task_config))
    if not result or not resp[TASK]:
        logger.error(f"create task error, url: {url}, param: {task_config}, result: {resp}")
        raise DruidCreateTaskErrorException(message_kv={RESULT_TABLE_ID: rt_info[RESULT_TABLE_ID]})

    # 获取正在执行的该任务地址
    task_id = resp[TASK]
    # 轮询结果
    return _get_task_location(overlord, task_id)


def _get_task_location(overlord, task_id, max_times=3):
    """
    :param overlord: overlord 节点
    :param task_id: 任务id
    :param max_times: 最大超时时间
    :return: 任务地址
    """
    if max_times < 0:
        return ""

    running_tasks = _get_tasks(overlord, TASK_TYPE_RUNNING)
    for task in running_tasks:
        if task[ID] == task_id:
            task_location = f"http://{task[LOCATION][HOST]}:{task[LOCATION][PORT]}{ENDPOINT_PUSH_EVENTS}"
            return task_location

    time.sleep(5)
    max_times = max_times - 1
    return _get_task_location(overlord, task_id, max_times)


def shutdown_task(rt_info):
    """
    :param rt_info: 结果表信息
    :return: 停止成功或者失败
    """
    druid, physical_tn, conn_info = _get_druid_storage_info(rt_info)
    zk_addr = conn_info[ZOOKEEPER_CONNECT]
    overlord = _get_role_leader(zk_addr, OVERLORD, druid[STORAGE_CLUSTER][VERSION])
    return _shutdown_task_with_retry(overlord, physical_tn)


def _shutdown_task_with_retry(overlord, data_source, max_times=3):
    """
    停止任务
    :param overlord: overlord 节点
    :param data_source: 数据源
    :param max_times: 最大次数
    :return: 停止task
    """
    if max_times < 0:
        raise DruidShutDownTaskException(message_kv={MESSAGE: "shut down overtime"})

    running_tasks = _get_tasks(overlord, TASK_TYPE_RUNNING)
    pending_tasks = _get_tasks(overlord, TASK_TYPE_PENDING)
    tasks = running_tasks + pending_tasks
    counter = 0
    for task in tasks:
        if task[ID].find(data_source) > 0:
            peon_url = f"http://{task[LOCATION][HOST]}:{task[LOCATION][PORT]}{ENDPOINT_SHUTDOWN_TASK}"
            resp = requests.post(peon_url)
            logger.info(f"shutdown task info, url: {peon_url}, result: {resp.content}")
            if resp.status_code != 200:
                logger.error(f"shutdown task exception, {resp}")
                raise DruidShutDownTaskException(message_kv={MESSAGE: resp})
            logger.info(f"shutdown task success, peon_url: {peon_url}, task_id: {task[ID]}")
        else:
            counter = counter + 1
    if counter == len(tasks):
        return True
    time.sleep(5)
    max_times = max_times - 1
    return _shutdown_task_with_retry(overlord, data_source, max_times)


def _get_druid_storage_info(rt_info):
    """
    获取存储基本信息
    :param rt_info: rt的信息
    :return: druid, physical_tn, conn_info
    """
    druid = rt_info[STORAGES][DRUID]
    physical_tn = druid[PHYSICAL_TABLE_NAME]
    conn_info = json.loads(druid[STORAGE_CLUSTER][CONNECTION_INFO])
    return (
        druid,
        physical_tn,
        conn_info,
    )


def _get_role_leader(zk_addr, zk_node, druid_version):
    """
    :param zk_addr: zk连接信息
    :param zk_node: zk节点类型
    :param druid_version: Druid版本
    :return: 获取leader
    """
    path = f"{ZK_DRUID_PATH}/{zk_node.lower() if druid_version == DRUID_VERSION_V1 else zk_node.upper()}"
    zk = KazooClient(hosts=zk_addr, read_only=True)
    zk.start()
    result = zk.get_children(path)
    zk.stop()
    if not result or len(result) == 0:
        logger.error(f"not found any zk path {path}, or this path is empty")
        raise DruidZkConfException()
    role = random.sample(result, 1)[0]

    if zk_node in ["overlord", "OVERLORD"]:
        leader_url = f"http://{role}/druid/indexer/v1/leader"
    elif zk_node in ["coordinator", "COORDINATOR"]:
        leader_url = f"http://{role}/druid/coordinator/v1/leader"
    else:
        logger.error(f"the zk path {path} is not for overlord or coordinator, please input a correct path")
        raise DruidZKPathException()

    resp = requests.get(leader_url, timeout=HTTP_REQUEST_TIMEOUT)
    if resp.status_code != 200:
        logger.error(f"failed to get leader from url: {leader_url}")
        raise DruidHttpRequestException()

    leader = resp.text.strip("http://")
    return leader


def _get_task_config(rt_info):
    """
    :param rt_info: 结果表信息
    :return: 获取Druid 任务配置
    """
    druid, physical_tn, conn_info = _get_druid_storage_info(rt_info)
    task_config_dict = {
        "availability_group": f"availability-group-{str(uuid.uuid4())[0:8]}",
        "required_capacity": DEFAULT_TASK_MEMORY,
        "data_source": physical_tn,
        "metrics_spec": _get_dimensions_and_metrics(rt_info)["metrics_fields"],
        "segment_granularity": DEFAULT_SEGMENT_GRANULARITY,
        "timestamp_column": DEFAULT_TIMESTAMP_COLUMN,
        "dimensions_spec": _get_dimensions_and_metrics(rt_info)["dimensions_fields"],
        "dimension_exclusions": [],
        "max_idle_time": DEFAULT_MAX_IDLE_TIME,
        "window_period": DEFAULT_WINDOW_PERIOD,
        "partition_num": random.randint(1, INT_MAX_VALUE),
        "context": {
            "druid.indexer.fork.property.druid.processing.buffer.sizeBytes": DEFAULT_TASK_MEMORY * 1024 * 1024 / 11,
            "druid.indexer.runner.javaOpts": "-Xmx%dM -XX:MaxDirectMemorySize=%dM"
            % (DEFAULT_TASK_MEMORY * 6 / 11 + 1, DEFAULT_TASK_MEMORY * 5 / 11 + 1),
        },
    }

    task_config = TASK_CONFIG_TEMPLATE.format(**task_config_dict).replace("'", '"')
    return task_config


def _get_dimensions_and_metrics(rt_info):
    """
    :param rt_info: 结果表信息
    :return: 返回纬度和度量字段
    """
    druid, physical_tn, conn_info = _get_druid_storage_info(rt_info)
    storage_config = json.loads(druid.get(STORAGE_CONFIG, "{}"))
    dimensions_fields = storage_config.get("dimensions_fields", [])
    metrics_fields = storage_config.get("metrics_fields", [])
    default_dimensions = [{NAME: str(field[FIELD_NAME]), TYPE: str(field[FIELD_TYPE])} for field in rt_info[FIELDS]]
    default_metrics = [{TYPE: "count", NAME: "__druid_reserved_count", "fieldName": ""}]
    dimensions_fields = dimensions_fields if dimensions_fields else default_dimensions
    metrics_fields = metrics_fields if metrics_fields else default_metrics
    return {"dimensions_fields": dimensions_fields, "metrics_fields": metrics_fields}


def _get_tasks(overlord_conn_info, task_type):
    """
    :param overlord_conn_info: overlord连接信息
    :param task_type: 任务类型
    :return: 该任务类型结果集
    """
    if task_type not in [TASK_TYPE_RUNNING, TASK_TYPE_PENDING]:
        raise NotSupportTaskTypeException(message_kv={TASK_TYPE, task_type})

    if task_type == TASK_TYPE_RUNNING:
        result, resp = get(f"http://{overlord_conn_info}{ENDPOINT_GET_RUNNING_TASKS}")
    else:
        result, resp = get(f"http://{overlord_conn_info}{ENDPOINT_GET_PENDING_TASKS}")

    if not result:
        raise DruidQueryTaskErrorException()

    return resp


def get_roles(cluster_name):
    """
    :param cluster_name: 集群名称
    :return:
    """
    cluster = model_manager.get_cluster_obj_by_name_type(cluster_name, DRUID)
    conn_info = json.loads(cluster.connection_info)
    zk_addr = conn_info[ZOOKEEPER_CONNECT]
    zk = KazooClient(hosts=zk_addr, read_only=True)
    zk.start()
    result = zk.get_children(ZK_DRUID_PATH)
    if not result or len(result) == 0:
        logger.error("Failed to get overload node")
        zk.stop()
        raise DruidZkConfException()
    data = dict()
    for role in result:
        data[role] = zk.get_children(f"{ZK_DRUID_PATH}/{role}")
    zk.stop()
    return data


def get_datasources(cluster_name):
    """
    :param cluster_name: 集群名称
    :return:
    """
    cluster = model_manager.get_cluster_obj_by_name_type(cluster_name, DRUID)
    conn_info = json.loads(cluster.connection_info)
    zk_addr = conn_info[ZOOKEEPER_CONNECT]
    coordinator = _get_role_leader(zk_addr, COORDINATOR, cluster.version)
    result, resp = get(f"http://{coordinator}{ENDPOINT_GET_DATASOURCES}")
    if not result:
        raise DruidQueryDataSourceException(message_kv={MESSAGE: resp})

    return resp


def get_workers(cluster_name):
    """
    :param cluster_name: 集群名称
    :return: workers信息
    """
    overlord = get_leader(cluster_name, OVERLORD)
    result, resp = get(f"http://{overlord}{ENDPOINT_GET_RUNNING_WORKERS}")
    if not result:
        raise DruidQueryWorkersException(message_kv={MESSAGE: resp})

    return resp


def get_historical(cluster_name):
    """
    :param cluster_name: 集群名称
    :return: historical容量
    """
    coordinator = get_leader(cluster_name, COORDINATOR)
    result, resp = get(f"http://{coordinator}{ENDPOINT_HISTORICAL_SIZES}")
    if not result:
        raise DruidQueryHistoricalException(message_kv={MESSAGE: resp})

    return resp


def get_cluster_capacity(cluster_name):
    """
    :param cluster_name: 集群名称
    :return: 容量信息
    """
    cluster_capacity = {
        "slot_capacity": 0,
        "slot_capacity_used": 0,
        "slot_usage": 0,
        "used_size": 0,
        "max_size": 0,
        "storage_usage": 0,
        "segments_count": 0,
        "timestamp": time.time(),
    }
    try:
        # 获取druid槽位信息
        worker_info = get_workers(cluster_name)
        if worker_info:
            for worker in worker_info:
                cluster_capacity["slot_capacity"] = cluster_capacity["slot_capacity"] + worker["worker"]["capacity"]
                cluster_capacity["slot_capacity_used"] = (
                    cluster_capacity["slot_capacity_used"] + worker["currCapacityUsed"]
                )

        # 获取historical 容量信息
        historical_info = get_historical(cluster_name)
        if historical_info:
            for historical in historical_info:
                if historical[TYPE] == "historical":
                    cluster_capacity["used_size"] = cluster_capacity["used_size"] + historical["currSize"]
                    cluster_capacity["max_size"] = cluster_capacity["max_size"] + historical["maxSize"]

        # 获取segments总数
        coordinator = get_leader(cluster_name, COORDINATOR)
        datasource_list_url = f"http://{coordinator}/druid/coordinator/v1/datasources/"
        ok, datasource_list = get(datasource_list_url)
        segments_sum = 0
        for physical_tn in datasource_list:
            segments_url = f"http://{coordinator}/druid/coordinator/v1/datasources/{physical_tn}"
            ok, datasource_meta = get(segments_url)
            segments_sum += datasource_meta[SEGMENTS][COUNT]
        cluster_capacity["segments_count"] = segments_sum
        cluster_capacity["slot_usage"] = (
            int(100 * cluster_capacity["slot_capacity_used"] / cluster_capacity["slot_capacity"])
            if cluster_capacity["slot_capacity"] > 0
            else 0
        )
        cluster_capacity["storage_usage"] = (
            int(100 * cluster_capacity["used_size"] / cluster_capacity["max_size"])
            if cluster_capacity["max_size"] > 0
            else 0
        )
        cluster_capacity[TIMESTAMP] = time.time()
    except Exception:
        logger.warning("failed to execute function druid.get_cluster_capacity", exc_info=True)

    return cluster_capacity


def get_table_capacity(conn_info):
    """
    读取druid集群容量数据
    :param conn_info: 集群链接信息
    :return:
    """
    url = f"http://{conn_info[HOST]}:{conn_info[PORT]}/druid/v2/sql/"
    sql = (
        '{"query": "SELECT datasource, sum(size * num_replicas)/1000000 as total_size, sum(num_rows) as total_nums '
        'FROM sys.segments WHERE is_available = 1 GROUP BY datasource"} '
    )
    rt_size = {}
    try:
        ok, table_capacity_list = post(url, params=json.loads(sql))
        if not ok or not table_capacity_list:
            return rt_size
        for table_capacity in table_capacity_list:
            rt_size[table_capacity[DATASOURCE]] = {
                TABLE_SIZE_MB: table_capacity["total_size"],
                TABLE_RECORD_NUMS: table_capacity["total_nums"],
                REPORT_TIME: time.time(),
            }
    except Exception:
        logger.warning("failed to execute function druid.get_table_capacity", exc_info=True)

    return rt_size


def get_leader(cluster_name, role_type):
    """
    :param cluster_name: 集群名称
    :param role_type: 角色类型
    :return: overlord or coordinator
    """
    cluster = model_manager.get_cluster_obj_by_name_type(cluster_name, DRUID)
    conn_info = json.loads(cluster.connection_info)
    zk_addr = conn_info[ZOOKEEPER_CONNECT]
    return _get_role_leader(zk_addr, role_type, cluster.version)


def get_tasks(cluster_name, task_type):
    """
    :param cluster_name: 集群名称
    :param task_type: 任务类型
    :return:
    """
    cluster = model_manager.get_cluster_obj_by_name_type(cluster_name, DRUID)
    conn_info = json.loads(cluster.connection_info)
    zk_addr = conn_info[ZOOKEEPER_CONNECT]
    overlord = _get_role_leader(zk_addr, OVERLORD, cluster.version)
    if task_type != TASK_TYPE_RUNNING and task_type != TASK_TYPE_PENDING:
        raise NotSupportTaskTypeException(message_kv={TASK_TYPE: task_type})
    elif task_type == TASK_TYPE_RUNNING:
        result, resp = get(f"http://{overlord}{ENDPOINT_GET_RUNNING_TASKS}")
    else:
        result, resp = get(f"http://{overlord}{ENDPOINT_GET_PENDING_TASKS}")

    if not result:
        raise DruidQueryTaskErrorException()

    return resp


def update_expires(rt_info, expires):
    """
    更新datasource的数据过期规则
    :param rt_info: 结果表
    :param expires: 过期时间
    :return:
    """
    druid, physical_tn, conn_info = _get_druid_storage_info(rt_info)
    expires = druid.get(EXPIRES, DEFAULT_DRUID_EXPIRES) if not expires else expires
    zk_addr = conn_info[ZOOKEEPER_CONNECT]
    coordinator = _get_role_leader(zk_addr, COORDINATOR, druid[STORAGE_CLUSTER][VERSION])
    rule_path = f"{ENDPOINT_DATASOURCE_RULE}/{physical_tn}"
    rule_url = f"http://{coordinator}{rule_path}"
    result, resp = get(rule_url)
    if not result:
        raise DruidQueryExpiresException(message_kv={MESSAGE: f"{physical_tn}获取数据过期时间异常"})

    rule = resp
    if not rule or len(rule) == 0:
        # 没有查询到过期规则，取默认的数据过期规则
        rule = DEFAULT_EXPIRES_RULE
    # 2 更新data_source中的数据过期时间
    rule[0]["period"] = f"P{expires.upper()}"

    resp = requests.post(rule_url, json=rule)
    if resp.status_code != 200:
        raise DruidUpdateExpiresException(message_kv={MESSAGE: f"{physical_tn}更新数据过期时间异常"})

    return True


def delete(rt_info, expires):
    """
    删除数据
    :param rt_info: 结果表
    :param expires: 过期时间
    :return:
    """
    druid, physical_tn, conn_info = _get_druid_storage_info(rt_info)
    zk_addr = conn_info[ZOOKEEPER_CONNECT]
    expires = druid.get(EXPIRES, "360d") if not expires else expires
    overlord = _get_role_leader(zk_addr, OVERLORD, druid[STORAGE_CLUSTER][VERSION])

    expires = translate_expires_day(expires)
    kill_interval = _get_kill_interval(expires)
    task_id = f'kill_{rt_info[RESULT_TABLE_ID]}_{kill_interval.replace("/", "_")}_{str(uuid.uuid4())[0:8]}'
    data = {TYPE: "kill", ID: task_id, "dataSource": physical_tn, INTERVAL: kill_interval}
    url = f"http://{overlord}{ENDPOINT_RUN_TASK}"
    logger.info(f"start delete data, url:{url}, params: {json.dumps(data)}")
    result, resp = post(url, data)
    if not result:
        raise DruidDeleteDataException(message_kv={MESSAGE: resp})

    return _check_delete_result(overlord, rt_info[RESULT_TABLE_ID], task_id)


def _get_kill_interval(expires):
    """
    获取kill的时间间隔
    :param expires: 过期时间
    :return:
    """
    date_diff = (datetime.today() + timedelta(-expires + 1)).strftime("%Y-%m-%dT00:00:00.000Z")
    time_utc = datetime.strptime(date_diff, UTC_FORMAT) - timedelta(hours=TIME_ZONE_DIFF)
    return f"{UTC_BEGIN_TIME}/{time_utc.strftime(UTC_FORMAT)}"


def _check_delete_result(overlord, result_table_id, task_id, max_times=60):
    """
    :param overlord: overload节点
    :param result_table_id: 结果表id
    :param task_id: 任务id
    :param max_times: 超时次数
    :return:
    """
    if max_times < 0:
        logger.error(f"deleting expired data failed, rt: {result_table_id}, task_id: {task_id}")
        raise DruidDeleteDataException(message_kv={MESSAGE: "删除过期数据失败, 超过最大重试次数"})

    time.sleep(5)
    result, resp = get(f"http://{overlord}{ENDPOINT_RUN_TASK}/{task_id}/status")
    if not result:
        raise DruidDeleteDataException(message_kv={MESSAGE: "检查任务运行状态异常"})

    result = resp
    if result.get(STATUS, {}).get(STATUS, "") == SUCCESS:
        return True
    else:
        max_times = max_times - 1
        logger.info(f"Enter the next poll, max_times: {max_times}, current result: {result}")
        return _check_delete_result(overlord, result_table_id, task_id, max_times)


def segments_compaction(cluster_name, delta_day, merge_days, timeout):
    """
    segments合并
    :param cluster_name: druid集群名
    :param delta_day: 合并跳过的天数
    :param merge_days: 合并的天数
    :param timeout: 合并操作的超时时间
    :return:
    """
    cluster = model_manager.get_storage_cluster_config(cluster_name, DRUID)
    zk_addr = json.loads(cluster[CONNECTION_INFO])[ZOOKEEPER_CONNECT]
    version = cluster[VERSION]
    coordinator = _get_role_leader(zk_addr, COORDINATOR, version)
    ok, datasources_used = get(f"http://{coordinator}{ENDPOINT_GET_DATASOURCES}")
    if not ok:
        return False

    for datasource in datasources_used:
        try:
            coordinator = _get_role_leader(zk_addr, COORDINATOR, version)
            ok, resp = get(f"http://{coordinator}/druid/coordinator/v1/datasources/{datasource}/")
            if not ok:
                continue
            last_day = datetime.strptime(resp[SEGMENTS][MINTIME], "%Y-%m-%dT%H:%M:%S.000Z").strftime("%Y-%m-%d")
            end_date = (datetime.today() - timedelta(delta_day)).strftime("%Y-%m-%d")
            begin_date = (datetime.today() - timedelta(delta_day + merge_days)).strftime("%Y-%m-%d")
            if end_date <= last_day:
                continue
            begin_date = last_day if last_day > begin_date else begin_date
            merge_segments(zk_addr, datasource, begin_date, end_date, version, timeout, merge_days)
        except Exception:
            logger.warning(f"segments compaction failed for datasource {datasource}", exc_info=True)

    return True

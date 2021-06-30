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
import posixpath
import threading
import time
import traceback
from os.path import normpath

import pyhdfs
import requests
from common.base_utils import model_to_dict
from common.local import get_request_username
from common.log import logger
from common.transaction import auto_meta_sync
from datahub.common.const import (
    ACTIVE,
    ADD_SQL,
    BK_BIZ_ID,
    BOOLEAN,
    CLUSTER_NAME,
    CONFIG,
    CONNECTION,
    CONNECTION_INFO,
    DATA_TYPE,
    DELETE_PATHS,
    DROP_SQL,
    EXIST_SQL,
    EXPIRES,
    FIELDS,
    GROUP_ID,
    HDFS,
    HDFS_MAINTAIN_SKIP_RTS,
    HDFS_URL,
    HOSTS,
    ICEBERG,
    INFO,
    LATEST_FILES,
    LENGTH,
    MAPLELEAF,
    MESSAGE,
    MODIFY_TIME,
    NAME,
    PARQUET,
    PARTITION_SPEC,
    PARTITIONS,
    PATH,
    PHYSICAL_TABLE_NAME,
    PORT,
    PROCESSING_TYPE,
    QUERYSET,
    RESULT_TABLE_ID,
    RESULT_TABLE_NAME,
    RUNNING,
    SNAPSHOT,
    STORAGE_CLUSTER,
    STORAGE_CONFIG,
    STORAGES,
    TABLE,
    TASKS_MAX,
    TODAY_DIRS,
    TOPICS,
    TOPICS_DIR,
    TYPE,
)
from datahub.databus.common_helper import add_task_log
from datahub.databus.model_manager import get_databus_config_value, get_shipper_by_name
from datahub.databus.models import DatabusConnectorTask
from datahub.databus.rt import get_databus_rt_info
from datahub.databus.task import task
from datahub.storekit import hive_util, iceberg, model_manager, util
from datahub.storekit.api import DataHubApi
from datahub.storekit.exceptions import (
    HdfsRestRequestError,
    MetaApiException,
    MigrateIcebergException,
    NoActiveNameNodeError,
    SchemaMismatchError,
)
from datahub.storekit.settings import (
    BKDATA_VERSION_TYPE,
    DAY_HOURS,
    HDFS_MAINTAIN_TIMEOUT,
    HIVE_SHOW_PARTITION_SQL,
    HIVE_SHOW_TABLE_SQL,
    ICEBERG_DEFAULT_PARTITION,
    PROCESSING_TYPE_SAME,
    PROCESSING_TYPES_NOT_ADDING_FIELDS,
    RTX_RECEIVER,
    SUPPORT_HIVE_VERSION,
    WEBHDFS_USER,
)


def initialize(rt_info, delta_day=None):
    """
    初始化rt对应的hdfs存储
    :param rt_info: rt的字段和配置信息
    :param delta_day: 指定向前追加的分区天数
    :return: 初始化rt对应的hdfs存储的结果
    """
    return prepare(rt_info, delta_day)


def info(rt_info):
    """
    获取rt的hdfs存储相关信息
    :param rt_info: rt的配置信息
    :return: rt的hdfs存储相关信息
    """
    biz_id = rt_info[BK_BIZ_ID]
    physical_table_name = rt_info[STORAGES][HDFS][PHYSICAL_TABLE_NAME]
    table_name = physical_table_name.split("/")[-1]
    processing_type = rt_info[PROCESSING_TYPE]
    hdfs = rt_info[STORAGES][HDFS]
    hdfs[INFO] = {TABLE: None, PARTITIONS: None, TODAY_DIRS: [], LATEST_FILES: []}
    # 获取hdfs的active name node，然后通过webhdfs接口获取目录列表、文件列表
    conn_info = json.loads(hdfs[STORAGE_CLUSTER][CONNECTION_INFO])
    hdfs_name_node = _get_valid_nn(conn_info[HOSTS], conn_info[PORT])
    webhdfs_addr = f"{hdfs_name_node}:{conn_info[PORT]}"
    is_query_set = processing_type in [QUERYSET, SNAPSHOT]
    if is_query_set:
        hdfs[INFO][LATEST_FILES] = _get_hdfs_dir_items(webhdfs_addr, physical_table_name)
    else:
        today = util.get_date_by_diff(0)
        hdfs[INFO][TODAY_DIRS] = _get_hdfs_dir_items(
            webhdfs_addr, f"{physical_table_name}/{today[0:4]}/{today[4:6]}/{today[6:8]}"
        )
        if hdfs[INFO][TODAY_DIRS]:
            latest_item = hdfs[INFO][TODAY_DIRS][0]
            for item in hdfs[INFO][TODAY_DIRS]:
                latest_item = item if item[MODIFY_TIME] > latest_item[MODIFY_TIME] else latest_item
            # 找到最新的目录，列出其中的文件
            hdfs[INFO][LATEST_FILES] = _get_hdfs_dir_items(webhdfs_addr, latest_item[PATH])

    if BKDATA_VERSION_TYPE in SUPPORT_HIVE_VERSION:
        # 获取hive相关的元信息
        hive_conn = _get_hive_connection_by_rt_id(rt_info[RESULT_TABLE_ID])

        hdfs[INFO][TABLE] = _format_sql_result(
            hive_util.exe_hive_sql_get_result(
                hive_conn, HIVE_SHOW_TABLE_SQL.format(biz_id=biz_id, table_name=table_name)
            )
        )
        if is_query_set:
            hdfs[INFO][PARTITIONS] = ""
        else:
            hdfs[INFO][PARTITIONS] = _format_sql_result(
                hive_util.exe_hive_sql_get_result(
                    hive_conn, HIVE_SHOW_PARTITION_SQL.format(biz_id=biz_id, table_name=table_name)
                )
            )

        close_hive_conn(hive_conn)

    return hdfs


def alter(rt_info, delta_day=None):
    """
    表结构变更需要同步到hive中
    :param rt_info: rt的配置信息
    :param delta_day: 指定向前追加的分区天数
    :return:
    """
    return prepare(rt_info, delta_day)


def delete(rt_info, with_data=True, with_hive_meta=True):
    """
    删除rt相关的hdfs元信息，以及hdfs上存储的数据。
    :param rt_info: rt的配置信息
    :param with_data: 是否删除hdfs上数据
    :param with_hive_meta: 是否删除hive表元信息
    """
    biz_id = rt_info[BK_BIZ_ID]
    physical_table_name = rt_info[STORAGES][HDFS][PHYSICAL_TABLE_NAME]
    table_name = physical_table_name.split("/")[-1]

    # 在hdfs上删除对应目录和文件（输入参数为路径数组）
    if with_data:
        conn_info = json.loads(rt_info[STORAGES][HDFS][STORAGE_CLUSTER][CONNECTION_INFO])
        hdfs_name_node = _get_valid_nn(conn_info[HOSTS], conn_info[PORT])
        _remove_hdfs_files(f"{hdfs_name_node}:{conn_info[PORT]}", [physical_table_name])
    # 在hive中删除表
    if BKDATA_VERSION_TYPE in SUPPORT_HIVE_VERSION and with_hive_meta:
        hive_conn = _get_hive_connection_by_rt_id(rt_info[RESULT_TABLE_ID])
        hive_util.exe_hive_ddl_sql(hive_conn, hive_util.generate_hive_drop_table(biz_id, table_name))
        close_hive_conn(hive_conn)

    return True


def prepare(rt_info, delta_day=None):
    """
    准备rt相关的hdfs存储，可能是创建，也可能是schema修改
    :param rt_info: rt的配置信息
    :param delta_day: 指定向前追加的分区天数
    :return: hdfs存储是否准备成功，True/False
    """
    if BKDATA_VERSION_TYPE not in SUPPORT_HIVE_VERSION:
        # 对于外部版本，无需初始化hive相关元数据
        return True

    biz_id = rt_info[BK_BIZ_ID]
    table_name = rt_info[STORAGES][HDFS][PHYSICAL_TABLE_NAME].split("/")[-1]
    processing_type = rt_info[PROCESSING_TYPE]

    is_query_set = processing_type in [QUERYSET, SNAPSHOT]

    # 检查是否hive表存在，不存在则创建
    connection = _get_hive_connection_by_rt_id(rt_info[RESULT_TABLE_ID])
    hive_util.exe_hive_ddl_sql(connection, hive_util.generate_hive_create_db(biz_id))  # 建库
    result = hive_util.exe_hive_sql_get_result(connection, hive_util.generate_hive_is_table_exist(biz_id, table_name))
    if result:
        # 暂时不支持数据探索结果表修改
        if is_query_set:
            close_hive_conn(connection)
            return True
        # 如果hive表存在，检查是否需要变更schema，如需要则变更schema
        columns = hive_util.trans_fields_to_hive_fields(rt_info[FIELDS])
        # TODO 此处rt关联的hdfs存储集群可能发生了变化，需要重建hive表
        hive_columns = _parse_hive_table_columns(
            hive_util.exe_hive_sql_get_result(connection, hive_util.generate_hive_desc_table(biz_id, table_name))
        )
        # rt表字段需是hive中表字段的子集，且字段类型相同。如果字段类型不一样，抛出异常
        to_add_columns, type_mismatch_columns = _diff_rt_and_hive_columns(columns, hive_columns)
        if type_mismatch_columns:
            # 如果hive表存在，字段类型不匹配，无法处理，抛出异常
            raise SchemaMismatchError(
                message_kv={
                    "cols": json.dumps(list(type_mismatch_columns.keys())),
                    "msg": json.dumps(type_mismatch_columns),
                }
            )
        elif to_add_columns:
            # 如果hive表存在，且需变更
            result = hive_util.exe_hive_ddl_sql(
                connection, hive_util.generate_hive_add_table_columns(biz_id, table_name, ", ".join(to_add_columns))
            )
        else:
            result = True  # 表存在，也无需修改，则认为prepare成功
    else:
        # 创建hive表
        physical_table_name = rt_info[STORAGES][HDFS][PHYSICAL_TABLE_NAME]
        conn_info = json.loads(rt_info[STORAGES][HDFS][STORAGE_CLUSTER][CONNECTION_INFO])
        hdfs_url = conn_info[HDFS_URL]
        data_type = PARQUET  # 默认hdfs上数据为parquet格式，下面代码兼容data_type字段不存在，或者值为空的场景
        if DATA_TYPE in rt_info[STORAGES][HDFS] and rt_info[STORAGES][HDFS][DATA_TYPE]:
            data_type = rt_info[STORAGES][HDFS][DATA_TYPE]

        if not delta_day and not is_query_set:
            delta_day = util.translate_expires_day(rt_info[STORAGES][HDFS][EXPIRES])
            if delta_day > 7 or delta_day < 0:
                delta_day = 7  # 对于过期时间超出七天的，以及永久存储的，都只创建最近七天的分区

        # 首先在hive中建表，先假定数据格式是parquet的
        columns = hive_util.trans_fields_to_hive_fields(rt_info[FIELDS], is_query_set)
        create_table_sql = hive_util.generate_hive_create_table_sql(
            biz_id, table_name, physical_table_name, ", ".join(columns), hdfs_url, data_type, not is_query_set
        )

        if is_query_set:
            if hive_util.exe_hive_ddl_sql(connection, create_table_sql):
                logger.info(
                    f"create hive table succeed for {rt_info[RESULT_TABLE_ID]}, processing_type: {processing_type}"
                )
                result = True
            else:
                result = False
        else:
            # 初始化时，按照过期日期将所有的数据目录添加到hive的分区中
            add_partitions_sql = hive_util.generate_hive_add_partition(
                biz_id, table_name, physical_table_name, delta_day
            )
            if hive_util.exe_hive_ddl_sql(connection, create_table_sql) and hive_util.exe_hive_ddl_sql(
                connection, add_partitions_sql
            ):
                logger.info(f"init hdfs meta succeed for {rt_info[RESULT_TABLE_ID]}")
                result = True
            else:
                result = False

    # 关闭hive的连接
    close_hive_conn(connection)
    return result


def maintain(rt_info, delta_day=1):
    """
    维护单个rt的hdfs存储相关hive元信息和数据文件
    :param rt_info: rt的配置信息
    :param delta_day: 维护日期偏移量
    """
    # 非数据探索rt才维护

    biz_id = rt_info[BK_BIZ_ID]
    physical_table_name = rt_info[STORAGES][HDFS][PHYSICAL_TABLE_NAME]
    table_name = physical_table_name.split("/")[-1]
    conn_info = json.loads(rt_info[STORAGES][HDFS][STORAGE_CLUSTER][CONNECTION_INFO])
    hdfs_name_node = _get_valid_nn(conn_info[HOSTS], conn_info[PORT])
    webhdfs_addr = f"{hdfs_name_node}:{conn_info[PORT]}"
    cluster_name = rt_info[STORAGES][HDFS][STORAGE_CLUSTER][CLUSTER_NAME]
    expires = rt_info[STORAGES][HDFS][EXPIRES]

    if rt_info[PROCESSING_TYPE] in [QUERYSET, SNAPSHOT]:
        hdfs_client = get_hdfs_client(webhdfs_addr)
        return maintain_datalab_parquet_rt(
            webhdfs_addr, hdfs_client, rt_info[RESULT_TABLE_ID], physical_table_name, expires
        )
    else:
        add_date = util.get_date_by_diff(delta_day)
        actions = _get_rt_maintain_actions(cluster_name, biz_id, table_name, physical_table_name, expires, add_date)
        # 删除数据
        _remove_hdfs_files(webhdfs_addr, actions[DELETE_PATHS])
        # 更新hive元数据，增加分区和删除分区
        if BKDATA_VERSION_TYPE in SUPPORT_HIVE_VERSION:
            hive_conn = _get_hive_connection_by_rt_id(rt_info[RESULT_TABLE_ID])
            _maintain_hive_meta(hive_conn, rt_info[RESULT_TABLE_ID], actions)
            close_hive_conn(hive_conn)

    return True


def _query_rts_by_processing_type(processing_type):
    """
    :param processing_type: 结果表类型
    :return: rt 列表
    """
    # 此处使用dict，在运用的时候，加速根据key检索
    rts = dict()
    page = 1
    while True:
        result = util.get_rts_by_processing_type(processing_type, page)
        # 当不存在rt返回时，则跳出循环
        if not result:
            break

        page += 1
        for rt_info in result:
            rts[rt_info[RESULT_TABLE_ID]] = rt_info[PROCESSING_TYPE]

    logger.info(f"{processing_type}: result table set: {rts}")
    return rts


def _query_datalab_rts():
    """
    :return: 数据探索rt列表
    """
    datalab_rts = dict()
    try:
        datalab_rts.update(_query_rts_by_processing_type(QUERYSET))
        datalab_rts.update(_query_rts_by_processing_type(SNAPSHOT))
    except Exception:
        logger.error("query datalab rts failed.", exc_info=True)
        raise MetaApiException(message_kv={MESSAGE: "query datalab rts failed"})
    return datalab_rts


def maintain_datalab_parquet_rt(webhdfs_addr, hdfs_client, result_table_id, root_path, expires):
    """
    :param webhdfs_addr: webhdfs_addr
    :param hdfs_client: hdfs_client
    :param result_table_id: 结果表id
    :param root_path: hdfs路径
    :param expires: 过期时间
    """
    # 对于无分区的结果表维护，只有一个数据文件，取数据文件的修改时间作为过期判断条件
    # 确认文件目录修改时间
    status_list = list_parquet_status(hdfs_client, root_path)
    result = True
    if not status_list:
        logger.info(f"{result_table_id} :not found file status, physical_table_name: {root_path}")
        return result

    delta_day = util.translate_expires_day(expires)
    delete_time = util.get_timestamp_diff(-(delta_day + 1))

    # 根据文件修改时间，删除过期文件
    for status in status_list:
        modify_time = status.modificationTime
        file_path = f"{root_path}/{status.pathSuffix}"
        logger.info(
            f"{result_table_id}: going to maintain datalab hdfs rt, status: {status}, modify_time: {modify_time}, "
            f"delete_time: {delete_time}, file_path: {file_path}"
        )
        if delete_time > modify_time:
            if not _remove_hdfs_files(webhdfs_addr, [file_path]):
                result = False
        else:
            logger.info(f"{result_table_id}: data not expired, no need to remove file")

    return result


def _maintain_hdfs_cluster(cluster_name, datalab_rts, delta_day=1):
    """
    对所有包含hdfs存储的rt进行维护，包含两部分：
        1）对于parquet格式表清理过期数据和维护hive中分区；
        2）对与iceberg格式表清理过期数据
    :param cluster_name 集群名称
    :param datalab_rts 数据探索结果表列表
    :param delta_day 维护日期偏移量
    """
    storage_rt_list = model_manager.get_storage_rt_objs_by_name_type(cluster_name, HDFS)
    other_rt_storages = []
    iceberg_rt_storages = []
    for rt_storage in storage_rt_list:
        if rt_storage.data_type == ICEBERG:
            iceberg_rt_storages.append(rt_storage)
        else:
            other_rt_storages.append(rt_storage)

    val = get_databus_config_value(HDFS_MAINTAIN_SKIP_RTS, "[]")
    try:
        skip_delete_rts = json.loads(val)
    except Exception:
        logger.info(f"bad config value for key {HDFS_MAINTAIN_SKIP_RTS}: {val}")
        skip_delete_rts = []

    # parquet格式表维护
    maintain_parquet(cluster_name, other_rt_storages, delta_day, skip_delete_rts, datalab_rts)

    # iceberg格式表维护，仅仅对datalab的rt进行处理
    iceberg_maintained = []
    for rt_storage in iceberg_rt_storages:
        rt_id = rt_storage.result_table_id
        if datalab_rts.get(rt_id):
            iceberg.maintain_datalab_iceberg_rt(rt_id, rt_storage.expires)
            iceberg_maintained.append(rt_id)

    logger.info(f"{cluster_name}: finished maintain iceberg datalab rts {json.dumps(iceberg_maintained)}")


def maintain_parquet(cluster_name, parquet_rt_storage_list, delta_day, skip_delete_rts, datalab_rts):
    """
    维护整个hdfs集群的非iceberg格式表
    :param cluster_name: 集群名
    :param parquet_rt_storage_list: parquet格式的rt存储信息列表
    :param delta_day: maintain的偏移时间
    :param skip_delete_rts: 跳过的rt集合
    :param datalab_rts: 数据探索rt列表，一般是queryset，snapshot等数据探索的结果表，该类暂时无分区
    """
    count = 0
    start_time_ts = time.time()
    hive_conn = None
    geog_area_conn = {}
    maintain_failed_rts = []
    # 避免此接口执行时间跨天，导致添加的分区发生变化。
    add_date = util.get_date_by_diff(delta_day)
    webhdfs_addr = _get_webhdfs_addr_by_cluster_name(cluster_name)
    hdfs_client = get_hdfs_client(webhdfs_addr)
    # 删除目录存在失败列表
    failed_remove_hdfs_rts = []
    for rt_storage in parquet_rt_storage_list:
        try:
            # 逐条处理，生成对应的actions，然后执行，避免将actions放入一个dict再遍历一轮
            count += 1
            rt = rt_storage.result_table_id
            biz_id = rt.split("_")[0]
            physical_table_name = rt_storage.physical_table_name
            if physical_table_name[0:1] != "/":  # 记录不合法的物理表名
                logger.warning(f"{cluster_name}: {rt} bad hdfs physical_table_name {physical_table_name}")
                continue
            table_name = physical_table_name.split("/")[-1]

            if datalab_rts.get(rt):
                if skip_delete_rts and rt in skip_delete_rts:
                    logger.info(f"{rt}: skip delete files datalab hdfs rt")
                    continue
                if not maintain_datalab_parquet_rt(
                    webhdfs_addr, hdfs_client, rt, physical_table_name, rt_storage.expires
                ):
                    failed_remove_hdfs_rts.append(rt)
            else:
                actions = _get_rt_maintain_actions(
                    cluster_name, biz_id, table_name, physical_table_name, rt_storage.expires, add_date
                )
                logger.info(f"{rt}: going to maintain hdfs {json.dumps(actions)}")
                if skip_delete_rts and rt in skip_delete_rts:
                    logger.info(f"{rt}: skip delete files {actions[DELETE_PATHS]} on HDFS and {actions[DROP_SQL]}")
                    actions[DROP_SQL] = ""
                else:
                    # 首先删除hdfs上过期目录，然后修改hive中的表分区。增加hive分区时，会自动在hdfs上创建对应分区目录
                    if not _remove_hdfs_files(webhdfs_addr, actions[DELETE_PATHS]):
                        failed_remove_hdfs_rts.append(rt)

                if BKDATA_VERSION_TYPE in SUPPORT_HIVE_VERSION:
                    #  确保hive connection非空，只被初始化一次
                    geog_area_code = util.query_result_table_geog_area(rt)
                    if not geog_area_conn.get(geog_area_code):
                        geog_area_conn[geog_area_code] = hive_util.get_hive_connection(geog_area_code)

                    hive_conn = geog_area_conn[geog_area_code]
                    if not _maintain_hive_meta(hive_conn, rt, actions):
                        maintain_failed_rts.append(rt)  # 可能flow只是创建了，没启动，所以不会建hive表
        except Exception:
            # 发生异常的时候，将hive_conn设置None，geog_area_conn 设置为空dict以便重新创建连接
            close_hive_conn(hive_conn)
            hive_conn = None
            geog_area_conn = {}
            time.sleep(3)  # 等待一小段时间，避免hiveserver故障时影响太多的rt的维护
            maintain_failed_rts.append(rt_storage.result_table_id)
            msg = (
                f"{rt_storage.storage_cluster_config.cluster_name}: failed to maintain hdfs for rt "
                f"{rt_storage.result_table_id}. {traceback.format_exc()}"
            )
            logger.error(msg)
            util.wechat_msg(RTX_RECEIVER, msg)  # 发送微信通知，便于定位问题
            util.mail_msg(RTX_RECEIVER, msg)

    # 最后关闭hive连接
    close_hive_conn(hive_conn)
    # 如果存在删除失败目录，则统一告警一次
    if failed_remove_hdfs_rts:
        msg = f"remove hdfs dir failed, rts: {failed_remove_hdfs_rts}"
        util.wechat_msg(RTX_RECEIVER, msg)
        util.mail_msg(RTX_RECEIVER, msg)

    # 记录总共处理的rt数量，以及异常的rt列表
    logger.info(
        f"{count} hdfs rts maintain takes {int(time.time() - start_time_ts)}(s), "
        f"failed are {json.dumps(maintain_failed_rts)}"
    )


def list_parquet_status(client, path):
    """
    :param client: client
    :param path: 路径
    :return: 状态
    """
    listing = []
    try:
        listing = client.list_status(path)
    except pyhdfs.HdfsFileNotFoundException:
        logger.error("file not found", exc_info=True)

    result = [status for status in listing if status.pathSuffix.endswith(f".{PARQUET}")]
    return result


def maintain_all_rts(delta_day=1):
    """
    按照集群并发维护关联hdfs的rt
    :param delta_day 维护日期偏移量
    """
    # 获取所有hdfs集群信息，按照集群纬度并发执行
    hdfs_clusters = model_manager.get_cluster_objs_by_type(HDFS)
    maintain_threads = []

    # datalab rt需要过滤
    datalab_rts = _query_datalab_rts()
    for hdfs_cluster in hdfs_clusters:
        maintain_cluster_thread = threading.Thread(
            target=_maintain_hdfs_cluster,
            name=f"{HDFS}-{hdfs_cluster.cluster_name}",
            args=(hdfs_cluster.cluster_name, datalab_rts, delta_day),
        )

        # 设置线程为守护线程，主线程结束后，结束子线程
        maintain_cluster_thread.setDaemon(True)

        maintain_threads.append(maintain_cluster_thread)
        maintain_cluster_thread.start()

    # join所有线程，等待所有集群检查都执行完毕
    # 设置超时时间，防止集群出现问题，一直阻塞，导致后续集群维护任务等待
    for th in maintain_threads:
        th.join(timeout=HDFS_MAINTAIN_TIMEOUT)

    return True


def delete_expire_data():
    """
    根据逐一curl hdfs的rest delete api
    """
    storage_rt_list = model_manager.get_storage_rt_objs_by_type(HDFS)
    webhdfs_addr_map = {}
    for rt_storage in storage_rt_list:
        cluster_name = rt_storage.storage_cluster_config.cluster_name
        # 数据探索和存储格式为iceberg的rt过滤掉
        rt_info = util.get_rt_info(rt_storage.result_table_id)
        if rt_info[PROCESSING_TYPE] in [QUERYSET, SNAPSHOT] or rt_info[STORAGES][HDFS][DATA_TYPE] == ICEBERG:
            continue

        physical_table_name = rt_storage.physical_table_name
        if cluster_name not in webhdfs_addr_map:
            webhdfs_addr_map[cluster_name] = _get_webhdfs_addr_by_cluster_name(cluster_name)
        delta_day = util.translate_expires_day(rt_storage.expires)
        if delta_day > 0:
            delete_date = util.get_date_by_diff(-(delta_day + 1))  # 加一天，expires内的都是需要保留的，超出的，需要删除
            delete_paths = [f"{physical_table_name}/{delete_date[0:4]}/{delete_date[4:6]}/{delete_date[6:8]}"]
            # 如果过期日期是本月的第一天，则把上个月的目录整体删除，如果跨年了，把上一年的目录删掉
            if delete_date[6:8] == "01":
                delete_last_month = util.get_date_by_diff(-(delta_day + 2))
                if delete_date[4:6] == "01":  # 1月1日，删掉去年的目录，避免一些目录漏删除
                    delete_paths.append(f"{physical_table_name}/{delete_last_month[0:4]}")
                else:
                    delete_paths.append(f"{physical_table_name}/{delete_last_month[0:4]}/{delete_last_month[4:6]}")
            # 大约有13000以上的hdfs相关rt，这里删除一个数据后等待一会，然后继续删除，避免删除太多导致namenode挂死
            if _remove_hdfs_files(webhdfs_addr_map[cluster_name], delete_paths):
                time.sleep(1)


def init_all_rts():
    """
    初始化所有的包含hdfs存储的rt相关元数据
    """
    if BKDATA_VERSION_TYPE not in SUPPORT_HIVE_VERSION:
        # 对于外部版本，无需初始化hive相关元数据
        return True

    bad_physical_table_names = {}
    fail_init_rts = []
    hdfs_cluster_url = {}
    geog_area_conn = {}
    connection = None
    # 获取所有关联hdfs存储的rt列表，通过游标遍历所有数据
    storage_rt_list = model_manager.get_storage_rt_objs_by_type(HDFS)
    for rt_storage in storage_rt_list:
        rt = rt_storage.result_table_id
        cluster_name = rt_storage.storage_cluster_config.cluster_name
        physical_table_name = rt_storage.physical_table_name
        logger.info(f"{cluster_name}: init {rt} hdfs meta with physical_table_name {physical_table_name}")
        if physical_table_name[0:1] != "/":  # 记录不合法的物理表名
            bad_physical_table_names[rt] = physical_table_name
            continue
        table_name = physical_table_name.split("/")[-1]
        rt_info = util.get_rt_info(rt)
        if not rt_info:
            logger.info(f"{cluster_name}: could not get rt info from meta for {rt}")
            fail_init_rts.append(rt)
            continue
        # 数据探索和存储格式iceberg的rt过滤掉
        if rt_info[PROCESSING_TYPE] in [QUERYSET, SNAPSHOT] or rt_info[STORAGES][HDFS][DATA_TYPE] == ICEBERG:
            fail_init_rts.append(rt)
            continue

        # 对于hdfs存储，这里需要生成hive表元信息，并写入
        biz_id = rt_info[BK_BIZ_ID]
        fields = rt_info[FIELDS]

        geog_area_code = util.query_result_table_geog_area(rt)
        if not geog_area_conn.get(geog_area_code):
            geog_area_conn[geog_area_code] = hive_util.get_hive_connection(geog_area_code)

        connection = geog_area_conn[geog_area_code]
        # 如果hive表已经存在了，则跳过初始化逻辑
        result = hive_util.exe_hive_sql_get_result(
            connection, hive_util.generate_hive_is_table_exist(biz_id, table_name)
        )
        if result:
            logger.info(f"{rt} hive table is already exists. {result}")
            continue
        conn_info = json.loads(rt_info[STORAGES][HDFS][STORAGE_CLUSTER][CONNECTION_INFO])
        if cluster_name not in hdfs_cluster_url:
            hdfs_cluster_url[cluster_name] = conn_info[HDFS_URL]

        data_type = PARQUET  # 默认hdfs上数据为parquet格式，下面代码兼容data_type字段不存在，或者值为空的场景
        if DATA_TYPE in rt_info[STORAGES][HDFS] and rt_info[STORAGES][HDFS][DATA_TYPE]:
            data_type = rt_info[STORAGES][HDFS][DATA_TYPE]

        delta_day = util.translate_expires_day(rt_info[STORAGES][HDFS][EXPIRES])
        if delta_day > 7 or delta_day < 0:
            delta_day = 7  # 对于过期时间超出七天的，以及永久存储的，都只创建最近七天的分区

        # 首先在hive中建表，先假定数据格式是parquet的
        columns = hive_util.trans_fields_to_hive_fields(fields)
        create_table_sql = hive_util.generate_hive_create_table_sql(
            biz_id, table_name, physical_table_name, ", ".join(columns), hdfs_cluster_url[cluster_name], data_type
        )
        # 初始化时，按照过期日期将所有的数据目录添加到hive的分区中
        add_partitions_sql = hive_util.generate_hive_add_partition(biz_id, table_name, physical_table_name, delta_day)

        if (
            hive_util.exe_hive_ddl_sql(connection, hive_util.generate_hive_create_db(biz_id))
            and hive_util.exe_hive_ddl_sql(connection, create_table_sql)
            and hive_util.exe_hive_ddl_sql(connection, add_partitions_sql)
        ):
            logger.info(f"init hdfs meta succeed for {rt}")
        else:
            fail_init_rts.append(rt)

    # 最后关闭hive连接
    close_hive_conn(connection)
    logger.warning(
        f"init hdfs meta failed for rts: {json.dumps(fail_init_rts)}, bad physical table name rts: "
        f"{json.dumps(bad_physical_table_names)}"
    )

    return True


def clusters():
    """
    获取hdfs存储集群列表
    :return: hdfs存储集群列表
    """
    result = model_manager.get_storage_cluster_configs_by_type(HDFS)
    return result


def get_filesystem_info(hosts, port):
    """
    获取hdfs集群容量信息
    :param hosts: name node主机列表
    :param port: name node端口
    :return:  hdfs集群的容量信息
    """
    host = hosts.split(",")[0]
    res = requests.get(f"http://{host}:{port}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem")
    if res.status_code == 200:
        logger.info(f"{host}:{port} filesystem info {res.text}")
        return res.json()
    else:
        logger.warning(f"{host}:{port}: get hdfs cluster info failed. {res.status_code} {res.text}")
        raise HdfsRestRequestError(message_kv={"msg": res.text})


def _get_webhdfs_addr_by_cluster_name(cluster_name):
    """
    根据hdfs集群的名称获取集群的webhdfs地址
    :param cluster_name: 集群名称
    :return: webhdfs的地址
    """
    hdfs_cluster = model_manager.get_storage_cluster_config(cluster_name, HDFS)
    if hdfs_cluster:
        try:
            # 可能hdfs集群配置中所有namenode都连不上
            hdfs_name_node = _get_valid_nn(hdfs_cluster[CONNECTION][HOSTS], hdfs_cluster[CONNECTION][PORT])
            return f"{hdfs_name_node}:{hdfs_cluster[CONNECTION][PORT]}"
        except NoActiveNameNodeError:
            logger.warning(f"hdfs cluster {cluster_name} has no active name node. {hdfs_cluster[CONNECTION]}")

    logger.warning(f"unable to get hdfs cluster {cluster_name} info to process")
    return "localhost:8081"  # 无法获取hdfs集群的webhdfs地址时，返回默认值


def _get_webhdfs_addr_by_rt(rt_info):
    """
    根据rt信息获取集群的webhdfs地址
    :param rt_info: 结果表信息
    :return: webhdfs的地址
    """
    # 可能hdfs集群配置中所有namenode都连不上
    hdfs_conn = json.loads(rt_info[STORAGES][HDFS][STORAGE_CLUSTER][CONNECTION_INFO])
    hosts, port = hdfs_conn[HOSTS], hdfs_conn[PORT]
    return f"{_get_valid_nn(hosts, port)}:{port}"


def _get_rt_maintain_actions(cluster_name, biz_id, table_name, physical_table_name, expires, add_date):
    """
    生成rt对应的hdfs存储维护操作集，包含增删分区语句、hdfs上数据清理目录、判断hive表是否存在sql语句等。
    :param cluster_name: hdfs的集群名称
    :param biz_id: rt的业务ID
    :param table_name: rt的表名称
    :param physical_table_name: rt的物理表名称
    :param expires: rt的过期时间
    :param add_date: 添加的分区的日期
    :return: 维护rt的hdfs存储的操作集
    """
    actions = {
        CLUSTER_NAME: cluster_name,
        DELETE_PATHS: [],
        DROP_SQL: "",
        ADD_SQL: "",
        EXIST_SQL: hive_util.generate_hive_is_table_exist(biz_id, table_name),
    }
    # 生成要删除的目录和hive中需要drop的分区
    delta_day = util.translate_expires_day(expires)
    if delta_day > 0:
        delete_date = util.get_date_by_diff(-(delta_day + 1))  # 加一天，expires内的都是需要保留的，超出的，需要删除
        actions[DELETE_PATHS].append(f"{physical_table_name}/{delete_date[0:4]}/{delete_date[4:6]}/{delete_date[6:8]}")
        # 如果过期日期是本月的第一天，则把上个月的目录整体删除，如果跨年了，把上一年的目录删掉
        if delete_date[6:8] == "01":
            delete_last_month = util.get_date_by_diff(-(delta_day + 2))
            if delete_date[4:6] == "01":  # 1月1日，删掉去年的目录，避免一些目录漏删除
                actions[DELETE_PATHS].append(f"{physical_table_name}/{delete_last_month[0:4]}")
            else:
                actions[DELETE_PATHS].append(f"{physical_table_name}/{delete_last_month[0:4]}/{delete_last_month[4:6]}")
        # TODO 此处删除分区的sql里，因为maintain_all接口执行时间非常长，导致日期也可能出现跨天的情况。
        actions[DROP_SQL] = hive_util.generate_hive_drop_date_partitions(biz_id, table_name, delete_date)
    # 生成hive中需要添加的分区，添加分区语句执行时，会自动创建分区对应的目录
    actions[ADD_SQL] = hive_util.generate_hive_add_partition_by_date(biz_id, table_name, physical_table_name, add_date)

    return actions


def _maintain_hive_meta(hive_conn, rt, actions):
    """
    维护rt在hive中的表结构和分区
    :param hive_conn: hive的链接
    :param rt: rt名称
    :param actions: 操作的sql语句
    :return: 是否维护成功，True/False
    """
    # 首先检查表是否存在
    hive_util.exe_hive_ddl_sql(hive_conn, hive_util.generate_hive_create_db(rt.split("_")[0]))
    result = hive_util.exe_hive_sql_get_result(hive_conn, actions[EXIST_SQL])
    if result:
        # 如果存在，则执行添加partition的语句和删除partition的语句
        succ = hive_util.exe_hive_ddl_sql(hive_conn, actions[ADD_SQL])
        if actions[DROP_SQL]:
            succ = succ and hive_util.exe_hive_ddl_sql(hive_conn, actions[DROP_SQL])
        return succ
    else:
        logger.warning(f"{rt}: hive table is missing, unable to add/drop partition")
        return False


def _get_hdfs_dir_items(webhdfs_addr, path):
    """
    获取hdfs上指定目录的文件列表
    :param webhdfs_addr: webhdfs的地址
    :param path: hdfs上目录
    :return: 目录里文件列表
    """
    result = []
    res = requests.get(f"http://{webhdfs_addr}/webhdfs/v1{path}?user.name={WEBHDFS_USER}&op=liststatus")
    if res.status_code == 200:
        for item in res.json()["FileStatuses"]["FileStatus"]:
            result.append(
                {
                    PATH: f'{path}/{item["pathSuffix"]}',
                    TYPE: item[TYPE],
                    LENGTH: item[LENGTH],
                    MODIFY_TIME: item["modificationTime"],
                }
            )
    else:
        logger.warning(f"{webhdfs_addr}: failed to list directory {path}. {res.status_code} {res.text}")

    return result


def _get_hdfs_dir_files(webhdfs_addr, path):
    """
    获取hdfs上指定目录的文件列表
    :param webhdfs_addr: webhdfs的地址
    :param path: hdfs上目录
    :return: 目录里文件列表
    """
    result = []
    res = requests.get(f"http://{webhdfs_addr}/webhdfs/v1{path}?user.name={WEBHDFS_USER}&op=liststatus")
    if res.status_code == 200:
        for item in res.json()["FileStatuses"]["FileStatus"]:
            result.append(item["pathSuffix"])
    else:
        logger.warning(f"{webhdfs_addr}: failed to list directory {path}. {res.status_code} {res.text}")

    return result


def _get_hdfs_dirs_by_date(physical_table_name, date):
    """
    根据日期获取指定日期的hdfs上数据目录列表
    :param physical_table_name: 物理表名称
    :param date: 日期
    :return: hdfs上的数据目录列表
    """
    return [f"{physical_table_name}/{date[0:4]}/{date[4:6]}/{date[6:8]}/{hour}" for hour in DAY_HOURS]


def _remove_hdfs_files(webhdfs_addr, delete_path_list):
    """
    删除hdfs上的目录和文件
    :param webhdfs_addr: hdfs集群的webhdfs地址
    :param delete_path_list: 待删除的目录列表
    :return: 是否全部删除成功，True/False
    """
    logger.info(f"{webhdfs_addr}: going to delete dirs {json.dumps(delete_path_list)}")
    result = True
    for path in delete_path_list:
        # 查询目录是否存在，若不存在，有可能是没有正常维护创建目录,这里日志记录。
        # 任何非404,均去删除，若删除失败则告警
        res = requests.get(f"http://{webhdfs_addr}/webhdfs/v1{path}?user.name={WEBHDFS_USER}&op=GETFILESTATUS")
        if res.status_code == 404:
            logger.info(f"{webhdfs_addr}: not found dir {path}")
            continue

        res = requests.delete(
            f"http://{webhdfs_addr}/webhdfs/v1{path}?user.name={WEBHDFS_USER}&op=DELETE&recursive=true"
        )
        if res.status_code != 200 or not res.json()[BOOLEAN]:
            logger.warning(f"{webhdfs_addr}: failed to delete directory {path}. {res.status_code} {res.text}")
            result = False  # 有目录删除失败时，标记下

    return result


def _add_hdfs_dirs(webhdfs_addr, add_path_lists):
    """
    在hdfs上创建目录
    :param webhdfs_addr: hdfs集群的webhdfs地址
    """
    for path in add_path_lists:
        res = requests.put(f"http://{webhdfs_addr}/webhdfs/v1{path}?user.name={WEBHDFS_USER}&op=MKDIRS")
        if res.status_code != 200 or not res.json()[BOOLEAN]:
            logger.warning(f"{webhdfs_addr}: failed to create directory {path}. {res.status_code} {res.text}")


def _get_valid_nn(hosts, port):
    """
    获取hdfs集群中active的name node的主机和端口
    :param hosts: name node主机列表
    :param port: name node端口
    :return:  active的name node的地址
    """
    for host in hosts.split(","):
        try:
            res = requests.get(f"http://{host}:{port}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus")
            if res.status_code == 200 and res.json()["beans"][0]["State"] == ACTIVE:
                return host
            else:
                logger.error(
                    f"get valid namenode failed, or not active namenode. response: {res.status_code} {res.text}."
                )
        except Exception:
            logger.error("get valid nn exception", exc_info=True)
    # 未找到active的name node
    raise NoActiveNameNodeError()


def get_offset_dir(webhdfs_addr, shipper_cluster, connector, topic_dir, partition_num):
    """
    获取offset文件所在目录
    :param shipper_cluster: shipper集群名称
    :param webhdfs_addr: hdfs地址
    :param topic_dir: topic目录
    :param connector: shipper connector名
    :param partition_num: kafka topic 分区数
    :return: offset文件所在目录名
    """
    path = generate_offset_dir(shipper_cluster, connector, topic_dir)
    files = _get_hdfs_dir_files(webhdfs_addr, path)
    # 只要任何分区号对应的目录能在hdfs中找到，就认定此hdfs目录可用; 否则说明offset目录不存在
    return path if [True for p in range(partition_num) if str(p) in files] else ""


def generate_offset_dir(shipper_cluster, connector, topic_dir):
    """
    生成offset文件所在目录
    :param shipper_cluster: shipper集群名称
    :param topic_dir: topic目录
    :param connector: shipper connector名
    :return: offset文件所在目录名
    """
    arr = shipper_cluster.split("-")
    if len(arr) == 3:
        # 对于内部版，集群名称类似  hdfs-inner2-M  ，这里取inner2作为offset目录的一部分，避免rt被删除再建时，kafka集群发生变化
        path = f"{topic_dir}/{arr[1]}/__offset__/{connector}"
    elif len(arr) == 4:
        # 对于海外版，集群名称类似  hdfs-inner-NA-M  ，这里取inner-NA作为offset目录的一部分，避免rt被删除再建时，kafka集群发生变化
        path = f"{topic_dir}/{arr[1]}-{arr[2]}/__offset__/{connector}"
    else:
        path = f"{topic_dir}/{shipper_cluster}/__offset__/{connector}"

    return path


def get_max_offset(files):
    """
    从文件信息列表中提取max offset
    :param files: 文件名列表
    :return: 字符串形式的offset值
    """
    offset_max = "-1"
    for offset in files:
        if int(offset) > int(offset_max):
            offset_max = offset
    return offset_max


def get_task_base_conf_by_name(name):
    """
    :param name: connector_task_name 名称
    """
    config = dict()
    shipper = get_shipper_by_name(name)
    if not shipper:
        logger.warning(f"{name}: has no shipper task")
        return config

    connector_config = json.loads(model_to_dict(shipper)[CONFIG])
    config[GROUP_ID] = connector_config.get(GROUP_ID, "")
    config[TOPICS] = connector_config.get(TOPICS, "")
    config[TOPICS_DIR] = normpath(connector_config.get(TOPICS_DIR, "/kafka/data/"))
    config[TASKS_MAX] = int(connector_config.get(TASKS_MAX, 1))
    config[NAME] = connector_config.get(NAME, name)

    return config


def read_offset(rt_info):
    """
    获取所有分区的offset
    :param rt_info: rt的详细信息
    :return: offset_msgs 和 offset_info
    """
    rt_id = rt_info[RESULT_TABLE_ID]
    task_config = get_task_base_conf_by_name(f"{HDFS}-table_{rt_id}")
    if not task_config:
        return {}

    try:
        partition_num = task_config[TASKS_MAX]
        webhdfs_addr = _get_webhdfs_addr_by_rt(rt_info)
        offset_dir = get_offset_dir(
            webhdfs_addr, task_config[GROUP_ID], task_config[NAME], task_config[TOPICS_DIR], partition_num
        )
        offset_msgs = {}
        if offset_dir:
            for p in range(partition_num):
                files = _get_hdfs_dir_files(webhdfs_addr, f"{offset_dir}/{p}")
                offset = get_max_offset(files) if files else "-1"
                topic_partition = f"table_{rt_id}-{p}"
                offset_msgs[topic_partition] = offset
            logger.info(f"rt {rt_id} get offset_msgs from hdfs offset dir: {offset_msgs}")

        return offset_msgs
    except Exception:
        logger.warning(f"failed to get offset_msgs for rt {rt_id}", exc_info=True)
        return {}


def migrate_iceberg(rt_info, start_shipper_task=True, create_table=True):
    """
    切换rt的hdfs存储格式为iceberg，可以重复执行
    :param rt_info: rt的详细信息
    :param start_shipper_task: 启动任务
    :param create_table: 是否尝试创建iceberg表，默认为true
    :return rt的hdfs存储信息
    """
    rt_id = rt_info[RESULT_TABLE_ID]
    # 数据按照id倒序排列，第一个即为最新的一条关联关系记录
    relation_obj = model_manager.get_all_storage_rt_objs_by_rt_type(rt_id, HDFS)[0]
    if relation_obj.active != 1:  # 此关联关系非active状态，无法进行后续操作
        raise MigrateIcebergException(message_kv={MESSAGE: "no valid hdfs storage."})

    databus_rt_info = get_databus_rt_info(rt_id)
    if rt_info[STORAGES][HDFS].get(DATA_TYPE, PARQUET) == PARQUET:
        if rt_info[PROCESSING_TYPE] in PROCESSING_TYPE_SAME:
            # 停当前HDFS分发任务，并删除分发任务记录。仅仅对STREAM等类型的processing_type有效
            task.stop_databus_task(databus_rt_info, [HDFS], True)
            task.remove_databus_task_routing(f"{HDFS}-table_{rt_id}")
            logger.info(f"{rt_id}: hdfs sink task stopped and deleted.")

        _set_hdfs_datatype_to_iceberg(rt_info, relation_obj)
        # 更新rt_info，因为数据类型和物理表名称以发生变化。
        rt_info = util.get_rt_info(rt_id)
    else:
        logger.info(f"{rt_id} no need to change hdfs data type, go ahead")

    # 创建iceberg表，重置offset，假定hubmanager调用成功
    if create_table:
        iceberg.create_iceberg_table(rt_info, False)

    # 增加是否启动hdfs分发，便于迁移时，不需要启动分发任务的场景
    if rt_info[PROCESSING_TYPE] in PROCESSING_TYPE_SAME and start_shipper_task:
        connector_name = f"{HDFS}-table_{rt_info[RESULT_TABLE_ID]}"
        tasks = DatabusConnectorTask.objects.filter(connector_task_name=connector_name, status=RUNNING.lower())
        if len(tasks) != 1 or not model_to_dict(tasks[0])[CLUSTER_NAME].startswith("hdfsiceberg-"):
            reset_by_hdfs_offset(rt_info)
            # 更新databus_rt_info，启动iceberg分发
            databus_rt_info = get_databus_rt_info(rt_id)
            task.add_databus_shipper_task(databus_rt_info, HDFS)
            task.start_databus_shipper_task(databus_rt_info, HDFS)
            add_task_log("add_connector", HDFS, databus_rt_info["rt.id"], ICEBERG, "successfully started the task")
            logger.info(f"{rt_id}: start iceberg hdfs sink task success.")
        else:
            logger.info(f"{rt_id}: hdfs iceberg shipper task already started!")


def _set_hdfs_datatype_to_iceberg(rt_info, relation_obj):
    """
    将rt的hdfs存储的数据类型修改为iceberg，并设定分区的配置，原data_type和物理表名称的映射等
    :param rt_info: rt的配置项
    :param relation_obj: rt的hdfs存储对象
    """
    hdfs = rt_info[STORAGES][HDFS]
    data_type = hdfs[DATA_TYPE] if hdfs.get(DATA_TYPE) else PARQUET
    storage_conf = json.loads(hdfs[STORAGE_CONFIG])
    storage_conf[data_type] = hdfs[PHYSICAL_TABLE_NAME]
    bk_biz_id = rt_info[BK_BIZ_ID]
    if PARTITION_SPEC not in storage_conf:
        add_time_fields = rt_info[PROCESSING_TYPE] not in PROCESSING_TYPES_NOT_ADDING_FIELDS
        storage_conf[PARTITION_SPEC] = ICEBERG_DEFAULT_PARTITION if add_time_fields else []

    relation_obj.storage_config = json.dumps(storage_conf)
    relation_obj.data_type = ICEBERG
    relation_obj.physical_table_name = iceberg.physical_table_name(bk_biz_id, rt_info[RESULT_TABLE_NAME])
    relation_obj.updated_by = get_request_username()
    with auto_meta_sync(using=MAPLELEAF):
        relation_obj.save()


def reset_by_hdfs_offset(rt_info):
    """
    将rt在hdfs中的当前offset写入iceberg的commitMsgs中
    :param rt_info: rt的字段和配置信息
    :return: 执行状态
    """
    params = iceberg.construct_connect_config(rt_info)
    offset_msgs = read_offset(rt_info)
    logger.info(f"rt {rt_info[RESULT_TABLE_ID]} reset iceberg offset with offset_msgs: {offset_msgs}")
    if not offset_msgs:
        return False
    params["offsetMsgs"] = offset_msgs
    rsp = DataHubApi.iceberg_offset_msgs.create(params)
    logger.info(f"get iceberg commit_msgs with params: {params}; response: {rsp.data}")
    if not rsp.is_success():
        return False

    return True


def get_partition_paths(rt_info):
    """
    获取表的分区路径列表
    :param rt_info: rt的字段和配置信息
    :return: 分区路径列表
    """
    if rt_info[STORAGES][HDFS][DATA_TYPE] == ICEBERG:
        params = iceberg.construct_connect_config(rt_info)
        rsp = DataHubApi.iceberg_partition_paths.create(params)
        if not rsp.is_success():
            logger.error(f"iceberg get partition paths failed with params: {params}; response: {rsp.data}")
            return []

        return rsp.data
    else:
        return []


def _format_sql_result(result_set):
    """
    将执行sql语句的结果集转换为字符串
    :param result_set: 结果集
    :return: 字符串的结果集
    """
    result = []
    if result_set:
        for row in result_set:
            result.append(" ".join(row))
    return "\n".join(result)


def _parse_hive_table_columns(desc_table_result):
    """
    解析desc table的结果，获取表中字段和类型并返回
    :param desc_table_result: desc table的执行结果
    :return: hive表中的字段和类型信息
    """
    result = {}
    if desc_table_result:
        for (column, type, comment) in desc_table_result:
            if column == "dt_par_unit":
                break
            result[column] = type

    return result


def _diff_rt_and_hive_columns(rt_columns, hive_columns):
    """
    对比rt的字段列表和hive表中的字段列表，找出需要添加的字段和字段类型不同的字段
    :param rt_columns: rt的字段列表，格式是数组，单条记录用空格分隔字段名称和类型，例如"column type"
    :param hive_columns: hive表的字段列表，格式是dict，key为字段名称，value为字段类型
    :return: 对比结果，包含需添加的字段列表和类型不匹配的字段列表
    """
    to_add_columns, type_mismatch_columns = [], {}
    for entry in rt_columns:
        arr = entry.split(" ")
        column, type = arr[0], arr[1]
        # hive表中获取的字段名称全部为小写，但rt的字段名称中可能有大写字母。这里rt的字段名称做了转义，需去掉转义字符
        col = column.lower().replace("`", "")
        if col in hive_columns:
            if type == hive_columns[col]:
                continue
            else:
                type_mismatch_columns[col] = f"({hive_columns[col]} != {type})"
        else:
            to_add_columns.append(entry)

    return to_add_columns, type_mismatch_columns


def _get_hive_connection_by_rt_id(result_table_id):
    """
    :param result_table_id: 结果表id
    :return: hive conn
    """
    geog_area_code = util.query_result_table_geog_area(result_table_id)
    return hive_util.get_hive_connection(geog_area_code)


def remove_offset_dir(rt_info, cluster_name=None):
    """
    :param rt_info: 结果表信息
    :param cluster_name: 集群名称
    """
    if rt_info[STORAGES][HDFS][DATA_TYPE] == ICEBERG:
        return

    rt_id = rt_info[RESULT_TABLE_ID]
    try:
        task_config = get_task_base_conf_by_name(f"{HDFS}-table_{rt_id}")
        if not task_config:
            return
        webhdfs_addr = (
            _get_webhdfs_addr_by_cluster_name(cluster_name) if cluster_name else _get_webhdfs_addr_by_rt(rt_info)
        )
        offset_dir = generate_offset_dir(task_config[GROUP_ID], task_config[NAME], task_config[TOPICS_DIR])
        _remove_hdfs_files(webhdfs_addr, [offset_dir])
    except Exception:
        logger.error(f"{rt_id}: failed to remove offset dir", exc_info=True)


def physical_table_name(rt_info, data_type=""):
    """
    查询物理表名
    :param rt_info: rt的字段和配置信息
    :param data_type: 数据格式
    :return: physical_table_name
    """
    hdfs = rt_info[STORAGES][HDFS]
    bk_biz_id = rt_info[BK_BIZ_ID]
    current_data_type = hdfs[DATA_TYPE]
    storage_config = json.loads(hdfs[STORAGE_CONFIG])
    if data_type == current_data_type:
        return hdfs[PHYSICAL_TABLE_NAME]
    elif data_type == ICEBERG:
        return iceberg.physical_table_name(bk_biz_id, rt_info[RESULT_TABLE_NAME])
    elif current_data_type == ICEBERG:
        return storage_config.get(data_type)
    else:
        # 当传入参数data_type未知时，返回当前的物理表名称
        return hdfs[PHYSICAL_TABLE_NAME]


def close_hive_conn(hive_conn):
    """
    :param hive_conn: hive 连接
    """
    try:
        if hive_conn:
            # 连接可能异常
            hive_conn.close()
    except Exception:
        logger.error("abnormal hive connection", exc_info=True)


def get_hdfs_client(hosts):
    """
    :param hosts: namenode 列表，格式为 "host:port;host2:port"
    """
    return pyhdfs.HdfsClient(hosts=hosts, user_name=WEBHDFS_USER)


def walk_dir(client, root_path, depth=1, onerror=None):
    """
    递归遍历hdfs上目录，可指定遍历的深度，默认只遍历一层
    :param client:
    :param root_path:
    :param depth:
    :param onerror:
    :return:
    """
    if depth <= 0:
        return
    try:
        listing = client.list_status(root_path)
    except pyhdfs.HdfsException as e:
        if onerror is not None:
            onerror(e)
        return
    dirs, filenames = [], []
    for f in listing:
        if f.type == "DIRECTORY":
            dirs.append(f.pathSuffix)
        elif f.type == "FILE":
            filenames.append(f.pathSuffix)
        else:  # pragma: no cover
            raise AssertionError(f"Unexpected type {f.type}")
    yield root_path, dirs, filenames
    for name in dirs:
        new_path = posixpath.join(root_path, name)
        yield from walk_dir(client, new_path, depth - 1, onerror)

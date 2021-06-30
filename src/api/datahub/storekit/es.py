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
import threading
import time

import requests
from common.base_crypt import BaseCrypt
from common.log import logger
from datahub.common.const import (
    ACTIONS,
    ADD,
    ALIAS,
    ALLOCATION,
    ANALYZED_FIELDS,
    CLUSTER_NAME,
    CLUSTER_TYPE,
    CONNECTION_INFO,
    DATE,
    DATE_FIELDS,
    DOC_VALUES,
    DOC_VALUES_FIELDS,
    DOUBLE,
    DTEVENTTIME,
    DTEVENTTIMESTAMP,
    ENABLE_REPLICA,
    ES,
    ES_CONF,
    ES_FIELDS,
    EXPIRES,
    FAILED,
    FALSE,
    FIELD_NAME,
    FIELD_TYPE,
    FIELDS,
    FLOAT,
    HAS_REPLICA,
    HOST,
    INCLUDE,
    INCLUDE_IN_ALL,
    INDEX,
    INDICES,
    INFO,
    INT,
    INTEGER,
    JSON_FIELDS,
    JSON_HEADERS,
    KEYWORD,
    LONG,
    MAPPINGS,
    NUMBER_OF_REPLICAS,
    OBJECT,
    ORDER,
    PASSWORD,
    PORT,
    PROPERTIES,
    REMOVE,
    RESULT_TABLE_ID,
    RESULT_TABLE_NAME,
    ROUTING,
    RT_CONF,
    RT_FIELDS,
    SAMPLE,
    SETTINGS,
    STATUS,
    STORAGE_CLUSTER,
    STORAGE_CONFIG,
    STORAGES,
    STORE_SIZE,
    STRING,
    SUCCESS,
    TAG,
    TEXT,
    TOP,
    TRUE,
    TYPE,
    USER,
    VERSION,
)
from datahub.storekit import model_manager, util
from datahub.storekit.exceptions import (
    ClusterNotFoundException,
    EsBadIndexError,
    EsRestRequestError,
    RtStorageNotExistsError,
)
from datahub.storekit.settings import (
    AUTO_CREATE_FIELD,
    DOCS_LIMIT_PER_SHARD,
    ES_MAINTAIN_TIMEOUT,
    EXCLUDE_ES_CLUSTER,
    FORCE_SPLIT_DAYS,
    HAS_COLD_NODES,
    HOT_INDEX_SAVE_DAYS,
    HTTP_REQUEST_TIMEOUT,
    INDEX_SPLIT_THRESHOLD_IN_BYTE,
    INITIAL_SHARD_MAX_SIZE_IN_BYTE,
    INITIAL_SHARD_NUM,
    MAX_SHARD_NUM,
    NODE_HAS_TAG,
    REPLICA_NUM,
    RESERVED_INDEX_NUM,
    RTX_RECEIVER,
    RUN_VERSION,
    SKIP_ES_INDEX_PREFIX,
    SKIP_RT_FIELDS,
    TAG_COLD,
    TAG_HOT,
    TOTAL_SHARDS_PER_NODE,
    VERSION_IEOD_NAME,
)


def initialize(rt_info):
    """
    初始化rt的es存储，包含创建索引、生成alias等操作
    :param rt_info: rt的字段和配置信息
    :return: 初始化操作结果
    """
    return prepare(rt_info)


def info(rt_info):
    """
    获取rt的es存储相关信息，包含索引列表、别名列表等信息
    :param rt_info: rt的字段和配置信息
    :return: rt的es相关信息
    """
    es = rt_info[STORAGES][ES]
    es[INFO] = {INDICES: [], MAPPINGS: {}, SETTINGS: {}, SAMPLE: {}}
    rt_id_lower = rt_info[RESULT_TABLE_ID].lower()
    # 获取索引列表，以及最新的索引的mapping
    es_addr, es_auth = parse_es_connection_info(rt_info[STORAGES][ES][STORAGE_CLUSTER][CONNECTION_INFO])
    # es中rt对应的索引命名规则为 rt_id + _ + yyyyMMdd + 编号，编号从00到99
    indices = _get_es_indices(es_addr, es_auth, rt_id_lower)  # es中索引名称需为小写字母
    valid_rt_indices, _ = _get_valid_rt_indices(indices)
    if rt_id_lower in valid_rt_indices:
        es[INFO][INDICES] = valid_rt_indices[rt_id_lower]
        max_index_name = valid_rt_indices[rt_id_lower][0]
        es[INFO][MAPPINGS] = _get_index_mapping_from_es(es_addr, es_auth, max_index_name)
        es[INFO][SETTINGS] = _get_index_settings_from_es(es_addr, es_auth, max_index_name)
        es[INFO][SAMPLE] = _get_sample_data_from_es(es_addr, es_auth, max_index_name)

    return es


def alter(rt_info):
    """
    修改rt的es存储相关信息，有可能需要创建新的索引，以及别名指向
    :param rt_info: rt的字段和配置信息
    :return: rt的es存储的变更结果
    """
    return prepare(rt_info)


def delete(rt_info):
    """
    删除rt的es存储相关配置，以及对应的索引和数据
    :param rt_info: rt的字段和配置信息
    :return: rt的es存储清理结果
    """
    rt_id_lower = rt_info[RESULT_TABLE_ID].lower()
    es_addr, es_auth = parse_es_connection_info(rt_info[STORAGES][ES][STORAGE_CLUSTER][CONNECTION_INFO])

    # es中rt对应的索引命名规则为 rt_id + _ + yyyyMMdd + 编号，编号从00到99
    indices = _get_es_indices(es_addr, es_auth, rt_id_lower)  # es中索引名称需为小写字母
    valid_rt_indices, _ = _get_valid_rt_indices(indices)
    if rt_id_lower in valid_rt_indices:
        logger.info(f"{es_addr}: going to delete indices {valid_rt_indices[rt_id_lower]}")
        _delete_index(es_addr, es_auth, ",".join(valid_rt_indices[rt_id_lower]))

    return True


def prepare(rt_info, force_create=False, force_shard_num=INITIAL_SHARD_NUM):
    """
    准备rt的es存储，这里可能是初始化，或者schema变化后的创建，或者不需要做任何事情
    :param force_shard_num: 强制分裂时指定分片数
    :param rt_info: rt的字段和配置信息
    :param force_create: 强制创建新索引
    :return: rt的es存储准备的结果
    """
    # 获取es集群的信息
    rt_id_lower = rt_info[RESULT_TABLE_ID].lower()
    conn_info = rt_info[STORAGES][ES][STORAGE_CLUSTER][CONNECTION_INFO]
    es_addr, es_auth = parse_es_connection_info(conn_info)

    # es中rt对应的索引命名规则为 rt_id + _ + yyyyMMdd + 编号，编号从00到99
    indices = _get_es_indices(es_addr, es_auth, rt_id_lower)  # es中索引名称需为小写字母
    valid_rt_indices, _ = _get_valid_rt_indices(indices)

    new_index_name = _get_new_index_name(rt_id_lower)  # 默认新建的索引名称
    shard, init_shard_size, max_shard_num, shard_docs_limit, total_shards_per_node = _get_init_shard_param(
        conn_info
    )  # 默认按照最小的分片数量创建
    need_create_index = False  # 默认无需创建索引

    if rt_id_lower in indices:
        # 不合法的索引名称，此时需要通知管理员手动处理这种场景
        msg = f"{es_addr}: unable to create index for {rt_id_lower} as index name is the same as alias"
        logger.warning(msg)
        util.wechat_msg(RTX_RECEIVER, msg)
        raise EsBadIndexError(message_kv={"msg": rt_id_lower})
    elif rt_id_lower in valid_rt_indices:
        # rt对应的索引已经存在，对比是否发生schema变化，如果有变化，则新创建索引
        max_index_name = valid_rt_indices[rt_id_lower][0]
        json_mapping = _get_index_mapping_from_es(es_addr, es_auth, max_index_name)
        logger.info(f"{es_addr}: {rt_id_lower} mapping in {max_index_name} is {json.dumps(json_mapping)}")
        new_index_name = _get_new_index_name(rt_id_lower, max_index_name)
        current_replica = _get_index_replica(es_addr, es_auth, max_index_name)
        if _is_schema_changed(rt_info, json_mapping) or _is_replica_changed(rt_info, current_replica):
            need_create_index = True
            index_size = _get_index_size(es_addr, es_auth, max_index_name)
            shard = shard if index_size < init_shard_size else max_shard_num
        else:
            logger.info(f"{es_addr}: schema unchanged for {rt_id_lower}, use {max_index_name}")
    else:
        need_create_index = True  # rt对应的索引不存在，需要创建

    if need_create_index or force_create:
        shard = force_shard_num if force_create else shard
        mapping = _construct_mapping(rt_info, shard, TAG_HOT, total_shards_per_node)
        logger.info(f"{es_addr}: {rt_id_lower} create index {new_index_name} with mapping {mapping}")
        return _create_es_index_in_cluster(rt_id_lower, es_addr, es_auth, new_index_name, mapping)

    return True


def check_schema(rt_info):
    """
    对比rt的schema和es中索引的schema，找出不一致的地方。rt字段类型有int/long/double/string，
    es中有text/keyword/integer/long/double/object等
    :param rt_info: rt的配置信息
    :return: schema不一致的地方
    """
    result = {RT_CONF: {}, RT_FIELDS: {}, ES_CONF: {}, ES_FIELDS: {}}
    # 获取es集群的信息
    rt_id_lower = rt_info[RESULT_TABLE_ID].lower()
    es_addr, es_auth = parse_es_connection_info(rt_info[STORAGES][ES][STORAGE_CLUSTER][CONNECTION_INFO])
    result[RT_CONF] = _trans_fields_to_es_conf(rt_info[FIELDS], json.loads(rt_info[STORAGES][ES][STORAGE_CONFIG]))
    for field in rt_info[FIELDS]:
        result[RT_FIELDS][field[FIELD_NAME]] = field[FIELD_TYPE]

    # es中rt对应的索引命名规则为 rt_id + _ + yyyyMMdd + 编号，编号从00到99
    indices = _get_es_indices(es_addr, es_auth, rt_id_lower)  # es中索引名称需为小写字母
    valid_rt_indices, _ = _get_valid_rt_indices(indices)
    if rt_id_lower in valid_rt_indices:
        max_index_name = valid_rt_indices[rt_id_lower][0]
        json_mapping = _get_index_mapping_from_es(es_addr, es_auth, max_index_name)

        version = rt_info[STORAGES][ES][STORAGE_CLUSTER][VERSION]
        index_type = rt_info[RESULT_TABLE_NAME].lower()  # index_type即为rt的result_table_name字段
        properties = json_mapping if _extract_big_version(version) >= 7 else json_mapping[index_type]
        result[ES_CONF] = _trans_mapping_to_es_conf(properties, version)

        field_props = properties[PROPERTIES]
        for field in field_props:
            result[ES_FIELDS][field] = field_props[field][TYPE]

    return result


def maintain(rt_info):
    """
    维护rt的es存储，按照规则新建es的索引，对索引增加别名，切换别名指向等等。
    :param rt_info: rt的字段和配置信息
    :return: 维护rt的es存储的结果
    """
    rt_id_lower = rt_info[RESULT_TABLE_ID].lower()
    es_addr, es_auth = parse_es_connection_info(rt_info[STORAGES][ES][STORAGE_CLUSTER][CONNECTION_INFO])

    # es中rt对应的索引命名规则为 rt_id + _ + yyyyMMdd + 编号，编号从00到99
    indices = _get_es_indices(es_addr, es_auth, rt_id_lower)  # es中索引名称需为小写字母
    valid_rt_indices, _ = _get_valid_rt_indices(indices)
    if rt_id_lower in valid_rt_indices:
        _maintain_rt_indices(rt_info, valid_rt_indices[rt_id_lower], es_addr, es_auth)

    return True


def maintain_all_rts():
    """
    维护系统中所有rt的es存储
    :return: 维护所有rt的es存储的结果
    """
    # 获取所有es集群信息，排除非用户数据的es集群
    es_clusters = model_manager.get_cluster_objs_by_type(ES)
    # 按照es集群并发执行，以便加速维护任务，现网每天第一次执行涉及很多索引创建，需耗时2小时左右。
    check_threads = []
    for es_cluster in es_clusters:
        if es_cluster.cluster_name in EXCLUDE_ES_CLUSTER:
            continue  # 跳过非用户数据的es集群

        # 获取es集群中的索引列表
        es_addr, es_auth = parse_es_connection_info(es_cluster.connection_info)
        check_cluster_thread = threading.Thread(
            target=_maintain_es_cluster, name=es_cluster.cluster_name, args=(es_cluster.cluster_name, es_addr, es_auth)
        )

        # 设置线程为守护线程，主线程结束后，结束子线程
        check_cluster_thread.setDaemon(True)

        check_threads.append(check_cluster_thread)
        check_cluster_thread.start()

    # join所有线程，等待所有集群检查都执行完毕
    # 设置超时时间，防止集群出现问题，一直阻塞，导致后续集群维护任务等待
    for th in check_threads:
        th.join(timeout=ES_MAINTAIN_TIMEOUT)

    return True


def clusters():
    """
    获取es存储集群列表
    :return: es存储集群列表
    """
    result = model_manager.get_storage_cluster_configs_by_type(ES)
    return result


def get_cluster_info(es_addr, es_auth):
    """
    获取es索引的settings设置
    :param es_addr: es集群地址
    :param es_auth: es鉴权信息
    :return: es集群信息
    """
    res = requests.get(f"http://{es_addr}/_cluster/stats", auth=es_auth, timeout=HTTP_REQUEST_TIMEOUT)
    if res.status_code == 200:
        return res.json()
    else:
        logger.warning(f"{es_addr}: get es cluster info failed. {res.status_code} {res.text}")
        raise EsRestRequestError(message_kv={"msg": res.text})


def parse_es_connection_info(connection_info):
    """
    解析es集群的连接串，将es集群地址和鉴权信息返回
    :param connection_info: es集群的连接串配置
    :return: 元组，包含es集群地址和鉴权信息。
    """
    es_conn = json.loads(connection_info)
    es_addr = f"{es_conn[HOST]}:{es_conn[PORT]}"
    if es_conn["enable_auth"]:
        es_conn[PASSWORD] = BaseCrypt.bk_crypt().decrypt(es_conn[PASSWORD])
    es_auth = (es_conn[USER], es_conn[PASSWORD])
    return es_addr, es_auth


def _get_init_shard_param(connection_info):
    """
    解析es集群的连接串，将es集群的初始shard数返回
    :param connection_info: es集群的连接串配置
    :return: 初始shard数。
    """
    es_conn = json.loads(connection_info)
    init_shard_num = es_conn.get("init_shard_num", INITIAL_SHARD_NUM)
    init_shard_size = es_conn.get("init_shard_size", INITIAL_SHARD_MAX_SIZE_IN_BYTE)
    max_shard_num = es_conn.get("max_shard_num", MAX_SHARD_NUM)
    shard_docs_limit = (
        es_conn["shard_docs_limit"]
        if ("shard_docs_limit" in es_conn and es_conn["shard_docs_limit"] < DOCS_LIMIT_PER_SHARD)
        else DOCS_LIMIT_PER_SHARD
    )
    total_shards_per_node = es_conn.get("total_shards_per_node", TOTAL_SHARDS_PER_NODE)
    return init_shard_num, init_shard_size, max_shard_num, shard_docs_limit, total_shards_per_node


def _get_hot_save_days(connection_info):
    """
    解析es集群的连接串，将es集群的热索引保留天数返回
    :param connection_info: es集群的连接串配置
    :return: 热索引保留天数。
    """
    es_conn = json.loads(connection_info)
    hot_save_days = es_conn["hot_save_days"] if "hot_save_days" in es_conn else HOT_INDEX_SAVE_DAYS
    return hot_save_days


def _get_has_cold_nodes(connection_info):
    """
    解析es集群的连接串，获取集群是否有冷节点
    :param connection_info: es集群的连接串配置
    :return: 集群是否有冷节点。
    """
    es_conn = json.loads(connection_info)
    has_cold_nodes = es_conn.get("has_cold_nodes", HAS_COLD_NODES)
    return has_cold_nodes


def _get_split_index_condition(connection_info):
    """
    解析es集群的连接串，将es集群的index分裂条件返回
    :param connection_info: es集群的连接串配置
    :return: 索引的分裂条件。
    """
    es_conn = json.loads(connection_info)
    index_split_threshold_in_byte = (
        es_conn["index_split_threshold_in_byte"]
        if "index_split_threshold_in_byte" in es_conn
        else INDEX_SPLIT_THRESHOLD_IN_BYTE
    )
    force_split_days = es_conn["force_split_days"] if "force_split_days" in es_conn else FORCE_SPLIT_DAYS
    return index_split_threshold_in_byte, force_split_days


def _maintain_es_cluster(es_cluster_name, es_addr, es_auth):
    """
    维护指定的es集群中的索引列表
    :param es_cluster_name: es集群名称
    :param es_addr: es集群地址
    :param es_auth: es鉴权信息
    """
    # 获取es集群中的索引列表
    indices = _get_es_indices(es_addr, es_auth)
    valid_rt_indices, bad_indices = _get_valid_rt_indices(indices)
    if bad_indices:
        logger.info(f"{es_addr}: bad indices {json.dumps(bad_indices)}")

    maintain_failed = []
    # 逐个rt进行维护，需注意rt是否还包含es存储，且存储的集群没有发生切换
    logger.info(f"{es_addr}: es maintain started for {es_cluster_name}")
    for rt_id_lower, sort_index_list in list(valid_rt_indices.items()):
        try:
            rt_info = util.get_rt_info(rt_id_lower)
            if rt_info and ES in rt_info[STORAGES]:
                rt_es_cluster_name = rt_info[STORAGES][ES][STORAGE_CLUSTER][CLUSTER_NAME]
                if rt_es_cluster_name != es_cluster_name:
                    logger.warning(
                        f"{es_addr}: rt es cluster changed to {rt_es_cluster_name}, unable to maintain "
                        f"{json.dumps(sort_index_list)}"
                    )
                else:
                    _maintain_rt_indices(rt_info, sort_index_list, es_addr, es_auth)
            else:
                # 如果rt删除了es节点，那么这段废弃数据将永远不会被删除
                raise RtStorageNotExistsError(message_kv={RESULT_TABLE_ID: rt_id_lower, TYPE: ES})
        except Exception:
            logger.warning(f"{es_addr}: failed to maintain indices {json.dumps(sort_index_list)}.", exc_info=True)
            maintain_failed.append(sort_index_list)
    logger.info(
        f"{es_addr}: es maintain finished for {len(list(valid_rt_indices.keys()))} rts, failed are "
        f"{json.dumps(maintain_failed)}"
    )


def _maintain_rt_indices(rt_info, sort_index_list, es_addr, es_auth):
    """
    维护rt对应的索引列表
    :param rt_info: rt的配置信息
    :param sort_index_list: rt的es索引列表，倒序排列
    :param es_addr: es集群地址
    :param es_auth: es鉴权信息
    :return: 维护结果
    """
    rt_id_lower = rt_info[RESULT_TABLE_ID].lower()
    logger.info(f"{es_addr}: going to maintain indices {json.dumps(sort_index_list)}")
    # 保留至少1个索引，删除超出过期时间所有索引，维护索引别名
    indices_to_delete = _expired_index_list(rt_id_lower, rt_info[STORAGES][ES][EXPIRES], sort_index_list)
    if indices_to_delete:
        logger.info(f"{es_addr}: going to delete indices {json.dumps(indices_to_delete)}")
        _delete_index(es_addr, es_auth, ",".join(indices_to_delete))

    # 判断是否需要分裂（500G，或者7天，或者docs超出限制）
    max_index_name = sort_index_list[0]
    index_size = _get_index_size(es_addr, es_auth, max_index_name)
    # 无论是否需要分裂索引，都需要在当前最大索引上加上当天的别名指向，因为部分当天的日志已写入此索引
    _alias_update(es_addr, es_auth, rt_id_lower, max_index_name)
    conn_info = rt_info[STORAGES][ES][STORAGE_CLUSTER][CONNECTION_INFO]
    shard, init_shard_size, max_shard_num, shard_docs_limit, total_shards_per_node = _get_init_shard_param(conn_info)

    # 获取index 主分片数和docs
    pri_shard_num, docs = _get_es_index_pri_docs(es_addr, es_auth, max_index_name)
    if _index_need_splitting(max_index_name, index_size, conn_info, pri_shard_num, docs, shard_docs_limit):
        new_index_name = _get_new_index_name(rt_id_lower, max_index_name)
        num_shards = shard if index_size < init_shard_size else max_shard_num
        mapping = _construct_mapping(rt_info, num_shards, TAG_HOT, total_shards_per_node)
        logger.info(f"{es_addr}: {rt_id_lower} create index {new_index_name} with mapping {mapping}")
        # 创建索引，同时挂载别名
        _create_es_index_in_cluster(rt_id_lower, es_addr, es_auth, new_index_name, mapping)

    # 海外版不存在冷节点，内部版有冷热节点，通过变量控制不同版本
    if _get_has_cold_nodes(conn_info):
        # 将过期的索引放入冷节点
        cold_sort_index_list = sort_index_list[1:]
        hot_index_date = util.get_date_by_diff(1 - _get_hot_save_days(conn_info))  # yyyyMMdd
        for one_index in cold_sort_index_list:
            # 保证当天的索引保持原样，跳过将其转到冷节点的逻辑 yyyyMMdd01
            if one_index in indices_to_delete or int(hot_index_date) <= int(one_index.split("_")[-1][0:8]):
                continue
            else:
                allocation_tag = _get_index_allocation_tag(es_addr, es_auth, one_index)
                # 把索引tag属性不是cold的索引修改为cold
                if allocation_tag != TAG_COLD:
                    logger.info(f"{es_addr}: going to move index {one_index} to cold tag")
                    # 设置冷节点单节点分片数
                    settings = {
                        "index.routing.allocation.include.tag": TAG_COLD,
                        "index.routing.allocation.total_shards_per_node": (REPLICA_NUM + 1) * max_shard_num,
                    }
                    _put_index_settings(es_addr, es_auth, one_index, settings)


def _create_es_index_in_cluster(rt, es_addr, es_auth, index_name, index_mapping_str):
    """
    在es集群中创建索引
    :param rt: rt名称
    :param es_addr: es集群地址
    :param es_auth: es集群鉴权
    :param index_name: 索引名称
    :param index_mapping_str: 索引的mapping
    :return: 是否创建成功
    """
    res = requests.put(
        url=f"http://{es_addr}/{index_name}?master_timeout=240s",
        json=json.loads(index_mapping_str),
        headers=JSON_HEADERS,
        auth=es_auth,
        timeout=600,
    )
    if res.status_code == 200:
        alias = rt.lower()
        # TODO 需要校验索引已经存在了，能被rest接口查询到
        if _alias_update(es_addr, es_auth, alias, index_name):
            # alias更新是异步操作，这里需要验证alias真的已经指向到新的index上，最多等待90s
            reties = 15
            while not _is_alias_point_to_index(es_addr, es_auth, alias, index_name) and reties > 0:
                time.sleep(6)
                reties -= 1
            if reties == 0:
                _delete_index(es_addr, es_auth, index_name)
                logger.warning(f"{es_addr}: update alias timeout for {rt}, delete the index {index_name}")
            else:
                logger.info(f"{es_addr}: create index {index_name} and update alias success for {rt}")
                return True
        else:
            _delete_index(es_addr, es_auth, index_name)
            logger.warning(f"{es_addr}: update alias failed for {rt}, delete the index {index_name}")
    else:
        # 创建es mapping失败，需要告警出来
        msg = f"{es_addr}: failed to create index {index_name} for {rt}. {res.status_code} {res.text}"
        logger.warning(msg)
        util.wechat_msg(RTX_RECEIVER, msg)

    return False


def _alias_update(es_addr, es_auth, alias, max_index_name):
    """
    获取别名（rt）指向的index名称和当日alias指向的index名称，如果和传入的索引相同，则无需修改别名
    :param es_addr: es集群地址
    :param es_auth: es鉴权信息
    :param alias: es索引的默认别名
    :param max_index_name: 当前最大的索引名称
    :return: 更新别名的结果，True/False
    """
    today = alias + "_" + util.get_date_by_diff(0)
    tomorrow = alias + "_" + util.get_date_by_diff(1)
    near_tomorrow = util.is_near_tomorrow()
    # 如果时间接近明天，则增加明天的日期作为别名
    if near_tomorrow:
        alias_ret = requests.get(
            url=f"http://{es_addr}/_alias/{alias},{today},{tomorrow}",
            auth=es_auth,
            timeout=HTTP_REQUEST_TIMEOUT,
        )
    else:
        alias_ret = requests.get(
            url=f"http://{es_addr}/_alias/{alias},{today}", auth=es_auth, timeout=HTTP_REQUEST_TIMEOUT
        )

    action = {ACTIONS: []}
    # 判断当前索引的别名，增加缺失的别名
    if alias_ret.status_code == 200 and max_index_name in alias_ret.json():
        alias_list = list(alias_ret.json()[max_index_name]["aliases"].keys())
        if alias not in alias_list:
            action[ACTIONS].append({REMOVE: {INDEX: f"{alias}_20*", ALIAS: alias}})
            action[ACTIONS].append({ADD: {INDEX: max_index_name, ALIAS: alias}})
        if today not in alias_list:
            action[ACTIONS].append({ADD: {INDEX: max_index_name, ALIAS: today}})
        if near_tomorrow and tomorrow not in alias_list:
            action[ACTIONS].append({ADD: {INDEX: max_index_name, ALIAS: tomorrow}})
    else:
        action[ACTIONS].append({REMOVE: {INDEX: f"{alias}_20*", ALIAS: alias}})
        action[ACTIONS].append({ADD: {INDEX: max_index_name, ALIAS: alias}})
        action[ACTIONS].append({ADD: {INDEX: max_index_name, ALIAS: today}})
        if near_tomorrow:
            action[ACTIONS].append({ADD: {INDEX: max_index_name, ALIAS: tomorrow}})

    if action[ACTIONS]:
        action = json.dumps(action)
        logger.info(f"{es_addr}: change alias for {max_index_name} {action}")
        # 修改别名的指向，原子操作
        res = requests.post(
            url=f"http://{es_addr}/_aliases?master_timeout=240s",
            data=action,
            headers=JSON_HEADERS,
            auth=es_auth,
            timeout=600,
        )
        if res.status_code != 200:
            logger.warning(f"{es_addr}: change alias failed {action}. {res.status_code} {res.text}")
            return False

    return True


def _is_alias_point_to_index(es_addr, es_auth, alias, index_name):
    """
    验证es中的别名是否指向指定的索引，返回验证结果
    :param es_addr: es集群地址
    :param es_auth: es权限校验信息
    :param alias: 索引的别名
    :param index_name: 索引名称
    :return: True/False
    """
    res = requests.get(url=f"http://{es_addr}/_alias/{alias}", auth=es_auth, timeout=HTTP_REQUEST_TIMEOUT)
    # 首先验证返回的结果中只包含一个key，然后验证key的值和索引名称相同，此时能确定alias指向了index，且唯一
    if res.status_code == 200:
        result = res.json()
        if len(result) == 1 and index_name in result:
            return True

    logger.warning(f"{es_addr}: alias {alias} is not point to index {index_name}. {res.status_code}, {res.text}")
    return False


def _delete_index(es_addr, es_auth, indices):
    """
    删除es集群中的指定索引
    :param es_addr: es集群地址
    :param es_auth: es权限校验信息
    :param indices: 索引名称，多个索引用逗号串起来
    :return: 删除成功与否，True/False
    """
    res = requests.delete(f"http://{es_addr}/{indices}", auth=es_auth, timeout=600)
    if res.status_code == 200:
        return True
    else:
        logger.warning(f"{es_addr}: failed to delete indices {indices}. {res.status_code} {res.text}")
        return False


def _get_valid_rt_indices(indices):
    """
    在输入的索引列表中找到合法的rt和rt对应的索引列表（倒序，最新时间的索引名称在前）。
    :param indices: 索引名称列表
    :return: 元组，第一个是rt和对应的索引列表的字典，第二个是不合法的索引列表
    """
    rt_sort_index_list = {}
    bad_indices = []

    for index_name in indices:
        # 符合要求的索引 611_etl_docker_2018070700 ，包含rtid + _ + yyyyMMdd + xx （xx编号可有可无，默认00）
        if re.search(r"^\d+_\w+_\d{8,}$", index_name) is None:
            # 不符合要求的es索引名称，不是es入库所使用的索引
            skip = False
            for prefix in SKIP_ES_INDEX_PREFIX:
                if index_name.startswith(prefix):
                    skip = True
                    break
            if not skip:
                bad_indices.append(index_name)
        else:
            rt = "_".join(index_name.split("_")[0:-1])
            if rt not in rt_sort_index_list:
                rt_sort_index_list[rt] = [index_name]
            else:
                rt_sort_index_list[rt].append(index_name)

    for index_name_list in list(rt_sort_index_list.values()):
        index_name_list.sort(reverse=True)

    return rt_sort_index_list, bad_indices


def _get_es_indices(es_addr, es_auth, index_prefix=""):
    """
    获取es集群中符合匹配规则的所有正常的索引列表，不包含状态为closed的索引。
    :param es_addr: es集群地址
    :param es_auth: es集群的鉴权信息
    :param index_prefix: 检索的es索引的前缀，默认为空字符串
    :return: es集群中正常的索引列表
    """
    res = requests.get(
        f"http://{es_addr}/_cat/indices?h=index,status&format=json&index={index_prefix}*",
        auth=es_auth,
        timeout=HTTP_REQUEST_TIMEOUT,
    )
    indices = []
    not_open_indices = []
    if res.status_code == 200:
        for item in res.json():
            if item[STATUS] == "open":
                indices.append(item[INDEX])
            else:
                not_open_indices.append(item[INDEX])
    else:
        logger.warning(f"{es_addr}: get indices list failed. {res.status_code} {res.text}")

    if not_open_indices:
        logger.info(f"{es_addr}: not open indices are {json.dumps(not_open_indices)}")

    return indices


def _get_es_index_pri_docs(es_addr, es_auth, index_name):
    """
    获取es集群中索引的docs。
    :param es_addr: es集群地址
    :param es_auth: es集群的鉴权信息
    :param index_name: 索引名称
    :return: es集群中索引的docs。
    """
    res = requests.get(
        f"http://{es_addr}/_cat/indices/{index_name}?v&s=index&format=json",
        auth=es_auth,
        timeout=HTTP_REQUEST_TIMEOUT,
    )
    docs = 0
    pri_shard_num = 0
    if res.status_code == 200 and res.json():
        docs = int(res.json()[0]["docs.count"])
        pri_shard_num = int(res.json()[0]["pri"])
    else:
        logger.warning(f"{es_addr}: get index docs failed. {res.status_code} {res.text}")

    return pri_shard_num, docs


def _trans_fields_to_es_conf(fields, es_storage_conf):
    """
    将rt的字段转换为es中的字段和类型
    :param fields: rt中的字段列表
    :param es_storage_conf: rt的es相关存储配置
    :return: es中的mapping相关配置
    """
    # 页面上配置支持分词字段、聚合字段、json字段三种配置。时间字段为默认的，用户不可配置。
    result_conf = {
        ANALYZED_FIELDS: [],
        DATE_FIELDS: [DTEVENTTIMESTAMP],  # 时间字段用户不可配置
        DOC_VALUES_FIELDS: [DTEVENTTIMESTAMP],  # 时间戳固定作为聚合字段
        JSON_FIELDS: [],
    }
    # 默认删除rt中的timestamp/offset字段，增加_iteration_idx字段，将字段映射为es中的字段配置
    for field in fields:
        field_name = field[FIELD_NAME]
        if field_name not in SKIP_RT_FIELDS:
            # TODO analyzed_fields（分词） 和 doc_values_fields（聚合） 应该互斥，keyword支持聚合，text不支持
            if ANALYZED_FIELDS in es_storage_conf and field_name in es_storage_conf[ANALYZED_FIELDS]:
                result_conf[ANALYZED_FIELDS].append(field_name)
            if JSON_FIELDS in es_storage_conf and field_name in es_storage_conf[JSON_FIELDS]:
                result_conf[JSON_FIELDS].append(field_name)
            if (
                field_name != DTEVENTTIMESTAMP
                and DOC_VALUES_FIELDS in es_storage_conf
                and field_name in es_storage_conf[DOC_VALUES_FIELDS]
            ):
                result_conf[DOC_VALUES_FIELDS].append(field_name)

    # TODO 兼容旧逻辑中将几个字段默认作为聚合字段的逻辑，后续需全部迁移到es的存储配置中
    for field_name in AUTO_CREATE_FIELD:
        if field_name not in result_conf[DOC_VALUES_FIELDS]:
            result_conf[DOC_VALUES_FIELDS].append(field_name)

    return result_conf


def _trans_mapping_to_es_conf(es_mapping, es_version):
    """
    将es索引的mapping转换为es存储的配置，以便于和rt的es存储配置对比。
    :param es_mapping: es索引的mapping，json对象
    :return: 索引的mapping转换的es存储的配置对象
    """
    result_conf = {ANALYZED_FIELDS: [], DATE_FIELDS: [], DOC_VALUES_FIELDS: [], JSON_FIELDS: []}
    for field_name, value in list(es_mapping[PROPERTIES].items()):
        if field_name == "_copy" and _extract_big_version(es_version) >= 6:
            # 跳过6.x版本中默认添加的_copy字段，此字段功能类似以前版本的_all字段
            continue
        if PROPERTIES in value or value[TYPE] == OBJECT:
            # json格式的字段无法分词，也无法聚合
            result_conf[JSON_FIELDS].append(field_name)
            continue
        if value[TYPE] == TEXT:
            # text字段即为分词的字段，无法用作聚合
            result_conf[ANALYZED_FIELDS].append(field_name)
        else:
            if DOC_VALUES not in value:
                # doc_values默认值为true，只有显示设置为false的时候，才会在mapping中体现
                result_conf[DOC_VALUES_FIELDS].append(field_name)
            if value[TYPE] == DATE:
                result_conf[DATE_FIELDS].append(field_name)

    # TODO 兼容旧逻辑中将几个字段默认作为聚合字段的逻辑，后续需全部迁移到es的存储配置中
    for field_name in AUTO_CREATE_FIELD:
        if field_name not in result_conf[DOC_VALUES_FIELDS]:
            result_conf[DOC_VALUES_FIELDS].append(field_name)

    return result_conf


def _is_schema_changed(rt_info, json_mapping):
    """
    根据rt的es存储配置计算es的mapping内容，和实际es集群中此rt对应的索引的mapping进行对比，返回对比结果
    :param rt_info: rt的配置
    :param json_mapping: rt对应es中索引的mapping
    :return: 是否rt对应的mapping发生了变化，True/False
    """
    config_from_api = json.loads(rt_info[STORAGES][ES][STORAGE_CONFIG])
    rt_es_config = _trans_fields_to_es_conf(rt_info[FIELDS], config_from_api)

    # from ES
    version = rt_info[STORAGES][ES][STORAGE_CLUSTER][VERSION]
    index_type = rt_info[RESULT_TABLE_NAME].lower()  # index_type即为rt的result_table_name字段
    properties = json_mapping if _extract_big_version(version) >= 7 else json_mapping[index_type]
    es_config = _trans_mapping_to_es_conf(properties, version)

    result = not _is_subset(rt_es_config, es_config)
    logger.info(
        f"{rt_info[RESULT_TABLE_ID]} es storage config changed is {result}. from rt conf/from es "
        f"index: {json.dumps(rt_es_config)}, {json.dumps(es_config)}"
    )
    return result


def _is_replica_changed(rt_info, current_replica):
    """
    根据rt的es存储配置中副本设置和实际索引中副本设置进行对比，返回是否副本设置相同
    :param rt_info: rt的配置
    :param current_replica: 当前索引的副本数量
    :return: 是否rt对应的副本设置发生了变化，True/False
    """
    config_from_api = json.loads(rt_info[STORAGES][ES][STORAGE_CONFIG])
    num_replica = _get_replica_num(rt_info[STORAGES][ES][STORAGE_CLUSTER][CONNECTION_INFO], config_from_api)
    return num_replica != current_replica


def _get_replica_num(conn_info, es_conf):
    """
    根据rt的es存储配置，以及配置文件中的配置，返回es存储的副本数
    :param conn_info: es集群配置
    :param es_conf: es配置项
    :return: es存储的副本数
    """
    conn = json.loads(conn_info)
    num_replica = 0
    if (
        ENABLE_REPLICA in conn
        and type(conn[ENABLE_REPLICA]) == bool
        and conn[ENABLE_REPLICA]
        and HAS_REPLICA in es_conf
        and type(es_conf[HAS_REPLICA]) == bool
        and es_conf[HAS_REPLICA]
    ):
        # 当集群配置了启用副本，且rt的存储配置上指定了副本时，设定索引的副本数
        num_replica = REPLICA_NUM

    return num_replica


def _construct_mapping(rt_info, num_shard, index_tag, total_shards_per_node=TOTAL_SHARDS_PER_NODE):
    """
    构造rt对应的es索引的mapping
    :param rt_info: rt的配置
    :param num_shard: es索引的分片数
    :param index_tag: es索引的tag
    :param total_shards_per_node: es索引单节点最大分片数
    :return: rt对应的es索引的mapping字符串
    """
    config_from_api = json.loads(rt_info[STORAGES][ES][STORAGE_CONFIG])
    num_replica = _get_replica_num(rt_info[STORAGES][ES][STORAGE_CLUSTER][CONNECTION_INFO], config_from_api)
    rt_es_config = _trans_fields_to_es_conf(rt_info[FIELDS], config_from_api)
    version = rt_info[STORAGES][ES][STORAGE_CLUSTER][VERSION]

    # ES 6.x使用的字段
    copy_to_field_name = "_copy"

    mapping_field_dict = {}
    rt_field_dict = {}
    for field_name, field_type in list(_trans_rt_fields(rt_info[FIELDS]).items()):
        rt_field_dict[field_name] = field_type
        mapping_dict_value = {}
        if _extract_big_version(version) < 6:
            mapping_dict_value[INCLUDE_IN_ALL] = FALSE
        # 分词字段、json字段、聚合字段存在互斥关系
        if field_name in rt_es_config[ANALYZED_FIELDS]:
            mapping_dict_value[TYPE] = TEXT
            mapping_dict_value[DOC_VALUES] = FALSE
            if _extract_big_version(version) >= 6:
                mapping_dict_value["copy_to"] = copy_to_field_name
            else:
                mapping_dict_value[INCLUDE_IN_ALL] = TRUE
        elif field_name in rt_es_config[JSON_FIELDS]:
            mapping_dict_value[TYPE] = OBJECT
        elif field_name in rt_es_config[DOC_VALUES_FIELDS]:
            mapping_dict_value[TYPE] = _convert_to_es_type(field_type)
        else:
            # 普通字段，设置为非聚合
            mapping_dict_value[TYPE] = _convert_to_es_type(field_type)
            mapping_dict_value[DOC_VALUES] = FALSE

        # 处理时间字段
        if field_name in rt_es_config[DATE_FIELDS]:
            mapping_dict_value[TYPE] = DATE
            mapping_dict_value["format"] = (
                "yyyy-MM-dd HH:mm:ss"
                if field_name == DTEVENTTIME
                else "epoch_millis"
                if field_name == DTEVENTTIMESTAMP
                else "strict_date_optional_time||yyyy-MM-dd HH:mm:ss||epoch_millis"
            )
        # 添加到mapping中
        mapping_field_dict[field_name] = mapping_dict_value

    logger.info(
        f"{rt_info[RESULT_TABLE_ID]}: rt fields {json.dumps(rt_field_dict)}, "
        f"mapping fields {json.dumps(mapping_field_dict)}"
    )
    index_type = rt_info[RESULT_TABLE_NAME].lower()  # index_type即为rt的result_table_name字段

    # 单节点最大分片数据，当存在副本且数据量很小时，可能存在分片比较集中的情况，但是默认只有3个分片而已。
    # 对于大索引，必须要求最大分片数超过或者等于热节点数（否则，当存在副本情况下，可能无法分配分片），且单个节点索引最大分片数为默认分片数的副本数倍数
    # total_shards_per_node 默认为2，避免节点故障无法分配分片
    index_mapping = {SETTINGS: {INDEX: {"number_of_shards": f"{num_shard}", NUMBER_OF_REPLICAS: f"{num_replica}"}}}

    if NODE_HAS_TAG:
        index_mapping[SETTINGS][INDEX][ROUTING] = {ALLOCATION: {INCLUDE: {TAG: f"{index_tag}"}}}

    # 只在内部版开启
    if RUN_VERSION == VERSION_IEOD_NAME:
        index_mapping[SETTINGS][INDEX][ROUTING][ALLOCATION]["total_shards_per_node"] = (
            total_shards_per_node + num_replica
        )

    dynamic_templates = [
        {"strings_as_keywords": {"match_mapping_type": STRING, "mapping": {"norms": FALSE, TYPE: KEYWORD}}}
    ]
    if _extract_big_version(version) >= 7:
        index_mapping[MAPPINGS] = {"dynamic_templates": dynamic_templates}
    else:
        index_mapping[MAPPINGS] = {f"{index_type}": {"dynamic_templates": dynamic_templates}}

    # 对于6.x版本的es，其mapping和旧版本（多数为5.x）不一样
    if _extract_big_version(version) >= 6:
        mapping_field_dict[copy_to_field_name] = {TYPE: TEXT}
    else:
        index_mapping[MAPPINGS][index_type]["_all"] = {"enabled": TRUE}

    if _extract_big_version(version) >= 7:
        index_mapping[MAPPINGS][PROPERTIES] = mapping_field_dict
    else:
        index_mapping[MAPPINGS][index_type][PROPERTIES] = mapping_field_dict

    return json.dumps(index_mapping)


def _get_index_mapping_from_es(es_addr, es_auth, index):
    """
    获取es中索引的mapping信息
    :param es_addr: es集群地址
    :param es_auth: es鉴权信息
    :param index: 索引名称
    :return: es索引的mapping信息
    """
    res = requests.get(f"http://{es_addr}/{index}/_mappings", auth=es_auth, timeout=HTTP_REQUEST_TIMEOUT)
    if res.status_code == 200:
        return res.json()[index][MAPPINGS]
    else:
        logger.warning(f"{es_addr}: get index {index} mappings failed. {res.status_code} {res.text}")
        raise EsRestRequestError(message_kv={"msg": res.text})


def _get_index_settings_from_es(es_addr, es_auth, index):
    """
    获取es索引的settings设置
    :param es_addr: es集群地址
    :param es_auth: es鉴权信息
    :param index: 索引名称
    :return: es索引的settings设置
    """
    res = requests.get(f"http://{es_addr}/{index}/_settings", auth=es_auth, timeout=HTTP_REQUEST_TIMEOUT)
    if res.status_code == 200:
        return res.json()
    else:
        logger.warning(f"{es_addr}: get index {index} settings failed. {res.status_code} {res.text}")
        raise EsRestRequestError(message_kv={"msg": res.text})


def _get_sample_data_from_es(es_addr, es_auth, index):
    """
    从指定索引中查找最新的十条数据并返回
    :param es_addr: es集群地址
    :param es_auth: es鉴权信息
    :param index: 索引名称
    :return: es索引中的最新十条数据
    """
    res = requests.post(
        f"http://{es_addr}/{index}/_search/",
        auth=es_auth,
        headers=JSON_HEADERS,
        data=json.dumps({"sort": [{DTEVENTTIMESTAMP: {ORDER: "desc"}}], "from": 0, "size": 10}),
    )
    if res.status_code == 200:
        return res.json()
    else:
        logger.warning(f"{es_addr}: query index {index} failed. {res.status_code} {res.text}")
        return {}


def _is_subset(small_conf_dict, big_conf_dict):
    """
    判断一个配置集是否为另一个配置集的子集，如果是，返回True，否则返回False
    :param small_conf_dict: 较小的配置集对象
    :param big_conf_dict: 较大的配置集对象
    :return: True/False
    """
    for key, value_list in list(small_conf_dict.items()):
        if key not in list(big_conf_dict.keys()):
            return False
        else:
            for value in value_list:
                if value not in big_conf_dict[key]:
                    return False
    return True


def _get_new_index_name(rt, max_index_name=None):
    """
    构造es中rt对应的最新索引名称
    :param rt: result table id
    :param max_index_name: es中此rt对应的最大的索引名称
    :return: rt最新的索引名称
    """
    today = util.get_date_by_diff(0)  # in case of 20180132 -> 20180201
    index_name = f"{rt}_{today}00"  # 默认索引名称为rt + _ + 当前日期 + 00
    if max_index_name:
        index_date_num = max_index_name.split("_")[-1]
        if today in index_date_num:  # 当前最大的索引名称为当天创建的，则在最后两位上加一
            index_name = f"{rt}_{int(index_date_num) + 1}"

    return index_name.lower()  # es 中索引只能是小写字符


def _trans_rt_fields(fields):
    """
    将rt的字段列表转换为在es中的字段列表
    :param fields: rt的字段列表
    :return: es中的字段列表，包含字段名称和类型
    """
    result = {DTEVENTTIMESTAMP: DATE}
    for field in fields:
        if field[FIELD_NAME] not in SKIP_RT_FIELDS:
            result[field[FIELD_NAME]] = field[FIELD_TYPE]
    return result


def _convert_to_es_type(field_type):
    """
    将rt的字段类型映射为es中的数据类型
    :param field_type: rt的字段类型
    :return: es中的数据类型
    """
    if INT == field_type:
        return INTEGER
    elif field_type in [LONG, FLOAT, DOUBLE]:
        return field_type
    else:
        return KEYWORD


def _expired_index_list(result_table_id, expires, index_name_list):
    """
    从index列表中获取待删除的index，这里要列表类似[rt_2019061400, rt_2019060600, rt_2019052900]，其中0529存储的
    是0529~0606的数据，清理时需要0606达到过期时间，并删除0529，不能看到0529已到清理时间就直接清除掉。
    :param result_table_id: rt的id
    :param expires: rt的过期时间配置
    :param index_name_list: rt的索引列表，倒序排列。
    :return: 需要删除的索引的列表
    """
    expired_index_name_list = []
    length = len(index_name_list)
    days = util.translate_expires_day(expires)
    if length <= RESERVED_INDEX_NUM or days <= 0:
        return expired_index_name_list

    expired_date = int(util.get_date_by_diff(-days))
    suffix_idx = len(result_table_id) + 1
    for i in range(length):
        # 截取索引名中尾部的时间那一段（591_etl_abc_2018090202 -> 2018090902，这里有可能最后一段是0）
        date_suffix = index_name_list[i][suffix_idx:]
        if len(date_suffix) < 8:
            # 不合法的索引名称
            expired_index_name_list.append(index_name_list[i])
        elif int(date_suffix[0:8]) < expired_date:
            # idx代表第一个创建日期小于expired_date的index的下标位置加1，即开始删除的位置
            idx = max(i + 1, RESERVED_INDEX_NUM)
            expired_index_name_list.extend(index_name_list[idx:])
            break

    logger.debug(f"{result_table_id}: indices expired are {json.dumps(expired_index_name_list)}")
    return expired_index_name_list


def _index_need_splitting(index, index_size, connection_info, pri_shard_num, docs, shard_docs_limit):
    """
    获取是否需要强制分裂当前的索引
    现在的分裂条件判断过程：
        1）index 为空不分裂
        2）docs数超出限制，分裂
        3）字节总量index size超过限制，分裂
        4）index不为空，且超出分裂日期，分裂
        5) 其他情况不分裂
    :param index: 索引名称
    :param index_size: 索引的字节数
    :param connection_info: 连接信息
    :param pri_shard_num: 主分片数据
    :param docs: index docs
    :param shard_docs_limit: 单分片docs限制
    :return: 是否需要分裂索引
    """
    if docs == 0:
        return False

    index_split_threshold_in_byte, force_split_days = _get_split_index_condition(connection_info)
    index_date = int(index.split("_")[-1][0:8])
    force_split_date = int(util.get_date_by_diff(-force_split_days))
    if (
        docs >= pri_shard_num * shard_docs_limit
        or index_size >= index_split_threshold_in_byte
        or force_split_date >= index_date
    ):
        return True

    return False


def _get_index_size(es_addr, es_auth, index):
    """
    获取当前索引的字节数
    :param es_addr: es集群地址
    :param es_auth: es鉴权信息
    :param index: 索引名称
    :return: 索引包含的字节数
    """
    res = requests.get(f"http://{es_addr}/{index}/_stats/store", auth=es_auth, timeout=HTTP_REQUEST_TIMEOUT)
    if res.status_code == 200:
        try:
            return res.json()[INDICES][index]["primaries"]["store"]["size_in_bytes"]
        except Exception:
            logger.info(f"{es_addr}: failed to get index {index} size. ", exc_info=True)
    else:
        logger.warning(f"{es_addr}: failed to get {index} stats. {res.status_code} {res.text}")

    return 0


def _get_index_allocation_tag(es_addr, es_auth, index):
    """
    获取es索引中allocation tag配置项的值
    :param es_addr: es集群地址
    :param es_auth: es鉴权信息
    :param index: 索引名称
    :return: allocatoin tag的值
    """
    tag = TAG_HOT  # 假定获取失败时，使用热节点的tag
    es_settings = _get_index_settings_from_es(es_addr, es_auth, index)
    try:
        tag = es_settings[index][SETTINGS][INDEX][ROUTING][ALLOCATION][INCLUDE][TAG]
    except Exception:
        logger.error(
            f"{es_addr}: failed to get {index} allocation tag from settings {json.dumps(es_settings)}.",
            exc_info=True,
        )

    return tag


def _get_index_replica(es_addr, es_auth, index):
    """
    获取es索引中number_of_replicas配置项的值
    :param es_addr: es集群地址
    :param es_auth: es鉴权信息
    :param index: 索引名称
    :return: number_of_replicas的值
    """
    replica = REPLICA_NUM  # 假定获取失败时，使用默认副本设置
    es_settings = _get_index_settings_from_es(es_addr, es_auth, index)
    try:
        replica = int(es_settings[index][SETTINGS][INDEX][NUMBER_OF_REPLICAS])
    except Exception:
        logger.error(
            f"{es_addr}: failed to get {index} number_of_replicas from settings {json.dumps(es_settings)}.",
            exc_info=True,
        )

    return replica


def _put_index_settings(es_addr, es_auth, index, put_dict):
    """
    更新es索引的settings中配置项
    :param es_addr: es集群地址
    :param es_auth: es鉴权信息
    :param index: 索引名称
    :param put_dict: 更新的配置项字典
    """
    url = f"http://{es_addr}/{index}/_settings?master_timeout=240s"
    res = requests.put(url, data=json.dumps(put_dict), headers=JSON_HEADERS, auth=es_auth, timeout=600)
    if res.status_code != 200:
        logger.warning(f"{es_addr}: failed to update index {index} settings {put_dict}. {res.status_code} {res.text}")


def _extract_big_version(version):
    """
    从给定的version中抽取大版本号,如：7.4.2 -> 7
    :param version: 完整的版本号
    :return:  数字类型的大版本号
    """
    return int(version.split(".")[0])


def route_es_request(uri, cluster_name):
    """
    :param uri: 请求相对路径
    :param cluster_name: 集群名称
    """
    cluster = model_manager.get_cluster_obj_by_name_type(cluster_name, ES)
    if not cluster:
        raise ClusterNotFoundException(message_kv={CLUSTER_TYPE: ES, CLUSTER_NAME: cluster_name})

    es_addr, es_auth = parse_es_connection_info(cluster.connection_info)

    url = f"http://{es_addr}/{uri}"
    res = requests.get(url=url, auth=es_auth, timeout=HTTP_REQUEST_TIMEOUT)
    logger.info(f"route es request, url: {url}, status: {res.status_code}")

    if res.status_code == 200:
        return res.text
    else:
        logger.warning(f"{es_addr}: route es request failed. {res.status_code} {res.text}")
        raise EsRestRequestError(message_kv={"msg": res.text})


def cat_indices(cluster_name, limit):
    """
    :param cluster_name: 集群名称
    :param limit: 结果表限制数
    """
    es_addr, es_auth = es_conn_info(cluster_name)

    url = f"http://{es_addr}/_cat/indices?v&s={STORE_SIZE}:desc&format=json&master_timeout=300s"
    res = requests.get(url=url, auth=es_auth, timeout=HTTP_REQUEST_TIMEOUT)
    logger.info(f"cat indices request, url: {url}, status: {res.status_code}")

    result = {TOP: [], INDICES: []}

    if res.status_code == 200:
        indices_list = res.json()
        result[INDICES] = indices_list

        # 过滤出大于条数阀值的rt列表，过滤掉非法index
        filter_indices = [s for s in indices_list if re.search(r"^\d+_\w+_\d{8,}$", s[INDEX]) is not None]
        range_index = len(filter_indices) if limit > len(filter_indices) else limit
        result[TOP] = [filter_indices[i][INDEX] for i in range(range_index)]
        return result
    else:
        logger.warning(f"{es_addr}: cat indices request failed. {res.status_code} {res.text}")
        raise EsRestRequestError(message_kv={"msg": res.text})


def del_indices(cluster_name, indices):
    """
    :param cluster_name: 集群名称
    :param indices: 索引列表，支持通配符
    """
    es_addr, es_auth = es_conn_info(cluster_name)
    index_list = indices.split(",")
    error_list = []
    success_list = []
    for index in index_list:
        url = f"http://{es_addr}/{index}?master_timeout=300s"
        try:
            res = requests.delete(url=url, auth=es_auth, timeout=HTTP_REQUEST_TIMEOUT)
            logger.info(f"del indices request, url: {url}, status: {res.status_code}")
            if res.status_code == 200:
                success_list.append(index)
            else:
                logger.error(f"{es_addr}: {index}: failed to del indices for {res.text}")
                error_list.append(index)
        except Exception:
            error_list.append(index)
            logger.error(f"{es_addr}: {index}: del indices exception.", exc_info=True)

    return {SUCCESS: success_list, FAILED: error_list}


def es_conn_info(cluster_name):
    """
    获取es连接信息
    :param cluster_name: 集群名称
    """
    cluster = model_manager.get_cluster_obj_by_name_type(cluster_name, ES)
    if not cluster:
        raise ClusterNotFoundException(message_kv={CLUSTER_TYPE: ES, CLUSTER_NAME: cluster_name})

    es_addr, es_auth = parse_es_connection_info(cluster.connection_info)
    return es_addr, es_auth

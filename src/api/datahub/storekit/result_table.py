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
import time

from common.exceptions import DataNotFoundError
from common.local import get_request_username
from common.log import logger
from common.transaction import auto_meta_sync
from datahub.common.const import (
    BK_BIZ_ID,
    COLUMN,
    CONNECTION_INFO,
    DATA_TYPE,
    DESCRIPTION,
    ES,
    EXPIRES,
    GENERATE_TYPE,
    HDFS,
    HDFS_URL,
    ICEBERG,
    KAFKA,
    MAPLELEAF,
    MYSQL,
    PHYSICAL_TABLE_NAME,
    PREVIOUS_CLUSTER_NAME,
    PRIORITY,
    PROCESSING_TYPE,
    PULSAR,
    RESULT_TABLE_ID,
    RESULT_TABLE_NAME,
    STORAGE_CLUSTER,
    STORAGES,
    TABLE,
    VALUE,
)
from datahub.storekit import StorageRuleConfig, es, hdfs, iceberg, model_manager, mysql
from datahub.storekit import storage_settings as s
from datahub.storekit.exceptions import StorageGetResultTableException
from datahub.storekit.settings import (
    DATABUS_CHANNEL_TYPE,
    STORAGE_EXCLUDE_QUERY,
    STORAGE_QUERY_ORDER,
)
from datahub.storekit.util import (
    get_rt_info,
    get_schema_and_sql,
    rt_storage_geog_area_check,
)


def get_new_physical_tn(rt_info, cluster_type, last_physical_tn=None, data_type=None):
    """
    获取存储的新物理表名称
    :param data_type: 用于hdfs创建physical_table_name，指明数据格式
    :param rt_info: result table id 的配置信息
    :param cluster_type: rt的存储类型
    :param last_physical_tn: 此rt此存储中使用过的最新的物理表名称，此物理表名必须是非active状态的
    :return: 新的物理表名称
    """
    rt_id, processing_type = rt_info[RESULT_TABLE_ID], rt_info[PROCESSING_TYPE]

    # 对于iceberg 单独生成物理规则
    if cluster_type == HDFS and data_type == ICEBERG:
        bk_biz_id = rt_info[BK_BIZ_ID]
        # result_table_name 使用小写，在hive元信息中hive表均为小写
        return iceberg.physical_table_name(bk_biz_id, rt_info[RESULT_TABLE_NAME])
    else:
        default_physical_tn = StorageRuleConfig.get_physical_table_name(rt_id, processing_type).get(cluster_type, rt_id)

    if cluster_type not in s.STORAGE_EXCLUDE_TABLE_NAME_MODIFY and last_physical_tn:
        # 对于部分存储，物理表名保存不变。如已生成过物理表名，则需计算下一个物理表名
        if last_physical_tn == default_physical_tn:
            return f"{default_physical_tn}__1"
        elif last_physical_tn.find("__") > 0:
            # 表名中有 __
            _list = last_physical_tn.split("__")
            # 新表名, physical_table_max尾部递增1
            return f'{"_".join(_list[0:-1])}__{int(_list[-1]) + 1}'
        else:
            # 如, tspider, hermes会有迁移前不规则表名情况
            logger.info(f"abnormal physical table name {last_physical_tn}, using new {default_physical_tn}")

    return default_physical_tn


def get_all_physical_tn(rt_info, with_tdw=True):
    """
    获取result table id对应不同存储的物理表名称字典
    :param rt_info: rt的配置信息
    :param with_tdw: 是否返回tdw表规则
    :return: rt的物理表名字典
    """
    execute_begin_time = int(time.time() * 1000)
    rt_id = rt_info[RESULT_TABLE_ID]
    # 获取不同存储的默认表名规则
    physical_tns = StorageRuleConfig.get_physical_table_name(rt_id, rt_info[PROCESSING_TYPE], with_tdw)
    # physical_table_name尾部会做+1处理的存储
    all_cluster_type = list(STORAGE_QUERY_ORDER.keys()) + STORAGE_EXCLUDE_QUERY
    allow_modify = [i for i in all_cluster_type if i not in s.STORAGE_EXCLUDE_TABLE_NAME_MODIFY]
    for cluster_type in allow_modify:
        objs = model_manager.get_all_storage_rt_objs_by_rt_type(rt_id, cluster_type)
        if objs:
            # 已经有关联关系，取当前物理表名的最大值
            physical_tns[cluster_type] = objs[0].physical_table_name

    cost_time = int(time.time() * 1000) - execute_begin_time
    logger.info(f"{rt_id} get all physical table names cost time: {cost_time}(ms)")
    return physical_tns


def delete_storage(rt_info, cluster_type):
    """
    删除rt关联存储的数据
    :param rt_info: rt的配置信息
    :param cluster_type: 存储类型
    """
    if cluster_type in rt_info[STORAGES]:
        if cluster_type == HDFS:
            hdfs.delete(rt_info)
        elif cluster_type == MYSQL:
            mysql.delete(rt_info)
        elif cluster_type == ES:
            es.delete(rt_info)
        else:
            logger.warning(f"{rt_info[RESULT_TABLE_ID]}: delete storage data in {cluster_type} not supported")
    else:
        logger.warning(f"{rt_info[RESULT_TABLE_ID]}: no {cluster_type} storage in rt")


def update_location(rt_info):
    """
    更新location
    :param rt_info: 结果表信息
    """
    conn = json.loads(rt_info[STORAGES][HDFS][STORAGE_CLUSTER][CONNECTION_INFO])
    location = None
    try:
        # 查询location
        location = iceberg.location(rt_info)
    except Exception:
        logger.error(f"{rt_info[RESULT_TABLE_ID]}: failed to query location", exc_info=True)

    if location:
        try:
            if location.startswith(f"{HDFS}://"):
                location = f'{conn[HDFS_URL]}/{location.split("/", 3)[3]}'
            else:
                location = f"{conn[HDFS_URL]}/{location}"

            iceberg.update_location(rt_info, location)
        except Exception:
            logger.error(f"{rt_info[RESULT_TABLE_ID]}: failed to update location", exc_info=True)


def change_hdfs_cluster(result_table_id, previous_cluster_name):
    """
    更换集群时，处理hdfs相关元信息
    :param result_table_id: 结果表id
    :param previous_cluster_name: 历史集群名称
    """
    rt_info = get_rt_info(result_table_id)
    if rt_info[STORAGES][HDFS][DATA_TYPE] == ICEBERG:
        # 当切换集群时，当前表已有数据保留
        update_location(rt_info)
    else:
        hdfs.delete(rt_info, False)  # 对于hdfs集群发生变化时，需要删除hive中的schema信息
        hdfs.remove_offset_dir(rt_info, previous_cluster_name)


def union_storage_config(added_storage_conf, previous_storage_config):
    """
    :param added_storage_conf: 需要追加的存储配置
    :param previous_storage_config: 上次最新的存储配置
    """
    last_storage_config = json.loads(previous_storage_config)
    for k, v in json.loads(added_storage_conf).items():
        last_storage_config[k] = v
    return json.dumps(last_storage_config)


def update_result_table_config(obj, params, storage_config):
    """
    :param obj: 配置对象
    :param params: 参数
    :param storage_config:存储配置
    """
    obj.expires = params.get(EXPIRES, obj.expires)
    obj.storage_config = storage_config
    obj.priority = params.get(PRIORITY, obj.priority)
    obj.generate_type = params.get(GENERATE_TYPE, obj.generate_type)
    obj.data_type = params.get(DATA_TYPE, obj.data_type)
    # 支持修改存储的物理表名称
    obj.physical_table_name = params.get(PHYSICAL_TABLE_NAME, obj.physical_table_name)
    # 支持通过接口参数更新上一个集群名称
    obj.previous_cluster_name = params.get(PREVIOUS_CLUSTER_NAME, obj.previous_cluster_name)
    obj.description = params.get(DESCRIPTION, obj.description)
    obj.updated_by = get_request_username()
    with auto_meta_sync(using=MAPLELEAF):
        obj.save()


def latest_storage_conf_obj(cluster_name, cluster_type, result_table_id, storage_channel_id):
    """
    获取最新存储配置
    :param cluster_name: 集群名称
    :param cluster_type: 存储类型
    :param result_table_id: 结果表id
    :param storage_channel_id: channel_id
    """
    # 区分channel和非channel存储，
    if cluster_type in DATABUS_CHANNEL_TYPE:
        cluster = None  # 当channel_id >= 0时，为kafka存储
        objs = model_manager.get_all_channel_objs_by_rt(result_table_id)  # 更新channel（kafka）存储
        target_id = storage_channel_id
    else:
        cluster = model_manager.get_cluster_obj_by_name_type(cluster_name, cluster_type)
        if not cluster:
            raise DataNotFoundError(
                message_kv={
                    TABLE: "storage_cluster_config",
                    COLUMN: "cluster_name, cluster_type",
                    VALUE: f"{cluster_name}, {cluster_type}",
                }
            )
        # 更新其他类型存储配置
        objs = model_manager.get_storage_rt_objs_by_rt_type(result_table_id, cluster_type)
        target_id = cluster.id

    # 校验存储集群与rt的区域,如果存在地理区域且相同则校验通过
    rt_storage_geog_area_check(target_id, cluster_type, result_table_id)
    return cluster, target_id, objs


def query_schema_and_sql(result_table_id, all_flag, storage_hint):
    """
    获取schema，sql
    :param result_table_id: 结果表id
    :param all_flag: 是否包含所有类型的存储
    :param storage_hint: 是否存储提示
    """
    rt_info = get_rt_info(result_table_id)
    if rt_info:
        data = get_schema_and_sql(rt_info, all_flag, storage_hint)
    else:
        raise StorageGetResultTableException(message_kv={RESULT_TABLE_ID: result_table_id})
    return data


def rt_phy_table_name(result_table_id, project_id=None, is_tdw=False):
    """
    rt物理表名
    :param is_tdw: 是否是tdw
    :param result_table_id: 结果表id
    :param project_id: 项目id
    :return:
    """
    rt_info = get_rt_info(result_table_id)
    if rt_info:
        return get_all_physical_tn(rt_info)
    else:
        raise StorageGetResultTableException(message_kv={RESULT_TABLE_ID: result_table_id})


def create_phy_table_name(result_table_id, storage, last_physical_tn, data_type, extra_params):
    """
    创建结果表，生成物理表名
    :param result_table_id: 结果表id
    :param storage: 存储
    :param last_physical_tn: 历史最新物理表名
    :param data_type: 数据格式
    :param extra_params: 参数
    """
    if storage in [KAFKA, PULSAR]:
        rt_info = get_rt_info(result_table_id)
        physical_table_name = (
            extra_params[PHYSICAL_TABLE_NAME]
            if extra_params.get(PHYSICAL_TABLE_NAME)
            else get_new_physical_tn(rt_info, storage, last_physical_tn)
        )
    else:
        rt_info = get_rt_info(result_table_id)
        physical_table_name = get_new_physical_tn(rt_info, storage, last_physical_tn, data_type)

    return rt_info, physical_table_name

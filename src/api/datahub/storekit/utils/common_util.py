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

from common.exceptions import DataNotFoundError
from common.log import logger
from datahub.common.const import (
    CLUSTER_NAME,
    CLUSTER_TYPE,
    COLUMN,
    CONNECTION_INFO,
    DATA,
    EXPIRES,
    MAPLELEAF,
    PHYSICAL_TABLE_NAME,
    PROCESSING_TYPE,
    RESULT_TABLE_ID,
    STORAGE_CLUSTER,
    STORAGE_CONFIG,
    STORAGES,
    TABLE,
    VALUE,
)
from datahub.storekit import exceptions, model_manager
from datahub.storekit import storage_settings as s
from datahub.storekit.settings import META_API_URL
from datahub.storekit.utils.http_util import get
from django.utils.translation import ugettext as _


def get_rt_info(result_table_id):
    """
    :param result_table_id:
    :return: result_table
    """
    result, res = get(
        f"{META_API_URL}/result_tables/{result_table_id}/"
        "?extra=True&tdw_related=cols_info&tdw_related=parts_info&related=fields&related=storages"
    )
    if not result:
        logger.error(f"request meta, get {result_table_id} result table result error, result: {result}, res: {res}")
        raise Exception(_(f"请求元数据接口，获取{result_table_id}的result_table信息异常"))
    if not res[DATA]:
        logger.error(f"get {result_table_id} data from meta error, result: {result}, res: {res}")
        raise exceptions.StorageGetResultTableException(message_kv={RESULT_TABLE_ID: result_table_id})
    return res[DATA]


def get_rt_cluster(rt_id, cluster_type):
    """
    获取rt_id, cluster_type对应的cluster_name
    """
    result_table = get_rt_info(rt_id)
    storages = result_table.get(STORAGES, {}).get(cluster_type, {})
    if storages == {}:
        raise Exception(_(f"没有找到result_table存储信息, cluster_type: {cluster_type}"))
    return storages.get(STORAGE_CLUSTER, {}).get(CLUSTER_NAME, "")


def get_conn_info(cluster_type, cluster_name):
    """
    获取存储的连接信息, eg:
    default, tspider --> {'host':'domain', 'port': 25000, 'user':'test', 'password':'test' }
    """
    cluster = model_manager.get_storage_cluster_config(cluster_name, cluster_type)
    if cluster:
        return json.loads(cluster[CONNECTION_INFO])
    else:
        raise DataNotFoundError(
            message_kv={
                TABLE: "storage_cluster_config",
                COLUMN: "cluster_name, cluster_type",
                VALUE: f"{cluster_name}, {cluster_type}",
            }
        )


def get_conn_info_by_rt(result_table_id, cluster_type):
    """获取result_table_id对应cluster_type的connection_info"""
    result_table = get_rt_info(result_table_id)
    if not result_table[STORAGES].get(cluster_type, {}):
        raise Exception(
            _(
                "%(result_table_id)s没有关联%(cluster_type)s类型的存储"
                % {RESULT_TABLE_ID: result_table_id, CLUSTER_TYPE: cluster_type}
            )
        )
    connection_info = result_table[STORAGES].get(cluster_type).get(STORAGE_CLUSTER, {}).get(CONNECTION_INFO, "")
    if not connection_info:
        raise Exception(
            _(
                "%(result_table_id)s获取%(cluster_type)s集群的连接信息异常"
                % {RESULT_TABLE_ID: result_table_id, CLUSTER_TYPE: cluster_type}
            )
        )
    return json.loads(connection_info)


def get_storage_config(result_table_id, cluster_type):
    """获取result_table_id对应cluster_type的storage_config"""
    result_table = get_rt_info(result_table_id)
    if not result_table[STORAGES].get(cluster_type, {}):
        raise Exception(
            _(
                "%(result_table_id)s没有关联%(cluster_type)s类型的存储"
                % {RESULT_TABLE_ID: result_table_id, CLUSTER_TYPE: cluster_type}
            )
        )
    storage_config = result_table[STORAGES].get(cluster_type).get(STORAGE_CONFIG, {})
    if not storage_config:
        raise Exception(
            _(
                "%(result_table_id)s获取%(cluster_type)s类型存储的存储配置异常"
                % {RESULT_TABLE_ID: result_table_id, CLUSTER_TYPE: cluster_type}
            )
        )
    return json.loads(storage_config)


def get_cluster_name(result_table_id, cluster_type):
    """获取result_table_id所在集群名称"""
    result_table = get_rt_info(result_table_id)
    if not result_table[STORAGES].get(cluster_type, {}):
        raise Exception(_(f"{result_table_id}没有关联此类型的存储"))
    cluster_name = result_table[STORAGES].get(cluster_type).get(STORAGE_CLUSTER, {}).get(CLUSTER_NAME, "")
    if not cluster_name:
        raise Exception(_(f"{result_table_id}获取集群名称异常"))
    return cluster_name


def get_processing_type(result_table):
    processing_type = result_table.get(PROCESSING_TYPE, "")
    if not processing_type:
        raise Exception(_("获取processing_type异常"))
    return processing_type


def get_db_table_name(result_table_id, cluster_type):
    """
    从meta获取physical_table_name。
    根据physical_table_name获取实际的db_name和table_name
    """
    result_table = get_rt_info(result_table_id)
    storages = result_table.get(STORAGES, {})
    if not storages.get(cluster_type, {}):
        raise Exception(_(f"没有关联{cluster_type}存储"))
    physical_table_name = storages.get(cluster_type, {}).get(PHYSICAL_TABLE_NAME, "")
    if not physical_table_name:
        raise Exception(_(f"{result_table_id}获取对应物理表名异常"))

    if cluster_type not in s.TABLE_NAME_SPLIT_BY_POINT:
        # 没有db_name
        return None, physical_table_name
    else:
        if physical_table_name.find(".") > 0:
            db_name = physical_table_name.split(".")[0]
            table_name = physical_table_name.split(".")[1]
            return db_name, table_name
        else:
            # tspider, 补齐db_name
            biz_id = physical_table_name.split("_")[-1]
            if biz_id.isdigit():
                return f"{MAPLELEAF}_{biz_id}", physical_table_name
            raise Exception(_(f"{result_table_id}获取物理表名异常"))


def get_rt_storages(rt_id):
    result_table = get_rt_info(rt_id)
    storages = result_table.get(STORAGES, {})
    if storages == {}:
        raise Exception(_(f"rt_id: {rt_id}没有存储信息"))
    return list(storages.keys())


def get_expires(result_table_id, cluster_type):
    """获取rt_id的过期时长expires"""
    result_table = get_rt_info(result_table_id)
    storages = result_table.get(STORAGES, {}).get(cluster_type, {})
    if storages == {}:
        raise Exception(_("{result_table_id}没有配置此类型存储的信息"))
    return storages.get(EXPIRES, "360d")


def translate_expires_day(expires):
    """
    转化expires为对应的天数
    :param expires: 带单位的过期时间
    :return: expires转换为天数的结果
    """
    # 永久保存，直接返回-1
    if expires.find("-1") >= 0:
        return "-1"
    if expires.find("-") >= 0 and not expires.find("-1") >= 0:
        raise Exception(_("小于0的过期天数，仅允许传递-1，表示永久存储"))

    expires_num, expires_unit = int(re.sub(r"\D", "", expires)), "".join(re.findall("[A-Za-z]", expires))
    if not expires_unit:
        return int(expires)
    if expires_unit.lower() == "d":
        return expires_num
    elif expires_unit.lower() == "w":
        return expires_num * 7
    elif expires_unit.lower() == "m":
        return expires_num * 31
    elif expires_unit.lower() == "y":
        return expires_num * 365
    else:
        raise Exception(_("时间单位错误。目前支持: 天d, 周w, 月m, 年y。"))


def get_date_by_diff(diff):
    """获取前后diff天对应的日期(格式: 年月日)"""
    return (datetime.today() + timedelta(diff)).strftime("%Y%m%d")


def get_timestamp_diff(diff):
    """获取前后diff天对应的时间戳(毫秒)"""
    tmp_str = (datetime.today() + timedelta(diff)).strftime("%Y-%m-%d %H:%M:%S")
    tmp_array = time.strptime(tmp_str, "%Y-%m-%d %H:%M:%S")
    return int(time.mktime(tmp_array)) * 1000


def check_expires_format(expires):
    """检查过期时间格式"""
    value = re.compile(r"^\d+(d|D)$")
    result = value.match(expires)
    if not result:
        raise Exception(_("请输入正确的过期时间格式, 如30D。过期天数不能小于0"))
    return True


def trans_head_to_tail(str):
    """字符串头部移到尾部, eg: 591_test_rt ----> test_rt_591"""
    _list = str.split("_")
    _list.append(_list[0])
    return "_".join(_list[1:])


def json_str_valid(str):
    try:
        json.loads(str)
    except Exception:
        msg = _("{str}不是json格式字符串")
        logger.error(msg)
        raise Exception(_(msg))
    return True

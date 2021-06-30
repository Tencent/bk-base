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

from conf.dataapi_settings import (
    IP_REDIS_V4_GSLB_RESULT_TABLE_ID,
    IP_REDIS_V4_TGEO_BASE_NETWORK_RESULT_TABLE_ID,
    IP_REDIS_V4_TGEO_GENERIC_BUSINESS_RESULT_TABLE_ID,
    IP_REDIS_V6_GSLB_RESULT_TABLE_ID,
    IP_REDIS_V6_TGEO_BASE_NETWORK_RESULT_TABLE_ID,
    IP_REDIS_V6_TGEO_GENERIC_BUSINESS_RESULT_TABLE_ID,
    IPV4_RESULT_TABLE_ID,
    IPV6_RESULT_TABLE_ID,
)
from django.utils.translation import ugettext_lazy as _

from dataflow.stream.api.api_helper import MetaApiHelper
from dataflow.stream.exceptions.comp_execptions import AssociateDataSourceError, ComponentNotSupportError
from dataflow.stream.handlers import processing_stream_info
from dataflow.stream.result_table.result_table_for_flink import load_flink_chain


def load_chain(heads_str, tails_str):
    """
    根据heads和tails获取任务的整个链路

    :param heads_str: 任务heads
    :param tails_str: 任务tails
    :return: 任务整个链路信息
    """
    # 根据tail的一个result table来判断是flink还是storm
    one_tail = tails_str.split(",")[0]
    stream_info = processing_stream_info.get(one_tail)
    if stream_info.component_type == "flink":
        return load_flink_chain(heads_str, tails_str)
    elif stream_info.component_type == "storm":
        from dataflow.stream.extend.result_table.result_table_for_storm import load_storm_chain

        return load_storm_chain(heads_str, tails_str)
    else:
        raise ComponentNotSupportError("the component %s is not supported." % stream_info.component_type)


def get_associate_data_source_info(result_table_id):
    from common.base_crypt import BaseCrypt

    table_info = MetaApiHelper.get_result_table(result_table_id, related=["fields", "storages"])

    if (
        "tredis" not in table_info["storages"]
        and "ipredis" not in table_info["storages"]
        and "ignite" not in table_info["storages"]
    ):
        raise AssociateDataSourceError(_("%s不能作为关联数据源" % result_table_id))

    kv_source_type = ""
    cluster_type = "tredis"
    # 用户自定义的关联表，如果同时存在tredis/ignite的话优先使用ignite
    if result_table_id in IPV4_RESULT_TABLE_ID:
        kv_source_type = "ipv4"
        cluster_type = "tredis"
    elif result_table_id in IPV6_RESULT_TABLE_ID:
        kv_source_type = "ipv6"
        cluster_type = "tredis"
    elif result_table_id in IP_REDIS_V4_TGEO_GENERIC_BUSINESS_RESULT_TABLE_ID:
        kv_source_type = "ipredisv4_tgb"
        cluster_type = "ipredis"
    elif result_table_id in IP_REDIS_V6_TGEO_GENERIC_BUSINESS_RESULT_TABLE_ID:
        kv_source_type = "ipredisv6_tgb"
        cluster_type = "ipredis"
    elif result_table_id in IP_REDIS_V4_TGEO_BASE_NETWORK_RESULT_TABLE_ID:
        kv_source_type = "ipredisv4_tbn"
        cluster_type = "ipredis"
    elif result_table_id in IP_REDIS_V6_TGEO_BASE_NETWORK_RESULT_TABLE_ID:
        kv_source_type = "ipredisv6_tbn"
        cluster_type = "ipredis"
    elif result_table_id in IP_REDIS_V4_GSLB_RESULT_TABLE_ID:
        kv_source_type = "ipredisv4_gslb"
        cluster_type = "ipredis"
    elif result_table_id in IP_REDIS_V6_GSLB_RESULT_TABLE_ID:
        kv_source_type = "ipredisv6_gslb"
        cluster_type = "ipredis"
    elif "ignite" in table_info["storages"]:
        kv_source_type = "ignite"
        cluster_type = "ignite"

    connection_info = json.loads(table_info["storages"][cluster_type]["storage_cluster"]["connection_info"])
    storage_config = json.loads(table_info["storages"][cluster_type]["storage_config"])

    storage_keys = storage_config["storage_keys"]
    password = connection_info["password"]
    # ignite `storage_keys` is array in meta;
    # ignite `password` should not decrypt
    if cluster_type != "ignite":
        storage_keys = storage_keys.split(",")
        password = BaseCrypt.bk_crypt().decrypt(password)

    storage_info = {
        "query_mode": "single",
        "redis_type": "private",
        "data_id": result_table_id,
        "host": connection_info["host"],
        "port": connection_info["port"],
        "password": password,
        "kv_source_type": kv_source_type,
    }
    # ignite add extra params for init ignite client
    if cluster_type == "ignite":
        storage_info.update({"ignite_user": connection_info["user"]})
        storage_info.update({"ignite_cluster": connection_info["cluster.name"]})
        physical_table_name = table_info["storages"]["ignite"]["physical_table_name"]
        cache_name = physical_table_name if "." not in physical_table_name else physical_table_name.split(".")[1]
        storage_info.update({"cache_name": cache_name})
        storage_info.update({"key_separator": storage_config["key_separator"]})
        # ignite存储key的拼接顺序非简单字典顺序,要使用指定的顺序来拼接
        storage_info.update({"key_order": storage_keys})

    fields = []
    for one_field in table_info["fields"]:
        tmp_field = {
            "name": one_field["field_name"],
            "type": one_field["field_type"],
            "description": one_field["description"],
            "is_key": 1 if one_field["field_name"] in storage_keys else 0,
        }
        fields.append(tmp_field)
    table_name = table_info["result_table_name"]
    bk_biz_id = table_info["bk_biz_id"]

    return {
        "storage_info": storage_info,
        "table_name": table_name,
        "bk_biz_id": bk_biz_id,
        "fields": fields,
    }

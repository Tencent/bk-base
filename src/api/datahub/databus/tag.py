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

from common.log import logger
from common.meta.common import create_tag_to_target, delete_tag_to_target
from common.transaction import auto_meta_sync
from datahub.common.const import OP, OUTER
from datahub.databus.api import MetaApi
from datahub.databus.exceptions import MetaResponseError
from datahub.databus.models import DatabusChannel

from datahub.databus import settings

CONNECTOR_CLUSTER = "connector_cluster"
CHANNEL_CLUSTER = "channel_cluster"
RAW_DATA = "raw_data"
RESULT_TABLE = "result_table"
RESULT_TABLE_KEY = "result_table_id"
STORAGE_CLUSTER = "storage_cluster"
GEOG_AREA = "geog_area"


def create_tag(target_type, target_id, tags):
    # 创建重复tag时会抛异常, 并没有提供相关异常捕获, 需要从msg判断
    try:
        with auto_meta_sync(using="default"):
            create_tag_to_target([(target_type, target_id)], tags)
    except Exception as e:
        if "Duplicate entry" not in str(e):
            raise e


def delete_tag(target_type, target_id, tags):
    # 当不存在时不会抛异常
    with auto_meta_sync(using="default"):
        delete_tag_to_target([(target_type, target_id)], tags)


def create_connector_tag(target_id, tags):
    create_tag(CONNECTOR_CLUSTER, target_id, tags)


def create_channel_tag(target_id, tags):
    create_tag(CHANNEL_CLUSTER, target_id, tags)


def delete_connector_tag(target_id, tags):
    delete_tag(CONNECTOR_CLUSTER, target_id, tags)


def delete_channel_tag(target_id, tags):
    delete_tag(CHANNEL_CLUSTER, target_id, tags)


def get_tag_from_meta(param):
    response = MetaApi.target.list(param, raise_exception=True)
    """
    {
        "count":1,
        "content":[
            {
                "id":123,
                "_target_type":"AccessRawData",
                "tags":{
                    "manage":{
                        "geog_area":[
                            {
                                "code":"inland",
                                "description":"中国内地（或简称内地）是指除香港、澳门、台湾以外的中华人民共和国主张管辖区，多用于与香港、澳门特别行政区同时出现的语境",
                                "alias":"中国内地",
                                "kpath":0,
                                "sync":0,
                                "seq_index":0,
                                "id":123,
                                "tag_type":"manage"
                            }
                        ]
                    },
                },
            }
        ]
    }
    """
    try:
        geog_area = response.data["content"][0]["tags"]["manage"]["geog_area"][0]["code"]
    except Exception as e:
        logger.error("meta response error: {}, data={}".format(e, response.data))
        raise MetaResponseError(message_kv={"field": "geog_area"})

    return geog_area


def get_tag_by_target_id(target_type, target_id, target_key="id"):
    """
    根据target_type和target_id查询地域tag
    :param target_type: target_type
    :param target_id: target_id
    :param target_key: 字段key
    :return: None if not found
    """
    param = {
        "target_type": target_type,
        "target_filter": json.dumps([{"k": target_key, "func": "eq", "v": target_id}]),
        "limit": settings.TAG_TARGET_LIMIT,
    }
    return get_tag_from_meta(param)


def get_tag_by_raw_data_id(raw_data_id):
    """
    根据raw_data_id查询地域tag
    :param raw_data_id:
    :return: None if not found
    """
    return get_tag_by_target_id(RAW_DATA, raw_data_id)


def get_tag_by_rt_id(rt_id):
    """
    根据rt_id查询地域tag
    :param rt_id:
    :return: None if not found
    """
    return get_tag_by_target_id(RESULT_TABLE, rt_id, target_key=RESULT_TABLE_KEY)


def get_tag_by_channel_id(channel_id):
    """
    根据channel_id查询地域tag
    :param channel_id:
    :return: None if not found
    """
    return get_tag_by_target_id(CHANNEL_CLUSTER, channel_id)


def get_tag_by_storage_id(storage_id):
    """
    根据storage_id查询地域tag
    :param storage_id:
    :return: None if not found
    """
    return get_tag_by_target_id(STORAGE_CLUSTER, storage_id)


def get_tag_by_cluster_id(cluster_id):
    """
    根据cluster_id查询地域tag
    :param cluster_id:
    :return: None if not found
    """
    return get_tag_by_target_id(CONNECTOR_CLUSTER, cluster_id)


def get_channels_by_tags(tags):
    """
    通过标签获取channel实例列表
    :param tags: 标签列表
    :return: [channel_id]
    """
    criterion = [{"k": "code", "func": "eq", "v": tag} for tag in tags]
    param = {
        "match_policy": "both",
        "target_type": CHANNEL_CLUSTER,
        "tag_filter": json.dumps([{"criterion": criterion, "condition": "OR"}]),
        "limit": settings.TAG_TARGET_LIMIT,
    }
    response = MetaApi.target.list(param, raise_exception=True)
    """
    {
        "count":5,
        "content":[
            {
                "tags":Object{...},
                "_target_type":"DatabusChannelClusterConfig",
                "id":10,
                ...
            },
            {
                "tags":Object{...},
                "_target_type":"DatabusChannelClusterConfig",
                "id":11,
                ...
            },
        ]
    }
    """
    channels = []
    try:
        for target in response.data["content"]:
            channels.append(int(target["id"]))
    except Exception as e:
        logger.error("meta response error: {}, data={}".format(e, response.data))
        raise MetaResponseError(message_kv={"field": "content/id"})
    return channels


def init_channel_role():
    """
    初始化channel的op和outer角色
    """
    channels = DatabusChannel.objects.all().values()

    # op tags
    tag = OP
    for obj in channels:
        if obj["cluster_role"] == tag:
            create_channel_tag(obj["id"], [tag])

    # outer tags
    tag = OUTER
    for obj in channels:
        if obj["cluster_role"] == tag:
            create_channel_tag(obj["id"], [tag])

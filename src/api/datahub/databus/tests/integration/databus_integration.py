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
import logging
import time

import requests
from conf.dataapi_settings import (
    DATABUS_API_HOST,
    DATABUS_API_PORT,
    DATAQUERY_API_HOST,
    DATAQUERY_API_PORT,
    META_API_HOST,
    META_API_PORT,
)

"""
模拟页面调用后台接口，将流程串起来，验证流程中各步骤的结果数据符合预期
"""

# 定义一些变量用于测试
HEADERS = {"Content-Type": "application/json"}
BASE_URL = "http://%s:%d/v3/databus" % (DATABUS_API_HOST, DATABUS_API_PORT)
STOREKIT_URL = "http://%s:%d/v3/storekit" % (DATABUS_API_HOST, DATABUS_API_PORT)
META_URL = "http://%s:%d/v3/meta" % (META_API_HOST, META_API_PORT)
QUERY_URL = "http://%s:%d/v3/dataquery" % (DATAQUERY_API_HOST, DATAQUERY_API_PORT)
BK_BIZ_ID = 591
CLEAN_TABLE_NAME = "integration_test"
RAW_DATA_ID = 391

# 初始化logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-5.5s]  %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger()


def verify_channel_clusters():
    """
    验证总线channel的集群配置
    """
    # 验证channels列表接口
    success, channels = get("%s/channels/" % BASE_URL, "")
    assert success
    # 至少channel集群中需要包含 outer/inner/queue 各一个
    assert len(channels) >= 3
    items = set()
    for channel in channels:
        items.add(channel["cluster_role"])
    assert "outer" in items
    assert "inner" in items
    assert "queue" in items
    # 可能还存在 config 类型的channel
    assert len(items) >= 3

    # 验证inner_to_use接口
    success, inner_channel = get("%s/channels/inner_to_use/" % BASE_URL, "")
    assert success
    assert inner_channel["cluster_type"] == "kafka"
    assert inner_channel["cluster_role"] == "inner"

    # 验证通过channel_id 和 channel_name 获取集群配置的接口
    success, channel_by_id = get("{}/channels/{}/".format(BASE_URL, inner_channel["id"]), "")
    assert success
    assert channel_by_id["id"] == inner_channel["id"]
    assert channel_by_id["cluster_name"] == inner_channel["cluster_name"]
    success, channel_by_name = get("{}/channels/{}/".format(BASE_URL, inner_channel["cluster_name"]), "")
    assert success
    assert channel_by_name["id"] == inner_channel["id"]
    assert channel_by_name["cluster_name"] == inner_channel["cluster_name"]


def verify_connector_clusters():
    """
    验证总线的任务集群
    """
    # 获取总线任务集群列表
    success, clusters = get("%s/clusters/" % BASE_URL, "")
    assert success
    assert len(clusters) >= 10
    items = set()
    for cluster in clusters:
        items.add(cluster["cluster_name"])

    # 获取总线
    success, inner_channel = get("%s/channels/inner_to_use/" % BASE_URL, "")
    assert success
    inner_channel_name = inner_channel["cluster_name"]
    assert "es-%s-M" % inner_channel_name in items
    assert "hdfs-%s-M" % inner_channel_name in items
    assert "mysql-%s-M" % inner_channel_name in items
    assert "mysql-%s-M" % inner_channel_name in items
    assert "queue-%s-M" % inner_channel_name in items
    assert "puller-bkhdfs-M" in items

    # 验证get_cluster_info 接口的返回
    success, clean_cluster_info = get(
        "{}/clusters/get_cluster_info/?cluster_name=es-{}-M".format(BASE_URL, inner_channel_name),
        "",
    )
    assert success
    assert "cluster.group.id" in clean_cluster_info
    assert "cluster.rest.port" in clean_cluster_info
    assert "cluster.bootstrap.servers" in clean_cluster_info
    assert clean_cluster_info["cluster.group.id"] == "es-%s-M" % inner_channel_name


def verify_clean_task(table_name):
    """
    验证清洗相关接口，包含启动清洗任务，验证清洗结果数据等
    :param table_name: 表名
    """
    # 获取清洗算子列表
    success, factors = get("%s/cleans/factors/" % BASE_URL, "")
    assert success
    assert len(factors) >= 8  # 默认至少有8个算子，例如赋值、取值、json反序列化、url反序列化等

    # 获取清洗时间配置列表
    success, time_formats = get("%s/cleans/time_formats/" % BASE_URL, "")
    assert success
    assert len(time_formats) >= 10  # 至少有10中时间格式，例如时间戳（毫秒、秒、分钟）、各种日期格式

    # 获取清洗的错误提示信息
    success, errors = get("%s/cleans/list_errors/" % BASE_URL, "")
    assert success
    assert "AccessByIndexFailedError" in errors
    assert "AccessByKeyFailedError" in errors
    assert "BadJsonObjectError" in errors
    assert "UrlDecodeError" in errors

    # 默认请求清洗列表接口，应该返回空集
    success, clean_list = get("%s/cleans/" % BASE_URL, "")
    assert success
    assert len(clean_list) == 0

    # 传入raw_data_id，应该能查询到清洗列表
    success, clean_list = get("{}/cleans/?raw_data_id={}".format(BASE_URL, RAW_DATA_ID), "")
    assert success
    assert len(clean_list) > 0

    # 检查rawdata最新的消息内容
    success, msgs = get("{}/rawdatas/{}/tail/".format(BASE_URL, RAW_DATA_ID), "")
    assert success
    assert len(msgs) == 10  # 默认返回最近十条数据

    # 创建清洗配置和任务
    result_table_id = "{}_{}".format(BK_BIZ_ID, table_name)
    params = {
        "raw_data_id": RAW_DATA_ID,
        "json_config": '{"extract": {"args": [], "next": {"next": [{"next": {"args": ["labelilpme"], "next": '
        '{"subtype": "assign_pos", "next": null, "type": "assign", "assign": [{"assign_to": "log", '
        '"type": "string", "index": 0}], "label": "labelhwbsp"}, "result": "labelilpme", "label": '
        '"labelilpme", "type": "fun", "method": "iterate"}, "subtype": "access_obj", "result": '
        '"labelaqvgj", "key": "_value_", "label": "labelaqvgj", "type": "access"}, {"subtype": '
        '"assign_obj", "next": null, "type": "assign", "assign": [{"assign_to": "ip", "type": "string", '
        '"key": "_server_"}, {"assign_to": "report_time", "type": "string", "key": "_time_"}, '
        '{"assign_to": "gseindex", "type": "long", "key": "_gseindex_"}, {"assign_to": "path", "type": '
        '"string", "key": "_path_"}], "label": "labelwnplb"}], "type": "branch", "name": "", "label":'
        ' null}, "result": "labelomplc", "label": "labelomplc", "type": "fun", "method": "from_json"}, '
        '"conf": {"timestamp_len": 0, "encoding": "UTF8", "time_format": "yyyy-MM-dd HH:mm:ss", '
        '"timezone": 8, "output_field_name": "timestamp", "time_field_name": "report_time"}}',
        "pe_config": "",
        "bk_biz_id": BK_BIZ_ID,
        "result_table_name": table_name,
        "result_table_name_alias": "清洗表测试",
        "description": "清洗测试",
        "fields": [
            {
                "field_type": "string",
                "field_alias": "i",
                "is_dimension": False,
                "field_name": "ip",
                "field_index": 1,
            },
            {
                "field_type": "string",
                "field_alias": "r",
                "is_dimension": False,
                "field_name": "report_time",
                "field_index": 2,
            },
            {
                "field_type": "long",
                "field_alias": "g",
                "is_dimension": False,
                "field_name": "gseindex",
                "field_index": 3,
            },
            {
                "field_type": "string",
                "field_alias": "p",
                "is_dimension": False,
                "field_name": "path",
                "field_index": 4,
            },
            {
                "field_type": "string",
                "field_alias": "l",
                "is_dimension": True,
                "field_name": "log",
                "field_index": 5,
            },
        ],
    }
    success, create_clean = post("%s/cleans/" % BASE_URL, params)
    assert success
    assert create_clean["processing_id"] == result_table_id
    assert create_clean["raw_data_id"] == RAW_DATA_ID
    assert create_clean["result_table"]["result_table_id"] == result_table_id
    assert create_clean["result_table"]["processing_type"] == "clean"
    assert "kafka" in create_clean["result_table"]["storages"]

    # 获取清洗配置信息
    success, clean_info = get("{}/cleans/{}/".format(BASE_URL, result_table_id), "")
    assert success
    assert clean_info["processing_id"] == result_table_id
    assert clean_info["raw_data_id"] == RAW_DATA_ID
    assert clean_info["result_table"]["result_table_id"] == result_table_id
    assert clean_info["result_table"]["processing_type"] == "clean"
    assert clean_info["status"] == "stopped"
    assert "kafka" in clean_info["result_table"]["storages"]

    # 启动清洗任务
    success, start_clean = post(
        "%s/tasks/" % BASE_URL,
        {"result_table_id": result_table_id, "storages": ["kafka"]},
    )
    assert success
    assert start_clean == u"任务(result_table_id：%s)启动成功!" % result_table_id

    # 验证清洗任务的状态
    success, clean_info = get("{}/cleans/{}/".format(BASE_URL, result_table_id), "")
    assert success
    assert clean_info["status"] == "started"

    # 等待一段时间，验证清洗rt中写入的数据
    time.sleep(30)
    success, clean_tail = get("{}/result_tables/{}/tail/".format(BASE_URL, result_table_id), "")
    assert success
    assert len(clean_tail) > 0
    assert "ip" in clean_tail[0]
    assert "report_time" in clean_tail[0]
    assert "gseindex" in clean_tail[0]
    assert "path" in clean_tail[0]
    assert "log" in clean_tail[0]

    # 停止清洗任务
    success, stop_clean = delete("{}/tasks/{}/".format(BASE_URL, result_table_id), {"storages": ["kafka"]})
    assert success
    assert stop_clean == u"result_table_id为%s的任务已成功停止!" % result_table_id

    # 验证清洗任务状态
    success, clean_info = get("{}/cleans/{}/".format(BASE_URL, result_table_id), "")
    assert success
    assert clean_info["status"] == "stopped"

    # 验证清洗结果不再输出新内容
    success, clean_tail_1 = get("{}/result_tables/{}/tail/".format(BASE_URL, result_table_id), "")
    assert success
    time.sleep(5)
    success, clean_tail_2 = get("{}/result_tables/{}/tail/".format(BASE_URL, result_table_id), "")
    assert success
    # 对比两次tail的结果
    assert len(clean_tail_1) == len(clean_tail_2)
    for i in range(0, len(clean_tail_1)):
        assert clean_tail_1[i]["ip"] == clean_tail_2[i]["ip"]
        assert clean_tail_1[i]["report_time"] == clean_tail_2[i]["report_time"]
        assert clean_tail_1[i]["gseindex"] == clean_tail_2[i]["gseindex"]
        assert clean_tail_1[i]["path"] == clean_tail_2[i]["path"]
        assert clean_tail_1[i]["log"] == clean_tail_2[i]["log"]


def verify_mysql_shipper_task(table_name):
    """
    验证mysql分发相关的接口（增加存储、启停任务、查询数据）
    :param table_name: 表名
    """
    result_table_id = "{}_{}".format(BK_BIZ_ID, table_name)
    # 首先获取可用的mysql集群存储
    success, mysql_clusters = get("%s/storage_cluster_configs/" % STOREKIT_URL, "cluster_type=mysql")
    assert success
    assert len(mysql_clusters) > 0

    # 组建参数，在rt上增加mysql存储
    rt_storage_params = {
        "result_table_id": result_table_id,
        "cluster_name": mysql_clusters[0]["cluster_name"],
        "cluster_type": mysql_clusters[0]["cluster_type"],
        "expires": 7,
        "storage_channel_id": 0,
        "storage_config": '{"indexed_fields": ["dtEventTimeStamp", "ip"]}',
        "priority": 0,
    }
    success, add_mysql = post("%s/storage_result_tables/" % STOREKIT_URL, rt_storage_params)
    assert success
    assert add_mysql["result_table_id"] == result_table_id
    assert add_mysql["cluster_name"] == mysql_clusters[0]["cluster_name"]
    assert add_mysql["cluster_type"] == "mysql"

    # 建表并初始化存储
    success, create_table = post(
        "{}/storage_result_tables/{}/create_table/".format(STOREKIT_URL, result_table_id),
        {"cluster_type": "mysql"},
    )
    assert success

    # 验证meta接口中获取的rt的存储，包含kafka和mysql两种
    success, rt_storages = get("{}/result_tables/{}/storages/".format(META_URL, result_table_id), "")
    assert success
    assert "kafka" in rt_storages
    assert "mysql" in rt_storages

    # 启动分发任务
    success, start_shipper = post(
        "%s/tasks/" % BASE_URL,
        {"result_table_id": result_table_id, "storages": ["mysql"]},
    )
    assert success
    assert start_shipper == u"任务(result_table_id：%s)启动成功!" % result_table_id

    # 等待分发任务将数据写入存储中
    time.sleep(30)

    # 获取查询相关的SQL语句
    success, schema_sql = get(
        "{}/storage_result_tables/{}/get_schema_and_sql/".format(STOREKIT_URL, result_table_id),
        "",
    )
    assert success
    assert "mysql" in schema_sql["storage"]
    sql = schema_sql["storage"]["mysql"]["sql"]

    # 由于查询这里默认查询thedate为当天的数据，但是分发mysql的任务仅仅执行了30s，不能保证thedate为当天的数据已写入db中
    sql = sql.replace(">='%s'" % get_date(), ">='20181201'")

    # 通过查询接口确认数据已写入db中
    success, query_result = post(
        "%s/query/" % QUERY_URL,
        {"sql": sql, "bk_app_code": "data", "prefer_storage": "mysql"},
    )
    assert success
    assert len(query_result["list"]) == 10  # 默认sql查询10条数据
    assert "report_time" in query_result["list"][0]
    assert "gseindex" in query_result["list"][0]
    assert "path" in query_result["list"][0]
    assert "log" in query_result["list"][0]

    # 停止总线分发任务
    success, stop_shipper = delete("{}/tasks/{}/".format(BASE_URL, result_table_id), {"storages": ["mysql"]})
    assert success
    assert stop_shipper == u"result_table_id为%s的任务已成功停止!" % result_table_id


def get_ts():
    """
    获取当前的timestamp（秒）
    :return: 当前的timestamp
    """
    return int(time.time())


def get_date():
    """
    返回当天的日期（年月日）
    """
    return time.strftime("%Y%m%d", time.localtime())


def _http_request(method, url, headers=None, data=None):
    _start_time = int(time.time() * 1000)
    try:
        if method == "GET":
            resp = requests.get(url=url, headers=headers, params=data)
        elif method == "HEAD":
            resp = requests.head(url=url, headers=headers)
        elif method == "POST":
            resp = requests.post(url=url, headers=headers, json=data)
        elif method == "DELETE":
            resp = requests.delete(url=url, headers=headers, json=data)
        elif method == "PUT":
            resp = requests.put(url=url, headers=headers, json=data)
        else:
            return False, None
    except requests.exceptions.RequestException:
        logger.exception("http request error! type: {}, url: {}, data: {}".format(method, url, str(data)))
        return False, None
    else:
        if resp.status_code != 200:
            logger.error(
                "http request error! type: %s, url: %s, data: %s, response_status_code: %s, response_content: %s"
                % (method, url, str(data), resp.status_code, resp.content)
            )
            return False, None
        logger.info(
            "http_request|success|%s|%s|%s|%d|" % (method, url, str(data), int(time.time() * 1000 - _start_time))
        )
        logger.info(resp.text)
        json_result = resp.json()
        if json_result["result"]:
            return True, json_result["data"]
        else:
            return False, json_result["data"]


def get(url, params=""):
    headers = {"Content-Type": "application/json"}
    params = params + "&bk_username=XXX" if params else "bk_username=XXX"
    return _http_request(method="GET", url=url, headers=headers, data=params)


def post(url, params={}):
    headers = {"Content-Type": "application/json"}
    params["bk_username"] = "XXX"
    return _http_request(method="POST", url=url, headers=headers, data=params)


def delete(url, params={}):
    headers = {"Content-Type": "application/json"}
    params["bk_username"] = "XXX"
    return _http_request(method="DELETE", url=url, headers=headers, data=params)


if __name__ == "__main__":
    logger.info("==== going to do integration test for databus module ====")
    table_name = "{}_{}".format(CLEAN_TABLE_NAME, get_ts())
    verify_channel_clusters()
    verify_connector_clusters()
    verify_clean_task(table_name)
    verify_mysql_shipper_task(table_name)
    logger.info("==== passed integration test for databus module ====")

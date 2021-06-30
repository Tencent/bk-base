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

import pytest
from common.api.base import DataResponse
from datahub.access.tests.utils import get, post
from datahub.databus.tests.fixture.rawdata_fixture import ARGS

from datahub.databus import channel, settings

# 设置zk的地址信息，清理数据
settings.KAFKA_OP_HOST = "kafka:9092"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_rawdata")
def test_set_topic(mocker):
    rawdata_id = ARGS["rawdata_id"]
    res = post(
        "/v3/databus/rawdatas/%s/set_topic/" % rawdata_id,
        {
            "retention_hours": 168,
            "retention_size_g": 10,
        },
    )

    assert res["result"]


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_rawdata")
def test_rawdata(mocker):
    rawdata_id = ARGS["rawdata_id"]

    # 检查分区数量
    res = get("/v3/databus/rawdatas/%s/partitions/" % rawdata_id)
    assert res["result"]
    assert res["data"] == 1

    # 检查offsets信息
    res = get("/v3/databus/rawdatas/%s/offsets/" % rawdata_id)
    assert res["result"]
    kafka_msgs_count = res["data"]["0"]["length"]
    kafka_msgs_tail_offset = res["data"]["0"]["tail"]

    # 写入几条数据到上报的kafka topic中
    channel._send_message_to_kafka(
        ARGS["kafka_bs"],
        ARGS["topic"],
        [
            {"key": "msgkey01", "value": "msg value 01"},
            {"key": "msgkey02", "value": "msg value 02"},
            {"key": "msgkey03", "value": "msg value 03"},
            {"key": "msgkey04", "value": "msg value 04"},
            {"key": "msgkey05", "value": "msg value 05"},
            {"key": "msgkey06", "value": "msg value 06"},
            {"key": "msgkey07", "value": "msg value 07"},
            {"key": "msgkey08", "value": "msg value 08"},
            {"key": "msgkey09", "value": "msg value 09"},
            {"key": "msgkey10", "value": "msg value 10"},
            {"key": "msgkey11", "value": "msg value 11"},
            {"key": "msgkey12", "value": "msg value 12"},
        ],
    )

    # 检查tail接口返回内容
    res = get("/v3/databus/rawdatas/%s/tail/" % rawdata_id)
    assert res["result"]
    assert len(res["data"]) == 10
    assert res["data"][0]["partition"] == 0
    assert res["data"][0]["value"] == "msg value 12"
    assert res["data"][0]["key"] == "msgkey12"
    assert res["data"][0]["topic"] == ARGS["topic"]
    assert res["data"][0]["offset"] == kafka_msgs_tail_offset + 12

    # 检查tail接口返回内容
    res = get("/v3/databus/channels/tail/?kafka={}&topic={}".format(ARGS["kafka_bs"], ARGS["topic"]))
    assert res["result"]
    assert len(res["data"]) == 10
    assert res["data"][0]["partition"] == 0
    assert res["data"][0]["value"] == "msg value 12"
    assert res["data"][0]["key"] == "msgkey12"
    assert res["data"][0]["topic"] == ARGS["topic"]
    assert res["data"][0]["offset"] == kafka_msgs_tail_offset + 12

    # 检查message接口返回内容
    res = get("/v3/databus/rawdatas/{}/message/?offset={}".format(rawdata_id, kafka_msgs_tail_offset + 10))
    assert res["result"]
    assert len(res["data"]) == 1
    assert res["data"][0]["partition"] == 0
    assert res["data"][0]["value"] == "msg value 10"
    assert res["data"][0]["key"] == "msgkey10"
    assert res["data"][0]["topic"] == ARGS["topic"]
    assert res["data"][0]["offset"] == kafka_msgs_tail_offset + 10

    # 检查message接口返回内容
    res = get(
        "/v3/databus/channels/message/?kafka=%s&topic=%s&offset=%s"
        % (ARGS["kafka_bs"], ARGS["topic"], kafka_msgs_tail_offset + 10)
    )
    assert res["result"]
    assert len(res["data"]) == 1
    assert res["data"][0]["partition"] == 0
    assert res["data"][0]["value"] == "msg value 10"
    assert res["data"][0]["key"] == "msgkey10"
    assert res["data"][0]["topic"] == ARGS["topic"]
    assert res["data"][0]["offset"] == kafka_msgs_tail_offset + 10

    # 检查每天数据量
    res = get("/v3/databus/rawdatas/%s/daily_count/" % rawdata_id)
    assert res["result"]
    assert res["data"] >= 12

    # 检查offsets信息
    res = get("/v3/databus/rawdatas/%s/offsets/" % rawdata_id)
    assert res["result"]
    assert "0" in res["data"]
    assert res["data"]["0"]["head"] == kafka_msgs_tail_offset - kafka_msgs_count + 1
    assert res["data"]["0"]["tail"] == kafka_msgs_tail_offset + 12
    assert res["data"]["0"]["length"] == kafka_msgs_count + 12

    # 检查badmsg信息，当前为空的
    res = get("/v3/databus/rawdatas/%s/badmsg/" % rawdata_id)
    assert res["result"]
    assert len(res["data"]) == 0

    # mock对access的rawdata更新接口调用
    mocker.patch(
        "databus.api.AccessApi.rawdata.partial_update",
        return_value=DataResponse({"result": False}),
    )
    # 扩容rawdata对应的kafka分区数量
    res = post("/v3/databus/rawdatas/%s/set_partitions/" % rawdata_id, {"partitions": 3})
    assert res["result"]
    assert res["data"]["topic"] == ARGS["topic"]
    assert res["data"]["partitions"] == 3
    assert "error" in res["data"]

    # 检查分区数量已扩容为3
    res = get("/v3/databus/rawdatas/%s/partitions/" % rawdata_id)
    assert res["result"]
    assert res["data"] == 3

    # 检查rawdata存在，但是对应channel不存在的场景
    res = get("/v3/databus/rawdatas/%s/partitions/" % ARGS["rawdata_id2"])
    assert res["result"]
    assert res["data"] == 0

    # 修改kafka topic脚本地址，用于测试异常情况
    # 扩容rawdata对应的kafka分区数量
    res = post("/v3/databus/rawdatas/%s/set_partitions/" % rawdata_id, {"partitions": 6})
    assert not res["result"]
    assert res["code"] == "1570019"

    # 写入几条数据到rawdata对应的bad msg的topic中
    channel._send_message_to_kafka(
        settings.KAFKA_OP_HOST,
        "{}_{}".format(settings.CLEAN_BAD_MSG_TOPIC_PREFIX, rawdata_id),
        [
            {
                "key": "msgkey01",
                "value": '{"msgValue": "abcabc", "error": "abcabc::com.tencent.bk.base.datahub.databus.pipe.exception.BadJsonObjectError: '
                'Unrecognized token xxx", "dataId": 42, "timestamp": 1541661688, "partition": 0, "msgKey": '
                '"", "rtId": "591_clean1108", "offset": 39597}',
            },
            {
                "key": "msgkey02",
                "value": '{"msgValue": "2018011023023|serw|cxx", "error": "[null, null, null]::com.tencent.bk.base.datahub.databus.pipe.'
                'exception.TimeFormatError", "dataId": 42, "timestamp": 1541316742, "partition": 0, "msgKey": '
                '"", "rtId": "591_etl_pizza_van", "offset": 39601}',
            },
            {"key": "msgkey03", "value": "msg value 03"},
        ],
    )

    # 检查badmsg信息
    res = get("/v3/databus/rawdatas/%s/badmsg/" % rawdata_id)
    assert res["result"]
    assert len(res["data"]) == 2
    assert res["data"][0]["raw_data_id"] == 42
    assert res["data"][0]["result_table_id"] == "591_etl_pizza_van"
    assert res["data"][1]["offset"] == 39597
    assert res["data"][1]["result_table_id"] == "591_clean1108"

    # 检查message接口返回内容(offset越界)
    res = get("/v3/databus/rawdatas/{}/message/?offset={}".format(rawdata_id, kafka_msgs_tail_offset + 100))
    assert not res["result"]
    assert res["code"] == "1570017"
    assert res["message"] == u"kafka topic partition中offset不存在！TopicPartition(topic=u'test591', partition=0)-100"

    # patch 一些方法调用
    mocker.patch("databus.channel._get_topic_partitions", return_value=[])
    res = get("/v3/databus/rawdatas/%s/offsets/" % rawdata_id)
    assert not res["result"]
    assert res["code"] == "1570015"
    assert res["message"] == u"初始化数据kafka topic失败。test591 -> kafka:9092"

    res = get("/v3/databus/rawdatas/%s/partitions/" % rawdata_id)
    assert not res["result"]
    assert res["code"] == "1570015"
    assert res["message"] == u"初始化数据kafka topic失败。test591 -> kafka:9092"


@pytest.mark.httpretty
@pytest.mark.django_db
def test_rawdata_exceptions(mocker):
    res = get("/v3/databus/rawdatas/%s/daily_count/" % ARGS["rawdata_id"])
    assert res["result"]

    res = get("/v3/databus/rawdatas/%s/tail/" % ARGS["rawdata_id"])
    assert res["result"]
    assert res["data"] == []

    res = get("/v3/databus/rawdatas/%s/message/" % ARGS["rawdata_id"])
    assert res["result"]
    assert res["data"] == []

    res = get("/v3/databus/rawdatas/%s/offsets/" % ARGS["rawdata_id"])
    assert res["result"]
    assert res["data"] == {}

    res = get("/v3/databus/rawdatas/%s/partitions/" % ARGS["rawdata_id"])
    assert res["result"]
    assert res["data"] == 0

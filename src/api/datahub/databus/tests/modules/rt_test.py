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
from datahub.access.tests import db_helper
from datahub.access.tests.utils import get, post
from datahub.databus.exceptions import RtKafkaStorageNotExistError
from datahub.databus.tests.fixture.rt_fixture import ARGS, add_result_table  # noqa

from datahub.databus import channel, rt, settings

# 设置zk的地址信息，清理数据
settings.KAFKA_OP_HOST = "kafka:9092"
settings.CONFIG_ZK_ADDR = "zookeeper:2181"


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_result_table")
def test_result_table_basic(mocker):
    # patch调用meta result table的接口请求
    mocker.patch(
        "databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(
            {
                "result": True,
                "data": {
                    "project_id": 4,
                    "result_table_id": ARGS["result_table_id"],
                    "project_name": "单元测试项目",
                    "count_freq": 0,
                    "description": "清洗测试",
                    "bk_biz_id": 591,
                    "sensitivity": "public",
                    "result_table_name_alias": "清洗测试",
                    "storages": {
                        "tredis": {
                            "storage_channel": {},
                            "description": "DataFlow",
                            "cluster_type": None,
                            "expires": "7d",
                            "storage_config": "{}",
                            "storage_cluster": {
                                "priority": 0,
                                "cluster_domain": "localhost",
                                "connection_info": "{}",
                                "cluster_type": "tredis",
                                "cluster_name": "tredis-test",
                                "cluster_group": "tredis",
                            },
                            "physical_table_name": "591_new_xxx",
                        },
                        "kafka": {
                            "storage_channel": {
                                "zk_port": 2191,
                                "id": ARGS["inner_channel_id"],
                                "cluster_name": "testinner",
                                "cluster_type": "kafka",
                                "cluster_role": "inner",
                                "priority": 0,
                                "cluster_domain": "x.x.x.x",
                                "cluster_port": 9092,
                                "zk_domain": "x.x.x.x",
                                "zk_root_path": "/",
                            },
                            "expires": "3",
                            "priority": 5,
                            "storage_config": "{}",
                            "physical_table_name": "table_591_new_xxx",
                            "storage_cluster": {},
                        },
                        "mysql": {
                            "storage_channel": {},
                            "description": "DataFlow",
                            "cluster_type": None,
                            "expires": "3d",
                            "priority": 0,
                            "storage_config": "{}",
                            "storage_cluster": {
                                "priority": 0,
                                "cluster_domain": "localhost",
                                "connection_info": "{}",
                                "cluster_type": "mysql",
                                "cluster_name": "mysql-test",
                                "belongs_to": "bkdata",
                                "version": "2.0.0",
                                "cluster_group": "mysql",
                            },
                            "physical_table_name": "mapleleaf_591.new_xxx_591",
                        },
                        "tsdb": {
                            "storage_channel": {},
                            "description": "DataFlow",
                            "cluster_type": None,
                            "expires": "3d",
                            "priority": 0,
                            "storage_config": '{"dim_fields": ["gseindex", "ip"]}',
                            "storage_cluster": {
                                "priority": 0,
                                "cluster_domain": "localhost",
                                "connection_info": "{}",
                                "cluster_type": "tsdb",
                                "cluster_name": "tsdb-test",
                                "belongs_to": "bkdata",
                                "version": "2.0.0",
                                "cluster_group": "tsdb",
                            },
                            "physical_table_name": "mapleleaf_591.new_xxx_591",
                        },
                    },
                    "fields": [
                        {
                            "field_type": "timestamp",
                            "is_dimension": False,
                            "field_name": "timestamp",
                            "field_alias": "内部时间字段",
                            "field_index": 0,
                        },
                        {
                            "field_type": "string",
                            "is_dimension": False,
                            "field_name": "ip",
                            "field_alias": "ip",
                            "field_index": 1,
                        },
                        {
                            "field_type": "string",
                            "is_dimension": False,
                            "field_name": "report_time",
                            "field_alias": "report_time",
                            "field_index": 2,
                        },
                        {
                            "field_type": "long",
                            "is_dimension": False,
                            "field_name": "gseindex",
                            "field_alias": "gseindex",
                            "field_index": 3,
                        },
                        {
                            "field_type": "string",
                            "is_dimension": True,
                            "field_name": "path",
                            "field_alias": "path",
                            "field_index": 4,
                        },
                        {
                            "field_type": "string",
                            "is_dimension": False,
                            "field_name": "log",
                            "field_alias": "log",
                            "field_index": 5,
                        },
                    ],
                    "processing_type": "clean",
                    "result_table_name": ARGS["resut_table_name"],
                },
            }
        ),
    )

    # 调用retrieve接口验证数据
    res = get("/v3/databus/result_tables/%s/" % ARGS["result_table_id"])
    assert res["result"]
    assert res["data"]["bk_biz_id"] == 591
    assert res["data"]["etl.id"] == 6
    assert res["data"]["dimensions"] == "gseindex,ip"
    assert res["data"]["msg.type"] == "avro"
    assert res["data"]["channel_cluster_index"] == ARGS["inner_channel_id"]
    assert res["data"]["bootstrap.servers"] == ARGS["kafka_bs"]
    assert res["data"]["data.id"] == ARGS["rawdata_id"]
    assert res["data"]["rt.id"] == ARGS["result_table_id"]
    assert res["data"]["table_name"] == ARGS["resut_table_name"]
    assert (
        res["data"]["columns"]
        == "gseindex=long,ip=string,log=string,path=string,report_time=string,timestamp=timestamp"
    )
    assert "kafka" in res["data"]["storages.list"]
    assert "mysql" in res["data"]["storages.list"]
    assert "tredis" in res["data"]["storages.list"]
    assert "tsdb" in res["data"]["storages.list"]

    # patch调用meta result table的接口请求，非清洗rt
    mocker.patch(
        "databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(
            {
                "result": True,
                "data": {
                    "project_id": 4,
                    "result_table_id": ARGS["result_table_id"],
                    "project_name": "单元测试项目",
                    "count_freq": 0,
                    "description": "test",
                    "bk_biz_id": 591,
                    "sensitivity": "public",
                    "result_table_name_alias": "test",
                    "storages": {},
                    "fields": [
                        {
                            "field_type": "timestamp",
                            "is_dimension": False,
                            "field_name": "timestamp",
                            "field_alias": "内部时间字段",
                            "field_index": 0,
                        },
                        {
                            "field_type": "string",
                            "is_dimension": False,
                            "field_name": "ip",
                            "field_alias": "ip",
                            "field_index": 1,
                        },
                    ],
                    "processing_type": "batch",
                    "result_table_name": ARGS["resut_table_name"],
                },
            }
        ),
    )

    # 调用retrieve接口验证数据，由于kafka channel不存在，会抛出异常
    # 调用get_rt_info接口验证数据
    res = get("/v3/databus/result_tables/get_rt_info/?rt_id=%s" % ARGS["result_table_id"])
    assert res["result"]
    assert res["data"]["bk_biz_id"] == 591
    assert res["data"]["etl.id"] == ""
    assert res["data"]["data.id"] == ""
    assert res["data"]["etl.conf"] == ""
    assert res["data"]["rt.id"] == ARGS["result_table_id"]
    assert res["data"]["table_name"] == ARGS["resut_table_name"]
    assert res["data"]["columns"] == "ip=string,timestamp=timestamp"

    # rt的存储信息，其中kafka指向testinner集群
    mocker.patch(
        "databus.api.MetaApi.result_tables.storages",
        return_value=DataResponse(
            {
                "result": True,
                "data": {
                    "kafka": {
                        "storage_channel": {
                            "zk_port": 2181,
                            "id": ARGS["inner_channel_id"],
                            "cluster_name": "testinner",
                            "cluster_type": "kafka",
                            "cluster_role": "inner",
                            "priority": 5,
                            "cluster_domain": "kafka",
                            "cluster_port": 9092,
                            "zk_domain": "zookeeper",
                            "zk_root_path": "/",
                        }
                    }
                },
            }
        ),
    )
    # 调用destinations接口验证数据
    res = get("/v3/databus/result_tables/%s/destinations/" % ARGS["result_table_id"])
    assert res["result"]
    assert len(res["data"]) == 1
    assert "kafka" in res["data"]

    # 调用notify_change接口验证数据
    res = get("/v3/databus/result_tables/%s/notify_change/" % ARGS["result_table_id"])
    assert res["result"]
    assert res["data"]

    # 调用is_batch_data接口验证数据，因为清洗rt对应的rawdata非tdw数据导入场景，返回结果为False
    res = get("/v3/databus/result_tables/%s/is_batch_data/" % ARGS["result_table_id"])
    assert res["result"]
    assert not res["data"]


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_result_table")
def test_set_topic(mocker):
    result_table_id = ARGS["result_table_id"]
    # patch调用meta result table的接口请求
    mocker.patch(
        "databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(
            {
                "result": True,
                "data": {
                    "result_table_id": ARGS["result_table_id"],
                    "processing_type": "clean",
                    "storages": {
                        "kafka": {
                            "storage_channel": {
                                "zk_port": 2181,
                                "id": ARGS["inner_channel_id"],
                                "cluster_name": "testinner",
                                "cluster_type": "kafka",
                                "cluster_role": "inner",
                                "priority": 5,
                                "cluster_domain": "kafka",
                                "cluster_port": 9092,
                                "zk_domain": "zookeeper",
                                "zk_root_path": "/",
                            }
                        }
                    },
                },
            }
        ),
    )

    res = post(
        "/v3/databus/result_tables/%s/set_topic/" % result_table_id,
        {
            "retention_hours": 168,
            "retention_size_g": 10,
        },
    )

    assert res["result"]


@pytest.mark.httpretty
@pytest.mark.django_db
@pytest.mark.usefixtures("add_result_table")
def test_result_table_kafka(mocker):
    result_table_id = ARGS["result_table_id"]

    # rt的存储信息，其中kafka指向testinner集群
    mocker.patch(
        "databus.api.MetaApi.result_tables.storages",
        return_value=DataResponse(
            {
                "result": True,
                "data": {
                    "kafka": {
                        "storage_channel": {
                            "zk_port": 2181,
                            "id": ARGS["inner_channel_id"],
                            "cluster_name": "testinner",
                            "cluster_type": "kafka",
                            "cluster_role": "inner",
                            "priority": 5,
                            "cluster_domain": "kafka",
                            "cluster_port": 9092,
                            "zk_domain": "zookeeper",
                            "zk_root_path": "/",
                        }
                    }
                },
            }
        ),
    )

    # 检查分区数量（默认topic不存在，调用接口后系统自动创建，并生成了一条空的消息）
    res = get("/v3/databus/result_tables/%s/partitions/" % result_table_id)
    assert res["result"]
    assert res["data"] == 1

    # 检查offsets信息（默认包含一个分区、一条消息）
    res = get("/v3/databus/result_tables/%s/offsets/" % result_table_id)
    assert res["result"]
    assert res["data"]["0"]["length"] == 1
    kafka_msgs_count = res["data"]["0"]["length"]
    kafka_msgs_tail_offset = res["data"]["0"]["tail"]

    # 写入几条数据（kafka inner中数据格式为avro格式，且一条kafka消息中包含多条数据）
    f = open("databus/tests/fixture/test_record.avro", "r")
    msg = f.read()
    f.close()

    channel._send_message_to_kafka(
        ARGS["kafka_bs"],
        ARGS["inner_topic"],
        [{"key": "msgkey01", "value": msg}, {"key": "msgkey02", "value": msg}],
    )

    # 检查tail接口返回内容
    res = get("/v3/databus/result_tables/%s/tail/" % result_table_id)
    assert res["result"]
    assert len(res["data"]) == 100
    assert res["data"][0]["report_time"] == "2018-11-01 10:10:08"
    assert "ip" in res["data"][0]
    assert res["data"][0]["gseindex"] == 73746
    assert res["data"][0]["path"] == "/var/log/gse/agent-20181101-00156.log"
    assert res["data"][0]["timestamp"] == 1541038208
    assert res["data"][0]["dtEventTimeStamp"] == 1541038208000
    assert res["data"][0]["dtEventTime"] == "2018-11-01 10:10:08"
    assert res["data"][0]["localTime"] == "2018-11-06 20:41:00"

    # 检查通过channel_views中的tail方法可以获取到同样的数据，并验证结果
    res = get(
        "/v3/databus/channels/tail/?kafka={}&topic={}&partitions=0&type=avro".format(
            ARGS["kafka_bs"], ARGS["inner_topic"]
        )
    )
    assert res["result"]
    assert len(res["data"]) == 100
    assert res["data"][0]["report_time"] == "2018-11-01 10:10:08"
    assert "ip" in res["data"][0]
    assert res["data"][0]["gseindex"] == 73746
    assert res["data"][0]["path"] == "/var/log/gse/agent-20181101-00156.log"
    assert res["data"][0]["timestamp"] == 1541038208
    assert res["data"][0]["dtEventTimeStamp"] == 1541038208000
    assert res["data"][0]["dtEventTime"] == "2018-11-01 10:10:08"
    assert res["data"][0]["localTime"] == "2018-11-06 20:41:00"

    # 检查message接口返回内容
    res = get("/v3/databus/result_tables/{}/message/?offset={}".format(result_table_id, kafka_msgs_tail_offset + 1))
    assert res["result"]
    assert len(res["data"]) == 1
    assert res["data"][0]["partition"] == 0
    assert len(res["data"][0]["value"]) == 3
    assert len(res["data"][0]["value"]["_value_"]) == 100
    assert res["data"][0]["value"]["_tagTime_"] == 1541508060
    assert res["data"][0]["key"] == "msgkey01"
    assert res["data"][0]["topic"] == ARGS["inner_topic"]
    assert res["data"][0]["offset"] == kafka_msgs_tail_offset + 1

    # 检查通过channel_views中的message接口获取消息内容
    res = get(
        "/v3/databus/channels/message/?kafka=%s&topic=%s&partitions=0&type=avro&offset=%s"
        % (ARGS["kafka_bs"], ARGS["inner_topic"], kafka_msgs_tail_offset + 1)
    )
    assert res["result"]
    assert len(res["data"]) == 1
    assert res["data"][0]["partition"] == 0
    assert len(res["data"][0]["value"]) == 3
    assert len(res["data"][0]["value"]["_value_"]) == 100
    assert res["data"][0]["value"]["_tagTime_"] == 1541508060
    assert res["data"][0]["key"] == "msgkey01"
    assert res["data"][0]["topic"] == ARGS["inner_topic"]
    assert res["data"][0]["offset"] == kafka_msgs_tail_offset + 1

    # 检查offsets信息
    res = get("/v3/databus/result_tables/%s/offsets/" % result_table_id)
    assert res["result"]
    assert "0" in res["data"]
    assert res["data"]["0"]["head"] == kafka_msgs_tail_offset - kafka_msgs_count + 1
    assert res["data"]["0"]["tail"] == kafka_msgs_tail_offset + 2
    assert res["data"]["0"]["length"] == kafka_msgs_count + 2

    # 检查通过channel_views中的message接口获取offsets信息
    res = get("/v3/databus/channels/offsets/?kafka={}&topic={}".format(ARGS["kafka_bs"], ARGS["inner_topic"]))
    assert res["result"]
    assert "0" in res["data"]
    assert res["data"]["0"]["head"] == kafka_msgs_tail_offset - kafka_msgs_count + 1
    assert res["data"]["0"]["tail"] == kafka_msgs_tail_offset + 2
    assert res["data"]["0"]["length"] == kafka_msgs_count + 2

    # patch调用meta result table的接口请求
    mocker.patch(
        "databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(
            {
                "result": True,
                "data": {
                    "result_table_id": ARGS["result_table_id"],
                    "processing_type": "clean",
                    "storages": {
                        "kafka": {
                            "storage_channel": {
                                "zk_port": 2181,
                                "id": ARGS["inner_channel_id"],
                                "cluster_name": "testinner",
                                "cluster_type": "kafka",
                                "cluster_role": "inner",
                                "priority": 5,
                                "cluster_domain": "kafka",
                                "cluster_port": 9092,
                                "zk_domain": "zookeeper",
                                "zk_root_path": "/",
                            }
                        }
                    },
                },
            }
        ),
    )
    # 扩容result_table对应的kafka分区数量
    res = post(
        "/v3/databus/result_tables/%s/set_partitions/" % result_table_id,
        {"partitions": 3},
    )
    assert res["result"]
    assert res["data"]["topic"] == ARGS["inner_topic"]
    assert res["data"]["partitions"] == 3

    # 扩容result_table对应的kafka分区数量，超过50个分区时，参数校验失败
    res = post(
        "/v3/databus/result_tables/%s/set_partitions/" % result_table_id,
        {"partitions": 60},
    )
    assert res["result"] is False
    assert res["code"] == "1500001"
    assert res["message"] == u"参数校验错误"

    # 扩容result_table对应的kafka分区数量
    res = post(
        "/v3/databus/result_tables/%s/set_partitions/" % result_table_id,
        {"partitions": 6},
    )
    assert not res["result"]
    assert res["code"] == "1570019"

    # 检查分区数量已扩容为3
    res = get("/v3/databus/result_tables/%s/partitions/" % result_table_id)
    assert res["result"]
    assert res["data"] == 3

    # 检查每天数据量（对清洗的rt，检查的是数据源rawdata的数据量）
    res = get("/v3/databus/result_tables/%s/daily_count/" % result_table_id)
    assert res["result"]
    assert res["data"] == 0

    # 删除clean中的记录，将rt标记为非清洗的rt
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            """
            DELETE FROM databus_clean_info
            """,
        )
    # 检查每天数据量（对非清洗的rt，检查的是kafka inner中topic里的数据量）
    res = get("/v3/databus/result_tables/%s/daily_count/" % result_table_id)
    assert res["result"]
    assert res["data"] >= 200

    rt_id_list = [ARGS["result_table_id"]]
    res = rt.get_channel_id_for_rt_list(rt_id_list)
    assert res["591_new_xxx"] == 1001

    # 将rt中kafka的存储去掉，验证抛出异常
    mocker.patch(
        "databus.api.MetaApi.result_tables.storages",
        return_value=DataResponse({"result": True, "data": {}}),
    )
    test = None
    try:
        test = rt.get_channel_id_for_rt_list(rt_id_list)
    except Exception as e:
        assert test is None
        assert e.__class__ == RtKafkaStorageNotExistError

    # 不存在kafka存储时，tail接口返回空数组
    res = get("/v3/databus/result_tables/%s/tail/" % result_table_id)
    assert res["result"]
    assert len(res["data"]) == 0

    # 不存在kafka存储时，获取分区数量接口返回0
    res = get("/v3/databus/result_tables/%s/partitions/" % result_table_id)
    assert res["result"]
    assert res["data"] == 0


@pytest.mark.httpretty
@pytest.mark.django_db
def test_exceptions(mocker):
    result_table_id = "591_bad_rt"
    # 测试rt对应的kafka存储不存在时，返回的分区数量

    settings.CONFIG_ZK_ADDR = "zookeeper:1000"  # 设置错误的zk地址，验证notify_change失败
    res = get("/v3/databus/result_tables/%s/notify_change/" % result_table_id)
    assert res["result"]
    assert not res["data"]
    settings.CONFIG_ZK_ADDR = "zookeeper:2181"  # 重置zk的地址

    # 调用is_batch_data接口验证数据，因为rt非清洗rt，返回结果为False
    res = get("/v3/databus/result_tables/%s/is_batch_data/" % result_table_id)
    assert res["result"]
    assert not res["data"]

    # rt的存储信息，其中kafka信息为空
    mocker.patch(
        "databus.api.MetaApi.result_tables.storages",
        return_value=DataResponse({"result": False, "data": {}}),
    )

    # 检查分区数量，由于rt没有kafka存储，返回分区数量为0
    res = get("/v3/databus/result_tables/%s/partitions/" % result_table_id)
    assert res["result"] is False
    assert res["code"] == "1570037"
    assert res["message"] == u"获取ResultTable的存储信息失败。None"

    res = get("/v3/databus/result_tables/%s/tail/" % result_table_id)
    assert res["result"] is False
    assert res["code"] == "1570037"
    assert res["message"] == u"获取ResultTable的存储信息失败。None"

    # 设置rt存储包含tsdb，但不包含kafka
    mocker.patch(
        "databus.api.MetaApi.result_tables.storages",
        return_value=DataResponse({"result": True, "data": {"tsdb": {}}}),
    )

    res = get("/v3/databus/result_tables/%s/message/" % result_table_id)
    assert res["result"]
    assert res["data"] == []

    res = get("/v3/databus/result_tables/%s/offsets/" % result_table_id)
    assert res["result"]
    assert res["data"] == {}

    # rt的信息，其中接口返回异常
    mocker.patch(
        "databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse({"result": False, "data": {}}),
    )

    res = post(
        "/v3/databus/result_tables/%s/set_partitions/" % result_table_id,
        {"partitions": 3},
    )
    assert not res["result"]
    assert res["code"] == "1500003"

    # rt的信息，其中接口返回正常，但是不包含kafka存储信息
    mocker.patch(
        "databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse({"result": True, "data": {"storages": {"tsdb": {}}}}),
    )

    res = post(
        "/v3/databus/result_tables/%s/set_partitions/" % result_table_id,
        {"partitions": 3},
    )
    assert not res["result"]
    assert res["code"] == "1570018"

    # patch调用meta result table的接口请求
    mocker.patch(
        "databus.api.MetaApi.result_tables.retrieve",
        return_value=DataResponse(
            {
                "result": True,
                "data": {
                    "project_id": 4,
                    "result_table_id": ARGS["result_table_id"],
                    "project_name": "单元测试项目",
                    "count_freq": 0,
                    "description": "test",
                    "bk_biz_id": 591,
                    "sensitivity": "public",
                    "result_table_name_alias": "test",
                    "storages": {
                        "kafka": {
                            "storage_channel": {
                                "zk_port": 2191,
                                "id": ARGS["inner_channel_id"],
                                "cluster_name": "testinner",
                                "cluster_type": "kafka",
                                "cluster_role": "inner",
                                "priority": 0,
                                "cluster_domain": "x.x.x.x",
                                "cluster_port": 9092,
                                "zk_domain": "x.x.x.x",
                                "zk_root_path": "/",
                            },
                            "expires": "3",
                            "priority": 5,
                            "storage_config": "{}",
                            "physical_table_name": "table_591_new_xxx",
                            "storage_cluster": {},
                        }
                    },
                    "fields": [
                        {
                            "field_type": "timestamp",
                            "is_dimension": False,
                            "field_name": "timestamp",
                            "field_alias": "内部时间字段",
                            "field_index": 0,
                        },
                        {
                            "field_type": "string",
                            "is_dimension": False,
                            "field_name": "ip",
                            "field_alias": "ip",
                            "field_index": 1,
                        },
                    ],
                    "processing_type": "batch",
                    "result_table_name": ARGS["resut_table_name"],
                },
            }
        ),
    )

    # 调用retrieve接口验证数据，由于kafka channel不存在，会抛出异常
    res = get("/v3/databus/result_tables/%s/" % ARGS["result_table_id"])
    assert not res["result"]
    assert res["code"] == "1570003"

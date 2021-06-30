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
from datahub.access.tests import db_helper

from datahub.databus import channel

# 测试用变量
ARGS = {
    "outer_channel_id": 1000,
    "inner_channel_id": 1001,
    "inner2_channel_id": 1002,
    "cluster_domain": "kafka",
    "cluster_port": 9092,
    "kafka_bs": "{}:{}".format("kafka", 9092),
    "rawdata_id": 123,
    "raw_data_name": "test",
    "bk_biz_id": 591,
    "outer_topic": "{}{}".format("new_xxx", 591),
    "resut_table_name": "new_xxx",
    "result_table_id": "{}_{}".format(591, "new_xxx"),
    "inner_topic": "table_{}_{}".format(591, "new_xxx"),
    "clean_id": 6,
    "processing_id": "{}_{}".format(591, "new_xxx"),
    "json_config": '{"extract": {"method": "from_json", "label": "labelomplc", "next": {"type": "branch", "name": "",'
    ' "next": [{"key": "_value_", "label": "labelaqvgj", "next": {"method": "iterate", "label": '
    '"labelilpme", "next": {"next": null, "type": "assign", "label": "labelhwbsp", "subtype": '
    '"assign_pos", "assign": [{"assign_to": "log", "index": 0, "type": "string"}]}, "result": '
    '"labelilpme", "args": ["labelilpme"], "type": "fun"}, "subtype": "access_obj", "result": '
    '"labelaqvgj", "type": "access"}, {"next": null, "type": "assign", "label": "labelwnplb", '
    '"subtype": "assign_obj", "assign": [{"assign_to": "ip", "type": "string", "key": "_server_"}, '
    '{"assign_to": "report_time", "type": "string", "key": "_time_"}, {"assign_to": "gseindex", "type":'
    ' "long", "key": "_gseindex_"}, {"assign_to": "path", "type": "string", "key": "_path_"}]}], '
    '"label": null}, "result": "labelomplc", "args": [], "type": "fun"}, "conf": {"output_field_name":'
    ' "timestamp", "timezone": 8, "timestamp_len": 0, "time_format": "yyyy-MM-dd HH:mm:ss", "encoding": '
    '"UTF8", "time_field_name": "report_time"}}',
}


@pytest.fixture
def add_result_table():
    with db_helper.open_cursor("mapleleaf") as cur:
        # 首先添加一条outer channel记录，用来指向kafka
        db_helper.insert(
            cur,
            "databus_channel_cluster_config",
            cluster_name="testouter",
            cluster_type="kafka",
            cluster_role="outer",
            cluster_domain=ARGS["cluster_domain"],
            cluster_backup_ips="",
            cluster_port=ARGS["cluster_port"],
            zk_domain="zookeeper",
            zk_port="2181",
            zk_root_path="/",
            active=True,
            priority=1,
            attribute="bkdata",
            created_by="",
            updated_by="",
            description="",
            id=ARGS["outer_channel_id"],
        )

    with db_helper.open_cursor("mapleleaf") as cur:
        # 首先添加一条inner channel记录，用来指向kafka
        db_helper.insert(
            cur,
            "databus_channel_cluster_config",
            cluster_name="testinner",
            cluster_type="kafka",
            cluster_role="inner",
            cluster_domain=ARGS["cluster_domain"],
            cluster_backup_ips="",
            cluster_port=ARGS["cluster_port"],
            zk_domain="zookeeper",
            zk_port="2181",
            zk_root_path="/",
            active=True,
            priority=5,
            attribute="bkdata",
            created_by="",
            updated_by="",
            description="",
            id=ARGS["inner_channel_id"],
        )

    with db_helper.open_cursor("mapleleaf") as cur:
        # 首先添加另一条inner channel记录（priority不一样），用来指向kafka
        db_helper.insert(
            cur,
            "databus_channel_cluster_config",
            cluster_name="testinner2",
            cluster_type="kafka",
            cluster_role="inner",
            cluster_domain=ARGS["cluster_domain"],
            cluster_backup_ips="",
            cluster_port=ARGS["cluster_port"],
            zk_domain="zookeeper",
            zk_port="2181",
            zk_root_path="/",
            active=True,
            priority=2,
            attribute="bkdata",
            created_by="",
            updated_by="",
            description="",
            id=ARGS["inner2_channel_id"],
        )

    with db_helper.open_cursor("mapleleaf") as cur:
        # 然后在rawdata中增加一条数据接入的记录
        db_helper.insert(
            cur,
            "access_raw_data",
            id=ARGS["rawdata_id"],
            bk_biz_id=ARGS["bk_biz_id"],
            raw_data_name=ARGS["raw_data_name"],
            raw_data_alias="for test",
            sensitivity="public",
            data_source="log",
            data_encoding="UTF8",
            data_category="log",
            data_scenario="log",
            bk_app_code="data",
            storage_channel_id=ARGS["outer_channel_id"],
            storage_partitions=3,
            created_by="",
            updated_by="",
            description="",
        )

    with db_helper.open_cursor("mapleleaf") as cur:
        # 然后在clean_info中增加一条清洗配置
        db_helper.insert(
            cur,
            "databus_clean_info",
            id=ARGS["clean_id"],
            processing_id=ARGS["processing_id"],
            raw_data_id=ARGS["rawdata_id"],
            pe_config="",
            json_config=ARGS["json_config"],
            status="started",
            created_by="",
            updated_by="",
            clean_config_name="",
            clean_result_table_name="",
            clean_result_table_name_alias="",
            description="",
        )

    try:
        yield 1
    finally:
        _delete_result_table(ARGS["inner_channel_id"], ARGS["inner_topic"])


def _delete_result_table(channel_id, rawdata_topic):
    # 删除kafka上rawdata对应的topic
    channel.delete_kafka_topic(channel_id, rawdata_topic)

    # 删除rawdata中的记录
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            """
            DELETE FROM access_raw_data
            """,
        )
    # 删除channel中的记录
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            """
            DELETE FROM databus_channel_cluster_config
            """,
        )
    # 删除clean中的记录
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            """
            DELETE FROM databus_clean_info
            """,
        )

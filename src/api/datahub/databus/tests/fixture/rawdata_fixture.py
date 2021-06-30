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

from datahub.databus import channel, settings

# 测试用变量
ARGS = {
    "channel_id": 1000,
    "clean_id": 6,
    "processing_id": "591_new_van",
    "cluster_domain": "kafka",
    "json_config": '{"abc": "saa"}',
    "cluster_port": 9092,
    "kafka_bs": "{}:{}".format("kafka", 9092),
    "rawdata_id": 123,
    "raw_data_name": "test",
    "rawdata_id2": 124,
    "raw_data_name2": "test2",
    "bk_biz_id": 591,
    "topic": "{}{}".format("test", 591),
}


@pytest.fixture
def add_rawdata():
    with db_helper.open_cursor("mapleleaf") as cur:
        # 首先添加一条channel记录，用来指向kafka
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
            id=ARGS["channel_id"],
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
            storage_channel_id=ARGS["channel_id"],
            storage_partitions=1,
            created_by="",
            updated_by="",
            description="",
        )

    with db_helper.open_cursor("mapleleaf") as cur:
        # 然后在rawdata中增加一条数据接入的记录
        db_helper.insert(
            cur,
            "access_raw_data",
            id=ARGS["rawdata_id2"],
            bk_biz_id=ARGS["bk_biz_id"],
            raw_data_name=ARGS["raw_data_name2"],
            raw_data_alias="for test",
            sensitivity="public",
            data_source="log",
            data_encoding="UTF8",
            data_category="log",
            data_scenario="log",
            bk_app_code="data",
            storage_channel_id=1,
            storage_partitions=1,
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

    # 初始化rawdata对应的kafka的topic
    channel.create_kafka_topic(ARGS["kafka_bs"], ARGS["topic"])

    try:
        yield 1
    finally:
        _delete_rawdata(
            ARGS["channel_id"],
            ARGS["topic"],
            "{}_{}".format(settings.CLEAN_BAD_MSG_TOPIC_PREFIX, ARGS["rawdata_id"]),
        )


def _delete_rawdata(channel_id, topic, badmsg_topic):
    # 删除kafka上rawdata对应的topic
    channel.delete_kafka_topic(channel_id, topic)

    # 删除rawdata对应的bad msg的topic
    channel.delete_kafka_topic(channel_id, badmsg_topic)
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

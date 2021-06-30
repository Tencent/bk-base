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


@pytest.fixture
def add_shipper_scenario_data():

    with db_helper.open_cursor("mapleleaf") as cur:

        db_helper.insert(
            cur,
            "databus_clean_info",
            processing_id="591_no_raw_data",
            raw_data_id=100,
            pe_config="",
            json_config="{}",
            status="stopped",
            created_by="",
            updated_by="",
            description="",
            clean_config_name="",
            clean_result_table_name="",
            clean_result_table_name_alias="",
            id=1000,
        )

        db_helper.insert(
            cur,
            "databus_clean_info",
            processing_id="591_with_raw_data",
            raw_data_id=101,
            pe_config="",
            json_config="{}",
            status="stopped",
            created_by="",
            updated_by="",
            description="",
            clean_config_name="",
            clean_result_table_name="",
            clean_result_table_name_alias="",
            id=1001,
        )

        db_helper.insert(
            cur,
            "access_raw_data",
            id=101,
            bk_biz_id=591,
            raw_data_name="raw_test",
            raw_data_alias="",
            sensitivity="",
            data_source="",
            data_encoding="gbk",
            data_category="",
            data_scenario="tdw",
            bk_app_code="",
            storage_channel_id=10,
            maintainer="",
            description="",
        )

        db_helper.insert(
            cur,
            "databus_channel_cluster_config",
            cluster_name="outer",
            cluster_type="kafka",
            cluster_role="outer",
            cluster_domain="kafka.domain1",
            cluster_backup_ips="",
            cluster_port=9092,
            zk_domain="zk.domain1",
            zk_port="2181",
            zk_root_path="/",
            active=True,
            priority=0,
            attribute="bkdata",
            created_by="",
            updated_by="",
            description="",
            id=10,
        )

        db_helper.insert(
            cur,
            "databus_channel_cluster_config",
            cluster_name="inner",
            cluster_type="kafka",
            cluster_role="inner",
            cluster_domain="kafka.domain1",
            cluster_backup_ips="",
            cluster_port=9092,
            zk_domain="zk.domain1",
            zk_port="2181",
            zk_root_path="/",
            active=True,
            priority=0,
            attribute="bkdata",
            created_by="",
            updated_by="",
            description="",
            id=11,
        )

    try:
        yield 1
    finally:
        _delete_shipper_scenario_data()


def _delete_shipper_scenario_data():
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            """
        DELETE FROM databus_clean_info
        """,
        )

    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            """
        DELETE FROM access_raw_data
        """,
        )

    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            """
        DELETE FROM databus_channel_cluster_config
        """,
        )

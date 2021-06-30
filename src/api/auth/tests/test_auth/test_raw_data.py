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

from unittest import mock

from auth.constants import SubjectTypeChoices
from auth.handlers.raw_data import RawDataHandler
from auth.models.auth_models import AuthResourceGroupRecord
from auth.tests.utils import BaseTestCase
from common.api.base import DataResponse

RAW_DATA_ID = 1

RAW_DATA = DataResponse(
    {
        "data": {
            "bk_biz_id": 591,
            "created_at": "2018-10-22T16:59:12",
            "data_source": "svr",
            "maintainer": "",
            "updated_by": None,
            "raw_data_name": "log_admin_test",
            "topic": "log_admin_test591",
            "sensitivity": "private",
            "storage_channel_id": None,
            "data_encoding": "UTF-8",
            "raw_data_alias": "测试log接入",
            "updated_at": None,
            "bk_app_code": "bk_data",
            "data_scenario": "log",
            "created_by": "",
            "data_category": "online",
            "id": 58,
            "description": "",
            "tags": {"manage": {"geog_area": [{"code": "NA", "alias": "北美"}]}},
        },
        "result": True,
        "message": "",
        "code": 1500201,
        "errors": None,
    }
)

CLUSTER_GROUP_CONFIGS = DataResponse(
    {
        "data": [
            {
                "cluster_group_id": "mysql_lol_1",
                "cluster_group_name": "aiops",
                "cluster_group_alias": "\\u667a\\u80fd\\u8fd0\\u8425AIOPS",
                "scope": "private",
                "created_by": "bkdata",
                "created_at": "2019-05-06 15:44:34",
                "updated_by": None,
                "updated_at": None,
                "description": None,
                "tags": {"manage": {"geog_area": [{"code": "NA", "alias": "北美"}]}},
            },
            {
                "cluster_group_id": "tredis_lol_1",
                "cluster_group_name": "analysis",
                "cluster_group_alias": "\\u6570\\u636e\\u5206\\u6790\\u9879\\u76ee",
                "scope": "private",
                "created_by": "bkdata",
                "created_at": "2019-05-06 15:44:34",
                "updated_by": None,
                "updated_at": None,
                "description": None,
                "tags": {"manage": {"geog_area": [{"code": "mainland", "alias": "中国大陆"}]}},
            },
            {
                "cluster_group_id": "mysql_lol_2",
                "cluster_group_name": "analysis",
                "cluster_group_alias": "\\u6570\\u636e\\u5206\\u6790\\u9879\\u76ee",
                "scope": "private",
                "created_by": "bkdata",
                "created_at": "2019-05-06 15:44:34",
                "updated_by": None,
                "updated_at": None,
                "description": None,
                "tags": {"manage": {"geog_area": [{"code": "NA", "alias": "北美"}]}},
            },
            {
                "cluster_group_id": "public_group",
                "cluster_group_name": "analysis",
                "cluster_group_alias": "\\u6570\\u636e\\u5206\\u6790\\u9879\\u76ee",
                "scope": "public",
                "created_by": "bkdata",
                "created_at": "2019-05-06 15:44:34",
                "updated_by": None,
                "updated_at": None,
                "description": None,
                "tags": {"manage": {"geog_area": [{"code": "NA", "alias": "北美"}]}},
            },
        ],
        "result": True,
        "message": "",
        "code": 1500200,
        "errors": None,
    }
)


class RawDataClusterGroupTestCase(BaseTestCase):
    @mock.patch("auth.api.AccessApi.get_raw_data", lambda return_value: RAW_DATA)
    @mock.patch("auth.api.MetaApi.cluster_group_configs", lambda return_value: CLUSTER_GROUP_CONFIGS)
    def test_query_cluster_group(self):
        AuthResourceGroupRecord.objects.create(
            subject_type=SubjectTypeChoices.RAWDATA,
            subject_id=RAW_DATA_ID,
            resource_group_id="mysql_lol_2",
            created_by="admin",
        )

        AuthResourceGroupRecord.objects.create(
            subject_type=SubjectTypeChoices.BIZ, subject_id="591", resource_group_id="mysql_lol_2", created_by="admin"
        )

        groups = RawDataHandler(raw_data_id=RAW_DATA_ID).list_cluster_group()

        group_ids = [group["cluster_group_id"] for group in groups]
        self.assertTrue("mysql_lol_2" in group_ids)
        self.assertTrue("mysql_lol_1" in group_ids)
        self.assertTrue("tredis_lol_1" in group_ids)

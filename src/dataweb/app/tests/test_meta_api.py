# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""
import json

import requests_mock

from apps.api import MetaApi

from tests.test_base import BaseTestCase


class TestIeodMetaAPI(BaseTestCase):
    def test_result_tables(self):
        data = []
        for _ in range(4):
            fdata = {
                "bk_biz_id": self.cn_faker.random_number(digits=6),
                "count_freq_unit": self.en_faker.word(),
                "result_table_name_alias": self.cn_faker.word(),
                "project_id": self.cn_faker.random_digit(),
                "count_freq": self.cn_faker.random_digit(),
                "updated_by": None,
                "platform": self.en_faker.word(),
                "created_at": "{} {}".format(self.cn_faker.past_date(), self.cn_faker.time()),
                "updated_at": "{} {}".format(self.cn_faker.past_date(), self.cn_faker.time()),
                "created_by": self.en_faker.user_name(),
                "result_table_id": "{}_{}".format(self.cn_faker.random_number(digits=6), self.en_faker.word()),
                "result_table_type": self.cn_faker.random_digit(),
                "result_table_name": self.en_faker.word(),
                "project_name": self.cn_faker.word(),
                "generate_type": self.en_faker.word(),
                "data_category": None,
                "is_managed": 1,
                "processing_type": "clean",
                "sensitivity": "private",
                "description": self.en_faker.sentence(),
                "tags": {"manage": {"geog_area": [{"code": "inland", "alias": self.cn_faker.country()}]}},
            }
            data.append(fdata)
        # 生成假数据
        ftext = json.dumps(
            {
                "result": "true",
                "data": data,
                "code": self.cn_faker.random_number(digits=6),
                "message": "ok",
                "errors": "null",
            }
        )

        with requests_mock.Mocker() as m:
            m.get("/dev/v3/meta/result_tables/", text=ftext)
            meta_result_tables = MetaApi.result_tables.list()
            self.assertEqual(meta_result_tables, data)

    def test_projects(self):
        data = []
        for _ in range(4):
            fdata = {
                "project_name": self.cn_faker.word(),
                "description": self.en_faker.sentence(),
                "created_at": "{} {}".format(self.cn_faker.past_date(), self.cn_faker.time()),
                "updated_at": "{} {}".format(self.cn_faker.past_date(), self.cn_faker.time()),
                "created_by": self.en_faker.name(),
                "deleted_by": "null",
                "bk_app_code": self.en_faker.word(),
                "project_id": 3,
                "active": "true",
                "deleted_at": "null",
                "updated_by": self.en_faker.user_name(),
                "tags": {"manage": {"geog_area": [{"code": "inland", "alias": "中国内地"}]}},
            }
            data.append(fdata)
        # 生成假数据
        ftext = json.dumps(
            {
                "result": "true",
                "data": data,
                "code": self.cn_faker.random_number(digits=6),
                "message": "ok",
                "errors": "null",
            }
        )

        with requests_mock.Mocker() as m:
            m.get("/dev/v3/meta/projects/", text=ftext)
            meta_projects = MetaApi.projects.list()
            self.assertEqual(meta_projects, data)

    def test_biz_list(self):
        data = []
        for _ in range(4):
            fdata = {
                "bk_biz_id": self.en_faker.random_number(digits=6),
                "bk_biz_name": self.cn_faker.word(),
                "maintainers": self.en_faker.user_name(),
                "description": "null",
            }
            data.append(fdata)
        # 生成假数据
        ftext = json.dumps(
            {
                "result": "true",
                "data": data,
                "code": self.cn_faker.random_number(digits=7),
                "message": "ok",
                "errors": "null",
            }
        )

        with requests_mock.Mocker() as m:
            m.get("/dev/v3/meta/bizs/", text=ftext)
            meta_biz_list = MetaApi.biz_list.list()
            self.assertEqual(meta_biz_list, data)

    def test_tdw_tables(self):
        data = []
        for _ in range(2):
            fdata = {
                "updated_at": "{}T{}+00:00".format(self.cn_faker.past_date(), self.cn_faker.time()),
                "original_tbl_id": "null",
                "cluster_id": self.en_faker.word(),
                "pri_part_format": "null",
                "sub_part_format": "null",
                "sub_part_key": "null",
                "sub_part_type": "null",
                "table_id": self.en_faker.word(),
                "created_by": "null",
                "count_freq_unit": "H",
                "synced_at": "{}T{}+00:00".format(self.cn_faker.past_date(), self.cn_faker.time()),
                "pri_part_key": "null",
                "table_comment": self.en_faker.word(),
                "result_table_id": "{}_{}".format(self.cn_faker.random_number(digits=6), self.cn_faker.time()),
                "count_freq": self.en_faker.random_digit(),
                "updated_by": "null",
                "data_type": "null",
                "table_type": "null",
                "db_name": self.en_faker.word(),
                "associated_lz_id": '{"import": "20190517170506348"}',
                "bk_biz_id": self.cn_faker.random_number(digits=6),
                "created_at": "{}T{}+00:00".format(self.cn_faker.past_date(), self.cn_faker.time()),
                "table_name": self.en_faker.word(),
                "synced_by": self.en_faker.name(),
                "usability": "OK",
                "pri_part_type": "null",
            }
            data.append(fdata)

        # 生成假数据
        ftext = json.dumps(
            {
                "result": "true",
                "data": data,
                "code": self.cn_faker.random_number(digits=7),
                "message": "ok",
                "errors": "null",
            }
        )

        with requests_mock.Mocker() as m:
            m.get("/dev/v3/meta/tdw/tables/", text=ftext)
            meta_tdw_tables = MetaApi.tdw_tables()
            self.assertEqual(meta_tdw_tables, data)

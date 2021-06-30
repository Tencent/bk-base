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
from mock import patch
import requests_mock

from apps.dataflow.handlers.result_table import ResultTable
from tests.test_base import BaseTestCase


def mock_project_data(params):
    return [
        {"bk_biz_id": 591, "result_table_id": "test1"},
        {"bk_biz_id": 591, "result_table_id": "test2"},
        {"bk_biz_id": 591, "result_table_id": "test3"},
        {"bk_biz_id": 591, "result_table_id": "test4"},
        {"bk_biz_id": 591, "result_table_id": "test5"},
        {"bk_biz_id": 591, "result_table_id": "test6"},
        {"bk_biz_id": 591, "result_table_id": "test7"},
        {"bk_biz_id": 591, "result_table_id": "test8"},
        {"bk_biz_id": 591, "result_table_id": "test9"},
    ]


def mock_list(project_id=None, bk_biz_id=None, is_query=False, has_fields=True, source_type="rtsource", action_id=None):
    if project_id is None:
        return [
            {"bk_biz_id": 591, "result_table_id": "test1"},
            {"bk_biz_id": 591, "result_table_id": "test2"},
            {"bk_biz_id": 591, "result_table_id": "test3"},
            {"bk_biz_id": 591, "result_table_id": "test11"},
        ]
    else:
        return [
            {"bk_biz_id": 591, "result_table_id": "test7"},
            {"bk_biz_id": 591, "result_table_id": "test9"},
            {"bk_biz_id": 591, "result_table_id": "test14"},
        ]


def mock_meta_result_table_list(self):
    data = [
        {
            "bk_biz_id": 591,
            "count_freq_unit": "S",
            "result_table_name_alias": "security",
            "project_id": 6967,
            "count_freq": 60,
            "updated_by": "angela46",
            "platform": "finally",
            "created_at": "2021-05-12 11:34:06",
            "updated_at": "2021-06-02 22:36:05",
            "created_by": "jacoblee",
            "result_table_id": "hospital",
            "result_table_type": None,
            "result_table_name": "ok",
            "project_name": "登录",
            "generate_type": "user",
            "data_category": "UTF8",
            "is_managed": 1,
            "processing_type": "model",
            "sensitivity": "private",
            "description": "Bring shoulder away much strategy.",
            "fields": [
                {
                    "field_type": "double",
                    "field_alias": "公司",
                    "description": None,
                    "roles": {"event_time": False},
                    "created_at": "2021-05-09 13:58:30",
                    "is_dimension": False,
                    "created_by": "rscott",
                    "updated_at": "2021-05-11 23:23:39",
                    "origins": None,
                    "id": 50431,
                    "field_name": "participant",
                    "field_index": 1,
                    "updated_by": "ggutierrez",
                },
                {
                    "field_type": "long",
                    "field_alias": "b",
                    "description": None,
                    "roles": {"event_time": True, "timestamp": True},
                    "created_at": "2021-05-18 07:43:26",
                    "is_dimension": False,
                    "created_by": "mahoneycurtis",
                    "updated_at": "2021-05-25 03:31:35",
                    "origins": None,
                    "id": 64544,
                    "field_name": "focus",
                    "field_index": 2,
                    "updated_by": "pmartinez",
                },
            ],
            "storages": {},
            "tags": {"manage": {"geog_area": [{"code": "out", "alias": "莫桑比克"}]}},
        },
        {
            "bk_biz_id": 591,
            "count_freq_unit": "S",
            "result_table_name_alias": "page",
            "project_id": 7877,
            "count_freq": 60,
            "updated_by": "brian59",
            "platform": "civil",
            "created_at": "2021-05-31 11:19:59",
            "updated_at": "2021-05-28 14:53:29",
            "created_by": "walledward",
            "result_table_id": "require",
            "result_table_type": None,
            "result_table_name": "hard",
            "project_name": "数据",
            "generate_type": "user",
            "data_category": "UTF8",
            "is_managed": 1,
            "processing_type": "model",
            "sensitivity": "private",
            "description": "Participant from lay professional whose will.",
            "fields": [
                {
                    "field_type": "double",
                    "field_alias": "如此",
                    "description": None,
                    "roles": {"event_time": False},
                    "created_at": "2021-05-26 05:40:38",
                    "is_dimension": False,
                    "created_by": "dfitzpatrick",
                    "updated_at": "2021-05-21 15:26:10",
                    "origins": None,
                    "id": 2566,
                    "field_name": "until",
                    "field_index": 1,
                    "updated_by": "srodriguez",
                },
                {
                    "field_type": "long",
                    "field_alias": "b",
                    "description": None,
                    "roles": {"event_time": True, "timestamp": True},
                    "created_at": "2021-05-23 08:49:06",
                    "is_dimension": False,
                    "created_by": "ecantu",
                    "updated_at": "2021-05-09 20:55:35",
                    "origins": None,
                    "id": 76367,
                    "field_name": "decade",
                    "field_index": 2,
                    "updated_by": "alejandrobrown",
                },
            ],
            "storages": {},
            "tags": {"manage": {"geog_area": [{"code": "effort", "alias": "阿鲁巴岛"}]}},
        },
    ]

    return data


def mock_judge_source_type(args, source_type):
    return True


def mock_true(args):
    return True


def mock_is_clean(args):
    return False


def mock_list_selected_history(args):
    return ["591_something_for_mock"]


class TestIeodResultTable(BaseTestCase):
    @patch("apps.api.MetaApi.result_tables.list", mock_meta_result_table_list)
    @patch("apps.dataflow.handlers.result_table.ResultTable.has_kafka", mock_true)
    @patch("apps.dataflow.handlers.result_table.ResultTable.has_hdfs", mock_true)
    def test_list(self):
        project_id = self.en_faker.random_number(digits=4)
        bk_biz_id = self.en_faker.random_number(digits=3)
        source_type = "rtsource"

        result_list = ResultTable.list(project_id, bk_biz_id=bk_biz_id, source_type=source_type)
        m_result = mock_meta_result_table_list(None)
        self.assertEqual(m_result, result_list)

    @patch("apps.dataflow.handlers.result_table.AuthApi.projects.data", mock_project_data)
    @patch("apps.dataflow.handlers.result_table.ResultTable.list", mock_list)
    def test_list_as_source_by_project_id(self):
        project_id = 3481
        source_type = "rtsource"
        bk_biz_id = 591
        fresult = [
            {"bk_biz_id": 591, "result_table_id": "test1"},
            {"bk_biz_id": 591, "result_table_id": "test2"},
            {"bk_biz_id": 591, "result_table_id": "test3"},
            {"bk_biz_id": 591, "result_table_id": "test7"},
            {"bk_biz_id": 591, "result_table_id": "test9"},
            {"bk_biz_id": 591, "result_table_id": "test14"},
        ]

        list_asbpi = ResultTable.list_as_source_by_project_id(project_id, bk_biz_id=bk_biz_id, source_type=source_type)
        self.assertEqual(list_asbpi, fresult)

    def test_list_rt_storage_info(self):
        data = []
        for _ in range(3):
            result_table_id = "{}_{}".format(self.en_faker.random_number(digits=3), self.en_faker.word())
            flist = {
                "status": "no-start",
                "node_config": {
                    "bk_biz_id": self.en_faker.random_number(digits=3),
                    "name": self.en_faker.word(),
                    "expires": 1,
                    "storage_keys": [],
                    "indexed_fields": [self.en_faker.word(), self.en_faker.word()],
                    "cluster": self.en_faker.word(),
                    "has_unique_key": False,
                    "from_result_table_ids": [result_table_id],
                    "result_table_id": result_table_id,
                },
                "node_id": self.en_faker.random_number(digits=3),
                "flow_name": self.cn_faker.word(),
                "frontend_info": {
                    "y": self.en_faker.random_number(digits=3),
                    "x": self.en_faker.random_number(digits=3),
                },
                "storage_type": self.en_faker.word(),
                "node_type": self.en_faker.word(),
                "node_name": "{}_{}".format(result_table_id, self.en_faker.word()),
                "version": "",
                "result_table_ids": [result_table_id],
                "flow_id": self.en_faker.random_number(digits=4),
                "has_modify": False,
            }
            data.append(flist)
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
            m.get(
                "/dev/v3/dataflow/flow/nodes/storage_nodes/",
                text=ftext,
            )
            storage_nodes = ResultTable.list_rt_storage_info([self.en_faker.random_number()])
            self.assertEqual(storage_nodes, data)

    @patch("apps.dataflow.handlers.result_table.ResultTable.has_kafka", mock_true)
    @patch("apps.dataflow.handlers.result_table.ResultTable.has_hdfs", mock_true)
    @patch("apps.dataflow.handlers.result_table.ResultTable.has_hdfs", mock_true)
    @patch("apps.dataflow.handlers.result_table.ResultTable.has_tredis", mock_true)
    @patch("apps.dataflow.handlers.result_table.ResultTable.has_ignite", mock_true)
    @patch("apps.dataflow.handlers.result_table.ResultTable.can_be_tdw_source", mock_true)
    def test_judge_source_type(self):
        r = ResultTable(self.en_faker.random_number())
        etl_result = r.judge_source_type("etl_source")
        self.assertEqual(etl_result, False)
        stream_result = r.judge_source_type("stream_source")
        self.assertEqual(stream_result, True)
        batch_result = r.judge_source_type("batch_source")
        self.assertEqual(batch_result, True)
        kv_result = r.judge_source_type("kv_source")
        self.assertEqual(kv_result, True)
        batch_kv_result = r.judge_source_type("batch_kv_source")
        self.assertEqual(batch_kv_result, True)
        tdw_result = r.judge_source_type("tdw_source")
        self.assertEqual(tdw_result, True)
        unified_kv_result = r.judge_source_type("unified_kv_source")
        self.assertEqual(unified_kv_result, True)

    def test_list_latest_data(self):
        empty_result = ResultTable.list_latest_data([])
        self.assertEqual(empty_result, {})

    def test_dmonitor_metrics(self):
        rt_condition = "logical_tag = ''"
        sql = (
            'SELECT *,logical_tag FROM "data_loss_output_total" '
            "where data_inc > 0 and ({}) group by logical_tag, component "
            "order by time desc limit 1".format(rt_condition)
        )
        result = ResultTable.dmonitor_metrics(sql)
        self.assertEqual(result, {"series": [], "data_format": "simple"})
        result = ResultTable.dmonitor_metrics(())
        self.assertEqual(result, {"series": []})

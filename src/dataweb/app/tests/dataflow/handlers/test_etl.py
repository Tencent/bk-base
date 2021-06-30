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
import re
import requests_mock
from unittest.mock import patch

from apps import exceptions
from apps.dataflow.handlers.etl import (
    ETL,
    AccessCalc,
    AssignCalc,
    CsvCalc,
    FromJsonCalc,
    FromJsonListCalc,
    FromUrlCalc,
    FunctionCalc,
    IterateCalc,
    PopCalc,
    RegexExtractCalc,
    ReplaceCalc,
    SplitCalc,
    SplitKvCalc,
    ZipCalc,
    get_calc_handler,
    get_calc_handler_by_node,
)
from apps.exceptions import ApiResultError
from tests.test_base import BaseTestCase


def mock_get_calc_handler(args):
    return FromJsonCalc


def mock_to_etl_node(args):
    return {
        "type": "fun",
        "method": "from_json",
        "result": "host_json",
        "label": args["label"],
        "args": [],
    }


def mock_get_calc_by_label(form_config, label):
    return {"input": "root", "calc_id": "from_json", "calc_params": {"result": "host_json"}, "label": label}


class TestIeodEtl(BaseTestCase):
    def test_get_calc_handler_by_node(self):
        result = get_calc_handler_by_node({"type": "fun", "method": "from_json"})
        self.assertEqual(FromJsonCalc, result)
        result = get_calc_handler_by_node({"type": "fun", "method": "from_json_list"})
        self.assertEqual(FromJsonListCalc, result)
        result = get_calc_handler_by_node({"type": "fun", "method": "csvline"})
        self.assertEqual(CsvCalc, result)
        result = get_calc_handler_by_node({"type": "fun", "method": "from_url"})
        self.assertEqual(FromUrlCalc, result)
        result = get_calc_handler_by_node({"type": "fun", "method": "split"})
        self.assertEqual(SplitCalc, result)
        result = get_calc_handler_by_node({"type": "fun", "method": "splitkv"})
        self.assertEqual(SplitKvCalc, result)
        result = get_calc_handler_by_node({"type": "fun", "method": "replace"})
        self.assertEqual(ReplaceCalc, result)
        result = get_calc_handler_by_node({"type": "fun", "method": "pop"})
        self.assertEqual(PopCalc, result)
        result = get_calc_handler_by_node({"type": "fun", "method": "zip"})
        self.assertEqual(ZipCalc, result)
        result = get_calc_handler_by_node({"type": "fun", "method": "iterate"})
        self.assertEqual(IterateCalc, result)
        result = get_calc_handler_by_node({"type": "fun", "method": "items"})
        self.assertEqual(IterateCalc, result)
        result = get_calc_handler_by_node({"type": "fun", "method": "regex_extract"})
        self.assertEqual(RegexExtractCalc, result)

        result = get_calc_handler_by_node({"type": "access"})
        self.assertEqual(AccessCalc, result)
        result = get_calc_handler_by_node({"type": "assign"})
        self.assertEqual(AssignCalc, result)
        with self.assertRaises(exceptions.ETLCheckError):
            get_calc_handler_by_node({"type": "wrong type"})

    def test_get_calc_handler(self):
        result = get_calc_handler("from_json")
        self.assertEqual(FromJsonCalc, result)
        result = get_calc_handler("from_json_list")
        self.assertEqual(FromJsonListCalc, result)
        result = get_calc_handler("csvline")
        self.assertEqual(CsvCalc, result)
        result = get_calc_handler("from_url")
        self.assertEqual(FromUrlCalc, result)
        result = get_calc_handler("split")
        self.assertEqual(SplitCalc, result)
        result = get_calc_handler("splitkv")
        self.assertEqual(SplitKvCalc, result)
        result = get_calc_handler("replace")
        self.assertEqual(ReplaceCalc, result)
        result = get_calc_handler("pop")
        self.assertEqual(PopCalc, result)
        result = get_calc_handler("zip")
        self.assertEqual(ZipCalc, result)
        result = get_calc_handler("iterate")
        self.assertEqual(IterateCalc, result)
        result = get_calc_handler("items")
        self.assertEqual(IterateCalc, result)
        result = get_calc_handler("regex_extract")
        self.assertEqual(RegexExtractCalc, result)

        with self.assertRaises(exceptions.ETLCheckError):
            get_calc_handler("wrong type")

    def test_start(self):
        result_table_id = "{}_{}".format(self.en_faker.random_int(), self.en_faker.word())
        fdata = "任务（result_table_id:{result_table_id})启动成功！".format(result_table_id=result_table_id)
        ftext = json.dumps(
            {
                "code": self.en_faker.random_number(digits=7),
                "data": fdata,
                "errors": None,
                "message": "ok",
                "request_id": self.en_faker.uuid4(),
                "result": True,
            }
        )
        with requests_mock.Mocker() as m:
            m.post("/dev/v3/databus/tasks/", text=ftext)
            result_table_id = result_table_id
            result = ETL(result_table_id).start()
            self.assertEqual(result, fdata)

        with self.assertRaises(ApiResultError):
            ETL(self.en_faker.random_int()).start()

    def test_stop(self):
        result_table_id = "{}_{}".format(self.en_faker.random_int(), self.en_faker.word())
        fdata = "任务{}stop了".format(result_table_id)
        ftext = json.dumps(
            {
                "code": self.en_faker.random_number(digits=7),
                "data": fdata,
                "errors": None,
                "message": "ok",
                "request_id": self.en_faker.uuid4(),
                "result": True,
            }
        )
        with requests_mock.Mocker() as m:
            matcher = re.compile("/dev/v3/databus/tasks/")
            m.delete(matcher, text=ftext)
            result_table_id = result_table_id
            result = ETL(result_table_id).stop()
            self.assertEqual(result, fdata)

    def test_form_to_etl(self):
        label_a = self.en_faker.word()
        label_b = self.en_faker.word()
        form_config = [
            {
                "input": "root",
                "calc_id": "from_json",
                "calc_params": {"result": self.en_faker.word(), "assign_method": self.en_faker.word()},
                "label": label_a,
            },
            {
                "input": label_a,
                "calc_id": "access",
                "calc_params": {
                    "location": self.en_faker.word(),
                    "result": self.en_faker.word(),
                    "assign_method": self.en_faker.word(),
                },
                "label": label_b,
            },
        ]
        result = ETL.form_to_etl(form_config)
        fresult = {
            "type": "fun",
            "method": "from_json",
            "result": form_config[0]["calc_params"]["result"],
            "label": form_config[0]["label"],
            "args": [],
            "next": {
                "type": "access",
                "subtype": "access_pos",
                "label": label_b,
                "index": form_config[1]["calc_params"]["location"],
                "result": form_config[1]["calc_params"]["result"],
                "default_type": "null",
                "default_value": "",
                "next": None,
            },
        }
        self.assertEqual(result, fresult)

    def test_extract_graph(self):
        form_config = [
            {
                "input": "root",
                "calc_id": "from_json",
                "calc_params": {"result": self.en_faker.word()},
                "label": "labela",
            },
            {
                "input": "labela",
                "calc_id": "access",
                "calc_params": {"location": self.en_faker.word(), "result": self.en_faker.word()},
                "label": "labelb",
            },
        ]
        result = ETL.extract_graph(form_config)
        self.assertEqual(result, {"root": ["labela"], "labela": ["labelb"], "labelb": []})

    @patch("apps.dataflow.handlers.etl.ETL.get_calc_by_label", mock_get_calc_by_label)
    @patch("apps.dataflow.handlers.etl.get_calc_handler", mock_get_calc_handler)
    @patch("apps.dataflow.handlers.etl.FunctionCalc.to_etl_node", mock_to_etl_node)
    def test_build_etl_node(self):
        label = "labela"
        graph = {"root": ["labela"], "labela": ["labelb", "labelc"], "labelb": [], "labelc": []}
        result = ETL.build_etl_node(label, [], graph)
        fresult = {
            "type": "fun",
            "method": "from_json",
            "result": "host_json",
            "label": "labela",
            "args": [],
            "next": {
                "type": "branch",
                "name": "",
                "label": None,
                "next": [
                    {
                        "type": "fun",
                        "method": "from_json",
                        "result": "host_json",
                        "label": "labelb",
                        "args": [],
                        "next": None,
                    },
                    {
                        "type": "fun",
                        "method": "from_json",
                        "result": "host_json",
                        "label": "labelc",
                        "args": [],
                        "next": None,
                    },
                ],
            },
        }
        self.assertEqual(fresult, result)

    def test_FunctionCalc_to_form_calc(self):
        node = {"args": ["", ""], "type": "fun", "method": "zip"}
        result = FunctionCalc.to_form_calc(node)
        self.assertEqual(result, {"input": "", "calc_id": None, "calc_params": {"result": None}, "label": None})

    def test_AssignCalc_to_form_calc(self):
        fdata = [self.en_faker.word() for _ in range(7)]
        node = {
            "keys": [_ for _ in fdata],
            "subtype": "assign_obj",
            "type": "assign",
            "assign": [{"type": "string", "assign_to": _, "key": _} for _ in fdata],
        }

        result = AssignCalc.to_form_calc(node)
        fresult = {
            "input": "",
            "calc_id": "assign",
            "calc_params": {
                "assign_method": "key",
                "fields": [
                    {"type": n["type"], "assign_to": n["assign_to"], "location": n["key"]} for n in node["assign"]
                ],
                "keys": fdata,
            },
            "label": None,
        }
        self.assertEqual(fresult, result)

    def test_get_calc_by_label(self):
        label = self.en_faker.word()
        fresult = {
            "input": self.en_faker.word(),
            "calc_id": "assign",
            "calc_params": {
                "assign_method": "key",
                "fields": [
                    {
                        "type": "string",
                        "assign_to": self.en_faker.word(),
                        "location": self.en_faker.word(),
                        "id": self.en_faker.word(),
                        "highlight": False,
                    }
                    for _ in range(3)
                ],
            },
            "keys": [],
            "label": label,
        }
        form_config = [
            {
                "input": self.en_faker.word(),
                "calc_id": "from_json",
                "calc_params": {"result": self.en_faker.word()},
                "label": self.en_faker.word(),
            },
            fresult,
            {
                "input": self.en_faker.word(),
                "calc_id": "access",
                "calc_params": {"location": self.en_faker.word(), "result": self.en_faker.word()},
                "label": self.en_faker.word(),
            },
        ]

        result = ETL.get_calc_by_label(form_config, label)
        self.assertEqual(fresult, result)

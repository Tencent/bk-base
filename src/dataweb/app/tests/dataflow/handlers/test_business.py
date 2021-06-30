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
from unittest.mock import patch

from apps.api import CCApi
from apps.dataflow.handlers.business import Business
from tests.test_base import BaseTestCase


def mock_list():
    return [
        {"bk_biz_id": "97089", "bk_biz_name": "manage"},
        {"bk_biz_id": "109602", "bk_biz_name": "trip"},
        {"bk_biz_id": "673435", "bk_biz_name": "happy"},
    ]


def mock_list_cloud_area():
    fdata = [
        {
            "bk_cloud_name": "test1",
            "bk_cloud_id": 0,
        },
        {
            "bk_cloud_name": "test2",
            "bk_cloud_id": 1111111111,
        },
        {
            "bk_cloud_name": "test3",
            "bk_cloud_id": 2222222222,
        },
    ]
    return fdata


def mock_puller_by_pagination(args, limit=200):
    return [
        {
            "bk_cloud_name": "test1",
            "bk_cloud_id": 0,
        },
        {
            "bk_cloud_name": "test2",
            "bk_cloud_id": 1111111111,
        },
        {
            "bk_cloud_name": "test3",
            "bk_cloud_id": 2222222222,
        },
    ]


def mock_get_app_list():
    flist = [
        {"bk_biz_id": "64529", "bk_biz_name": "sister"},
        {"bk_biz_id": "608836", "bk_biz_name": "natural"},
        {"bk_biz_id": "777121", "bk_biz_name": "five"},
    ]
    return flist


def mock_get_name_dict_without_args():
    return {64529: "sister", 68836: "natural", 777121: "five"}


def mock_get_name_dict(args, **kwargs):
    return {64529: "sister", 68836: "natural", 777121: "five"}


def mock_get_agent_status(args):
    return {"666:127.0.0.1": {"ip": "127.0.0.1", "bk_cloud_id": 666, "bk_agent_alive": 0}}


def mock_get_biz_user(args):
    pass


def mock_get_cloud_area_map(args):
    return {0: "test1", 1111111111: "test2", 2222222222: "test3"}


class TestIeodBusiness(BaseTestCase):
    @patch("apps.api.CCApi.get_app_list", mock_get_app_list)
    def test_list(self):
        fdata = mock_get_app_list()
        fresult = sorted(
            ({"bk_biz_id": str(_["bk_biz_id"]), "bk_biz_name": _["bk_biz_name"]} for _ in fdata),
            key=lambda _d: int(_d["bk_biz_id"]),
        )
        result = Business.list()
        self.assertEqual(result, fresult)

    @patch("apps.dataflow.handlers.business.Business.list_cloud_area", mock_list_cloud_area)
    def test_get_cloud_area_map(self):
        fresult = {0: "test1", 1111111111: "test2", 2222222222: "test3"}
        result = Business.get_cloud_area_map()
        self.assertEqual(result, fresult)

    @patch("apps.dataflow.handlers.business.Business.puller_by_pagination", mock_puller_by_pagination)
    def test_list_cloud_area(self):

        fresult = [
            {"bk_cloud_name": "CC1.0默认云区域", "bk_cloud_id": 1},
            {"bk_cloud_name": "CC3.0默认云区域", "bk_cloud_id": 0},
            {"bk_cloud_name": "test2", "bk_cloud_id": 1111111111},
            {"bk_cloud_name": "test3", "bk_cloud_id": 2222222222},
        ]
        result = Business.list_cloud_area()
        self.assertEqual(result, fresult)

    def test_puller_by_pagination(self):
        ftext = json.dumps(
            {
                "code": 0,
                "permission": "null",
                "result": "true",
                "request_id": self.cn_faker.uuid4(),
                "message": "success",
                "data": {
                    "count": 3,
                    "info": "",
                },
            }
        )
        with requests_mock.Mocker() as m:
            m.post("/api/c/compapi/v2/cc/search_cloud_area/", text=ftext)
            result = Business.puller_by_pagination(CCApi.search_cloud_area, limit=200)
            self.assertEqual(result, [])

    @patch("apps.dataflow.handlers.business.Business.get_name_dict", mock_get_name_dict_without_args)
    def test_wrap_biz_name(self):
        data = [
            {
                "target_details": {
                    "xxx": {
                        "bk_biz_id": 68836,
                    }
                }
            }
        ]
        key_path = ["target_details", "xxx"]
        result = Business.wrap_biz_name(data=data, key_path=key_path)
        self.assertEqual(data, result)

    @patch("apps.dataflow.handlers.business.Business.get_name_dict", mock_get_name_dict)
    def test_get_biz_name(self):
        b = Business(68836)
        result = b.get_biz_name()
        self.assertEqual(result, mock_get_name_dict(None)[68836])

    @patch("apps.dataflow.handlers.business.Business.list", mock_list)
    def test_get_name_dict(self):
        result = Business.get_name_dict()
        fresult = {int(_biz["bk_biz_id"]): _biz["bk_biz_name"] for _biz in mock_list()}
        self.assertEqual(result, fresult)

    def test_check_bk_cloud_id(self):
        b = Business(self.en_faker.random_int())
        hosts_for_false = [
            {"ip": "127.0.0.1", "bk_cloud_id": 1},
            {"ip": "127.0.0.1", "bk_cloud_id": 2},
            {"ip": "127.0.0.1", "bk_cloud_id": 3},
        ]
        result = b._check_bk_cloud_id(hosts_for_false)
        self.assertEqual(result, False)
        hosts_for_true = []
        result = b._check_bk_cloud_id(hosts_for_true)
        self.assertEqual(result, True)

    def test_get_agent_status(self):
        hosts = [{"ip": "127.0.0.1", "plat_id": 1, "bk_cloud_id": 666}]
        fresult = {"666:127.0.0.1": {"ip": "127.0.0.1", "bk_cloud_id": 666, "bk_agent_alive": 0}}
        b = Business(self.en_faker.random_int())
        result = b._get_agent_status(hosts=hosts)
        self.assertEqual(result, fresult)

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

from apps.api import BKLoginApi, BKPAASApi, CCApi, GseApi

from tests.test_base import BaseTestCase


# 测ieod环境下的commonAPI
class TestIeodCommonModuleAPI(BaseTestCase):
    def test_bklogin_get_all_user(self):
        data = []
        for _ in range(3):
            fname = {"chinese_name": self.cn_faker.name(), "english_name": self.en_faker.name()}
            data.append(fname)
        # 生成假数据
        fdata = json.dumps(
            {
                "code": "00",
                "message": self.cn_faker.sentence(),
                "data": data,
                "result": "true",
            }
        )
        # 制作假结果
        for i in data:
            i["username"] = i["english_name"]

        with requests_mock.Mocker() as m:
            m.get(
                "/component/compapi/tof3/get_all_staff_info",
                text=fdata,
            )

            bklogin_get_all_user = BKLoginApi.get_all_user()
            self.assertEqual(bklogin_get_all_user, data)

    def test_cc_get_app_list(self):
        data = []
        for _ in range(3):
            finfo = {
                "bk_biz_id": self.cn_faker.random_number(digits=6),
                "default": self.cn_faker.random_int(),
                "bk_biz_name": self.en_faker.word(),
            }
            data.append(finfo)
        # 生成假数据
        ftext = json.dumps(
            {
                "code": 0,
                "permission": "null",
                "result": "true",
                "request_id": self.cn_faker.uuid4(),
                "message": "success",
                "data": {
                    "count": 2,
                    "info": data,
                },
            }
        )
        # 制造假结果
        fresult = sorted(
            ({"bk_biz_id": str(_["bk_biz_id"]), "bk_biz_name": _["bk_biz_name"]} for _ in data),
            key=lambda _d: int(_d["bk_biz_id"]),
        )
        with requests_mock.Mocker() as m:
            m.post("/api/c/compapi/v2/cc/search_business/", text=ftext)
            ccapi_get_app_list = CCApi.get_app_list()
            self.assertEqual(ccapi_get_app_list, fresult)

    def test_gse_get_agent_status(self):
        data = []
        for _ in range(3):
            fdata = {"bk_biz_id": self.cn_faker.random_number(digits=6), "bk_location": self.en_faker.word()}
            data.append(fdata)
        ftext = {
            "code": 0,
            "permission": self.en_faker.word(),
            "result": "true",
            "request_id": self.cn_faker.uuid4(),
            "message": self.en_faker.word(),
            "data": data,
        }
        with requests_mock.Mocker() as m:
            m.post(
                "/api/c/compapi/v2/gse/get_agent_status",
                text=json.dumps(ftext),
            )
            m.post(
                "/api/c/compapi/v2/cc/get_biz_location/",
                text=json.dumps(ftext),
            )
            api_params = {
                "bk_biz_id": self.cn_faker.random_number(digits=6),
                "bk_supplier_id": 0,
                "hosts": [
                    {
                        "ip": self.cn_faker.ipv4(),
                        "plat_id": self.cn_faker.random_digit(),
                        "bk_cloud_id": self.cn_faker.random_number(digits=5),
                    }
                ],
                "force_v3": True,
            }
            gseapi_get_agent_status = GseApi.get_agent_status(api_params)
            self.assertEqual(gseapi_get_agent_status, data)

    def test_bk_paas_get_app_info(self):
        fmessage = []
        fname = self.en_faker.name()
        for _ in range(4):
            fdata = {
                "bk_biz_id": self.cn_faker.random_number(digits=6),
                "bk_location": self.en_faker.word(),
                "state_msg": self.en_faker.word(),
                "first_online_time": "{} {}".format(self.cn_faker.past_date(), self.cn_faker.time()),
                "created_date": "{} {}".format(self.cn_faker.past_date(), self.cn_faker.time()),
                "developer": "{};{};{}".format(fname, self.en_faker.name(), self.en_faker.name()),
                "name": self.en_faker.word(),
                "ops": "{};{}".format(fname, self.en_faker.name()),
                "tag": self.en_faker.word(),
                "biz": "",
                "code": self.en_faker.word(),
                "id": self.en_faker.random_number(digits=5),
                "group": self.en_faker.word(),
                "state": self.en_faker.random_digit(),
                "first_test_time": "{} {}".format(self.cn_faker.past_date(), self.cn_faker.time()),
            }
            fmessage.append(fdata)
        ftext = json.dumps(
            {
                "result": True,
                "message": fmessage,
            }
        )
        # 制造假结果
        fresult = sorted(
            ({"app_code": str(_["code"]), "app_name": _["name"]} for _ in fmessage), key=lambda _d: str(_d["app_name"])
        )
        with requests_mock.Mocker() as m:
            m.get(
                "/app_api/get_app_list_by_state/",
                text=ftext,
            )

            bkpaas_get_app_info = BKPAASApi.get_app_info()
            bkpaas_get_app_info = sorted(bkpaas_get_app_info, key=lambda e: e.__getitem__("app_name"))
            self.assertEqual(bkpaas_get_app_info, fresult)

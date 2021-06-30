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

import mock

from api.base import DataResponse
from collection.usermanage.user_info import UserInfoCollector
from collection.usermanage.processes.user_info import process_user_info
from tests import BaseTestCase

ONE_USERINFO = {
    "status": "NORMAL",
    "domain": "tencent.com",
    "telephone": "1300000000",
    "create_time": "2020-01-15T10:21:56.000000Z",
    "country_code": "86",
    "logo": None,
    "iso_code": "CN",
    "id": 1095,
    "display_name": "测试人",
    "leader": [{"username": "user_leader", "display_name": "测试上级", "id": 1607}],
    "username": "admin_user",
    "update_time": "2020-01-15T10:21:56.000000Z",
    "wx_userid": "",
    "staff_status": "IN",
    "password_valid_days": -1,
    "qq": "",
    "language": "zh-cn",
    "enabled": True,
    "time_zone": "Asia/Shanghai",
    "departments": [
        {
            "order": 1,
            "id": 4942,
            "full_name": "XX策划组",
            "name": "XX策划组",
        }
    ],
    "email": "admin_user@tencent.com",
    "extras": {"gender": "女", "postname": "XX策划组员工"},
    "position": 0,
    "category_id": 2,
}

USERS_INFO = {
    "result": True,
    "data": {"count": 1, "results": [ONE_USERINFO]},
    "code": "0",
}


def get_users_info_side_effect(params, raise_exception=True, retry_times=10):
    if params["page"] == 1:
        return DataResponse(USERS_INFO)
    else:
        return DataResponse(
            {
                "code": 1640001,
                "code_name": "INVALID_ARGS",
                "message": 'Parameters error [reason="参数不正确，请进行检查"]',
                "result": False,
                "data": None,
            }
        )


def dataflow_model_side_effect(params, raise_exception=True, retry_times=10):
    return DataResponse({"result": True, "data": {"flow_id": 4528376}, "code": "0"})


class TestUserInfo(BaseTestCase):
    @mock.patch("collection.usermanage.user_info.usermanage_api.list_users")
    @mock.patch("collection.common.collect.RawDataProducer")
    def test_batch_report(self, patch_producer, patch_list_users):
        patch_list_users.side_effect = get_users_info_side_effect
        # patch_list_users
        producer_mock = mock.Mock()
        patch_producer.return_value = producer_mock

        config = {"bk_biz_id": 1, "raw_data_name": "xxxxx"}
        UserInfoCollector(config).batch_report()

        assert patch_list_users.call_count == 1
        assert producer_mock.produce_message.call_count == 1

    @mock.patch("api.dataflow_api.flows.start")
    @mock.patch("api.datamanage_api.generate_datamodel_instance")
    def test_process_node_register(self, patch_datamanager_api, patch_flow_api):
        patch_datamanager_api.side_effect = dataflow_model_side_effect
        patch_flow_api.side_effect = dataflow_model_side_effect
        process_user_info()
        assert patch_datamanager_api.call_count == 1

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
import json
from unittest import mock

from auth.bkiam.backend import IAMBackend
from auth.handlers.role.role_handler import RoleHandler, threading
from common.transaction import meta_sync
from rest_framework.test import APITestCase

# from common.api.modules.meta import MetaApi
# from common.api.base import DataResponse


def is_api_success(case, resp):
    case.assertEqual(resp.status_code, 200)
    content = json.loads(resp.content)
    case.assertEqual(content["result"], True)

    return content["data"]


class ThreadPatchInOneThread:
    """
    将多线程 Mock 为普通行为，
    """

    def __init__(self, target, args):
        self.target = target
        self.args = args

    def start(self):
        self.target(*self.args)

    def join(self):
        pass


class BaseTestCase(APITestCase):
    databases = "__all__"

    def setUp(self):
        # 规避单元测试在多线程中无法回滚事务的问题
        patch_thread = mock.Mock()
        patch_thread.side_effect = ThreadPatchInOneThread
        threading.Thread = patch_thread

        patch_sync = mock.Mock()
        meta_sync.sync_model_data = patch_sync

        # 避免测试过程产生大量通知单据
        clear_ticket_notification()

        # 全局生效，避免调用接口
        # MetaApi.content_language_configs.list = MagicMock(return_value=DataResponse({
        #     'data': [
        #         {
        #             'content_key': '业务负责人',
        #             'language': 'en',
        #             'content_value': 'BusinessOwner'
        #         },
        #         {
        #             'content_key': '平台超级管理员',
        #             'language': 'en',
        #             'content_value': 'System super administrator'
        #         }
        #     ],
        #     'result': True,
        #     'message': '',
        #     'errors': None,
        #     'code': '1500200'
        # }))

        # 单元测试时，默认不开启 IAM 同步
        IAMBackend.SUPPORT_IAM = False
        RoleHandler.SUPPORT_IAM = False

    def get(self, url, params=None):
        params = params if params is not None else {}
        if "bk_username" not in params:
            params["bk_username"] = "jtest"
        if "bk_app_code" not in params:
            params["bk_app_code"] = "data"

        return self.client.get(url, params)

    def post(self, url, params=None):
        params = params if params is not None else {}
        if "bk_username" not in params:
            params["bk_username"] = "jtest"
        if "bk_app_code" not in params:
            params["bk_app_code"] = "data"
        return self.client.post(url, json.dumps(params), content_type="application/json")

    def put(self, url, params=None):
        params = params if params is not None else {}
        if "bk_username" not in params:
            params["bk_username"] = "jtest"
        if "bk_app_code" not in params:
            params["bk_app_code"] = "data"
        return self.client.put(url, json.dumps(params), content_type="application/json")

    def delete(self, url, params=None):
        params = params if params is not None else {}
        if "bk_username" not in params:
            params["bk_username"] = "jtest"
        if "bk_app_code" not in params:
            params["bk_app_code"] = "data"
        return self.client.delete(url, json.dumps(params), content_type="application/json")

    def is_api_success(self, resp):
        self.assertEqual(resp.status_code, 200)
        content = json.loads(resp.content)
        self.assertEqual(content["result"], True)

        return content["data"]

    def is_api_failure(self, resp):
        self.assertEqual(resp.status_code, 200)
        content = json.loads(resp.content)
        self.assertEqual(content["result"], False)

        return content


def clear_ticket_notification():
    """
    清理单据通知机制
    """
    from auth.core.signal import ticket_finished, ticket_processing
    from auth.core.ticket_notification import approve_finished_send_notice, send_notice

    ticket_finished.disconnect(approve_finished_send_notice)
    ticket_processing.disconnect(send_notice)


def build_ticket_notification():
    """
    按需将单据绑定加回去
    """
    from auth.core.signal import ticket_finished, ticket_processing
    from auth.core.ticket_notification import approve_finished_send_notice, send_notice

    ticket_finished.connect(approve_finished_send_notice)
    ticket_processing.connect(send_notice)


def compress_str(s):
    """压缩字符串，将标准空白符进行

    :param s: 输入字符串
    :type s: string
    :return: 压缩后的字符串
    :rtype: string
    """
    return " ".join(s.split())

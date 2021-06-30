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
from unittest import mock
from django.test import TestCase
from faker import Faker


class BaseTestCase(TestCase):
    def setUp(self):
        from apps.utils.local import activate_request

        self.req = mock.Mock()
        self.req.user.username = "admin"
        self.auth_info = mock.Mock()
        self.req.COOKIES = {}
        activate_request(self.req)
        self.cn_faker = Faker("zh_CN")
        self.en_faker = Faker("en_US")

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

import requests
import urllib3

# from django.conf import settings
from django.http import SimpleCookie
from django.test import Client, TestCase

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TEST_BIZ_ID = 2
TEST_DATA_ID = 68
JSON_CONTENT = "application/json"
PAAS_USERNAME = ""
PAAS_PASSWORD = ""


class MyTestClient(Client):
    def __init__(self):
        super(MyTestClient, self).__init__()
        login_cookies = mock_login()
        simple_cookies = SimpleCookie()
        for key, value in list(login_cookies.items()):
            simple_cookies[key] = value
        self.cookies = simple_cookies

    @staticmethod
    def assert_response(response):
        """
        断言请求是否正确返回
        :param response:
        :return: 返回数据中的data字段
        """
        assert response.status_code == 200
        json_response = json.loads(response.content)
        try:
            assert json_response.get("result")
        except AssertionError as e:
            print("[RESPONSE ERROR]:%s" % response.content)
            raise e

        return json_response["data"]

    @staticmethod
    def transform_data(data, content_type=JSON_CONTENT):
        """
        根据content_type转化请求参数
        :param data:
        :param content_type:
        :return:
        """
        if content_type == JSON_CONTENT:
            data = json.dumps(data)
        return data

    def get(self, path, data=None, secure=True, **extra):
        response = super(MyTestClient, self).get(path, data=data, secure=secure, **extra)

        return self.assert_response(response)

    def post(self, path, data=None, content_type=JSON_CONTENT, follow=False, secure=True, **extra):
        data = self.transform_data(data, content_type)
        response = super(MyTestClient, self).post(
            path, data=data, content_type=JSON_CONTENT, follow=follow, secure=secure, **extra
        )
        return self.assert_response(response)

    def patch(self, path, data=None, content_type=JSON_CONTENT, follow=False, secure=True, **extra):
        data = self.transform_data(data, content_type)
        response = super(MyTestClient, self).patch(
            path, data=data, content_type=JSON_CONTENT, follow=follow, secure=secure, **extra
        )
        return self.assert_response(response)

    def put(self, path, data=None, content_type=JSON_CONTENT, follow=False, secure=True, **extra):
        data = self.transform_data(data, content_type)
        response = super(MyTestClient, self).put(
            path, data=data, content_type=JSON_CONTENT, follow=follow, secure=secure, **extra
        )
        return self.assert_response(response)

    def delete(self, path, data=None, content_type=JSON_CONTENT, follow=False, secure=True, **extra):
        data = self.transform_data(data, content_type)
        response = super(MyTestClient, self).delete(
            path, data=data, content_type=JSON_CONTENT, follow=follow, secure=secure, **extra
        )
        return self.assert_response(response)


def mock_login():
    """
    模拟登录
    """
    # 获取csrftoken
    session = requests.session()
    # session.verify = False
    # res = session.get(url=settings.LOGIN_URL)
    # bklogin_csrftoken = res.cookies['bklogin_csrftoken']
    #
    # # 更新请求头部，发出登录请求
    # session.headers.update({
    #     'Content-Type': 'application/x-www-form-urlencoded',
    #     'X-CSRFToken': bklogin_csrftoken,
    #     'referer': settings.LOGIN_URL
    # })
    # session.post(
    #     url=settings.LOGIN_URL,
    #     data='username={}&password={}&csrfmiddlewaretoken={}'.format(
    #         PAAS_USERNAME, PAAS_PASSWORD, bklogin_csrftoken
    #     )
    # )
    return session.cookies


class MyTestCase(TestCase):
    client_class = MyTestClient
    client = MyTestClient()
    recursion_type = [dict, list]
    # string_type = [str, str]

    def runTest(self):
        print(self)

    def assertDataStructure(self, result_data, expected_data, value_eq=False, list_exempt=False):
        """
        将数据的结构以及类型进行断言验证
        :param result_data: 后台返回的数据
        :param expected_data: 希望得到的数据
        :param value_eq: 是否对比值相等
        :param list_exempt: 是否豁免列表的比对
        """
        expected_data = self.transform_to_unicode(expected_data)
        result_data = self.transform_to_unicode(result_data)
        result_data_type = type(result_data)

        # 判断类型是否一致
        self.assertEqual(result_data_type, type(expected_data))

        # 判断类型是否为字典
        if result_data_type is dict:
            # 將传入的预给定信息，将键值分别取出
            for expected_key, expected_value in list(expected_data.items()):
                # 判断键是否存在
                self.assertTrue(expected_key in list(result_data.keys()), msg="key:[%s] is expected" % expected_key)

                expected_value = self.transform_to_unicode(expected_value)
                result_value = self.transform_to_unicode(result_data[expected_key])

                # 返回None时忽略 @todo一刀切需要调整
                if expected_value is None or result_value is None:
                    return

                # 取出后台返回的数据result_data，判断是否与给定的类型相符
                result_value_type = type(result_value)
                expected_value_type = type(expected_value)
                self.assertEqual(
                    result_value_type,
                    expected_value_type,
                    msg="type error! Expect [{}] to be [{}], but got [{}]".format(
                        expected_key, expected_value_type, result_value_type
                    ),
                )

                if value_eq:
                    self.assertEqual(result_value, expected_value)

                # 判断该类型是否为字典或者列表
                if expected_value_type in self.recursion_type:
                    # 进行递归
                    self.assertDataStructure(result_value, expected_value, value_eq=value_eq, list_exempt=list_exempt)

        #  判断类型是否为列表
        elif result_data_type is list:
            # 列表不为空且不进行列表比对的豁免
            if result_data and expected_data and not list_exempt:

                if value_eq:
                    # 比对列表内的值是否相等
                    self.assertListEqual(result_data, expected_data)
                else:
                    # 否则认为列表里所有元素的数据结构都是一致的
                    _expected_data = expected_data[0]
                    for _data in result_data:
                        if type(_data) in self.recursion_type:
                            self.assertDataStructure(_data, _expected_data, value_eq=value_eq, list_exempt=list_exempt)

        # 判断值是否一致
        elif value_eq:
            self.assertEqual(result_data, expected_data)

    def assertListEqual(self, list1, list2, msg=None, is_sort=False):
        if is_sort:
            list1.sort()
            list2.sort()
        super(MyTestCase, self).assertListEqual(list1, list2, msg=msg)

    @staticmethod
    def transform_to_unicode(data):
        """
        统一把字符串数据转为Unicode
        :param data:
        :return: Unicode
        """
        # if type(data) is str:
        #     return str(data, "utf-8")
        return data

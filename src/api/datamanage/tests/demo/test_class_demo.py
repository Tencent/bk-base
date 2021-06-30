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

import mock
import pytest
from django.test import TestCase
from rest_framework.reverse import reverse

from datamanage.pro.dataquality.models.audit import DataQualityAuditRule, DataQualityAuditRuleTemplate
from datamanage.pro.dataquality.models.correction import DataQualityCorrectHandlerTemplate
from datamanage.pro.dataquality.correction_views import DataQualityCorrectConfigViewSet
from tests.utils import UnittestClient


@pytest.mark.usefixtures('django_db_setup', 'patch_auth_check')
class CRUDDemoTest(TestCase):
    """
    增删改查类接口单元测试Demo
    """

    databases = "__all__"
    primary_key_id = None

    def setUp(self):
        obj = DataQualityCorrectHandlerTemplate.objects.create(
            handler_template_name='demo_filling',
            handler_template_alias='Demo Filling',
            handler_template_type='fulling',
            handler_template_config='{}',
            active=1,
            created_by='unittest',
            description='demo',
        )
        self.primary_key = obj.id

    def test_create_correction_handler(self):
        create_params = {
            'handler_template_name': 'demo_filling',
            'handler_template_alias': 'Demo Filling',
            'handler_template_type': 'filling',
            'handler_template_config': '{}',
            'active': 1,
            'created_by': 'unittest',
            'description': 'demo',
        }

        # 单元测试客户端，用户请求reverse生成的内部接口，且封装后能返回与API模块调用时相同的Response
        client = UnittestClient()
        # 通过reverse函数反向获取请求的URL，这里创建和列表接口是在注册的资源名称后面加"-list"后缀
        url = reverse('data_quality_correct_handlers-list')
        response = client.post(url, create_params)

        # 两种Assert方式都可以
        self.assertTrue(response.is_success())
        assert response.data.get('id') is not None

    def test_update_correction_handler(self):
        update_params = {
            'handler_template_config': '{"is_demo": trues}',
        }
        client = UnittestClient()
        # 针对详情类资源（retrieve, update, patch, put, delete等方法），在资源名称后面加"-detail"后缀
        url = reverse('data_quality_correct_handlers-detail', [self.primary_key])
        response = client.patch(url, update_params)
        assert response.is_success() is True

    def test_delete_correction_handler(self):
        client = UnittestClient()
        url = reverse('data_quality_correct_handlers-detail', [self.primary_key])
        response = client.delete(url)
        assert response.is_success() is True


class FunctionDemoTest(TestCase):
    def test_function_in_model(self):
        """
        测试Model中的函数
        """
        rule_template = DataQualityAuditRuleTemplate.objects.create(
            template_name='custom',
            template_alias='custom',
            template_config='{}',
            active=1,
            description='custom',
        )

        audit_rule = DataQualityAuditRule.objects.create(
            data_set_id='591_datamonitor_clean_test_data',
            bk_biz_id=591,
            rule_name='自定义规则',
            rule_template=rule_template,
            rule_config='''[
                {
                    "function": "greater_or_equal",
                    "operation": null,
                    "constant": {
                        "constant_value": "10",
                        "constant_type": "float"
                    },
                    "metric": {
                        "metric_type": "data_flow",
                        "metric_name": "output_count",
                        "metric_field": ""
                    }
                }
            ]''',
            rule_config_alias='',
            description='demo',
        )
        rule_config_alias = audit_rule.get_rule_config_alias()
        assert rule_config_alias != ''

    @mock.patch('datamanage.utils.api.meta.MetaApi.result_tables.retrieve')
    def test_function_in_viewset(self, mock_metaapi_result):
        """
        测试ViewSet中的函数
        """
        mock_metaapi_result.return_value = {
            'processing_type': 'stream',
            'fields': [
                {
                    'field_name': 'timestamp',
                    'field_type': 'timestamp',
                },
                {
                    'field_name': 'client_ip',
                    'field_type': 'string',
                },
                {
                    'field_name': 'uid',
                    'field_type': 'string',
                },
                {
                    'field_name': 'field1',
                    'field_type': 'int',
                },
            ],
        }

        params = json.loads(
            '''{
            "data_set_id": "591_test_correction",
            "source_sql": "SELECT province, client_ip, uid, country, isp, metric2, metric1, time, uid_type, \
            wrong_field2 FROM 591_datamonitor_clean_test_data",
            "correct_configs": [
                {
                    "field": "isp",
                    "correct_config_item_id": null,
                    "correct_config_detail": {
                        "rules": [
                            {
                                "condition": {
                                    "condition_name": "custom_sql_condition",
                                    "condition_type": "custom",
                                    "condition_value": "uid_type = 3 AND uid = 'user4'"
                                },
                                "handler": {
                                    "handler_name": "fixed_filling",
                                    "handler_type": "filling",
                                    "handler_value": "100"
                                }
                            }
                        ],
                        "output": {
                            "generate_new_field": false,
                            "new_field": ""
                        }
                    },
                    "correct_config_alias": null
                },
                {
                    "field": "client_ip",
                    "correct_config_item_id": null,
                    "correct_config_detail": {
                        "rules": [
                            {
                                "condition": {
                                    "condition_name": "ip_verify",
                                    "condition_type": "builtin",
                                    "condition_value": "udf_regexp(client_ip,\
                                    '^[0-9]{1,3}\\\\.[0-9]{1,3}\\\\.[0-9]{1,3}\\\\.[0-9]{1,3}$') = 0"
                                },
                                "handler": {
                                    "handler_name": "fixed_filling",
                                    "handler_type": "filling",
                                    "handler_value": "127.0.0.1"
                                }
                            }
                        ],
                        "output": {
                            "generate_new_field": false,
                            "new_field": ""
                        }
                    },
                    "correct_config_alias": null
                }
            ]
        }'''
        )
        view_set = DataQualityCorrectConfigViewSet()
        sql = view_set.generate_correct_sql(**params)
        assert sql.startswith('SELECT')

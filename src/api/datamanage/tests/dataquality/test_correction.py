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

import pytest
from django.test import TestCase

from datamanage.pro.dataquality.correction_views import DataQualityCorrectConfigViewSet


@pytest.mark.usefixtures('django_db_setup')
class DataQualityCorrectionTest(TestCase):
    databases = "__all__"

    def test_generate_correct_sql(self):
        params = json.loads(
            '''{
            "data_set_id": "591_test_correction",
            "source_sql": "SELECT province,client_ip,uid,country,isp,metric2,metric1,time,uid_type,wrong_field2\
                            FROM 591_datamonitor_clean_test_data",
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
        self.assertEqual(sql[:6], 'SELECT')

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

from datamanage.tests.datamodel.conftest import (
    MODEL_ID,
    BK_USERNAME,
    CALCULATION_ATOM_NAME,
    INDICATOR_NAME,
    INDICATOR_ID,
)
from datamanage.pro.datamodel.dmm.manager import IndicatorManager
from datamanage.pro.datamodel.models.indicator import DmmModelIndicatorStage


@pytest.mark.usefixtures('django_db_setup', 'patch_meta_sync', 'patch_sql_validate')
class IndicatorTest(TestCase):
    """
    指标创建/修改/删除相关测试
    """

    databases = "__all__"

    def setUp(self):
        pass

    @pytest.mark.run(order=1)
    @pytest.mark.usefixtures('init_complete_model')
    def test_create_indicator(self):
        indicator_params = json.loads(
            '''
        {
            "model_id": %s,
            "indicator_name": "unittest_indicator_name",
            "indicator_id": %s,
            "indicator_alias": "创建指标的中文名",
            "description": "创建指标的描述",
            "calculation_atom_name": "%s",
            "aggregation_fields":[],
            "filter_formula":"where price is not null",
            "scheduling_content":{
                "window_type":"scroll",
                "count_freq":30,
                "window_time":10,
                "waiting_time":0,
                "expired_time":0,
                "window_lateness":{
                    "lateness_count_freq":60,
                    "allowed_lateness":false,
                    "lateness_time":1
                },
                "session_gap":0
            },
            "parent_indicator_name":null,
            "scheduling_type":"stream"
        }'''
            % (MODEL_ID, INDICATOR_ID, CALCULATION_ATOM_NAME)
        )
        indicator_dict = IndicatorManager.create_indicator(indicator_params, BK_USERNAME)
        assert indicator_dict['indicator_name'] is not None

    @pytest.mark.run(order=2)
    @pytest.mark.usefixtures('init_complete_model', 'init_indicator')
    def test_update_indicator(self):
        indicator_params = json.loads(
            '''
        {
            "model_id": %s,
            "indicator_id": %s,
            "indicator_alias": "修改指标的中文名",
            "description": "修改指标的描述",
            "filter_formula":"where channel_id is not null",
            "parent_indicator_name":null
        }'''
            % (MODEL_ID, INDICATOR_ID)
        )
        indicator_dict = IndicatorManager.update_indicator(INDICATOR_NAME, indicator_params, BK_USERNAME)
        assert indicator_dict['indicator_name'] is not None

    @pytest.mark.run(order=3)
    @pytest.mark.usefixtures('init_complete_model', 'init_indicator')
    def test_delete_indicator(self):
        IndicatorManager.delete_indicator(INDICATOR_NAME, MODEL_ID, BK_USERNAME)
        assert DmmModelIndicatorStage.objects.filter(indicator_name=INDICATOR_NAME, model_id=MODEL_ID).exists() is False

    @pytest.mark.run(order=4)
    @pytest.mark.usefixtures('init_complete_model', 'init_indicator')
    def test_get_indicator_info(self):
        indicator_dict = IndicatorManager.get_indicator_info(INDICATOR_NAME, MODEL_ID)
        assert indicator_dict['indicator_id'] == INDICATOR_ID

    @pytest.mark.run(order=5)
    @pytest.mark.usefixtures('init_complete_model', 'init_indicator')
    def test_get_indicator_list(self):
        indicator_list, _, _ = IndicatorManager.get_indicator_list({'model_id': MODEL_ID})
        assert len(indicator_list) > 0

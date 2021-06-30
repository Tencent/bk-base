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

from datamanage.pro.datamodel.models.datamodel import DmmModelRelation
from datamanage.tests.datamodel.conftest import MODEL_ID, BK_USERNAME, DIM_MODEL_ID
from datamanage.pro.datamodel.dmm.manager import MasterTableManager


@pytest.mark.usefixtures('django_db_setup', 'patch_meta_sync', 'patch_sql_validate')
class MasterTableTest(TestCase):
    """
    数据模型主表创建/修改相关测试
    """

    databases = "__all__"

    def setUp(self):
        pass

    @pytest.mark.usefixtures('init_complete_model', 'init_model_info')
    def test_update_master_table(self):
        master_table_fields_params = json.loads(
            '''
        {
            "model_id": %s,
            "fields": [
                {
                    "field_name": "price",
                    "field_alias": "道具价格",
                    "field_index": 1,
                    "field_type": "long",
                    "field_category": "measure",
                    "is_primary_key": false,
                    "description": "道具价格",
                    "field_constraint_content": {
                        "op":"AND",
                        "groups":[
                            {
                                "op":"AND",
                                "items":[
                                    {
                                        "constraint_id":"not_null",
                                        "constraint_content":null
                                    }
                                ]
                            },
                            {
                                "op":"AND",
                                "items":[
                                    {
                                        "constraint_id":"gte",
                                        "constraint_content":"1"
                                    },
                                    {
                                        "constraint_id":"lte",
                                        "constraint_content":"100"
                                    }
                                ]
                            }
                        ]
                    },
                    "field_clean_content": {
                        "clean_option": "SQL",
                        "clean_content": "price  as price"
                    },
                    "source_model_id": null,
                    "source_field_name": null
                },
                {
                    "field_name": "channel_id",
                    "field_alias": "渠道号",
                    "field_index": 2,
                    "field_type": "string",
                    "field_category": "dimension",
                    "is_primary_key": false,
                    "description": "渠道号",
                    "field_constraint_content": {
                        "op":"AND",
                        "groups":[
                            {
                                "op":"AND",
                                "items":[
                                    {
                                        "constraint_id":"value_enum",
                                        "constraint_content":"1,2"
                                    }
                                ]
                            }
                        ]
                    },
                    "field_clean_content": null,
                    "source_model_id": null,
                    "source_field_name": null
                },
                {
                    "field_name": "_time_",
                    "field_alias": "时间字段",
                    "field_index": 3,
                    "field_type": "timestamp",
                    "field_category": "dimension",
                    "is_primary_key": false,
                    "description": "平台内置时间字段，数据入库后将转换为可查询字段，比如 dtEventTime/dtEventTimeStamp/localtime",
                    "field_constraint_content": [],
                    "field_clean_content": null,
                    "source_model_id": null,
                    "source_field_name": null
                }
            ],
            "model_relation": []
        }'''
            % MODEL_ID
        )
        ret_dict = MasterTableManager.update_master_table(MODEL_ID, master_table_fields_params, bk_username=BK_USERNAME)
        assert ret_dict['step_id'] is not None

        master_table_fields_params['model_relation'] = [
            {
                "model_id": MODEL_ID,
                "field_name": "channel_id",
                "related_method": "left-join",
                "related_model_id": DIM_MODEL_ID,
                "related_field_name": "channel_id",
            }
        ]
        MasterTableManager.update_master_table(MODEL_ID, master_table_fields_params, bk_username=BK_USERNAME)
        assert DmmModelRelation.objects.filter(model_id=MODEL_ID).exists()

    @pytest.mark.usefixtures('init_model_info', 'init_master_table')
    def test_get_master_table_info(self):
        master_table_dict = MasterTableManager.get_master_table_info(
            MODEL_ID, with_time_field=True, with_details=['deletable', 'editable']
        )
        assert len(master_table_dict['fields']) > 0

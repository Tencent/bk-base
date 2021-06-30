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
import pytest

INSTANCE_ID = 1
PROJECT_ID = 1
MODEL_ID = 1
FLOW_ID = 1
BK_USERNAME = 'unittest'
VERSION_ID = '96wsipgxk5hc4nzd7jqey1mbv8fau0lt'
MAIN_TABLE_ID = '591_main_table'
BK_BIZ_ID = 591
INDICATOR_RESULT_TABLE_ID = '591_indicator_table'
CALCULATION_ATOM_NAME = 'max_price'


def _init_complete_model_instance():
    """构建一个完整的模型实例作为初始化的测试数据"""
    from datamanage.pro.datamodel.models import application as ins_models

    ins_models.DmmModelInstance.objects.create(
        instance_id=INSTANCE_ID,
        project_id=PROJECT_ID,
        model_id=MODEL_ID,
        flow_id=FLOW_ID,
        version_id=VERSION_ID,
        created_by=BK_USERNAME,
    )
    ins_models.DmmModelInstanceSource.objects.create(
        instance_id=INSTANCE_ID,
        input_type='main_table',
        input_result_table_id='591_source_table1',
        created_by=BK_USERNAME,
    )
    ins_models.DmmModelInstanceSource.objects.create(
        instance_id=INSTANCE_ID,
        input_type='dimension_table',
        input_result_table_id='591_source_dim_table1',
        created_by=BK_USERNAME,
    )
    ins_models.DmmModelInstanceTable.objects.create(
        result_table_id='591_main_table',
        bk_biz_id=BK_BIZ_ID,
        instance_id=INSTANCE_ID,
        model_id=MODEL_ID,
        flow_node_id=1,
        created_by=BK_USERNAME,
    )
    ins_models.DmmModelInstanceField.objects.create(
        instance_id=INSTANCE_ID,
        model_id=MODEL_ID,
        field_name='price',
        input_result_table_id='591_source_table1',
        input_field_name='source_field1',
        application_clean_content={
            'clean_option': 'SQL',
            'clean_content': 'source_field1 * 100 as price',
        },
        created_by=BK_USERNAME,
    )
    ins_models.DmmModelInstanceField.objects.create(
        instance_id=INSTANCE_ID,
        model_id=MODEL_ID,
        field_name='channel_id',
        input_result_table_id='591_source_table1',
        input_field_name='channel_id',
        application_clean_content={
            'clean_option': 'SQL',
            'clean_content': '',
        },
        created_by=BK_USERNAME,
    )
    ins_models.DmmModelInstanceRelation.objects.create(
        instance_id=INSTANCE_ID,
        model_id=MODEL_ID,
        related_model_id=2,
        field_name='channel_id',
        input_result_table_id='591_source_dim_table1',
        input_field_name='channel_id',
        created_by=BK_USERNAME,
    )
    ins_models.DmmModelInstanceField.objects.create(
        instance_id=INSTANCE_ID,
        model_id=MODEL_ID,
        field_name='channel_name',
        input_result_table_id='591_source_dim_table1',
        input_field_name='channel_name',
        application_clean_content={
            'clean_option': 'SQL',
            'clean_content': '',
        },
        created_by=BK_USERNAME,
    )
    ins_models.DmmModelInstanceIndicator.objects.create(
        result_table_id=INDICATOR_RESULT_TABLE_ID,
        project_id=PROJECT_ID,
        bk_biz_id=BK_BIZ_ID,
        instance_id=INSTANCE_ID,
        model_id=MODEL_ID,
        parent_result_table_id='591_main_table',
        flow_node_id=1000,
        calculation_atom_name=CALCULATION_ATOM_NAME,
        aggregation_fields=[],
        filter_formula='',
        scheduling_type='stream',
        scheduling_content={
            'window_type': 'slide',  # 窗口类型
            'window_time': 0,  # 窗口长度
            'waiting_time': 0,  # 延迟时间
            'count_freq': 30,  # 统计频率
            'window_lateness': {
                'allowed_lateness': True,  # 是否计算延迟数据
                'lateness_time': 1,  # 延迟时间
                'lateness_count_freq': 60,  # 延迟统计频率
            },
            'session_gap': None,  # 会话窗口时间
            'expired_time': 60,  # 会话过期时间
        },
    )


@pytest.fixture(scope='class')
def init_complete_model_instance():
    """构建一个完整的模型实例作为初始化的测试数据"""
    _init_complete_model_instance()

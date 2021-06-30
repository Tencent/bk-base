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
from datetime import datetime

import mock
import pytest

from common.api.base import DataResponse

from datamanage.tests.datamodel.init_data import init_complete_model_instance  # noqa


MODEL_ID = 1
DIM_MODEL_ID = 2
INSTANCE_ID = 1
PROJECT_ID = 1
FLOW_ID = 1
BK_BIZ_ID = 591
BK_USERNAME = 'unittest'
VERSION_ID = '96wsipgxk5hc4nzd7jqey1mbv8fau0lt'
INDICATOR_RESULT_TABLE_ID = '591_indicator_table'
MAIN_TABLE_ID = '591_main_table'
INDICATOR_RESULT_TABLE_ID = '591_indicator_table'
CALCULATION_ATOM_NAME = 'max_price'
INDICATOR_NAME = 'max_price_1d'
INDICATOR_ID = 1
UNKNOWN_INSTANCE_ID = 10000
UNKNOWN_MODEL_ID = 10000
UNKNOWN_TABLE_ID = '591_unknown_table'
OPERATION_LOG_PARAMS = {
    "page": 1,
    "page_size": 10,
    'conditions': [{'key': 'object_operation', 'value': ['release']}, {'key': 'object_type', 'value': ['model']}],
    'start_time': '2021-03-01 00:00:00',
    'end_time': None,
    'order_by_created_at': 'desc',
}


@pytest.fixture(scope='class')
def delete_model_instance_indicator():
    from datamanage.pro.datamodel.models import application as ins_models

    ins_models.DmmModelInstanceIndicator.objects.filter(
        instance_id=INSTANCE_ID,
        result_table_id=INDICATOR_RESULT_TABLE_ID,
    ).get().delete()


@pytest.fixture(scope='class')
def init_complete_model():
    """构建完整的数据模型"""
    from datamanage.pro.datamodel.models.datamodel import (
        DmmModelInfo,
        DmmModelField,
        DmmModelRelease,
        DmmModelRelation,
        DmmModelFieldStage,
    )
    from datamanage.pro.datamodel.models.indicator import DmmModelCalculationAtom, DmmModelIndicator

    BK_USERNAME = 'unittest'

    # 1) 创建主表
    fact_model_params = json.loads(
        '''{
        "model_id": %s,
        "model_name": "fact_model_name",
        "model_alias": "创建模型的中文名",
        "model_type": "fact_table",
        "description": "创建模型的描述",
        "project_id": 3
    }'''
        % MODEL_ID
    )
    fact_model_params['created_by'] = BK_USERNAME
    fact_model_params['created_at'] = datetime.now()
    model_obj = DmmModelInfo(**fact_model_params)
    model_obj.save()
    fact_model_id = model_obj.model_id
    project_id = model_obj.project_id

    # 2) 创建主表字段
    master_table_fields_params = json.loads(
        '''[
        {
            "model_id": %s,
            "field_name": "price",
            "field_alias": "道具价格",
            "field_index": 1,
            "field_type": "long",
            "field_category": "measure",
            "description": "道具价格",
            "field_constraint_content": null,
            "field_clean_content": {
                "clean_option": "SQL",
                "clean_content": "price  as price"
            },
            "source_model_id": null,
            "source_field_name": null
        },
        {
            "model_id": %s,
            "field_name": "channel_id",
            "field_alias": "渠道号",
            "field_index": 2,
            "field_type": "string",
            "field_category": "dimension",
            "description": "渠道号",
            "field_constraint_content": [],
            "field_clean_content": null,
            "source_model_id": null,
            "source_field_name": null
        },
        {
            "model_id": %s,
            "field_name": "channel_name",
            "field_alias": "渠道号名称",
            "field_index": 3,
            "field_type": "string",
            "field_category": "dimension",
            "description": "渠道号",
            "field_constraint_content": [],
            "field_clean_content": null,
            "source_model_id": %s,
            "source_field_name": "channel_name"
        },
        {
            "model_id": %s,
            "field_name": "time",
            "field_alias": "时间字段",
            "field_index": 4,
            "field_type": "timestamp",
            "field_category": "dimension",
            "description": "平台内置时间字段，数据入库后将装换为可查询字段，比如 dtEventTime/dtEventTimeStamp/localtime",
            "field_constraint_content": [],
            "field_clean_content": null,
            "source_model_id": null,
            "source_field_name": null
        }
    ]'''
        % (MODEL_ID, MODEL_ID, MODEL_ID, DIM_MODEL_ID, MODEL_ID)
    )
    for field_dict in master_table_fields_params:
        field_dict['model_id'] = fact_model_id
        field_dict['created_by'] = BK_USERNAME
        DmmModelField.objects.create(**field_dict)

    master_table_fields_params[0]['origin_fields'] = ['price']

    # 3) 创建统计口径
    calc_atom_params = json.loads(
        '''{
        "model_id": %s,
        "calculation_atom_name": "%s",
        "calculation_atom_alias": "max_price",
        "description": "max_price",
        "field_type": "long",
        "calculation_content": {
            "option": "SQL",
            "content": {
                "calculation_formula":"max(price)"
            }
        }
    }'''
        % (MODEL_ID, CALCULATION_ATOM_NAME)
    )
    calc_atom_params['model_id'] = fact_model_id
    calc_atom_params['created_by'] = BK_USERNAME
    calc_atom_params['project_id'] = project_id
    DmmModelCalculationAtom.objects.create(**calc_atom_params)

    # 4）创建指标
    indicator_params = json.loads(
        '''{
        "model_id": %s,
        "indicator_name":"%s",
        "indicator_alias":"1d最大价格",
        "description":"1d最大价格",
        "calculation_atom_name":"%s",
        "aggregation_fields":[],
        "filter_formula":"",
        "scheduling_type":"batch",
        "scheduling_content":{
            "window_type":"fixed",
            "count_freq":1,
            "schedule_period":"day",
            "fixed_delay":0,
            "dependency_config_type":"unified",
            "unified_config":{
                "window_size":1,
                "window_size_period":"day",
                "dependency_rule":"all_finished"
            },
            "advanced":{
                "recovery_times":3,
                "recovery_enable":false,
                "recovery_interval":"60m"
            }
        },
        "parent_indicator_name":null
    }'''
        % (MODEL_ID, INDICATOR_NAME, CALCULATION_ATOM_NAME)
    )
    indicator_params['model_id'] = fact_model_id
    indicator_params['created_by'] = BK_USERNAME
    indicator_params['project_id'] = project_id
    DmmModelIndicator.objects.create(**indicator_params)

    # 5) 创建维度模型
    dimension_model_params = json.loads(
        '''{
        "model_id": %s,
        "model_name": "dimension_model_name",
        "model_alias": "创建维度模型的中文名",
        "model_type": "dimension_table",
        "description": "创建维度模型的描述",
        "project_id": 3
    }'''
        % DIM_MODEL_ID
    )
    dimension_model_params['created_by'] = BK_USERNAME
    dim_model_obj = DmmModelInfo(**dimension_model_params)
    dim_model_obj.save()
    dimension_model_id = dim_model_obj.model_id

    # 6）创建维度模型主表字段
    dimension_master_table_params = json.loads(
        '''[
        {
            "model_id": %s,
            "field_name": "channel_id",
            "field_alias": "渠道号",
            "field_index": 1,
            "field_type": "string",
            "field_category": "dimension",
            "is_primary_key": true,
            "description": "渠道号",
            "field_constraint_content": [],
            "field_clean_content": null,
            "source_model_id": null,
            "source_field_name": null
        },
        {
            "model_id": %s,
            "field_name": "channel_name",
            "field_alias": "渠道号名称",
            "field_index": 2,
            "field_type": "string",
            "field_category": "dimension",
            "is_primary_key": false,
            "description": "渠道号名称",
            "field_constraint_content": [],
            "field_clean_content": null,
            "source_model_id": null,
            "source_field_name": null
        }
    ]'''
        % (DIM_MODEL_ID, DIM_MODEL_ID)
    )
    for field_dict in dimension_master_table_params:
        field_dict['model_id'] = dimension_model_id
        field_dict['created_by'] = BK_USERNAME
        DmmModelField.objects.create(**field_dict)
        DmmModelFieldStage.objects.create(**field_dict)

    # 7）创建事实表和维度表关联
    dimension_relation_params = json.loads(
        '''{
        "model_id": %s,
        "field_name": "channel_id",
        "related_method": "left-join",
        "related_model_id": %s,
        "related_field_name": "channel_id"
    }'''
        % (MODEL_ID, DIM_MODEL_ID)
    )
    dimension_relation_params['model_id'] = fact_model_id
    dimension_relation_params['related_model_id'] = dimension_model_id
    field_dict['created_by'] = BK_USERNAME
    DmmModelRelation.objects.create(**dimension_relation_params)

    # 8) 模型发布
    datamodel_release_dict = json.loads(
        '''{
        "model_content": {
            "model_id": %s,
            "model_detail": {
                "indicators": [
                    {
                        "model_id": null,
                        "calculation_atom_name": "%s",
                        "description": "1d最大价格",
                        "aggregation_fields_alias": [

                        ],
                        "created_at": "2020-11-20 19:05:26",
                        "created_by": "admin",
                        "indicator_alias": "1d最大价格",
                        "scheduling_type": "batch",
                        "updated_at": "2020-11-20 19:05:26",
                        "filter_formula": "",
                        "parent_indicator_name": null,
                        "scheduling_content": {
                            "window_type": "fixed",
                            "count_freq": 1,
                            "dependency_config_type": "unified",
                            "schedule_period": "day",
                            "fixed_delay": 0,
                            "unified_config": {
                                "window_size": 1,
                                "dependency_rule": "all_finished",
                                "window_size_period": "day"
                            },
                            "advanced": {
                                "recovery_enable": false,
                                "recovery_times": 3,
                                "recovery_interval": "60m"
                            }
                        },
                        "updated_by": null,
                        "aggregation_fields": [

                        ],
                        "project_id": 3,
                        "indicator_name": "max_price_1d"
                    }
                ],
                "fields": [

                ],
                "model_relation": [

                ],
                "calculation_atoms": [
                ]
            },
            "description": "创建模型的描述",
            "tags": [

            ],
            "table_alias": "",
            "created_by": "admin",
            "publish_status": "published",
            "model_alias": "创建模型的中文名",
            "active_status": "active",
            "updated_at": "2020-11-20 19:05:26",
            "table_name": "",
            "latest_version": null,
            "model_type": "fact_table",
            "step_id": 5,
            "created_at": "2020-11-20 19:05:26",
            "project_id": 3,
            "model_name": "fact_model_name",
            "updated_by": null
        },
        "version_log": "v1.0.0",
        "version_id": "%s"
    }'''
        % (MODEL_ID, CALCULATION_ATOM_NAME, VERSION_ID)
    )
    fields_extra_infos = {
        'price': {},
        'channel_id': {
            'is_join_field': True,
            'is_extended_field': False,
            'is_generated_field': False,
            'join_field_name': None,
        },
        'channel_name': {
            'is_join_field': False,
            'is_extended_field': True,
            'is_generated_field': False,
            'join_field_name': 'channel_id',
        },
        'time': {},
    }
    for table_field_info in master_table_fields_params:
        table_field_info.update(fields_extra_infos.get(table_field_info.get('field_name'), {}))
    datamodel_release_dict['model_id'] = fact_model_id
    datamodel_release_dict['model_content']['model_detail']['fields'] = master_table_fields_params
    datamodel_release_dict['model_content']['model_detail']['calculation_atoms'] = [calc_atom_params]
    datamodel_release_dict['model_content']['model_detail']['indicators'] = [indicator_params]
    datamodel_release_dict['model_content']['model_detail']['model_relation'] = [dimension_relation_params]
    datamodel_release_dict['created_by'] = BK_USERNAME
    datamodel_release_object = DmmModelRelease(**datamodel_release_dict)
    datamodel_release_object.save()

    model_obj.latest_version = datamodel_release_object
    model_obj.save()


@pytest.fixture(scope='class')
def patch_application_console_build():
    build_func = mock.Mock()
    build_func.return_value = 'SELECT * FROM unittest'

    with mock.patch('datamanage.pro.datamodel.application.jobs.console.Console.build', build_func):
        yield


@pytest.fixture(scope='class')
def patch_sql_validate():
    patch_func = mock.Mock()
    patch_func.return_value = (True, {'source_columns': {}, 'condition_columns': []})

    with mock.patch('datamanage.pro.datamodel.handlers.verifier.SQLVerifier.check_res_schema', patch_func):
        yield


@pytest.fixture(scope='function')
def patch_source_columns():
    patch_func = mock.Mock()
    patch_func.return_value = {'fields': {'price': ['source_field1']}}

    with mock.patch(
        target='datamanage.pro.datamodel.dmm.model_instance_manager.get_sql_source_columns_mapping', new=patch_func
    ):
        yield


@pytest.fixture(scope='function')
def patch_flow_create():
    patch_func = mock.Mock()
    patch_func.return_value = DataResponse(
        {
            'data': {
                'flow_id': FLOW_ID,
            },
            'result': True,
            'code': '1500200',
            'message': 'ok',
            'errors': {},
        }
    )

    with mock.patch(
        target='datamanage.pro.datamodel.dmm.model_instance_flow_manager.DataflowApi.flows.create', new=patch_func
    ):
        yield


@pytest.fixture(scope='function')
def patch_flow_nodes_create():
    patch_func = mock.Mock()
    patch_func.return_value = DataResponse(
        {
            'data': {
                'flow_id': FLOW_ID,
                'node_ids': [1, 2, 3, 4, 5],
            },
            'result': True,
            'code': '1500200',
            'message': 'ok',
            'errors': {},
        }
    )

    with mock.patch(
        target='datamanage.pro.datamodel.views.application_model_views.DataflowApi.flows.create_by_nodes',
        new=patch_func,
    ):
        yield


@pytest.fixture(scope='function')
def patch_rt():
    def side_effect(params, *args, **kwargs):
        result_table_id = params.get('result_table_id')

        return DataResponse(
            {
                'data': {
                    'result_table_id': result_table_id,
                    'result_table_name': '',
                    'result_table_name_alias': '',
                },
                'result': True,
                'code': '1500200',
                'message': 'ok',
                'errors': {},
            }
        )

    patch_func = mock.MagicMock(side_effect=side_effect)

    with mock.patch(
        target='datamanage.pro.datamodel.views.application_model_views.MetaApi.result_tables.retrieve', new=patch_func
    ):
        yield


@pytest.fixture(scope='function')
def patch_rt_fields():
    result_table_fields = {
        '591_source_table1': [
            {'field_name': 'price', 'field_type': 'long'},
            {'field_name': 'channel_id', 'field_type': 'string'},
        ],
        '591_source_dim_table1': [
            {'field_name': 'channel_id', 'field_type': 'string'},
            {'field_name': 'channel_name', 'field_type': 'string'},
        ],
    }

    def side_effect(params, *args, **kwargs):
        result_table_id = params.get('result_table_id')

        return DataResponse(
            {
                'data': result_table_fields.get(result_table_id, []),
                'result': True,
                'code': '1500200',
                'message': 'ok',
                'errors': {},
            }
        )

    patch_func = mock.MagicMock(side_effect=side_effect)

    with mock.patch(
        target='datamanage.pro.datamodel.dmm.model_instance_manager.MetaApi.result_tables.fields', new=patch_func
    ):
        yield


@pytest.fixture(scope='class')
def init_model_info():
    """构建完整的数据模型"""
    # 创建模型基本信息
    from datamanage.pro.datamodel.models.datamodel import DmmModelInfo

    fact_model_params = json.loads(
        '''{
        "model_id": %s,
        "model_name": "fact_model_name",
        "model_alias": "创建模型的中文名",
        "model_type": "fact_table",
        "description": "创建模型的描述",
        "project_id": %s
    }'''
        % (MODEL_ID, PROJECT_ID)
    )
    fact_model_params['created_by'] = BK_USERNAME
    fact_model_params['created_at'] = datetime.now()
    model_obj = DmmModelInfo(**fact_model_params)
    model_obj.save()


@pytest.fixture(scope='class')
def init_master_table():
    """构建模型主表"""
    from datamanage.pro.datamodel.models.datamodel import (
        DmmModelFieldStage,
    )

    master_table_field_list = json.loads(
        '''
    [
        {
            "field_name": "price",
            "field_alias": "道具价格",
            "field_index": 1,
            "field_type": "long",
            "field_category": "measure",
            "is_primary_key": false,
            "description": "道具价格",
            "field_constraint_content": null,
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
            "field_constraint_content": [],
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
    ]'''
    )
    for field_dict in master_table_field_list:
        field_dict['created_by'] = BK_USERNAME
        field_dict['model_id'] = MODEL_ID
        field_obj = DmmModelFieldStage(**field_dict)
        field_obj.save()


@pytest.fixture(scope='class')
def init_calculation_atom():
    """构建统计口径"""
    from datamanage.pro.datamodel.models.indicator import DmmModelCalculationAtom

    calc_atom_params = json.loads(
        '''
    {
        "model_id": %s,
        "project_id": %s,
        "calculation_atom_name": "%s",
        "calculation_atom_alias": "创建统计口径的中文名",
        "description": "创建统计口径的描述",
        "field_type": "long",
        "calculation_content": {
            "option": "TABLE",
            "content": {
                "calculation_field": "price",
                "calculation_function": "max",
                "calculation_formula":null
            }
        }
    }'''
        % (MODEL_ID, PROJECT_ID, CALCULATION_ATOM_NAME)
    )
    calc_atom_params['created_by'] = BK_USERNAME
    calc_atom_obj = DmmModelCalculationAtom(**calc_atom_params)
    calc_atom_obj.save()


@pytest.fixture(scope='class')
def init_indicator():
    """构建指标"""
    from datamanage.pro.datamodel.models.indicator import DmmModelIndicatorStage

    indicator_params = json.loads(
        '''
    {
        "model_id": %s,
        "project_id": %s,
        "indicator_name": "%s",
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
        % (MODEL_ID, PROJECT_ID, INDICATOR_NAME, INDICATOR_ID, CALCULATION_ATOM_NAME)
    )
    indicator_params['created_by'] = BK_USERNAME
    indicator_obj = DmmModelIndicatorStage(**indicator_params)
    indicator_obj.save()

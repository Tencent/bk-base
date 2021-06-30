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


from common.base_utils import model_to_dict

from datamanage.pro import exceptions as dm_pro_errors
from datamanage.pro.datamodel.models.indicator import DmmModelCalculationAtom


class ModelInstanceMixin(object):
    def prepare_fact_sql_generate_params(self, model_instance, model_info, instance_sources, instance_table_fields):
        """准备生成模型应用SQL的参数

        :param model_instance: 模型应用实例实体
        :param model_info: 模型版本发布信息
        :param instance_sources: 记录了模型应用实例各类型输入表的ID
            {
                'main_table': '1_main_table',
                'dimension_tables': ['1_dim_table1', '1_dim_table2']
            }
        :param instance_table_fields: 模型应用实例主表字段列表

        :return: 用于生成模型应用SQL参数，形如:
            {
                "node_type": "fact_table",
                "node_conf": {
                    "main_table": "1_main_table",
                    "fields": [
                        {
                            "field_name": "field1",
                            "field_index": 1,
                            "field_type": "int",
                            "field_clean_content": {
                                "clean_option": "SQL",
                                "clean_content": ""
                            },
                            ...
                            "dmm_relation": {
                                "related_method": "left-join",
                                ...
                            },
                            "ins_field": {
                                "input_result_table_id": "1_source_table",
                                "input_field_name": "source_field1",
                                "application_clean_content": {
                                    "clean_option": "SQL",
                                    "clean_content": ""
                                },
                                ...
                            },
                            "ins_relation": {
                                "input_result_table_id": "",
                                "input_field_name": "",
                                ...
                            }
                        }
                    ]
                }
            }

        :raises DataModelNotExistError: 数据模型不存在
        """
        # 从模型发布版本中把字段信息和关联信息都转化成字典已被查询
        model_fields = {}
        for field_info in model_info.get('model_detail', {}).get('fields', []):
            model_fields[field_info.get('field_name')] = field_info

        model_relations = {}
        for relation_info in model_info.get('model_detail', {}).get('model_relation', []):
            relation_key = (
                relation_info.get('model_id'),
                relation_info.get('field_name'),
                relation_info.get('related_model_id'),
            )
            model_relations[relation_key] = relation_info

        # 构建生成SQL时依赖的字段信息
        fields = []
        for instance_table_field in instance_table_fields:
            field_name = instance_table_field.field_name

            # 如果从模型发布版本信息中
            if field_name not in model_fields:
                raise dm_pro_errors.FieldNotExistError(
                    '无法在模型({model_id})的发布版本({version_id})中找到字段映射配置中的字段({field_name})'.format(
                        model_id=model_instance.model_id,
                        version_id=model_instance.version_id,
                        field_name=field_name,
                    )
                )

            # 把application_clean_content里内容为空的字段统一成application_clean_content为空字典的结构
            if not instance_table_field.application_clean_content.get('clean_content'):
                instance_table_field.application_clean_content = {}

            # 从模型发布的详情里获取模型字段的详情，作为组装字段参数的基础
            field_params = model_fields[field_name]
            field_params['ins_field'] = model_to_dict(instance_table_field)

            # 如果模型应用实例时进行了维度关联，则需要找到该模型定义时字段关联所用的关联关系，从而获取其中的关联方法
            if instance_table_field.relation:
                relation_key = (model_instance.model_id, field_name, instance_table_field.relation.related_model_id)
                if relation_key not in model_relations:
                    raise dm_pro_errors.ModelRelationNotExistError(
                        message_kv={
                            'field_name': field_name,
                            'model_id': model_instance.model_id,
                            'related_model_id': instance_table_field.relation.related_model_id,
                        }
                    )

                field_params['dmm_relation'] = model_relations[relation_key]
                field_params['ins_relation'] = model_to_dict(instance_table_field.relation)
            else:
                field_params['dmm_relation'] = {}
                field_params['ins_relation'] = {}

            fields.append(field_params)

        return {
            'node_type': model_instance.model.model_type,
            'node_conf': {
                'main_table': instance_sources['main_table'],
                'fields': fields,
            },
        }

    def prepare_ind_sql_generate_params(self, model_instance, instance_indicator):
        """准备生成模型应用SQL的参数

        :param model_instance: 模型应用实例实体
        :param instance_indicator: 模型应用指标实体

        :return: 用于生成模型应用SQL参数，形如:
            {
                "node_type": "indicator",
                "node_conf": {
                    "main_table": "1_main_table",
                    "calculation_atom": {
                        "calculation_atom_name": "test_atom",
                        "field_type": "int",
                        "calculation_formula": "count(field1)"
                    },
                    "ins_indicator": {
                        "aggregation_fields": "dim1",
                        "filter_fomula": "field1 is not null"
                    }
                }
            }

        :raises CalculationAtomNotExistError: 统计口径不存在
        """
        try:
            calculation_atom = DmmModelCalculationAtom.objects.get(
                calculation_atom_name=instance_indicator.calculation_atom_name,
            )
        except DmmModelCalculationAtom.DoesNotExist:
            raise dm_pro_errors.CalculationAtomNotExistError()

        return {
            'node_type': 'indicator',
            'node_conf': {
                'main_table': instance_indicator.parent_result_table_id,
                'calculation_atom': {
                    'calculation_atom_name': calculation_atom.calculation_atom_name,
                    'field_type': calculation_atom.field_type,
                    'calculation_formula': calculation_atom.calculation_formula,
                },
                'ins_indicator': {
                    'aggregation_fields': instance_indicator.aggregation_fields,
                    'filter_formula': instance_indicator.filter_formula,
                },
            },
        }

    def get_related_model_about_model_inst(self, model_instance):
        """获取模型实例主表相关的所有模型ID

        :param model_instance: 模型应用实例

        :return: 模型ID列表
        """
        pass

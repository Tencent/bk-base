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


import copy

from django.utils.translation import ugettext_lazy as _

from common.local import get_request_username

from datamanage.pro import exceptions as dm_pro_errors
from datamanage.pro.datamodel.handlers.verifier import SQLVerifier
from datamanage.pro.datamodel.models.application import (
    DmmModelInstance,
    DmmModelInstanceTable,
    DmmModelInstanceIndicator,
    DmmModelInstanceField,
    DmmModelInstanceRelation,
    DmmModelInstanceSource,
)
from datamanage.pro.datamodel.models.datamodel import DmmModelInfo, DmmModelRelease
from datamanage.pro.datamodel.dmm.manager import IndicatorManager
from datamanage.pro.datamodel.models.model_dict import (
    ModelInputNodeType,
    InstanceInputType,
    NODE_TYPE_INSTANCE_INPUT_MAPPINGS,
    APPLICATION_EXCLUDE_FIELDS,
    SCHEDULING_INDICATOR_NODE_TYPE_MAPPINGS,
    BATCH_CHANNEL_CLUSTER_TYPE,
    BATCH_CHANNEL_NODE_DEFAULT_CONFIG,
    DataModelNodeType,
    SchedulingEngineType,
    SchedulingType,
    BATCH_CHANNEL_NODE_TYPE,
    RELATION_STORAGE_NODE_DEFAULT_CONFIG,
    RELATION_STORAGE_NODE_TYPE,
    CleanOptionType,
)
from datamanage.pro.datamodel.utils import get_sql_source_columns_mapping, get_sql_fields_alias_mapping
from datamanage.utils.api import MetaApi


class ModelInstanceManager(object):
    SQL_VERIFY_TMP_TABLE_NAME = 'tmp_table'

    @staticmethod
    def get_model_instance_by_id(model_instance_id):
        """根据模型实例ID获取模型实例

        :param model_instance_id 模型实例ID

        :return: 模型实例实体
        """
        try:
            model_instance = DmmModelInstance.objects.get(instance_id=model_instance_id)
        except DmmModelInstance.DoesNotExist:
            raise dm_pro_errors.ModelInstanceNotExistError(message_kv={'model_instance_id': model_instance_id})

        return model_instance

    @staticmethod
    def get_model_instance_by_rt(result_table_id):
        """根据结果表ID获取数据模型实例

        :param result_table_id 主表或者指标表结果表ID

        :return: 模型实例实体
        """
        try:
            model_instance_table = ModelInstanceManager.get_model_instance_table_by_rt(result_table_id)
            return model_instance_table.instance
        except dm_pro_errors.ModelInstanceHasNoTableError:
            pass

        try:
            model_instance_indicator = ModelInstanceManager.get_model_instance_indicator_by_rt(result_table_id)
            return model_instance_indicator.instance
        except dm_pro_errors.InstanceIndicatorNotExistError:
            pass

        raise dm_pro_errors.ResultTableNotGenerateByAnyInstanceError(
            message_kv={
                'result_table_id': result_table_id,
            }
        )

    @staticmethod
    def get_model_instances_by_flow_id(flow_id):
        """根据数据流ID获取数据模型实例

        :param flow_id 数据流ID

        :return: 数据模型实例列表
        """
        model_instances = DmmModelInstance.objects.filter(flow_id=flow_id)

        if model_instances.count() == 0:
            raise dm_pro_errors.FlowHasNoInstanceError(message_kv={'flow_id': flow_id})

        return model_instances

    @staticmethod
    def get_model_instance_by_node_id(node_id):
        """根据节点ID获取数据模型实例

        :param node_id 数据流节点ID

        :return: 模型实例实体
        """
        try:
            model_instance_table = DmmModelInstanceTable.objects.filter(flow_node_id=node_id).get()
            return model_instance_table.instance
        except DmmModelInstanceTable.DoesNotExist:
            pass

        try:
            model_instance_indicator = DmmModelInstanceIndicator.objects.filter(flow_node_id=node_id).get()
            return model_instance_indicator.instance
        except DmmModelInstanceIndicator.DoesNotExist:
            pass

        raise dm_pro_errors.ModelInstanceNotExistError('无法通过节点ID({node_id})找到数据模型实例'.format(node_id=node_id))

    @staticmethod
    def get_model_instance_table_by_id(model_instance_id):
        """根据模型实例ID获取模型实例主表

        :param model_instance_id 模型实例ID

        :return: 模型实例主表实体
        """
        try:
            model_instance_table = DmmModelInstanceTable.objects.filter(instance_id=model_instance_id).get()
        except DmmModelInstanceTable.DoesNotExist:
            raise dm_pro_errors.ModelInstanceHasNoTableError(message_kv={'model_instance_id': model_instance_id})

        return model_instance_table

    @staticmethod
    def get_model_instance_table_by_rt(result_table_id):
        """根据结果表ID获取数据模型实例主表

        :param result_table_id 结果表ID

        :return: 数据模型实例主表
        """
        try:
            model_instance_table = DmmModelInstanceTable.objects.filter(result_table_id=result_table_id).get()
        except DmmModelInstanceTable.DoesNotExist:
            raise dm_pro_errors.ModelInstanceHasNoTableError(_('无法根据结果表ID找到数据模型实例主表'))

        return model_instance_table

    @staticmethod
    def get_model_instance_indicator_by_rt(result_table_id):
        """根据结果表ID获取模型应用实例指标

        :param result_table_id 结果表ID

        :return: 模型实例指标
        """
        try:
            instance_indicator = DmmModelInstanceIndicator.objects.filter(result_table_id=result_table_id).get()
        except DmmModelInstanceIndicator.DoesNotExist:
            raise dm_pro_errors.InstanceIndicatorNotExistError(message_kv={'result_table_id': result_table_id})

        return instance_indicator

    @staticmethod
    def get_model_by_id(model_id):
        """根据model_id获取数据模型

        :param model_id: 模型ID

        :return: 数据模型实体

        :raises DataModelNotExistError: 数据模型不存在
        """
        try:
            model = DmmModelInfo.objects.get(model_id=model_id)
        except DmmModelInfo.DoesNotExist:
            raise dm_pro_errors.DataModelNotExistError()

        return model

    @staticmethod
    def get_model_release_by_id_and_version(model_id, version_id):
        """根据model_id和version_id获取模型发布时的全部信息

        :param model_id: 模型ID
        :param version_id: 模型版本ID

        :return: 已发布的模型信息

        :raises ModelVersionNotExistError: 无法找到模型关于某版本的信息
        """
        try:
            model_release = DmmModelRelease.objects.filter(model_id=model_id, version_id=version_id).get()
        except DmmModelRelease.DoesNotExist:
            raise dm_pro_errors.ModelVersionNotExistError(
                message_kv={
                    'model_id': model_id,
                    'version_id': version_id,
                }
            )

        try:
            model = DmmModelInfo.objects.filter(model_id=model_id).get()
            model_release.model_content.update(
                {
                    'model_alias': model.model_alias,
                    'description': model.description,
                    'model_type': model.model_type,
                }
            )
        except DmmModelInfo.DoesNotExist:
            raise dm_pro_errors.ModelVersionNotExistError(
                message_kv={
                    'model_id': model_id,
                    'version_id': version_id,
                }
            )

        return model_release

    @staticmethod
    def get_model_and_check_latest_version(model_id, version_id):
        """获取数据模型并检测当前版本是否为最新版本

        :param model_id 模型ID
        :param current_version_id 待检测模型版本ID

        :return: 数据模型最新版本信息
        """
        model = ModelInstanceManager.get_model_by_id(model_id)
        if version_id != model.latest_version_id:
            raise dm_pro_errors.ModelVersionNotLatestError(
                message_kv={
                    'version_id': version_id,
                    'latest_version_id': model.latest_version_id,
                }
            )

        model_release = ModelInstanceManager.get_model_release_by_id_and_version(model_id, version_id)
        return model_release

    @staticmethod
    def get_instance_sources_by_instance(model_instance):
        """获取模型应用实例输入表信息

        :param model_instance: 数据模型实例

        :return: 包含输入来源主表和维度表的字典
        """
        instance_sources = {
            InstanceInputType.MAIN_TABLE.value: [],
            InstanceInputType.DIM_TABLE.value: [],
        }

        for instance_source in DmmModelInstanceSource.objects.filter(instance=model_instance):
            if instance_source.input_type not in instance_sources:
                continue
            instance_sources[instance_source.input_type].append(instance_source.input_result_table_id)

        return {
            'main_table': instance_sources[InstanceInputType.MAIN_TABLE.value][0],
            'dimension_tables': instance_sources[InstanceInputType.DIM_TABLE.value],
        }

    @staticmethod
    def get_model_instance_indicators(model_instance):
        """获取模型实例的指标列表

        :param model_instance: 数据模型实例

        :return: 指标列表
        """
        return DmmModelInstanceIndicator.objects.filter(instance=model_instance)

    @staticmethod
    def get_model_instance_tail_indicators(model_instance):
        """获取模型实例中结尾的指标节点

        :param model_instance: 数据模型实例

        :return: 尾部指标列表
        """
        instance_indicators = ModelInstanceManager.get_model_instance_indicators(model_instance)

        parent_indicators = set()

        for instance_indicator in instance_indicators:
            if instance_indicator.parent_result_table_id:
                parent_indicators.add(instance_indicator.parent_result_table_id)

        indicators = []
        for instance_indicator in instance_indicators:
            if instance_indicator.result_table_id not in parent_indicators:
                indicators.append(instance_indicator)

        return indicators

    @staticmethod
    def create_instance_table_fields(model_instance, fields_params):
        """创建所有模型实例映射字段和维表关联关系

        :param model_instance: 数据模型实例
        :param fields_params: 字段映射配置参数

        :return: DmmModelInstanceField实例列表
        """
        instance_table_fields = []

        for field_params in fields_params:
            instance_table_field = ModelInstanceManager.create_instance_table_field(model_instance, field_params)
            instance_table_fields.append(instance_table_field)

        return instance_table_fields

    @staticmethod
    def create_instance_table_field(model_instance, field_params, created_by=None):
        """创建单个模型实例映射字段和维表关联关系

        :param model_instance: 数据模型实例
        :param field_params: 单个字段映射配置参数
        :param created_by: 创建用户名

        :return: DmmModelInstanceField实例
        """
        model_id = field_params['model_id']
        field_name = field_params['field_name']

        instance_table_field = DmmModelInstanceField.objects.create(
            instance=model_instance,
            model_id=model_id,
            field_name=field_name,
            input_result_table_id=field_params['input_result_table_id'],
            input_field_name=field_params['input_field_name'],
            application_clean_content=field_params['application_clean_content'],
            created_by=created_by or model_instance.created_by,
        )
        instance_table_field.relation = None
        if field_params.get('relation'):
            relation = DmmModelInstanceRelation.objects.create(
                instance=model_instance,
                model_id=model_id,
                field_name=field_name,
                related_model_id=field_params['relation']['related_model_id'],
                input_result_table_id=field_params['relation']['input_result_table_id'],
                input_field_name=field_params['relation']['input_field_name'],
                created_by=created_by or model_instance.created_by,
            )
            instance_table_field.relation = relation
        return instance_table_field

    @staticmethod
    def save_model_instance_sources(model_instance, from_result_tables_params):
        """根据上游节点类型生成模型实例所有用到的输入表及其类型

        :param model_instance: 数据模型实例
        :param from_result_tables_params: 来源输入表列表

        :return: 包含输入来源主表和维度表的字典
        """
        instance_sources = {
            InstanceInputType.MAIN_TABLE.value: [],
            InstanceInputType.DIM_TABLE.value: [],
        }

        source_queryset = DmmModelInstanceSource.objects.filter(instance=model_instance)
        old_instance_sources = {item.input_result_table_id: item for item in source_queryset}

        for from_result_table_info in from_result_tables_params:
            # 获取当前来源表的输入类型
            input_type = ModelInstanceManager.get_input_type_by_from_rt_info(
                from_result_table_info, len(from_result_tables_params)
            )
            input_result_table_id = from_result_table_info.get('result_table_id')

            # 记录各种类型输入表的数量以用于校验和结果返回
            instance_sources[input_type].append(input_result_table_id)

            if input_result_table_id not in old_instance_sources:
                DmmModelInstanceSource.objects.create(
                    instance=model_instance,
                    input_type=input_type,
                    input_result_table_id=input_result_table_id,
                    created_by=model_instance.created_by,
                )
                continue

            # 如果同名输入表类型被变更了，需要对当前输入信息进行更新
            instance_source = old_instance_sources.pop(input_result_table_id)
            if instance_source.input_type != input_type:
                instance_source.input_type = input_type
                instance_source.updated_by = get_request_username()
                instance_source.save()

        if not instance_sources[InstanceInputType.MAIN_TABLE.value]:
            raise dm_pro_errors.NoMainSourceTableError()

        if len(instance_sources[InstanceInputType.MAIN_TABLE.value]) > 1:
            raise dm_pro_errors.MultiMainSourceTableError()

        return {
            'main_table': instance_sources[InstanceInputType.MAIN_TABLE.value][0],
            'dimension_tables': instance_sources[InstanceInputType.DIM_TABLE.value],
        }

    @staticmethod
    def get_input_type_by_from_rt_info(from_result_table_info, from_count):
        """从来源结果表信息及其节点类型获取该表的输入类型

        :param from_result_table_info 来源结果表信息
        :param from_count 来源表数量

        :return: 来源表输入类型
        """
        node_type = from_result_table_info.get('node_type')
        if node_type not in NODE_TYPE_INSTANCE_INPUT_MAPPINGS:
            raise dm_pro_errors.NodeTypeNotSupportedError(
                message_kv={
                    'node_type': node_type,
                }
            )

        # 对于离线关联输入节点，如果只有一个输入表，则将作为主表汇总到实例输入表中
        if (
            node_type in [ModelInputNodeType.KV_SOURCE.value, ModelInputNodeType.UNIFIED_KV_SOURCE.value]
            and from_count == 1
        ):
            input_type = InstanceInputType.MAIN_TABLE.value
        else:
            input_type = NODE_TYPE_INSTANCE_INPUT_MAPPINGS[node_type].value

        return input_type

    @staticmethod
    def update_model_instance(model_instance, update_params, fact_sql_base_params=None):
        """更新模型应用实例

        :param model_instance: 模型应用实例实体
        :param update_params: 更新所用参数

        :return: 更新后的模型应用实例
        """
        if 'model_id' in update_params:
            model_instance.model_id = update_params['model_id']

        if update_params['upgrade_version']:
            version_id = update_params['version_id']
            model_release = ModelInstanceManager.get_model_and_check_latest_version(model_instance.model_id, version_id)
            model_instance.version_id = model_release.version_id

            if fact_sql_base_params:
                fact_sql_base_params['model_info'] = model_release.model_content

        model_instance.updated_by = get_request_username()
        model_instance.save()
        return model_instance

    @staticmethod
    def update_model_instance_table(model_instance, model_id):
        """更新模型应用实例主表

        :param model_instance: 模型应用实例实体
        :param model_id: 待更新的数据模型ID

        :return: 更新后的模型应用实例

        :raises ModelInstanceHasNoTableError: 当前实例没有任何主表
        """
        model_instance_table = ModelInstanceManager.get_model_instance_table_by_id(model_instance.instance_id)

        model_instance_table.model_id = model_id
        model_instance_table.updated_by = get_request_username()
        model_instance_table.save()
        return model_instance_table

    @staticmethod
    def update_instance_table_fields(model_instance, fields_params):
        """更新模型应用实例主表字段映射关系

        :param model_instance: 模型应用实例实体
        :param fields_params: 更新所用字段映射参数

        :return: 更新后的字段映射信息列表
        """
        src_fields = DmmModelInstanceField.objects.filter(instance=model_instance)
        src_fields_relations = DmmModelInstanceRelation.objects.filter(instance=model_instance)

        # 获取旧模型应用实例主表的字段
        for src_field in src_fields:
            src_field.relation = None
        src_fields_dict = {field.field_name: field for field in src_fields}
        for relation in src_fields_relations:
            src_fields_dict[relation.field_name].relation = relation

        # 应用实例主表字段信息进行更新或按修改情况进行增删
        instance_table_fields = []
        for field_params in fields_params:
            field_name = field_params['field_name']

            if field_name not in src_fields_dict:
                # 如果新增了字段映射配置，则创建
                instance_table_field = ModelInstanceManager.create_instance_table_field(
                    model_instance,
                    field_params,
                    get_request_username(),
                )
                continue
            else:
                # 如果当前字段映射配置已存在，则更新
                instance_table_field = ModelInstanceManager.update_instance_table_field(
                    model_instance,
                    src_fields_dict[field_name],
                    field_params,
                )
                del src_fields_dict[field_name]

            instance_table_fields.append(instance_table_field)

        # 对于已经删除的字段映射配置，删除之
        for field_name, src_instance_table_field in list(src_fields_dict.items()):
            if src_instance_table_field.relation:
                src_instance_table_field.relation.delete()
            src_instance_table_field.delete()

        return instance_table_fields

    @staticmethod
    def get_inst_table_fields_by_inst(model_instance):
        """获取模型应用实例字段映射信息

        :param model_instance: 数据模型实例

        :return: 字段映射信息列表
        """

        instance_fields = DmmModelInstanceField.objects.filter(instance=model_instance)
        instance_fields_relations = DmmModelInstanceRelation.objects.filter(instance=model_instance)

        # 获取旧模型应用实例主表的字段
        for instance_field in instance_fields:
            instance_field.relation = None
        instance_fields_dict = {field.field_name: field for field in instance_fields}
        for relation in instance_fields_relations:
            instance_fields_dict[relation.field_name].relation = relation

        return list(instance_fields_dict.values())

    @staticmethod
    def update_instance_table_field(model_instance, instance_table_field, field_params):
        """更新单个模型应用实例字典映射配置及其关联关系

        :param model_instance: 模型应用实例
        :param instance_table_field: 模型应用实例主表字段
        :param field_params: 更新所需参数

        :return: 更新后的模型应用实例主表字段
        """
        instance_table_field.model_id = model_instance.model_id
        instance_table_field.input_result_table_id = field_params['input_result_table_id']
        instance_table_field.input_field_name = field_params['input_field_name']
        instance_table_field.application_clean_content = field_params['application_clean_content']

        if field_params.get('relation'):
            # 当该字段原本直接来源于主表，如今更新后通过维度表关联生成，则会进入该分支创建relation
            if instance_table_field.relation is None:
                relation = DmmModelInstanceRelation.objects.create(
                    instance=model_instance,
                    model_id=model_instance.model_id,
                    field_name=instance_table_field.field_name,
                    related_model_id=field_params['relation']['related_model_id'],
                    input_result_table_id=field_params['relation']['input_result_table_id'],
                    input_field_name=field_params['relation']['input_field_name'],
                    created_by=model_instance.updated_by,
                )
                instance_table_field.relation = relation
            else:
                instance_table_field.relation.related_model_id = field_params['relation']['related_model_id']
                instance_table_field.relation.input_result_table_id = field_params['relation']['input_result_table_id']
                instance_table_field.relation.input_field_name = field_params['relation']['input_field_name']
                instance_table_field.relation.save()
        elif instance_table_field.relation:
            # 如果取消了维度表关联，则删除原来关联关系
            instance_table_field.relation.delete()
            instance_table_field.relation = NotImplemented

        instance_table_field.save()

        return instance_table_field

    @staticmethod
    def validate_model_structure(instance_table_fields, model_info):
        """校验模型应用时字段映射结构是否与发布模型结构完全一致

        :param instance_table_fields: 应用字段列表
        :param model_info: 模型版本信息
        """
        model_fields, model_relations = {}, {}

        for model_field_info in model_info.get('model_detail', {}).get('fields', []):
            if model_field_info.get('field_name') in APPLICATION_EXCLUDE_FIELDS:
                continue
            model_fields[model_field_info.get('field_name')] = model_field_info

        for model_relation_info in model_info.get('model_detail', {}).get('model_relation', []):
            model_relations[model_relation_info.get('field_name')] = model_relation_info

        for instance_table_field in instance_table_fields:
            field_name = instance_table_field.field_name

            if field_name not in model_fields:
                raise dm_pro_errors.ModelInstanceStructureNotSameError()

            model_field_info = model_fields.pop(field_name)
            if not ModelInstanceManager.compare_fields(instance_table_field, model_field_info):
                raise dm_pro_errors.ModelInstanceStructureNotSameError()

            if instance_table_field.relation:
                if field_name not in model_relations:
                    raise dm_pro_errors.ModelInstanceStructureNotSameError()

                model_relation_info = model_relations.pop(field_name)
                if not ModelInstanceManager.compare_relations(instance_table_field, model_relation_info):
                    raise dm_pro_errors.ModelInstanceStructureNotSameError()

        # 如果pop出左右待对比的模型字段和模型关联后还有剩余的字段信息或关联信息，说明结构不一致
        if model_fields or model_relations:
            raise dm_pro_errors.ModelInstanceStructureNotSameError()

    @staticmethod
    def compare_fields(instance_table_field, model_field_info):
        """比较模型应用字段结构是否与发布时结构一致

        :param instance_table_field: 应用实例字段
        :param model_field_info: 模型发布时字段信息

        :return: 如果结构一致，则返回true
        """
        return True

    @staticmethod
    def compare_relations(instance_table_field, model_relation_info):
        """比较模型应用关联关系是否与发布时结构一致

        :param instance_table_field: 有非None的relation的应用实例字段信息
        :param model_relation_info: 模型发布时关联信息

        :return: 如果结构一致，则返回true
        """
        if instance_table_field.relation.related_model_id != model_relation_info.get('related_model_id'):
            return False

        return True

    @staticmethod
    def validate_instance_table_fields(instance_table_fields, instance_sources):
        """校验模型应用字段映射是否合法

        :param instance_table_fields: 应用字段列表
        :param instance_sources: 应用输入表
        """
        for instance_table_field in instance_table_fields:
            ModelInstanceManager.validate_clean_content(instance_table_field, instance_sources)

            alias_name = ModelInstanceManager.validate_alias_name(instance_table_field)

            ModelInstanceManager.validate_source_columns(instance_table_field, alias_name)

    @staticmethod
    def validate_input_table(instance_table_fields, instance_sources, model_content):
        """校验模型应用字段来源表信息

        所有字段映射中，模型的主表字段应该全部来自输入主表字段，模型的关联字段要么来自输入主表字段，要么来自输入维表字段，并且是一组的

        :param instance_table_fields: 应用字段列表
        :param instance_sources: 应用输入表
        :param model_content: 该模型发布版本详情
        """
        model_fields_sources = ModelInstanceManager.generate_model_fields_sources(model_content)

        # 由于校验关联字段和费关联字段的逻辑区别较大，因此会在两个循环中分别校验维度关联字段和其他字段（主表、扩展、生成字段）
        ModelInstanceManager.validate_input_relation_fields(
            instance_table_fields, instance_sources, model_fields_sources
        )
        ModelInstanceManager.validate_input_non_relation_fields(
            instance_table_fields, instance_sources, model_fields_sources
        )

    @staticmethod
    def validate_input_relation_fields(instance_table_fields, instance_sources, model_fields_sources):
        """校验维度关联字段

        :param instance_table_fields: 应用字段列表
        :param instance_sources: 应用输入表
        :param model_fields_sources: 模型发布是的输出字段详情
        """
        for instance_table_field in instance_table_fields:
            field_name = instance_table_field.field_name

            if not instance_table_field.input_field_name:
                continue

            if instance_table_field.relation:
                related_input_field_name = instance_table_field.relation.input_field_name
                related_input_result_table_id = instance_table_field.relation.input_result_table_id

                # 补充维度关联字段的输入表信息，以备后续校验维度扩展字段时使用
                model_fields_sources[field_name].update(
                    {
                        'input_result_table_id': related_input_result_table_id,
                    }
                )

                # 这里允许维度关联字段不进行维度关联，并从主表里选择所有维度扩展字段
                if not related_input_field_name:
                    continue

                related_field_name = model_fields_sources[field_name].get('related_field_name')
                if related_field_name != related_input_field_name:
                    raise dm_pro_errors.DimFieldMustEqualFieldNameError(
                        message_kv={
                            'related_input_result_table_id': related_input_result_table_id,
                            'related_input_field_name': related_input_field_name,
                            'source_field': related_field_name,
                        }
                    )

                # 关联字段的关联输入必须来源于输入的维度表
                if related_input_result_table_id not in instance_sources['dimension_tables']:
                    raise dm_pro_errors.DimFieldMustFromDimTableError(
                        message_kv={
                            'field_name': field_name,
                        }
                    )

    @staticmethod
    def validate_input_non_relation_fields(instance_table_fields, instance_sources, model_fields_sources):
        """校验主表字段、维度扩展字和生成字段

        :param instance_table_fields: 应用字段列表
        :param instance_sources: 应用输入表
        :param model_fields_sources: 模型发布是的输出字段详情
        """
        for instance_table_field in instance_table_fields:
            field_name = instance_table_field.field_name
            input_table = instance_table_field.input_result_table_id

            if not instance_table_field.input_field_name:
                continue

            if not instance_table_field.relation:
                # 如果非扩展维度扩展字段且来源表不等于输入主表，则不合法
                is_join_field = model_fields_sources[field_name]['is_join_field']
                is_extended_field = model_fields_sources[field_name]['is_extended_field']

                if not is_extended_field:
                    # 如果字段是生成字段，则不对主表输入字段做校验
                    if not is_join_field:
                        origin_fields = model_fields_sources[field_name].get('origin_fields', [])
                        is_generated_field = len(origin_fields) != 1 or origin_fields[0] != field_name
                    else:
                        is_generated_field = False

                    # 如果不是生成字段，且不是生成字段，则该字段必须来源于主表
                    if (
                        not is_generated_field
                        and instance_table_field.input_field_name
                        and input_table != instance_sources['main_table']
                    ):
                        raise dm_pro_errors.MainFieldMustFromMainTableError(
                            message_kv={
                                'field_name': instance_table_field.field_name,
                            }
                        )
                # 如果是扩展字段，对于非生成字段（仅由其他字段生成），则允许当前字段来源于主表或者关联维度所在的输入表
                else:
                    ModelInstanceManager.validate_extended_field(
                        instance_table_field, instance_sources, model_fields_sources
                    )

    @staticmethod
    def validate_extended_field(instance_table_field, instance_sources, model_fields_sources):
        """校验维度扩展字段是否正确

        :param instance_table_field: 输入字段映射
        :param instance_sources: 应用输入表
        :param model_fields_sources: 模型发布是的输出字段详情
        """
        field_name = instance_table_field.field_name
        input_table = instance_table_field.input_result_table_id
        input_field = instance_table_field.input_field_name
        join_field_name = model_fields_sources[field_name]['join_field_name']
        related_result_table_id = model_fields_sources[join_field_name]['input_result_table_id']

        if input_table not in [instance_sources['main_table'], related_result_table_id]:
            raise dm_pro_errors.ExtendedFieldInputError(
                message_kv={
                    'input_result_table_id': instance_sources['main_table'],
                    'related_result_table_id': related_result_table_id,
                }
            )

        if input_table != instance_sources['main_table'] and not related_result_table_id:
            raise dm_pro_errors.DimFieldRequiredError(
                message_kv={
                    'extended_field': field_name,
                    'join_field': join_field_name,
                    'related_field': model_fields_sources[join_field_name].get('related_field_name'),
                }
            )

        source_field_name = model_fields_sources[field_name].get('source_field_name')
        if source_field_name != input_field:
            raise dm_pro_errors.DimFieldMustEqualFieldNameError(
                message_kv={
                    'related_input_result_table_id': input_table,
                    'related_input_field_name': input_field,
                    'source_field': source_field_name,
                }
            )

    @staticmethod
    def generate_model_fields_sources(model_content):
        """根据模型发布详情生成模型字段与原模型的关联关系，以便在其他校验中使用

        :param model_content: 该模型发布版本详情

        :return: 模型发布是的输出字段详情
        """
        model_fields_sources = {}

        for field_info in model_content.get('model_detail', {}).get('fields', []):
            field_name = field_info.get('field_name')
            if field_name in APPLICATION_EXCLUDE_FIELDS:
                continue
            model_fields_sources[field_name] = {
                'source_model_id': field_info.get('source_model_id'),
                'source_field_name': field_info.get('source_field_name'),
                'join_field_name': field_info.get('join_field_name'),
                'is_extended_field': field_info.get('is_extended_field'),
                'is_join_field': field_info.get('is_join_field'),
                'origin_fields': field_info.get('origin_fields'),
            }

        for field_relation_info in model_content.get('model_detail', {}).get('model_relation', []):
            field_name = field_relation_info.get('field_name')
            if field_name in APPLICATION_EXCLUDE_FIELDS or field_name not in model_fields_sources:
                continue

            model_fields_sources[field_name]['related_field_name'] = field_relation_info.get('related_field_name')

        return model_fields_sources

    @staticmethod
    def validate_clean_content(instance_table_field, instance_sources):
        """校验应用阶段清洗逻辑

        1.输入字段和应用清洗逻辑不能同时为空
        2.当且仅当应用输出字段的来源表是主表时，才允许用户配置字段清洗逻辑，这个结论适用于普通字段，维度关联字段，维度关联扩展字段

        :param instance_table_field 应用实例字段
        :param instance_sources: 应用输入表
        """
        clean_content = (instance_table_field.application_clean_content or {}).get('clean_content')
        input_result_table_id = instance_table_field.input_result_table_id

        if input_result_table_id and input_result_table_id != instance_sources['main_table']:
            if clean_content:
                raise dm_pro_errors.DimFieldCannotCleanError(
                    message_kv={
                        'field_name': instance_table_field.field_name,
                    }
                )

    @staticmethod
    def validate_alias_name(instance_table_field):
        """校验数据模型应用清洗逻辑是否alias成当前字段

        :param instance_table_field 应用实例字段

        :return: alias的名称
        """
        clean_content = (instance_table_field.application_clean_content or {}).get('clean_content')

        if not clean_content:
            return

        # 获取当前
        alias_mappings = get_sql_fields_alias_mapping({instance_table_field.field_name: clean_content})
        alias_name = alias_mappings.get(instance_table_field.field_name, {}).get('alias')

        if alias_name and alias_name != instance_table_field.field_name:
            raise dm_pro_errors.AliasNotMappingFieldError(message_kv={'alias_name': alias_name})

        return alias_name

    @staticmethod
    def validate_source_columns(instance_table_field, alias_name):
        """校验数据模型应用清洗逻辑是否使用了非input_field_name之外的字段

        :param instance_table_field 应用实例字段
        :param alias_name 清洗逻辑alias名称
        """
        clean_content = (instance_table_field.application_clean_content or {}).get('clean_content')

        if not clean_content:
            return

        # 构建临时SQL来校验自定义应用清理规则中是否包含非选中输入字段以外的其他字段
        simple_sql = SQLVerifier.gen_simple_sql(
            table_name=instance_table_field.input_result_table_id,
            fields='{} AS `{}`'.format(
                clean_content,
                instance_table_field.field_name,
            )
            if not alias_name
            else clean_content,
        )
        source_columns = get_sql_source_columns_mapping(simple_sql)
        source_columns = {k.strip('`'): v for k, v in list(source_columns.get('fields', {}).items())}
        source_column_list = source_columns.get(instance_table_field.field_name, []) or []

        # 来源字段最多只允许一个且来源于input_field_name
        if len(source_column_list) > 1:
            source_column_list.remove(instance_table_field.input_field_name)
            raise dm_pro_errors.OtherFieldsNotPermittedError(message_kv={'fields': ','.join(source_column_list)})

        if len(source_column_list) == 1 and source_column_list[0] != instance_table_field.input_field_name:
            raise dm_pro_errors.OtherFieldsNotPermittedError(message_kv={'fields': ','.join(source_column_list)})

        if len(source_column_list) == 0 and instance_table_field.input_field_name:
            raise dm_pro_errors.InputFieldNotUsedInCleanError(
                message_kv={'input_field_name': instance_table_field.input_field_name},
            )

    @staticmethod
    def validate_same_indicator(model_instance, instance_indicator):
        """校验是否在一个模型实例下创建相同的指标

        :param model_instance 数据模型实例
        :param instance_indicator 数据模型实例指标
        """
        instance_indicators = ModelInstanceManager.get_model_instance_indicators(model_instance)

        for other_instance_indicator in instance_indicators:
            if other_instance_indicator.result_table_id != instance_indicator.result_table_id:
                if IndicatorManager.compare_indicators(other_instance_indicator, instance_indicator):
                    raise dm_pro_errors.SameIndicatorNotPermittedError()

    @staticmethod
    def generate_instance_fields_mappings(model_instance):
        """生成实例字段映射列表

        :param model_instance 数据模型实例

        :return: 字段映射列表
        """
        fields = {}

        # 模型实例主表字段信息
        instance_table_fields = DmmModelInstanceField.objects.filter(instance=model_instance)
        for instance_table_field in instance_table_fields:
            fields[instance_table_field.field_name] = {
                'field_name': instance_table_field.field_name,
                'input_result_table_id': instance_table_field.input_result_table_id,
                'input_field_name': instance_table_field.input_field_name,
                'input_table_field': '{}/{}'.format(
                    instance_table_field.input_result_table_id,
                    instance_table_field.input_field_name,
                )
                if instance_table_field.input_field_name
                else '',
                'mapping_type': 'field',
                'constant': '',
                'application_clean_content': instance_table_field.application_clean_content,
            }

            if instance_table_field.application_clean_content.get('clean_option') == CleanOptionType.Constant.value:
                fields[instance_table_field.field_name].update(
                    {
                        'mapping_type': 'constant',
                        'constant': instance_table_field.application_clean_content.get('clean_content'),
                    }
                )

        # 补充模型实例主表字段关联关系
        instance_table_fields_relations = DmmModelInstanceRelation.objects.filter(instance=model_instance)
        for relation in instance_table_fields_relations:
            if relation.field_name not in fields:
                raise dm_pro_errors.RelationFieldNotExistError(message_kv={'field_name': relation.field_name})
            fields[relation.field_name]['relation'] = {
                'related_model_id': relation.related_model_id,
                'input_result_table_id': relation.input_result_table_id,
                'input_field_name': relation.input_field_name,
                'input_table_field': '{}/{}'.format(
                    relation.input_result_table_id,
                    relation.input_field_name,
                )
                if relation.input_field_name
                else '',
            }

        # 补充发布时模型字段信息
        model_release = ModelInstanceManager.get_model_release_by_id_and_version(
            model_instance.model_id, model_instance.version_id
        )
        model_release_fields = model_release.model_content.get('model_detail', {}).get('fields', [])

        for model_field_info in model_release_fields:
            field_name = model_field_info.get('field_name')

            # 过滤掉不需要进行字段映射的字段
            if field_name in APPLICATION_EXCLUDE_FIELDS:
                continue

            if field_name not in fields:
                raise dm_pro_errors.ModelInstanceStructureNotSameError()

            ModelInstanceManager.fill_model_release_field_info(fields[field_name], model_field_info)

        fields_mappings = sorted(list(fields.values()), key=lambda x: x['field_index'])
        return fields_mappings

    @staticmethod
    def generate_init_fields_mappings(model_content):
        """根据模型ID和版本ID生成初始化的字段映射列表

        :param model_content 模型发布的内容

        :return: 初始化字段映射列表
        """

        model_release_fields = model_content.get('model_detail', {}).get('fields', [])
        model_release_relations = model_content.get('model_detail', {}).get('model_relation', [])

        fields_mappings = {}

        for model_field_info in model_release_fields:
            field_name = model_field_info.get('field_name')

            # 过滤掉不需要进行字段映射的字段
            if field_name in APPLICATION_EXCLUDE_FIELDS:
                continue

            fields_mappings[field_name] = {
                'field_name': field_name,
                'input_result_table_id': '',
                'input_field_name': '',
                'input_table_field': '',
                'mapping_type': 'field',
                'constant': '',
                'application_clean_content': {
                    'clean_option': 'SQL',
                    'clean_content': '',
                },
            }

            ModelInstanceManager.fill_model_release_field_info(fields_mappings[field_name], model_field_info)

        # 补充维度表关联关系信息
        for relation in model_release_relations:
            field_name = relation.get('field_name')
            if field_name not in fields_mappings:
                raise dm_pro_errors.RelationFieldNotExistError(
                    message_kv={
                        'field_name': field_name,
                    }
                )
            fields_mappings[field_name].update(
                {
                    'relation': {
                        'related_model_id': relation.get('related_model_id'),
                        'input_result_table_id': '',
                        'input_field_name': '',
                        'input_table_field': '',
                    },
                }
            )

        fields_mappings = sorted(list(fields_mappings.values()), key=lambda x: x['field_index'])

        return fields_mappings

    @staticmethod
    def fill_model_release_field_info(field_mapping, model_field_info):
        field_mapping.update(
            {
                'is_generated_field': ModelInstanceManager.check_is_generated_field(
                    field_mapping['field_name'], model_field_info
                ),
                'field_alias': model_field_info.get('field_alias'),
                'description': model_field_info.get('description'),
                'field_index': model_field_info.get('field_index'),
                'field_type': model_field_info.get('field_type'),
                'field_category': model_field_info.get('field_category'),
                'is_primary_key': model_field_info.get('is_primary_key'),
                'is_join_field': model_field_info.get('is_join_field'),
                'is_extended_field': model_field_info.get('is_extended_field'),
                'join_field_name': model_field_info.get('join_field_name'),
                'field_constraint_content': model_field_info.get('field_constraint_content'),
                'field_clean_content': model_field_info.get('field_clean_content'),
                'is_opened_application_clean': bool(field_mapping['application_clean_content'].get('clean_content'))
                if field_mapping['mapping_type'] == 'field'
                else False,
            }
        )

    @staticmethod
    def check_is_generated_field(field_name, model_field_info):
        """检测当前字段是否是生成字段

        :param field_name 字段名称
        :param model_field_info 字段信息

        :return: 是否是生成字段
        """
        origin_fields = model_field_info.get('origin_fields', [])
        is_join_field = model_field_info.get('is_join_field')
        is_extended_field = model_field_info.get('is_extended_field')
        if not is_join_field and not is_extended_field:
            return len(origin_fields) != 1 or origin_fields[0] != field_name
        else:
            return False

    @staticmethod
    def generate_inst_default_ind_nodes(model_instance_id, model_content, bk_biz_id, indicator_names=None):
        """生成模型实例的默认指标节点配置

        :param model_instance_id 模型实例ID
        :param model_content 模型发布内容
        :param bk_biz_id 应用业务ID
        :param indicator_names 指标名称列表

        :return: 模型实例的默认指标节点和存储节点的配置列表
        """
        indicator_node_configs = {}
        for indicator_info in model_content.get('model_detail', {}).get('indicators', []):
            indicator_name = indicator_info.get('indicator_name')

            if indicator_names and indicator_name not in indicator_names:
                continue

            indicator_node_configs[indicator_name] = ModelInstanceManager.generate_ind_node_default_config(
                model_instance_id,
                bk_biz_id,
                indicator_info,
            )

        return list(indicator_node_configs.values())

    @staticmethod
    def generate_ind_node_default_config(
        model_instance_id, bk_biz_id, indicator_info, table_custom_name='', table_alias=''
    ):
        """生成单个节点默认配置

        :param model_instance_id 模型实例ID
        :param bk_biz_id 业务ID
        :param indicator_info 指标信息
        :param table_custom_name 表名自定义部分
        :param table_alias 表中文名

        :return: 指标节点默认配置
        """
        indicator_name = indicator_info.get('indicator_name')
        node_type = SCHEDULING_INDICATOR_NODE_TYPE_MAPPINGS[indicator_info.get('scheduling_type')]

        (
            default_table_prefix_name,
            default_table_custom_name,
            default_table_suffix_name,
        ) = ModelInstanceManager.get_indicator_table_split_names(indicator_info)
        table_custom_name = table_custom_name or default_table_custom_name

        table_name = '{}_{}_{}'.format(default_table_prefix_name, table_custom_name, default_table_suffix_name)
        table_alias = table_alias or indicator_info.get('indicator_alias')

        window_config = indicator_info.get('scheduling_content', {})

        # 临时方案：补充指标节点没有使用到的实时或者离线配置
        if indicator_info.get('scheduling_type') == SchedulingType.STREAM.value:
            window_config.update(
                {
                    'session_gap': 0,
                }
            )
        elif indicator_info.get('scheduling_type') == SchedulingType.BATCH.value:
            window_config.update(
                {
                    'fallback_window': 1,
                    'data_start': 0,
                    'data_end': 23,
                    'delay': 0,
                    'delay_period': 'hour',
                    'custom_config': {},
                }
            )
            if 'unified_config' in window_config:
                window_config['unified_config'].update(
                    {
                        'window_delay': 0,
                    }
                )
            if 'advanced' in window_config:
                window_config['advanced'].update(
                    {
                        'start_time': '',
                    }
                )

        return {
            'node_type': node_type,
            'config': {
                'dedicated_config': {
                    'model_instance_id': model_instance_id,
                    'calculation_atom_name': indicator_info.get('calculation_atom_name'),
                    'aggregation_fields': indicator_info.get('aggregation_fields', []),
                    'filter_formula': indicator_info.get('filter_formula'),
                    'scheduling_type': indicator_info.get('scheduling_type'),
                },
                'window_config': window_config,
                'outputs': [
                    {
                        'bk_biz_id': bk_biz_id,
                        'fields': [],
                        'output_name': table_alias,
                        'table_name': table_name,
                        'table_custom_name': table_custom_name,
                    }
                ],
                'name': indicator_info.get('indicator_alias'),
            },
            'result_table_id': '{}_{}'.format(bk_biz_id, table_name),
            'result_table_name_alias': table_alias,
            'indicator_name': indicator_name,
            'indicator_alias': indicator_info.get('indicator_alias'),
            'parent_indicator_name': indicator_info.get('parent_indicator_name'),
        }

    @staticmethod
    def attach_batch_channel_nodes(indicator_node_configs, main_table, bk_biz_id):
        """在所有指标节点配置之间补充离线channel节点配置

        :param indicator_node_configs 指标节点配置
        :param main_table 主表结果表ID
        :param bk_biz_id 业务ID

        :return: 模型实例的默认指标节点和离线channel节点的配置
        """
        dataflow_node_configs = {}
        indicator_node_configs = {
            node_config.get('indicator_name'): node_config for node_config in indicator_node_configs
        }

        main_table_has_batch_channel = ModelInstanceManager.check_main_table_has_spec_storage(
            main_table, BATCH_CHANNEL_CLUSTER_TYPE
        )

        for indicator_name, indicator_node_config in list(indicator_node_configs.items()):
            parent_indicator_name = indicator_node_config.pop('parent_indicator_name')
            indicator_node_type = indicator_node_config['node_type']
            node_key = '{}_{}'.format(indicator_node_type, indicator_name)

            # 用node_key作为初始化节点配置唯一标识，同时用parent_node_key来标明其关联关系
            indicator_node_config['node_key'] = node_key
            if not parent_indicator_name:
                indicator_node_config['parent_node_key'] = None

            dataflow_node_configs[node_key] = indicator_node_config

            # 只有当前节点是离线指标节点时，才需要考虑是否要在前面增加离线channel节点
            if indicator_node_type == DataModelNodeType.BATCH_INDICATOR.value:
                if parent_indicator_name:
                    # TODO 后续如果支持子指标，这里需要根据配置是否相同来找已生成的父指标
                    if parent_indicator_name not in indicator_node_configs:
                        raise dm_pro_errors.ReleasedModelIndicatorError()
                    parent_node_type = indicator_node_configs[parent_indicator_name]['node_type']

                    # 如果父指标也是离线指标，则不需要在中间添加存储节点
                    if parent_node_type == DataModelNodeType.BATCH_INDICATOR.value:
                        continue
                elif main_table_has_batch_channel:
                    continue

                # 增加离线channel节点
                channel_node_type = BATCH_CHANNEL_NODE_DEFAULT_CONFIG['node_type']
                channel_node_key = '{}_{}'.format(channel_node_type, parent_indicator_name)

                if channel_node_key not in dataflow_node_configs:
                    parent_node_key = dataflow_node_configs[node_key]['parent_node_key']
                    dataflow_node_configs[channel_node_key] = copy.deepcopy(BATCH_CHANNEL_NODE_DEFAULT_CONFIG)
                    dataflow_node_configs[channel_node_key]['node_key'] = channel_node_key
                    dataflow_node_configs[channel_node_key]['parent_node_key'] = parent_node_key
                    dataflow_node_configs[channel_node_key]['config']['bk_biz_id'] = bk_biz_id

                # 调整原来指标节点的parent_node_key为当前channel节点的node_key
                dataflow_node_configs[node_key]['parent_node_key'] = channel_node_key

        ModelInstanceManager.fill_channel_node_names(dataflow_node_configs, main_table)

        return list(dataflow_node_configs.values())

    @staticmethod
    def attach_relation_storage_node(main_table, bk_biz_id):
        relation_node_configs = copy.deepcopy(RELATION_STORAGE_NODE_DEFAULT_CONFIG)
        relation_node_type = relation_node_configs.get('node_type')
        relation_node_key = '{}_None'.format(relation_node_type)
        relation_node_configs['node_key'] = relation_node_key
        relation_node_configs['parent_node_key'] = None
        relation_node_configs['config']['bk_biz_id'] = bk_biz_id

        relation_node_configs['config']['name'] = '{}({})'.format(
            ModelInstanceManager.get_parent_result_table_name_alias(
                {},
                main_table,
                None,
            ),
            RELATION_STORAGE_NODE_TYPE,
        )

        return relation_node_configs

    @staticmethod
    def fill_channel_node_names(dataflow_node_configs, main_table):
        """补充所有channel节点的节点名称

        :param dataflow_node_configs 数据流节点配置
        :param main_table 主表结果表ID
        """
        for node_key, node_config in list(dataflow_node_configs.items()):
            if node_config['node_type'] == BATCH_CHANNEL_NODE_TYPE:
                node_config['config']['name'] = '{}({})'.format(
                    ModelInstanceManager.get_parent_result_table_name_alias(
                        dataflow_node_configs,
                        main_table,
                        node_config['parent_node_key'],
                    ),
                    BATCH_CHANNEL_NODE_TYPE,
                )

    @staticmethod
    def get_parent_result_table_name_alias(dataflow_node_configs, main_table, parent_node_key):
        """获取上游节点结果表名称

        :param dataflow_node_configs 数据流节点配置
        :param main_table 主表结果表ID
        :param parent_node_key 父节点key

        :return: 父节点结果表中文名
        """
        result_table_id = None

        if parent_node_key is None:
            result_table_id = main_table
        elif parent_node_key in dataflow_node_configs:
            result_table_id = dataflow_node_configs[parent_node_key]['result_table_id']
        else:
            # TODO 后续如果支持子指标，有可能父指标已经生成，因此不再dataflow_node_configs配置中
            # 这种情况一般出现在模型升级后在实时指标后面增加了离线指标
            raise NotImplementedError()

        result_table_info = MetaApi.result_tables.retrieve(
            {'result_table_id': result_table_id}, raise_exception=True
        ).data

        return result_table_info.get('result_table_name_alias')

    @staticmethod
    def attach_node_level_info(dataflow_node_configs):
        """在默认指标节点配置中增加层级信息

        :parma dataflow_node_configs 默认指标节点配置和存储节点配置

        :return: 附带节点层级信息的节点配置列表
        """
        dataflow_node_configs = {node_config['node_key']: node_config for node_config in dataflow_node_configs}

        for node_key, node_config in list(dataflow_node_configs.items()):
            ModelInstanceManager.find_node_level(node_config, dataflow_node_configs)

        return list(dataflow_node_configs.values())

    @staticmethod
    def find_node_level(node_config, dataflow_node_configs):
        parent_node_key = node_config['parent_node_key']

        if parent_node_key is None:
            node_config['level'] = 0
            return

        parent_node_config = dataflow_node_configs.get(parent_node_key)
        if parent_node_config is None:
            return

        if 'level' not in parent_node_config or parent_node_config['level'] is None:
            ModelInstanceManager.find_node_level(parent_node_config, dataflow_node_configs)

        node_config['level'] = parent_node_config['level'] + 1

    @staticmethod
    def check_main_table_has_spec_storage(main_table, cluster_type):
        """检查主表结果表是否有指定存储

        :param main_table 主表结果表
        :param clsuter_type 检查的存储类型

        :return: 是否有指定的存储
        """
        storages = MetaApi.result_tables.storages({'result_table_id': main_table}, raise_exception=True).data

        return cluster_type in (storages or {})

    @staticmethod
    def get_indicator_table_split_names(indicator_info):
        """获取指标结果表拆分名字

        :param indicator_info 指标信息

        :return: 指标表名前缀，指标表名自定义部分，指标表名后缀
        """
        indicator_name = indicator_info.get('indicator_name')
        table_prefix_name = indicator_info.get('calculation_atom_name')
        table_suffix_name = ''
        table_custom_name = ''

        # 除了前缀的统计口径名称，还要去掉"_"的一个字符，才是用户可自定义的部分
        table_custom_name_with_suffix = indicator_name[len(table_prefix_name) + 1 :]
        table_custom_name_tokens = table_custom_name_with_suffix.split('_')

        if len(table_custom_name_tokens) > 0:
            table_suffix_name = table_custom_name_tokens[-1]
            if len(table_custom_name_tokens) > 1:
                table_custom_name = '_'.join(table_custom_name_tokens[:-1])

        return table_prefix_name, table_custom_name, table_suffix_name

    @staticmethod
    def autofill_input_table_field_by_param(fields, input_table_params):
        """根据来源输入表按字段名相同的规则自动为字段映射填充默认值

        :param fields 待填充的字段映射列表
        :param input_table_parmas 来源输入表参数

        :return: 自动填充后的字段映射列表
        """
        main_table = input_table_params['main_table']
        dimension_tables = input_table_params['dimension_tables']

        table_fields = ModelInstanceManager.prepare_table_fields(
            [main_table] + dimension_tables,
        )
        join_field_table_mappings = ModelInstanceManager.prepare_join_field_table_mappings(
            fields,
            dimension_tables,
            table_fields,
        )

        for field_info in fields:
            field_name = field_info.get('field_name')
            is_join_field = field_info.get('is_join_field')
            is_extended_field = field_info.get('is_extended_field')
            is_generated_field = field_info.get('is_generated_field')
            join_field_name = field_info.get('join_field_name')

            if is_generated_field:
                continue

            if not is_join_field and not is_extended_field and field_name in table_fields.get(main_table, []):
                ModelInstanceManager.fill_input_table_field(field_info, main_table, field_name)

            if is_join_field:
                if field_name in table_fields.get(main_table, []):
                    ModelInstanceManager.fill_input_table_field(field_info, main_table, field_name)

                target_dim_table = join_field_table_mappings.get(field_name)
                if field_name in table_fields.get(target_dim_table, []):
                    ModelInstanceManager.fill_input_table_field(field_info['relation'], target_dim_table, field_name)

            if is_extended_field:
                target_dim_table = join_field_table_mappings.get(join_field_name)
                if field_name in table_fields.get(target_dim_table, []):
                    ModelInstanceManager.fill_input_table_field(field_info, target_dim_table, field_name)
                elif field_name in table_fields.get(main_table, []):
                    ModelInstanceManager.fill_input_table_field(field_info, main_table, field_name)

        return fields

    @staticmethod
    def prepare_table_fields(input_tables):
        """通过接口获取所有输入表的字段列表

        :param input_tables 输入表列表

        :return: 输入表及其字段列表的映射字典
        """
        table_fields = {}
        for result_table_id in input_tables:
            result_table_fields = MetaApi.result_tables.fields(
                {'result_table_id': result_table_id}, raise_exception=True
            ).data
            table_fields[result_table_id] = [field_info.get('field_name') for field_info in result_table_fields]

        return table_fields

    @staticmethod
    def prepare_join_field_table_mappings(fields, dimension_tables, table_fields):
        """对于进行维度关联的字段，从维度表中匹配其可能关联的维度表

        这里存在一个维度字段在多个输入维度表中都有的情况，对于这种情况，会根据其维度扩展字段选择匹配度（同名字段最多）最高的维度表

        :param fields 字段映射列表
        :param dimension_tables 维度表列表
        :param table_fields 结果表字段信息

        :return: 维度关联字段所用关联表信息
        """
        join_field_table_mappings = {}

        join_field_extended_fields = ModelInstanceManager.get_join_field_extended_fields(fields)

        for field_info in fields:
            field_name = field_info.get('field_name')
            if field_info.get('is_join_field'):
                for dimension_table in dimension_tables:
                    dim_table_fields = table_fields[dimension_table]

                    for dim_field in dim_table_fields:
                        # 如果字段名相同则认为该维度表可以作为维度关联字段可用的维度输入表
                        if field_name == dim_field:
                            # 如果已经匹配其他维度表，则对其扩展字段进行比较，以匹配度最高的表作为维度输入表
                            if field_name in join_field_table_mappings:
                                fit_dimension_table = ModelInstanceManager.find_fit_dim_table_by_extd_fields(
                                    extended_fields=join_field_extended_fields[field_name],
                                    old_dim_table=join_field_table_mappings[field_name],
                                    new_dim_table=dimension_table,
                                    table_fields=table_fields,
                                )
                                join_field_table_mappings[field_name] = fit_dimension_table
                            else:
                                join_field_table_mappings[field_name] = dimension_table

                            # 一张表内字段不可能重复，因此只要匹配到一个字段与关联字段相同，剩余的字段就不需要继续监测
                            break

        return join_field_table_mappings

    @staticmethod
    def get_join_field_extended_fields(fields):
        """按维度关联字段对所有维度扩展字段进行分组

        :param fields 字段映射列表

        :return: 关联字段的扩展字段列表
        """
        join_field_extended_fields = {}

        for field_info in fields:
            if field_info.get('is_join_field'):
                join_field_extended_fields[field_info.get('field_name')] = []

        for field_info in fields:
            if field_info.get('is_extended_field'):
                join_field_name = field_info.get('join_field_name')
                if join_field_name in join_field_extended_fields:
                    join_field_extended_fields[join_field_name].append(field_info.get('field_name'))

        return join_field_extended_fields

    @staticmethod
    def find_fit_dim_table_by_extd_fields(extended_fields, old_dim_table, new_dim_table, table_fields):
        """根据字段集筛选相似度更高的维度表

        :param extended_fields 维度扩展字段
        :param src_dim_table 旧匹配的维度表
        :param new_dim_table 新匹配的维度表
        :param table_fields 各表字段信息

        :return: 相似度更高的维度表
        """
        src_dim_table_fields = set(table_fields.get(old_dim_table, []))
        new_dim_table_fields = set(table_fields.get(new_dim_table, []))
        extended_fields_set = set(extended_fields)

        if len(src_dim_table_fields - extended_fields_set) < len(new_dim_table_fields - extended_fields_set):
            return old_dim_table
        else:
            return new_dim_table

    @staticmethod
    def fill_input_table_field(input_info, result_table_id, field_name):
        """填充默认输入表和输入字段信息

        :param input_info 字段映射输入信息
        :param result_table_id 默认结果表ID
        :param field_name 默认字段名
        """
        input_info['input_result_table_id'] = result_table_id
        input_info['input_field_name'] = field_name
        if field_name:
            input_info['input_table_field'] = '{}/{}'.format(result_table_id, field_name)
        else:
            input_info['input_table_field'] = ''

    @staticmethod
    def generate_sql_verify_sql_parts(instance_table_fields):
        """生成SQL校验组装SQL部分的构建参数"""
        sql_parts_fields = {}
        for instance_table_field in instance_table_fields:
            clean_content = (instance_table_field.application_clean_content or {}).get('clean_content')
            sql_parts_fields[instance_table_field.field_name] = clean_content or instance_table_field.input_field_name

        return {
            'table_name': ModelInstanceManager.SQL_VERIFY_TMP_TABLE_NAME,
            'fields': sql_parts_fields,
        }

    @staticmethod
    def generate_sql_verify_scopes_params(instance_sources, instance_table_fields):
        """生成SQL校验器构建参数

        :param instance_sources 来源输入表
        :param instance_table_fields 主表输出字段

        :return: SQL校验器构建参数
        """
        table_fields = {}
        input_result_tables = [instance_sources['main_table']]
        input_result_tables.extend(instance_sources['dimension_tables'])

        for result_table_id in input_result_tables:
            fields = MetaApi.result_tables.fields({'result_table_id': result_table_id}, raise_exception=True).data
            table_fields[result_table_id] = {item.get('field_name'): item for item in fields}

        scopes_params_fields = {}
        for instance_table_field in instance_table_fields:
            input_field_name = instance_table_field.input_field_name
            input_result_table_id = instance_table_field.input_result_table_id

            if not input_field_name:
                continue

            scopes_params_fields[input_field_name] = {
                'field_name': input_field_name,
                'field_type': table_fields[input_result_table_id].get(input_field_name, {}).get('field_type'),
            }

        return {
            ModelInstanceManager.SQL_VERIFY_TMP_TABLE_NAME: list(scopes_params_fields.values()),
        }

    @staticmethod
    def generate_sql_verify_require_schema(result_table_id, model_content):
        """生成SQL校验逻辑调用参数

        :param result_table_id 输出结果表ID
        :param model_content 数据模型发布内容

        :return: SQL校验逻辑调用参数
        """
        require_schema = {result_table_id: []}
        for model_field_info in model_content.get('model_detail', {}).get('fields', []):
            field_name = model_field_info.get('field_name')

            # 过滤掉不需要进行字段映射的字段
            if field_name in APPLICATION_EXCLUDE_FIELDS:
                continue

            # 过滤掉生成字段（生成字段已经在构建阶段校验过了，因此这里不需要再进行校验）
            if ModelInstanceManager.check_is_generated_field(field_name, model_field_info):
                continue

            require_schema[result_table_id].append(
                {
                    'field_name': field_name,
                    'field_type': model_field_info.get('field_type'),
                }
            )
        return require_schema

    @staticmethod
    def validate_data_model_sql(bk_biz_id, instance_sources, instance_table_id, instance_table_fields, model_content):
        """校验数据模型应用SQL

        :param bk_biz_id 业务ID
        :param instance_sources 来源输入表
        :param instance_table_id 主表输出结果表ID
        :param instance_table_fields 主表输出字段
        :param model_content 数据模型发布内容
        """
        # 准备校验类实例化所需的sql_parts参数
        sql_parts = ModelInstanceManager.generate_sql_verify_sql_parts(instance_table_fields)

        # 准备校验类实例化所需的scopes参数
        scopes_params = ModelInstanceManager.generate_sql_verify_scopes_params(
            instance_sources,
            instance_table_fields,
        )

        verifier = SQLVerifier(bk_biz_id, sql_parts=sql_parts, scopes=scopes_params)

        # 准备校验函数所需的require_schema参数
        require_schema = ModelInstanceManager.generate_sql_verify_require_schema(instance_table_id, model_content)

        is_valid, message = verifier.check_res_schema(
            require_schema,
            [SchedulingEngineType.FLINK.value, SchedulingEngineType.SPARK.value],
            agg=False,
        )
        if not is_valid:
            raise dm_pro_errors.SqlValidateError(
                message_kv={
                    'error_info': '{} [ERROR_SQL: {}]'.format(message, verifier.show_sql),
                }
            )

    @staticmethod
    def filter_inds_by_instance_inds(model_indicators, instance_indicators):
        """过滤得到当前模型实例已有的指标

        :param model_indicators 当前模型默认指标
        :param instance_indicators 当前模型实例已有的指标

        :return: 当前实例已使用的指标
        """
        indicator_names = set()

        for instance_indicator in instance_indicators:
            for model_indicator_info in model_indicators:
                if IndicatorManager.compare_indicators(instance_indicator, model_indicator_info):
                    indicator_names.add(model_indicator_info.get('indicator_name'))

        return list(indicator_names)

    @staticmethod
    def generate_indicators_menu(model_content, selected_indicator_names):
        """生成指标列表目录

        :param model_content 模型内容
        :param selected_indicator_names 已选择的指标名称

        :return: 指标列表目录
            [
                {
                    id: 'xx',        # 唯一值，传接口也传该值
                    alias: 'xx',     # 中文名
                    name: 'xx',      # 英文名
                    type: 'xx',      # 用于图标展示，以及决定是否是指标,
                    folder: true/false,      # 是否是目录文件夹
                    count: 3,        # 数目
                    children: []     # 子节点
                }
            ]
        """
        model_indicators = model_content.get('model_detail', {}).get('indicators', [])

        total_indicator_count = len(model_indicators)
        calculation_atom_indicators = {}

        # 按统计口径把模型指标进行分类
        for model_indicator_info in model_indicators:
            calculation_atom_name = model_indicator_info.get('calculation_atom_name')
            if calculation_atom_name not in calculation_atom_indicators:
                calculation_atom_indicators[calculation_atom_name] = {
                    'calculation_atom_alias': model_indicator_info.get('calculation_atom_alias'),
                    'indicators': [],
                }

            calculation_atom_indicators[calculation_atom_name]['indicators'].append(model_indicator_info)

        calculations_menu = []
        selected_indicator_count = 0
        for calculation_atom_name, calculation_atom_info in list(calculation_atom_indicators.items()):
            indicators_menu, selected_calculation_indicator_count = ModelInstanceManager.get_child_indicators_menu(
                calculation_atom_info, selected_indicator_names
            )

            selected_indicator_count += selected_calculation_indicator_count

            calculations_menu.append(
                {
                    'id': calculation_atom_name,
                    'alias': calculation_atom_info['calculation_atom_alias'],
                    'name': calculation_atom_name,
                    'type': 'calculation_atom',
                    'folder': True,
                    'count': len(indicators_menu),
                    'children': indicators_menu,
                }
            )
            # 如果当前统计口径所有指标都被选上，则当前目录回填时也认为被选中
            if selected_calculation_indicator_count == len(indicators_menu):
                selected_indicator_names.append(calculation_atom_name)

        # 如果所有指标都被选上，则当前目录回填时也认为被选中
        model_name = model_content.get('model_name')
        if total_indicator_count == selected_indicator_count:
            selected_indicator_names.append(model_name)
        return {
            'tree': [
                {
                    'id': model_name,
                    'alias': model_content.get('model_alias'),
                    'name': model_content.get('model_name'),
                    'type': 'model',
                    'folder': True,
                    'count': total_indicator_count,
                    'children': calculations_menu,
                }
            ]
            if total_indicator_count > 0
            else [],
            'selected_nodes': selected_indicator_names,
        }

    @staticmethod
    def get_child_indicators_menu(calculation_atom_info, selected_indicator_names, parent_indicator_name=None):
        """获取指标目录

        :param calculation_atom_info: 统计口径信息，包含口径下的所有指标信息
        :param selected_indicator_names: 已选择的指标名称
        :param parent_indicator_name: 父指标名称

        :return: 指标目录及已选择的该统计口径下的指标数量
        """
        indicators_menu = []
        selected_indicator_count = 0
        for model_indicator_info in calculation_atom_info['indicators']:
            if model_indicator_info.get('parent_indicator_name') != parent_indicator_name:
                continue

            indicator_name = model_indicator_info.get('indicator_name')

            child_menu, child_selected_indicator_count = ModelInstanceManager.get_child_indicators_menu(
                calculation_atom_info, selected_indicator_names, indicator_name
            )
            child_indicator_length = len(child_menu)

            indicators_menu.append(
                {
                    'id': indicator_name,
                    'alias': model_indicator_info.get('indicator_alias'),
                    'name': model_indicator_info.get('indicator_name'),
                    'type': '{}_indicator'.format(model_indicator_info.get('scheduling_type')),
                    'folder': child_indicator_length > 0,
                    'count': child_indicator_length or None,
                    'children': child_menu,
                }
            )
            # 统计已被选中的指标名称
            if indicator_name in selected_indicator_names:
                selected_indicator_count += 1

            selected_indicator_count += child_selected_indicator_count

        return indicators_menu, selected_indicator_count

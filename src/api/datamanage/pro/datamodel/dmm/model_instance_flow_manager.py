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

from common.business import Business

from datamanage.pro import exceptions as dm_pro_errors
from datamanage.pro.datamodel.dmm.model_instance_manager import ModelInstanceManager
from datamanage.pro.datamodel.models.model_dict import (
    BATCH_CHANNEL_CLUSTER_TYPE,
    DataModelNodeType,
    SchedulingType,
    ModelInputNodeType,
    DataModelType,
    RELATION_STORAGE_CLUSTER_TYPE,
)
from datamanage.utils.api import DataflowApi, MetaApi, StorekitApi


class VirtualNodeGenerator(object):
    """虚拟节点自增ID生成器"""

    def __init__(self, bk_biz_id):
        self._id = 0
        self._bk_biz_id = bk_biz_id
        self._nodes = {}

    def new_id(self, node_type, table_name='', table_alias='', result_table_id='', cluster_type=''):
        """生成虚拟节点的ID，并根据该ID记录节点的表名，节点类型等信息

        :param node_type 节点类型
        :param table_name 表名
        :param table_alias 表中文名
        :param result_table_id 结果表ID
        :param cluster_type 存储节点存储类型

        :return: 新节点ID
        """
        node_id = self._id
        self._id += 1

        if not result_table_id:
            result_table_id = '{}_{}'.format(self._bk_biz_id, table_name)

        self._nodes[node_id] = {
            'id': node_id,
            'result_table_id': result_table_id,
            'result_table_name_alias': table_alias,
            'node_type': node_type,
            'cluster_type': cluster_type,
        }
        return node_id

    def get_node(self, node_id):
        """根据虚拟节点ID获取节点信息

        :param node_id 节点ID

        :return: 节点信息
        """
        return self._nodes.get(node_id, {})

    def get_storage_nodes(self, node_id):
        """根据虚拟节点ID获取该节点的存储节点列表

        :param node_id 节点ID

        :return: 节点存储节点列表
        """
        storage_nodes = []

        result_table_id = self._nodes.get(node_id, {}).get('result_table_id')
        for storage_node_id, storage_node_info in list(self._nodes.items()):
            if storage_node_id != node_id and result_table_id == storage_node_info.get('result_table_id'):
                storage_nodes.append(storage_node_info)

        return storage_nodes

    @property
    def nodes(self):
        return self._nodes


class ModelInstanceFlowManager(object):
    FLOW_NAME_TEMPLATE = _('[{bk_biz_name}] 数据模型({model_alias})应用任务')
    STREAM_SOURCE_NODE_TYPE = 'stream_source'
    KV_SOURCE_NODE_TYPE = 'unified_kv_source'
    DATA_MODEL_NODE_TYPE = DataModelNodeType.DATA_MODEL.value
    STREAM_INDICATOR_NODE_TYPE = DataModelNodeType.STREAM_INDICATOR.value
    BATCH_INDICATOR_NODE_TYPE = DataModelNodeType.BATCH_INDICATOR.value

    @staticmethod
    def validate_data_model_dataflow_params(main_table_params, indicators_params, model_content):
        """检查创建数据模型应用任务的参数是否符合逻辑、是否有缺漏

        :param main_table_params 数据模型应用主表参数
        :param indicators_params 数据模型应用指标表参数
        :param model_content 数据模型内容
        """
        # 校验主表是否需要离线存储
        # TODO 后续如果支持子指标，这里需要进行扩展，校验指标与指标之间是否包含离线存储
        ModelInstanceFlowManager.validate_main_table_batch_channel(main_table_params, indicators_params, model_content)

    @staticmethod
    def validate_main_table_batch_channel(main_table_params, indicators_params, model_content):
        """检查主表是否在有离线指标的情况下缺少离线channel

        :param main_table_params 数据模型应用主表参数
        :param indicators_params 数据模型应用指标表参数
        :param model_content 数据模型内容
        """
        main_table_storages = main_table_params.get('storages', [])
        main_table_cluster_type_set = set([storage_info.get('cluster_type') for storage_info in main_table_storages])

        # 如果本身已经带离线channel存储，则不需要进行判断
        if BATCH_CHANNEL_CLUSTER_TYPE in main_table_cluster_type_set:
            return

        model_indicator_list = model_content.get('model_detail', {}).get('indicators', [])
        model_indicators = {item.get('indicator_name'): item for item in model_indicator_list}

        target_indicators = indicators_params if indicators_params is not None else list(model_indicators.values())

        for indicator_params in target_indicators:
            indicator_name = indicator_params.get('indicator_name')
            if indicator_name not in model_indicators:
                raise dm_pro_errors.IndicatorNotExistError(
                    '当前版本模型没有指标({indicator_name})'.format(
                        indicator_name=indicator_name,
                    )
                )
            indicator_info = model_indicators[indicator_name]

            # 在没有离线channel的情况下，只要该模型应用包含离线指标，则报错，让用户提供HDFS存储
            if indicator_info.get('scheduling_type') == SchedulingType.BATCH.value:
                raise dm_pro_errors.BatchChannelIsNecessaryError()

    @staticmethod
    def create_data_model_dataflow(project_id, bk_biz_id, model_release):
        """根据业务ID和模型的发布信息创建模型应用的数据开发任务

        :param project_id 项目ID
        :param bk_biz_id 业务ID
        :param model_release 数据模型发布内容，DmmModelRelease实例

        :return: 数据模型应用的数据开发任务ID
        """
        biz = Business(bk_biz_id)

        model_name = model_release.model_content.get('model_name')
        model_alias = model_release.model_content.get('model_alias')
        res = DataflowApi.flows.create(
            {
                'flow_name': ModelInstanceFlowManager.FLOW_NAME_TEMPLATE.format(
                    bk_biz_name=biz.bk_biz_name, model_alias=model_alias
                ),
                'description': _(
                    '通过接口创建的，基于数据模型(模型名称: {model_name}, 模型中文名: {model_alias}, 模型版本: {version_id})的应用任务'
                ).format(
                    model_name=model_name,
                    model_alias=model_alias,
                    version_id=model_release.version_id,
                ),
                'project_id': project_id,
            },
            raise_exception=True,
        )

        return res.data.get('flow_id')

    @staticmethod
    def generate_stream_source_node_params(source_main_table, node_generator):
        """生成创建数据源节点的参数

        :param source_main_table 来源输入表名
        :param node_generator 虚拟节点生成器

        :return: 创建实时输入源的请求参数
        """
        result_table_info = MetaApi.result_tables.retrieve(
            {'result_table_id': source_main_table},
            raise_exception=True,
        ).data

        node_type = ModelInstanceFlowManager.STREAM_SOURCE_NODE_TYPE
        result_table_name_alias = result_table_info.get('result_table_name_alias')
        return {
            'node_type': node_type,
            'id': node_generator.new_id(
                node_type, result_table_id=source_main_table, table_alias=result_table_name_alias
            ),
            'bk_biz_id': result_table_info.get('bk_biz_id'),
            'result_table_id': source_main_table,
            'name': result_table_name_alias,
            'from_nodes': [],
        }

    @staticmethod
    def generate_kv_source_node_params(source_dim_table, node_generator):
        """生成创建关联数据源节点的参数

        :param source_dim_table 来源维度表ID
        :param node_generator 虚拟节点生成器

        :return: 创建关联数据源的请求参数
        """
        result_table_info = MetaApi.result_tables.retrieve(
            {'result_table_id': source_dim_table},
            raise_exception=True,
        ).data

        node_type = ModelInstanceFlowManager.KV_SOURCE_NODE_TYPE
        result_table_name_alias = result_table_info.get('result_table_name_alias')
        return {
            'node_type': node_type,
            'id': node_generator.new_id(
                node_type, result_table_id=source_dim_table, table_alias=result_table_name_alias
            ),
            'bk_biz_id': result_table_info.get('bk_biz_id'),
            'result_table_id': source_dim_table,
            'name': result_table_name_alias,
            'from_nodes': [],
        }

    @staticmethod
    def generate_data_model_node_params(
        model_instance_id,
        bk_biz_id,
        project_id,
        model_content,
        model_version_id,
        main_table_params,
        input_params,
        node_generator,
    ):
        """生成数据模型应用节点的参数

        :param model_instance_id 数据模型实例ID
        :param bk_biz_id 业务ID
        :param project_id 项目ID
        :param model_content 数据模型内容
        :param model_version_id 模型版本ID
        :param main_table_params 主表参数
        :param input_params 输入表参数
        :param node_generator 虚拟节点生成器

        :return: 创建数据模型应用节点的请求参数
        """
        biz = Business(bk_biz_id)

        fields_mappings = main_table_params.get(
            'fields', ModelInstanceManager.generate_init_fields_mappings(model_content)
        )
        ModelInstanceManager.autofill_input_table_field_by_param(fields_mappings, input_params)

        node_type = ModelInstanceFlowManager.DATA_MODEL_NODE_TYPE
        table_name = main_table_params.get('table_name') or model_content.get('model_name')
        return {
            'node_type': node_type,
            'id': node_generator.new_id(node_type, table_name, table_name),
            'bk_biz_id': bk_biz_id,
            'name': model_content.get('model_alias'),
            'from_nodes': list(ModelInstanceFlowManager.generate_from_nodes_by_source_nodes(node_generator)),
            'window_config': None,
            'outputs': ModelInstanceFlowManager.generate_data_model_outputs(
                biz,
                table_name,
                main_table_params,
                model_content,
            ),
            'dedicated_config': {
                'model_instance_id': model_instance_id,
                'model_group': 'current',
                'model_id': model_content.get('model_id'),
                'version_id': model_version_id,
                'project_id': project_id,
                'fields': fields_mappings,
            },
        }

    @staticmethod
    def generate_from_nodes_by_source_nodes(node_generator):
        """生成来源节点配置

        :param node_generator 虚拟节点生成器

        :return: 来源节点配置
        """
        for node_id, node_info in list(node_generator.nodes.items()):
            source_node_types = ModelInputNodeType.get_enum_value_list()
            if node_info['node_type'] in source_node_types:
                yield {
                    'id': node_id,
                    'from_result_table_ids': [node_info.get('result_table_id')],
                }

    @staticmethod
    def generate_data_model_outputs(biz, table_name, main_table_params, model_content):
        """根据主表参数和模型发布内容作为默认值来生成数据模型节点的输出部分

        :param biz 业务信息
        :param table_name 输出表名
        :param main_table_params 主表配置参数
        :param model_content 模型发布内容

        :return: 数据模型节点输出部分
        """
        return [
            {
                'bk_biz_id': biz.bk_biz_id,
                'fields': [],
                'validate': {
                    'table_name': {
                        'status': False,
                        'errorMsg': '',
                    },
                    'output_name': {
                        'status': False,
                        'errorMsg': '',
                    },
                    'field_config': {
                        'status': False,
                        'errorMsg': '',
                    },
                },
                'table_name': table_name,
                'output_name': main_table_params.get('table_alias')
                or '[{bk_biz_name}]{model_alias}'.format(
                    bk_biz_name=biz.bk_biz_name,
                    model_alias=model_content.get('model_alias'),
                ),
            }
        ]

    @staticmethod
    def generate_storage_node_params(bk_biz_id, result_table_info, storage_params, node_generator):
        """生成存储节点配置

        :param bk_biz_id 业务ID
        :param result_table_id 结果表ID
        :param storage_params 存储配置
        :param node_generator 虚拟节点生成器

        :return: 存储节点配置
        """
        cluster_type = storage_params['cluster_type']

        result_table_id = result_table_info.get('result_table_id')
        result_table_name_alias = result_table_info.get('result_table_name_alias')
        parent_node_id = result_table_info.get('id')

        if cluster_type == 'ignite':
            node_type = 'ignite'
        else:
            node_type = '{}_storage'.format(cluster_type)

        storage_node_params = {
            'id': node_generator.new_id(
                node_type,
                table_alias=result_table_name_alias,
                result_table_id=result_table_id,
                cluster_type=cluster_type,
            ),
            'node_type': node_type,
            'bk_biz_id': bk_biz_id,
            'cluster': storage_params['cluster_name'],
            'from_result_table_ids': [result_table_id],
            'from_nodes': [{'id': parent_node_id, 'from_result_table_ids': [result_table_id]}],
            'result_table_id': result_table_id,
            'name': '{}({}_storage)'.format(result_table_name_alias, cluster_type),
            'expires': storage_params['expires'],
        }

        if 'specific_params' in storage_params:
            storage_node_params.update(storage_params['specific_params'])

        return storage_node_params

    @staticmethod
    def attach_ind_node_dataflow_params(
        bk_biz_id, indicators_nodes_default_configs, main_table_info, main_table_storages_nodes, node_generator
    ):
        """基于指标节点的默认参数，添加dataflow相关的参数

        :param bk_biz_id 业务ID
        :param indicators_nodes_default_configs 指标节点默认指标配置列表
        :param main_table_info 主表信息
        :param main_table_storages_nodes 主表存储节点信息字典
        :param node_generator 虚拟节点生成器

        :return: 补充了dataflow节点创建信息的指标节点配置列表
        """
        indicators_nodes_params = []

        for default_config in indicators_nodes_default_configs:
            indicator_config = default_config.get('config', {}).get('dedicated_config', {})
            node_type = default_config.get('node_type')
            node_config = default_config.get('config')

            # 上游节点ID取决于当前指标是否是离线指标
            parent_node_id = ModelInstanceFlowManager.get_indicator_parent_node_id(
                indicator_config,
                main_table_info,
                main_table_storages_nodes,
            )
            node_config.update(
                {
                    'from_nodes': [
                        {'id': parent_node_id, 'from_result_table_ids': [main_table_info.get('result_table_id')]}
                    ],
                }
            )
            node_config.update(
                {
                    'id': node_generator.new_id(
                        node_type,
                        table_alias=default_config.get('result_table_name_alias'),
                        result_table_id=default_config.get('result_table_id'),
                    ),
                    'node_type': node_type,
                    'bk_biz_id': bk_biz_id,
                    'name': default_config.get('indicator_alias'),
                    'indicator_name': default_config.get('indicator_name'),
                }
            )
            indicators_nodes_params.append(node_config)

        return indicators_nodes_params

    @staticmethod
    def get_indicator_parent_node_id(indicator_config, main_table_info, main_table_storages_nodes):
        """根据指标配置获取指标父节点ID

        :param indicator_config 指标配置
        :param main_table_info 主表信息
        :param main_table_storages_nodes 主表存储节点信息字典

        :return: 指标父节点ID
        """
        if indicator_config.get('scheduling_type') == SchedulingType.BATCH.value:
            for storage_node in main_table_storages_nodes:
                if storage_node.get('cluster_type') == BATCH_CHANNEL_CLUSTER_TYPE:
                    return storage_node.get('id')
            return main_table_info.get('id')
        else:
            return main_table_info.get('id')

    @staticmethod
    def generate_model_inds_nodes_params(
        model_instance_id,
        model_content,
        bk_biz_id,
        main_table_info,
        main_table_storages_nodes,
        indicators_params,
        node_generator,
    ):
        """根据默认指标配置和自定义指标配置

        :param model_instance_id 数据模型实例ID
        :param model_content 模型内容
        :param bk_biz_id 业务
        :param main_table_info 主表信息
        :param main_table_storages_nodes 主表存储节点信息字典
        :param indicators_params 指标参数
        :param node_generator 虚拟节点生成器

        :return: 数据模型指标节点相关信息
        """

        indicators_nodes_params = []

        # 生成需要创建的数据模型指标节点的配置及离线存储节点的配置
        if indicators_params is None:
            indicators_nodes_default_configs = ModelInstanceManager.generate_inst_default_ind_nodes(
                model_instance_id,
                model_content,
                bk_biz_id,
            )

            indicators_nodes_params = ModelInstanceFlowManager.attach_ind_node_dataflow_params(
                bk_biz_id,
                indicators_nodes_default_configs,
                main_table_info,
                main_table_storages_nodes,
                node_generator,
            )
        else:
            model_indicator_list = model_content.get('model_detail', {}).get('indicators', [])
            model_indicators = {item.get('indicator_name'): item for item in model_indicator_list}

            # 使用用户配置生成指标节点参数
            indicators_nodes_params = ModelInstanceFlowManager.generate_custom_inds_nodes_params(
                model_instance_id,
                bk_biz_id,
                model_indicators,
                main_table_info,
                main_table_storages_nodes,
                indicators_params,
                node_generator,
            )

        return indicators_nodes_params

    @staticmethod
    def generate_custom_inds_nodes_params(
        model_instance_id,
        bk_biz_id,
        model_indicators,
        main_table_info,
        main_table_storages_nodes,
        indicators_params,
        node_generator,
    ):
        """根据用户指标配置生成指标节点参数

        :param model_instance_id 数据模型实例ID
        :param bk_biz_id 业务
        :param model_indicators 模型发布指标配置
        :param main_table_info 主表信息
        :param main_table_storages_nodes 主表存储节点信息字典
        :param indicators_params 指标参数
        :param default_storages_params 指标默认存储配置
        :param node_generator 虚拟节点生成器

        :return: 节点配置
        """
        indicators_nodes_default_configs = []

        for indicator_params in indicators_params:
            indicator_name = indicator_params.get('indicator_name')

            # 根据指标名称获取发布模型中该指标的信息
            if indicator_name not in model_indicators:
                raise dm_pro_errors.IndicatorNotExistError(
                    '当前版本模型没有指标({indicator_name})'.format(
                        indicator_name=indicator_name,
                    )
                )
            indicator_info = model_indicators[indicator_name]

            # 根据指标参数获取指标节点配置
            indicators_nodes_default_configs.append(
                ModelInstanceManager.generate_ind_node_default_config(
                    model_instance_id,
                    bk_biz_id,
                    indicator_info,
                    indicator_params.get('table_custom_name'),
                    indicator_params.get('table_alias'),
                )
            )

        return ModelInstanceFlowManager.attach_ind_node_dataflow_params(
            bk_biz_id,
            indicators_nodes_default_configs,
            main_table_info,
            main_table_storages_nodes,
            node_generator,
        )

    @staticmethod
    def generate_inds_storages_nodes_params(
        bk_biz_id, indicators_nodes, indicators_params, default_storages_params, node_generator
    ):
        """生成所有指标节点的存储节点配置列表

        :param bk_biz_id 业务ID
        :param indicators_nodes 指标节点列表
        :param indicators_params 指标参数
        :param default_storages_params 指标默认存储配置
        :param node_generator 虚拟节点生成器

        :return: 存储节点配置列表
        """
        storages_nodes_params = []

        for indicator_node in indicators_nodes:
            indicator_name = indicator_node.get('indicator_name')
            indicator_node_id = indicator_node.get('id')

            storages_params = ModelInstanceFlowManager.get_indicator_storages_params(
                indicator_name,
                indicators_params,
                default_storages_params,
            )

            for storage_params in storages_params:
                storages_nodes_params.append(
                    ModelInstanceFlowManager.generate_storage_node_params(
                        bk_biz_id,
                        node_generator.get_node(indicator_node_id),
                        storage_params,
                        node_generator,
                    )
                )

        return storages_nodes_params

    @staticmethod
    def get_indicator_storages_params(indicator_name, indicators_params, default_storages_params):
        """获取指标存储参数

        :param indicator_name 指标名称
        :param indicators_params 指标参数
        :param default_storages_params 默认存储参数

        :return: 指标存储参数
        """
        storages_params = copy.copy(default_storages_params)

        if indicators_params is not None:
            for indicator_params in indicators_params:
                if indicator_params.get('indicator_name') == indicator_name:
                    if indicator_params.get('storages'):
                        storages_params = indicator_params.get('storages')

        return storages_params

    @staticmethod
    def check_dataflow_instances(flow_id):
        """检查任务中的数据模型实例任务是否合法

        :param flow_id 数据流ID
        """
        model_instances = ModelInstanceManager.get_model_instances_by_flow_id(flow_id)

        for model_instance in model_instances:
            ModelInstanceFlowManager.check_instance_task(model_instance)

        return True

    @staticmethod
    def check_instance_task(model_instance):
        """检查数据模型实例任务是否合法

        :param model_instance 待检查的数据模型实例
        """
        model_release = ModelInstanceManager.get_model_release_by_id_and_version(
            model_instance.model_id, model_instance.version_id
        )

        if model_release.model_content.get('model_type') == DataModelType.FACT_TABLE.value:
            instance_indicators = ModelInstanceManager.get_model_instance_tail_indicators(model_instance)

            for instance_indicator in instance_indicators:
                ModelInstanceFlowManager.check_rt_has_queryable_storages(instance_indicator.result_table_id)
        elif model_release.model_content.get('model_type') == DataModelType.DIMENSION_TABLE.value:
            instance_table = ModelInstanceManager.get_model_instance_table_by_id(model_instance.instance_id)

            ModelInstanceFlowManager.check_rt_has_relation_storages(instance_table.result_table_id)

    @staticmethod
    def check_rt_has_queryable_storages(result_table_id):
        """检查结果表是否有可查询的存储

        :param result_table_id 结果表ID
        """
        storage_info = StorekitApi.schema_and_sql(
            {
                'result_table_id': result_table_id,
            },
            raise_exception=True,
        ).data

        if not storage_info.get('order'):
            raise dm_pro_errors.IndicatorMustBeQueryableError(message_kv={'result_table_id': result_table_id})

    @staticmethod
    def check_rt_has_relation_storages(result_table_id):
        """检查结果表是否有关联存储

        :param result_table_id 结果表ID
        """
        storage_info = StorekitApi.schema_and_sql(
            {
                'result_table_id': result_table_id,
            },
            raise_exception=True,
        ).data

        if not storage_info.get('storages') or RELATION_STORAGE_CLUSTER_TYPE not in storage_info['storages']:
            raise dm_pro_errors.DimensionTableHasNoRelationError(
                message_kv={
                    'result_table_id': result_table_id,
                }
            )

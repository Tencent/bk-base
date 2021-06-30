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
import gevent
import datetime

from django.db.models import Q, Prefetch
from django.utils.translation import ugettext_lazy as _
from rest_framework.response import Response
from common.decorators import list_route

from common.log import logger
from common.views import APIViewSet
from common.decorators import params_valid
from common.base_utils import model_to_dict

from datamanage import exceptions as dm_errors
from datamanage.utils.api import AccessApi, MetaApi
from datamanage.utils.time_tools import tznow
from datamanage.lite.dmonitor.constants import CLUSTER_NODE_TYPES_MAPPINGS
from datamanage.lite.dmonitor.mixins.base_mixins import BaseMixin
from datamanage.lite.dmonitor.serializers import (
    DmonitorFlowSerializer,
    DmonitorDataFlowSerializer,
    DmonitorDataOperationSerializer,
)
from datamanage.lite.dmonitor.flow_models import (
    AccessRawData,
    DatabusCleanInfo,
    DatabusShipperInfo,
    DatabusTransformProcessing,
    DataflowNodeRelation,
    Dataflow,
    DataflowNode,
)


class DmonitorFlowViewSet(BaseMixin, APIViewSet):
    @params_valid(serializer=DmonitorFlowSerializer)
    def list(self, request, params):
        """
        @api {get} /datamanage/dmonitor/flows/ 查询数据平台所有flows信息

        @apiVersion 1.0.0
        @apiGroup DmonitorFlow
        @apiName get_dmonitor_flow_infos

        @apiParam {int} [update_duration] 增量更新周期
        @apiParam {int} [with_nodes] 是否包含节点信息及相关任务信息

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                }
            }
        """
        flow_infos = []
        update_duration = params.get('update_duration', 0)
        with_nodes = params.get('with_nodes', 0)

        from gevent import monkey

        monkey.patch_all()

        gevent.joinall(
            [
                gevent.spawn(self.get_dataflow_infos, flow_infos, update_duration, with_nodes),
                gevent.spawn(self.get_rawdataflow_infos, flow_infos, update_duration, with_nodes),
            ]
        )

        return Response(flow_infos)

    @list_route(methods=['get'], url_path='dataflow')
    @params_valid(serializer=DmonitorDataFlowSerializer)
    def dataflow(self, request, params):
        """
        @api {get} /datamanage/dmonitor/flows/dataflow/ 查询数据平台所有dataflows信息

        @apiVersion 1.0.0
        @apiGroup DmonitorFlow
        @apiName get_dmonitor_dataflow_infos

        @apiParam {int} [update_duration] 增量更新周期
        @apiParam {bool} [with_nodes] 增量更新周期

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                }
            }
        """
        flow_infos = []
        update_duration = params.get('update_duration', 0)
        with_nodes = params.get('with_nodes', False)
        only_running = params.get('only_running', False)

        self.get_dataflow_infos(flow_infos, update_duration, with_nodes, only_running)

        return Response(flow_infos)

    def get_dataflow_infos(self, flow_infos, update_duration=0, with_nodes=False, only_running=False):
        try:
            # 获取dataflow的flow信息
            if update_duration:
                update_time = tznow() - datetime.timedelta(seconds=update_duration)
                queryset = Dataflow.objects.filter(active=True).filter(
                    Q(updated_at__gte=update_time) | Q(created_at__gte=update_time)
                )
            else:
                queryset = Dataflow.objects.filter(active=True)
            node_query = DataflowNode.objects.all()
            node_relation_query = DataflowNodeRelation.objects.all()

            if only_running:
                queryset = queryset.filter(status='running')
                node_query = node_query.filter(status='running')

            if with_nodes:
                queryset = queryset.prefetch_related(
                    Prefetch('node_set', queryset=node_query, to_attr='node_list'),
                    Prefetch('relation_set', queryset=node_relation_query, to_attr='node_relations'),
                )

            for flow_item in list(queryset):
                flow_info = {
                    'flow_type': 'dataflow',
                    'flow_id': flow_item.flow_id,
                    'flow_name': flow_item.flow_name,
                    'active': flow_item.active,
                    'project_id': flow_item.project_id,
                    'status': flow_item.status,
                    'bk_app_code': flow_item.bk_app_code,
                }
                if with_nodes:
                    flow_info['nodes'] = {str(item.node_id): model_to_dict(item) for item in flow_item.node_list}

                    flow_info['relations'] = {}
                    for relation in flow_item.node_relations:
                        if relation.node_id not in flow_info['relations']:
                            flow_info['relations'][relation.node_id] = []
                        flow_info['relations'][relation.node_id].append(
                            {
                                'result_table_id': relation.result_table_id,
                            }
                        )
                flow_infos.append(flow_info)
        except Exception as e:
            logger.error('Build dataflow info error: %s' % e)

    def get_rawdataflow_infos(self, flow_infos, update_duration=0, with_nodes=False):
        try:
            # 获取rawdata的flow信息
            if update_duration:
                update_time = tznow() - datetime.timedelta(seconds=update_duration)
                queryset = AccessRawData.objects.filter(Q(updated_at__gte=update_time) | Q(created_at__gte=update_time))
            else:
                queryset = AccessRawData.objects.all()

            if with_nodes:
                clean_task_query = DatabusCleanInfo.objects.all()
                queryset = queryset.prefetch_related(
                    Prefetch('clean_set', queryset=clean_task_query, to_attr='clean_list'),
                )

            for flow_item in queryset:
                flow_info = {
                    'flow_type': 'rawdata',
                    'flow_id': 'rawdata%s' % flow_item.id,
                    'flow_name': flow_item.raw_data_alias,
                    'bk_app_code': flow_item.bk_app_code,
                    'topic': '{}{}'.format(flow_item.raw_data_name, flow_item.bk_biz_id),
                    'bk_biz_id': flow_item.bk_biz_id,
                    'id': flow_item.id,
                    'raw_data_id': flow_item.id,
                    'raw_data_name': flow_item.raw_data_name,
                    'raw_data_alias': flow_item.raw_data_alias,
                    'data_scenario': flow_item.data_scenario,
                    'data_source': flow_item.data_source,
                    'data_encoding': flow_item.data_encoding,
                    'data_category': flow_item.data_category,
                }

                if with_nodes:
                    flow_info['nodes'] = {item.processing_id: model_to_dict(item) for item in flow_item.clean_list}

                flow_infos.append(flow_info)

        except Exception as e:
            logger.error('Build raw_data info error: %s' % e)


class DmonitorDatasetViewSet(BaseMixin, APIViewSet):
    lookup_field = 'data_set_id'
    MAX_RESULT_TABLE_COUNT = 200000
    MAX_RAW_DATA_COUNT = 50000
    MAX_RESULT_TABLE_STORAGE_COUNT = 500000
    MAX_DATA_PROCESSING_COUNT = 200000
    MAX_DATA_TRANSFERRING_COUNT = 200000

    @params_valid(serializer=DmonitorFlowSerializer)
    def list(self, request, params):
        """
        @api {get} /datamanage/dmonitor/data_sets/ 查询数据平台所有data_sets的信息

        @apiVersion 1.0.0
        @apiGroup DmonitorDataset
        @apiName get_dmonitor_data_sets_infos

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                }
            }
        """
        from gevent import monkey

        monkey.patch_all()

        data_set_infos = {}
        result_table_ids = set()
        update_duration = params.get('update_duration', 0)

        gevent.joinall(
            [
                gevent.spawn(self.get_result_table_infos, data_set_infos, result_table_ids, update_duration),
                gevent.spawn(self.get_raw_data_infos, data_set_infos, update_duration),
            ]
        )

        gevent.joinall(
            [
                gevent.spawn(self.update_clean_table_info, data_set_infos, result_table_ids, update_duration),
                gevent.spawn(self.update_dataflow_info, data_set_infos, result_table_ids, update_duration),
            ]
        )

        return Response(data_set_infos)

    def get_result_table_infos(self, data_set_infos, result_table_ids, update_duration=0):
        # 获取所有结果表信息
        try:
            result_tables = self.get_table_records(
                'result_table',
                ['result_table_id', 'bk_biz_id', 'project_id', 'processing_type', 'generate_type'],
                self.MAX_RESULT_TABLE_COUNT,
                update_duration,
            )
            result_table_storages = self.get_table_records(
                'storage_result_table',
                [
                    'id',
                    'storage_channel_id',
                    'storage_cluster_config_id',
                    'result_table_id',
                    'active',
                    'physical_table_name',
                ],
                self.MAX_RESULT_TABLE_STORAGE_COUNT,
                update_duration,
            )
            storage_cluster_configs = self.get_table_records(
                'storage_cluster_config',
                ['cluster_name', 'cluster_type', 'connection_info', 'id'],
            )
            channel_cluster_configs = self.get_table_records(
                'databus_channel_cluster_config',
                ['cluster_name', 'cluster_type', 'cluster_domain', 'cluster_port', 'id'],
            )

            # 构建结果表详情
            for result_table in result_tables:
                result_table_id = result_table.get('result_table_id')
                result_table.update(
                    {
                        'storages': {},
                        'data_set_id': result_table_id,
                        'data_set_type': 'result_table',
                        'related_flows': [],
                    }
                )
                data_set_infos[result_table_id] = result_table
                result_table_ids.add(result_table_id)

            # 构建存储集群详情
            storage_cluster_config_infos = {}
            channel_cluster_config_infos = {}
            for storage_cluster_config in storage_cluster_configs:
                id = storage_cluster_config.get('id')
                storage_cluster_config['storage_cluster_config_id'] = storage_cluster_config['id']
                storage_cluster_config_infos[id] = storage_cluster_config
            for channel_cluster_config in channel_cluster_configs:
                id = channel_cluster_config.get('id')
                channel_cluster_config['channel_cluster_config_id'] = channel_cluster_config['id']
                channel_cluster_config_infos[id] = channel_cluster_config

            # 构建rt存储信息
            for result_table_storage in result_table_storages:
                if not result_table_storage.get('active'):
                    continue
                result_table_id = result_table_storage.get('result_table_id')
                if result_table_id not in data_set_infos:
                    data_set_infos[result_table_id] = {
                        'data_set_id': result_table_id,
                        'result_table_id': result_table_id,
                        'data_set_type': 'result_table',
                        'storages': {},
                        'related_flows': [],
                    }
                storage_cluster_config_id = result_table_storage.get('storage_cluster_config_id')
                channel_cluster_config_id = result_table_storage.get('storage_channel_id')
                result_table_storage.update(
                    {
                        'storage_cluster': {},
                        'storage_channel': {},
                    }
                )
                if storage_cluster_config_id and storage_cluster_config_id in storage_cluster_config_infos:
                    result_table_storage['storage_cluster'] = storage_cluster_config_infos[storage_cluster_config_id]
                    cluster_type = result_table_storage['storage_cluster'].get('cluster_type')
                if channel_cluster_config_id and channel_cluster_config_id in channel_cluster_config_infos:
                    result_table_storage['storage_channel'] = channel_cluster_config_infos[channel_cluster_config_id]
                    cluster_type = result_table_storage['storage_channel'].get('cluster_type')
                data_set_infos[result_table_id]['storages'][cluster_type] = result_table_storage

        except Exception as e:
            logger.error(_('Build result table info error: {error}').format(error=e))

    def get_raw_data_infos(self, data_set_infos, update_duration=0):
        # 获取数据源信息
        try:
            raw_datas = self.get_table_records(
                'access_raw_data',
                [
                    'id',
                    'bk_biz_id',
                    'raw_data_name',
                    'data_source',
                    'data_scenario',
                    'bk_app_code',
                    'storage_channel_id',
                    'data_encoding',
                    'data_category',
                ],
                self.MAX_RAW_DATA_COUNT,
                update_duration,
            )

            for raw_data in raw_datas:
                raw_data_id = raw_data.get('id')
                flow_id = 'rawdata%s' % raw_data_id
                raw_data.update(
                    {
                        'data_set_id': raw_data_id,
                        'data_set_type': 'raw_data',
                        'flow_id': flow_id,
                        'flow_type': 'rawdata',
                        'topic': '{}{}'.format(raw_data.get('raw_data_name'), raw_data.get('bk_biz_id')),
                        'storage_id': raw_data.get('storage_channel_id'),
                        'storage_type': 'channel',
                        'related_flows': [flow_id],
                    }
                )
                data_set_infos[raw_data_id] = raw_data
        except Exception as e:
            logger.error(_('Build raw data info error: {error}').format(error=e))

    def update_clean_table_info(self, data_set_infos, result_table_ids, update_duration=0):
        # 赋予清洗表flow的信息
        if update_duration:
            queryset = DatabusCleanInfo.objects.filter(processing_id__in=list(result_table_ids))
        else:
            queryset = DatabusCleanInfo.objects.all()

        for clean_table in queryset.iterator():
            if clean_table.processing_id in data_set_infos:
                data_set_infos[clean_table.processing_id]['flow_type'] = 'rawdata'
                flow_id = 'rawdata%s' % clean_table.raw_data_id
                data_set_infos[clean_table.processing_id]['flow_id'] = flow_id
                data_set_infos[clean_table.processing_id]['related_flows'] = [flow_id]
                data_set_infos[clean_table.processing_id]['status'] = clean_table.status

    def update_dataflow_info(self, data_set_infos, result_table_ids, update_duration=0):
        # 赋予普通的结果表flow和node的信息
        if update_duration:
            queryset = DataflowNodeRelation.objects.filter(result_table_id__in=list(result_table_ids))
        else:
            queryset = DataflowNodeRelation.objects.all()

        for flow_node in queryset.iterator():
            result_table_id = flow_node.result_table_id
            flow_id = flow_node.flow_id
            node_type = flow_node.node_type
            if result_table_id in data_set_infos:
                if node_type.find('source') >= 0:
                    data_set_infos[result_table_id]['related_flows'].append(flow_id)
                    continue

                if data_set_infos[result_table_id].get('flow_type') != 'rawdata':
                    if 'flow_id' in data_set_infos[result_table_id]:
                        if data_set_infos[result_table_id]['flow_id'] == flow_id:
                            data_set_infos[result_table_id]['nodes'][node_type] = flow_node.node_id
                        else:
                            data_set_infos[result_table_id]['related_flows'].append(flow_id)
                            logger.warning(
                                _(
                                    'ResultTable({result_table_id} as two nodes '
                                    'in flow1({flow_id1}) and flow2({flow_id2})'
                                ).format(
                                    result_table_id=result_table_id,
                                    flow_id1=data_set_infos[result_table_id]['flow_id'],
                                    flow_id2=flow_id,
                                )
                            )
                            continue
                    else:
                        data_set_infos[result_table_id]['flow_id'] = flow_id
                        data_set_infos[result_table_id]['flow_type'] = 'dataflow'
                        data_set_infos[result_table_id]['related_flows'].append(flow_id)
                        data_set_infos[result_table_id]['nodes'] = {node_type: flow_node.node_id}

    def retrieve(self, request, data_set_id):
        """
        @api {get} /datamanage/dmonitor/data_sets/:data_set_id/ 查询单个dataset的信息

        @apiVersion 1.0.0
        @apiGroup DmonitorDataset
        @apiName get_dmonitor_data_set_infos

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                }
            }
        """
        data_set_info = {}

        if data_set_id.isdigit():
            # 获取数据源信息
            res = AccessApi.rawdata.retrieve({'raw_data_id': data_set_id, 'bk_username': 'datamanage'})
            raw_data_id = data_set_info.get('id')
            flow_id = 'rawdata%s' % raw_data_id
            if res.is_success() and res.data:
                data_set_info = res.data
                raw_data_id = raw_data_id
                data_set_info['data_set_id'] = data_set_id
                data_set_info['data_set_type'] = 'raw_data'
                data_set_info['flow_id'] = flow_id
                data_set_info['flow_type'] = 'rawdata'
                data_set_info['storage_id'] = data_set_info['storage_channel_id']
                data_set_info['storage_type'] = 'channel'
                data_set_info['related_flows'] = [flow_id]
        else:
            # 获取结果表信息
            res = MetaApi.result_tables.retrieve({'result_table_id': data_set_id, 'related': 'storages'})
            if res.is_success() and res.data:
                data_set_info = res.data
                data_set_info['data_set_id'] = data_set_id
                data_set_info['data_set_type'] = 'result_table'
                data_set_info['related_flows'] = []

                # 赋予清洗表flow的信息
                for clean_table in DatabusCleanInfo.objects.filter(processing_id=data_set_id):
                    flow_id = 'rawdata%s' % clean_table.raw_data_id
                    data_set_info['flow_type'] = 'rawdata'
                    data_set_info['flow_id'] = flow_id
                    data_set_info['related_flows'] = [flow_id]

                # 赋予普通的结果表flow和node的信息
                for flow_node in DataflowNodeRelation.objects.filter(result_table_id=data_set_id):
                    result_table_id = flow_node.result_table_id
                    flow_id = flow_node.flow_id
                    node_type = flow_node.node_type
                    if node_type.find('source') >= 0:
                        data_set_info['related_flows'].append(flow_id)
                        continue

                    if data_set_info.get('flow_type') != 'rawdata':
                        if 'flow_id' in data_set_info:
                            if data_set_info['flow_id'] == flow_id:
                                data_set_info['nodes'][node_type] = flow_node.node_id
                            else:
                                data_set_info['related_flows'].append(flow_id)
                                logger.warning(
                                    _(
                                        'ResultTable({result_table_id} as two nodes '
                                        'in flow1({flow_id1}) and flow2({flow_id2})'
                                    ).format(
                                        result_table_id=result_table_id,
                                        flow_id1=data_set_info['flow_id'],
                                        flow_id2=flow_id,
                                    )
                                )
                                continue
                        else:
                            data_set_info['flow_id'] = flow_id
                            data_set_info['flow_type'] = 'dataflow'
                            data_set_info['related_flows'].append(flow_id)
                            data_set_info['nodes'] = {node_type: flow_node.node_id}

        return Response(data_set_info)


class DmonitorDataOperationsViewSet(BaseMixin, APIViewSet):
    lookup_field = 'data_operation_id'
    MAX_DATA_PROCESSING_COUNT = 200000
    MAX_DATA_TRANSFERRING_COUNT = 200000

    @params_valid(serializer=DmonitorDataOperationSerializer)
    def retrieve(self, request, data_operation_id, params):
        data_operation = None
        try:
            res = MetaApi.data_processings.retrieve({'processing_id': data_operation_id})
            if res.is_success():
                data_operation = res.data
        except Exception:
            pass

        try:
            res = MetaApi.data_transferrings.retrieve({'transferring_id': data_operation_id})
            if res.is_success():
                data_operation = res.data
        except Exception:
            pass

        if not data_operation:
            raise dm_errors.DataOperationNotExistError()
        return Response(data_operation)

    @params_valid(serializer=DmonitorDataOperationSerializer)
    def list(self, request, params):
        """
        @api {get} /datamanage/dmonitor/data_operations/ 查询数据平台所有数据处理和数据传输

        @apiVersion 1.0.0
        @apiGroup DmonitorDataset
        @apiName get_dmonitor_data_sets_relations

        @apiParam {bool} [with_relations] 是否包含关联关系
        @apiParam {bool} [with_status] 是否包含状态信息
        @apiParam {bool} [with_node] 是否包含节点或上下游节点信息

        @apiSuccessExample Success-Response:
            HTTP/1.1 200 OK
            {
                "errors": {},
                "message": "ok",
                "code": "1500200",
                "result": true,
                "data": {
                }
            }
        """
        from gevent import monkey

        monkey.patch_all()

        with_relations = params['with_relations']
        with_status = params['with_status']
        with_node = params['with_node']

        data_operations, relations, cluster_configs, task_status = {}, [], {}, {}
        event_list = [
            gevent.spawn(self.fetch_data_processings, data_operations),
            gevent.spawn(self.fetch_data_transferrings, data_operations),
        ]

        if with_relations:
            event_list.extend(
                [
                    gevent.spawn(self.fetch_data_processing_relations, relations),
                    gevent.spawn(self.fetch_data_transferring_relations, relations),
                    gevent.spawn(self.get_channel_cluster_configs, cluster_configs),
                    gevent.spawn(self.get_storage_cluster_configs, cluster_configs),
                ]
            )

        if with_status:
            event_list.extend(
                [
                    gevent.spawn(self.get_databus_clean_status, task_status),
                    gevent.spawn(self.get_databus_shipper_status, task_status),
                    gevent.spawn(self.get_databus_transform_status, task_status),
                    gevent.spawn(self.get_dataflow_status, task_status),
                ]
            )

        processing_infos, result_table_nodes, node_infos = {}, {}, {}
        if with_node:
            event_list.extend(
                [
                    gevent.spawn(self.get_dataflow_processing, processing_infos),
                    gevent.spawn(self.get_clean_processing, processing_infos, node_infos),
                    gevent.spawn(self.get_dataflow_nodes, node_infos),
                    # 暂不处理data_transferring上下游节点ID信息
                    # gevent.spawn(self.get_dataflow_node_relation, result_table_nodes),
                ]
            )

        gevent.joinall(event_list)

        if with_relations:
            self.attach_relation_info(data_operations, relations, cluster_configs)

        if with_status:
            self.attach_status_info(data_operations, task_status)

        if with_node:
            self.attach_node_info(data_operations, processing_infos, node_infos, result_table_nodes, cluster_configs)

        return Response(data_operations)

    def attach_relation_info(self, data_operations, relations, cluster_configs):
        for data_operation in list(data_operations.values()):
            data_operation.update(
                {
                    'inputs': [],
                    'outputs': [],
                }
            )
        for relation in relations:
            processing_id = relation.get('processing_id')
            transferring_id = relation.get('transferring_id')
            storage_key = self.generate_storage_key(relation)
            if processing_id not in data_operations and transferring_id not in data_operations:
                continue
            if relation['data_directing'] == 'input':
                data_operations[processing_id or transferring_id]['inputs'].append(
                    {
                        'data_set_id': relation.get('data_set_id'),
                        'storage_key': storage_key,
                        'cluster_type': cluster_configs.get(storage_key, {}).get('cluster_type'),
                        'cluster_name': cluster_configs.get(storage_key, {}).get('cluster_name'),
                    }
                )
            elif relation['data_directing'] == 'output':
                data_operations[processing_id or transferring_id]['outputs'].append(
                    {
                        'data_set_id': relation.get('data_set_id'),
                        'storage_key': storage_key,
                        'cluster_type': cluster_configs.get(storage_key, {}).get('cluster_type'),
                        'cluster_name': cluster_configs.get(storage_key, {}).get('cluster_name'),
                    }
                )

    def attach_status_info(self, data_operations, task_status):
        for task_id, data_operation in list(data_operations.items()):
            if (
                data_operation['data_operation_type'] == 'data_transferring'
                and data_operation['transferring_type'] == 'puller'
            ):
                data_operation['status'] = 'started'
            else:
                data_operation['status'] = task_status.get(task_id, None)

    def attach_node_info(self, data_operations, processing_infos, node_infos, *args, **kwargs):
        for data_operation in list(data_operations.values()):
            if data_operation['data_operation_type'] == 'data_processing':
                processing_id = data_operation.get('processing_id')
                if processing_id in processing_infos:
                    data_operation.update(
                        {
                            'flow_id': processing_infos[processing_id].get('flow_id'),
                            'node_id': processing_infos[processing_id].get('node_id'),
                        }
                    )
                    if data_operation['node_id'] in node_infos:
                        data_operation.update(
                            {
                                'node_name': node_infos[data_operation['node_id']].get('node_name'),
                            }
                        )
                continue

            # 暂不处理data_transferring上下游节点ID信息
            # for input_data_set in data_operation.get('inputs', []):
            #     input_result_table = input_data_set.get('data_set_id')
            #     input_storage_key = input_data_set.get('storage_key')

            #     # 获取节点信息，主要包含该RT下所有的node_id及对应node_type的信息
            #     if input_result_table not in result_table_nodes:
            #         continue
            #     result_table_node = result_table_nodes[input_result_table]

            #     # 获取存储信息
            #     if input_storage_key not in cluster_configs:
            #         continue
            #     cluster_config = cluster_configs[input_storage_key]

            #     # 根据“节点类型-存储类型”映射表来获取当前input的node信息
            #     # 该映射表后续会由计算模块提供字典表进行替代，目前只能人工维护
            #     input_node_info = self.get_node_info_by_cluster(result_table_node, cluster_config, node_infos)
            #     input_data_set.update({
            #         'node_id': input_node_info.get('node_id'),
            #         'node_name': input_node_info.get('node_name'),
            #     })

    def generate_storage_key(self, relation):
        storage_id = None
        storage_type = relation['storage_type']
        if storage_type == 'storage':
            storage_id = relation['storage_cluster_config_id']
        elif storage_type == 'channel':
            storage_id = relation['channel_cluster_config_id']
        return '{}_{}'.format(storage_type, storage_id)

    def get_node_info_by_cluster(self, result_table_node, cluster_config, node_infos):
        # 根据“存储类型-节点类型”映射表获取节点类型，如果能找到，则该节点类型对应的节点ID为当前要找的节点ID
        cluster_type = cluster_config.get('cluster_type')
        if cluster_type in CLUSTER_NODE_TYPES_MAPPINGS:
            target_node_type = CLUSTER_NODE_TYPES_MAPPINGS[cluster_type]
            if target_node_type in result_table_node['nodes']:
                target_node_id = result_table_node['nodes'][target_node_type]
                if target_node_id in node_infos:
                    return node_infos[target_node_id]

        for node_type, node_id in list(result_table_node['nodes'].items()):
            if node_id not in node_infos:
                continue

            # 对于存储节点，节点配置中会包含cluster的名称，如果该名称与集群配置中的cluster_name相同，则返回该节点配置
            node_config = node_infos[node_id].get('node_config', {})
            if 'cluster' in node_config and node_config['cluster'] == cluster_config['cluster_name']:
                return node_infos[node_id]

    def fetch_data_processings(self, data_operations):
        data_processings = self.get_table_records(
            'data_processing',
            ['processing_id', 'project_id', 'processing_type'],
            self.MAX_DATA_PROCESSING_COUNT,
        )
        for item in data_processings:
            item['data_operation_type'] = 'data_processing'
            data_operations[item['processing_id']] = item

    def fetch_data_transferrings(self, data_operations):
        data_transferrings = self.get_table_records(
            'data_transferring',
            ['transferring_id', 'project_id', 'transferring_type'],
            self.MAX_DATA_TRANSFERRING_COUNT,
        )
        for item in data_transferrings:
            item['data_operation_type'] = 'data_transferring'
            data_operations[item['transferring_id']] = item

    def fetch_data_processing_relations(self, relations):
        data_processing_relations = self.get_table_records(
            'data_processing_relation',
            [
                'data_directing',
                'data_set_id',
                'storage_cluster_config_id',
                'channel_cluster_config_id',
                'storage_type',
                'processing_id',
            ],
            self.MAX_DATA_PROCESSING_COUNT * 2,
        )
        relations.extend(data_processing_relations)

    def fetch_data_transferring_relations(self, relations):
        data_transferring_relations = self.get_table_records(
            'data_transferring_relation',
            [
                'data_directing',
                'data_set_id',
                'storage_cluster_config_id',
                'channel_cluster_config_id',
                'storage_type',
                'transferring_id',
            ],
            self.MAX_DATA_TRANSFERRING_COUNT * 2,
        )
        relations.extend(data_transferring_relations)

    def get_databus_clean_status(self, task_status):
        queryset = DatabusCleanInfo.objects.values('status', 'processing_id')
        for item in queryset:
            processing_id = item['processing_id']
            if processing_id is not None:
                task_status[processing_id] = item['status']

    def get_databus_shipper_status(self, task_status):
        queryset = DatabusShipperInfo.objects.values('status', 'processing_id', 'transferring_id')
        for item in queryset:
            processing_id = item['processing_id']
            transferring_id = item['transferring_id']
            if transferring_id:
                task_status[transferring_id] = item['status']
                continue
            if processing_id:
                task_status[processing_id] = item['status']

    def get_databus_transform_status(self, task_status):
        queryset = DatabusTransformProcessing.objects.values('status', 'processing_id')
        for item in queryset:
            processing_id = item['processing_id']
            if processing_id is not None:
                task_status[processing_id] = item['status']

    def get_dataflow_status(self, task_status):
        queryset = DataflowNodeRelation.objects.values('result_table_id', 'node_type', 'node__status')
        for item in queryset:
            if 'source' in item['node_type']:
                continue
            if 'storage' in item['node_type']:
                continue
            processing_id = item['result_table_id']
            if processing_id is not None:
                task_status[processing_id] = item['node__status']

    def get_dataflow_processing(self, processing_infos):
        dataflow_processing_data = self.get_table_records(
            'dataflow_processing',
            ['flow_id', 'node_id', 'processing_id'],
        )
        for dataflow_processing in dataflow_processing_data:
            processing_infos[dataflow_processing.get('processing_id')] = {
                'flow_id': dataflow_processing.get('flow_id'),
                'node_id': dataflow_processing.get('node_id'),
            }

    def get_clean_processing(self, processing_infos, node_infos):
        queryset = DatabusCleanInfo.objects.values('processing_id', 'raw_data_id', 'clean_config_name')
        for item in queryset:
            flow_id = 'rawdata{}'.format(item.get('raw_data_id'))
            processing_infos[item.get('processing_id')] = {
                'flow_id': flow_id,
                'node_id': item.get('processing_id'),
            }
            node_infos[item.get('processing_id')] = {
                'node_name': item.get('clean_config_name'),
            }

    def get_dataflow_nodes(self, node_infos):
        node_info_data = self.get_table_records(
            'dataflow_node_info',
            ['node_id', 'node_name', 'node_config'],
        )
        for node_info in node_info_data:
            node_infos[node_info.get('node_id')] = {
                'node_name': node_info.get('node_name'),
                'node_config': json.loads(node_info.get('node_config', '{}')),
            }

    def get_dataflow_node_relation(self, result_table_nodes):
        relations = self.get_table_records(
            'dataflow_node_relation',
            ['node_id', 'node_type', 'result_table_id'],
        )
        for relation in relations:
            result_table_id = relation.get('result_table_id')
            node_id = relation.get('node_id')
            node_type = relation.get('node_type')
            if result_table_nodes not in result_table_nodes:
                result_table_nodes[result_table_id] = {
                    'nodes': {},
                }
            result_table_nodes[result_table_id]['nodes'][node_type] = node_id

    def get_channel_cluster_configs(self, cluster_configs):
        channel_cluster_configs = self.get_table_records(
            'databus_channel_cluster_config',
            ['cluster_name', 'cluster_type', 'cluster_domain', 'cluster_port', 'id'],
        )
        for channel_cluster_config in channel_cluster_configs:
            channel_cluster_config['channel_cluster_config_id'] = channel_cluster_config.get('id')
            storage_key = 'channel_{}'.format(channel_cluster_config.get('id'))
            cluster_configs[storage_key] = channel_cluster_config

    def get_storage_cluster_configs(self, cluster_configs):
        storage_cluster_configs = self.get_table_records(
            'storage_cluster_config',
            ['cluster_name', 'cluster_type', 'connection_info', 'id'],
        )
        for storage_cluster_config in storage_cluster_configs:
            storage_cluster_config['storage_cluster_config_id'] = storage_cluster_config.get('id')
            storage_key = 'storage_{}'.format(storage_cluster_config.get('id'))
            cluster_configs[storage_key] = storage_cluster_config

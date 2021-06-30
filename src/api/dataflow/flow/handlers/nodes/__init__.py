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

from dataflow.flow.handlers.node_factory import NODE_FACTORY
from dataflow.flow.handlers.nodes.process_node.batchv2_node import BatchV2Node
from dataflow.flow.handlers.nodes.process_node.data_model_app_node import DataModelAppNode
from dataflow.flow.handlers.nodes.process_node.data_model_batch_indicator_node import DataModelIndicatorOfflineNode
from dataflow.flow.handlers.nodes.process_node.data_model_stream_indicator_node import DataModelIndicatorRealTimeNode
from dataflow.flow.handlers.nodes.process_node.flink_streaming_node import FlinkStreamingNode
from dataflow.flow.handlers.nodes.process_node.merge_node import MergeNode
from dataflow.flow.handlers.nodes.process_node.model_app_node import ModelAppNode
from dataflow.flow.handlers.nodes.process_node.model_node import ModelNode
from dataflow.flow.handlers.nodes.process_node.model_ts_custom_node import ModelTSCustomNode
from dataflow.flow.handlers.nodes.process_node.process_model_node import ProcessModelNode
from dataflow.flow.handlers.nodes.process_node.spark_structured_streaming_node import SparkStructuredStreamingNode
from dataflow.flow.handlers.nodes.process_node.split_node import SplitNode
from dataflow.flow.handlers.nodes.process_node.stream_node import StreamNode
from dataflow.flow.handlers.nodes.source_node.batch_kv_source_node import BatchKVSourceNode
from dataflow.flow.handlers.nodes.source_node.batch_source_node import BatchSourceNode
from dataflow.flow.handlers.nodes.source_node.stream_source_node import StreamSourceNode
from dataflow.flow.handlers.nodes.source_node.unified_kv_source import UnifiedKVSourceNode
from dataflow.flow.handlers.nodes.storage_node.clickhouse_storage_node import ClickHouseStorageNode
from dataflow.flow.handlers.nodes.storage_node.druid_storage_node import DruidStorageNode
from dataflow.flow.handlers.nodes.storage_node.elastic_storage_node import ElasticStorageNode
from dataflow.flow.handlers.nodes.storage_node.hdfs_storage_node import HdfsStorageNode
from dataflow.flow.handlers.nodes.storage_node.ignite_storage import IgniteStorageNode
from dataflow.flow.handlers.nodes.storage_node.mysql_storage_node import MysqlStorageNode
from dataflow.flow.handlers.nodes.storage_node.queue_pulsar_storage import QueuePulsarStorageNode
from dataflow.flow.handlers.nodes.storage_node.queue_storage_node import QueueStorageNode
from dataflow.flow.node_types import NodeTypes

NODE_FACTORY.add_node_class(NodeTypes.BATCH_KV_SOURCE, BatchKVSourceNode)
NODE_FACTORY.add_node_class(NodeTypes.STREAM_SOURCE, StreamSourceNode)
NODE_FACTORY.add_node_class(NodeTypes.BATCH_SOURCE, BatchSourceNode)
NODE_FACTORY.add_node_class(NodeTypes.UNIFIED_KV_SOURCE, UnifiedKVSourceNode)

# 计算节点
NODE_FACTORY.add_node_class(NodeTypes.STREAM, StreamNode)
NODE_FACTORY.add_node_class(NodeTypes.BATCHV2, BatchV2Node)
NODE_FACTORY.add_node_class(NodeTypes.SPARK_STRUCTURED_STREAMING, SparkStructuredStreamingNode)
NODE_FACTORY.add_node_class(NodeTypes.FLINK_STREAMING, FlinkStreamingNode)
NODE_FACTORY.add_node_class(NodeTypes.DATA_MODEL_APP, DataModelAppNode)
NODE_FACTORY.add_node_class(NodeTypes.DATA_MODEL_STREAM_INDICATOR, DataModelIndicatorRealTimeNode)
NODE_FACTORY.add_node_class(NodeTypes.DATA_MODEL_BATCH_INDICATOR, DataModelIndicatorOfflineNode)
NODE_FACTORY.add_node_class(NodeTypes.MERGE_KAFKA, MergeNode)
NODE_FACTORY.add_node_class(NodeTypes.SPLIT_KAFKA, SplitNode)
NODE_FACTORY.add_node_class(NodeTypes.MODEL_APP, ModelAppNode)
NODE_FACTORY.add_node_class(NodeTypes.MODEL, ModelNode)
NODE_FACTORY.add_node_class(NodeTypes.PROCESS_MODEL, ProcessModelNode)
NODE_FACTORY.add_node_class(NodeTypes.MODEL_TS_CUSTOM, ModelTSCustomNode)

# 存储节点
NODE_FACTORY.add_node_class(NodeTypes.ES_STORAGE, ElasticStorageNode)
NODE_FACTORY.add_node_class(NodeTypes.MYSQL_STORGAE, MysqlStorageNode)
NODE_FACTORY.add_node_class(NodeTypes.HDFS_STORAGE, HdfsStorageNode)
NODE_FACTORY.add_node_class(NodeTypes.CLICKHOUSE_STORAGE, ClickHouseStorageNode)
NODE_FACTORY.add_node_class(NodeTypes.QUEUE_STORAGE, QueueStorageNode)
NODE_FACTORY.add_node_class(NodeTypes.DRUID_STORAGE, DruidStorageNode)
NODE_FACTORY.add_node_class(NodeTypes.QUEUE_PULSAR_STORAGE, QueuePulsarStorageNode)
NODE_FACTORY.add_node_class(NodeTypes.IGNITE_STORAGE, IgniteStorageNode)

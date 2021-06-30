/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
 */

/* eslint-disable max-len */

import { mergeDeep } from '@/common/js/util.js';
import extend from '@/extends/index';

const extendConfig = extend.callJsFragmentFn('DataflowV2Extend', this) || {};
const flowConfig = {
  processing: {
    groupIcon: 'icon-cal-node',
    groupTemplate: 'graph-ractangle',
    isGroup: true,
    instances: {
      batchv2: {
        iconName: 'icon-offline',
        component: import('@/pages/DataGraph/Graph/Nodes/GraphNode.OfflineNode.tsx'),
        webNodeType: 'batchv2',
        width: '80%',
        doc: 'zh/user-guide/dataflow/components/processing/batch.html',
      },
      batch: {
        iconName: 'icon-offline',
        component: import('@/pages/DataGraph/Graph/Children/OfflineNodeNew'),
        webNodeType: 'offline',
        width: '80%',
        doc: 'zh/user-guide/dataflow/components/processing/batch.html',
      },
      stream: {
        iconName: 'icon-realtime',
        component: import('@/pages/DataGraph/Graph/Children/RealtimeNew'),
        webNodeType: 'realtime',
        width: '80%',
        doc: 'zh/user-guide/dataflow/components/processing/stream.html',
      },
      merge: {
        iconName: 'icon-merge',
        component: import('@/pages/DataGraph/Graph/Children/merge'),
        webNodeType: 'merge',
        width: 600,
        doc: 'zh/user-guide/dataflow/components/processing/merge.html',
      },
      split: {
        iconName: 'icon-split',
        component: import('@/pages/DataGraph/Graph/Children/split'),
        webNodeType: 'split',
        width: 600,
        doc: 'zh/user-guide/dataflow/components/processing/split.html',
      },
    },
  },
  source: {
    groupIcon: 'icon-data_source',
    groupTemplate: 'graph-round',
    isGroup: true,
    instances: {
      batch_kv_source: {
        iconName: 'icon-batch_kv_source',
        component: import('@/pages/DataGraph/Graph/Children/sourceNode'),
        webNodeType: 'batch_kv_source',
        width: 600,
        doc: 'zh/user-guide/dataflow/components/source/batch-kv-source.html',
      },
      batch_source: {
        iconName: 'icon-batch_source',
        component: import('@/pages/DataGraph/Graph/Children/sourceNode'),
        webNodeType: 'batch_source',
        width: 600,
        doc: 'zh/user-guide/dataflow/components/source/batch-source.html',
      },
      stream_source: {
        iconName: 'icon-stream_source',
        component: import('@/pages/DataGraph/Graph/Children/sourceNode'),
        webNodeType: 'stream_source',
        width: 600,
        doc: 'zh/user-guide/dataflow/components/source/stream-source.html',
      },
      unified_kv_source: {
        iconName: 'icon-unified_kv_source',
        component: import('@/pages/DataGraph/Graph/Children/sourceNode'),
        webNodeType: 'unified_kv_source',
        width: 600,
        doc: '',
      },
    },
  },
  storage: {
    groupIcon: 'icon-save-node',
    groupTemplate: 'graph-square',
    isGroup: true,
    instances: {
      clickhouse_storage: {
        iconName: 'icon-clickhouse_storage',
        component: import('@/pages/DataGraph/Graph/Children/mySqlSaveNode'),
        webNodeType: 'clickhouse_storage',
        width: 600,
        doc: '',
      },
      tcaplus_storage: {
        iconName: 'icon-tcaplus_storage',
        component: import('@/pages/DataGraph/Graph/Children/mySqlSaveNode'),
        webNodeType: 'tcaplus_storage',
        width: 600,
        doc: '',
      },
      hdfs_storage: {
        iconName: 'icon-hdfs_storage',
        component: import('@/pages/DataGraph/Graph/Children/hdfsNode'),
        webNodeType: 'hdfs_storage',
        width: 600,
        doc: 'zh/user-guide/dataflow/components/storage/hdfs.html',
      },
      queue_storage: {
        iconName: 'icon-queue_storage',
        component: import('@/pages/DataGraph/Graph/Children/mySqlSaveNode'),
        webNodeType: 'queue_storage',
        width: 600,
        doc: 'zh/user-guide/dataflow/components/storage/queue.html',
      },
      druid_storage: {
        iconName: 'icon-druid_storage',
        component: import('@/pages/DataGraph/Graph/Children/mySqlSaveNode'),
        webNodeType: 'druid_storage',
        width: 600,
        doc: 'zh/user-guide/dataflow/components/storage/druid.html',
      },
      mysql_storage: {
        iconName: 'icon-mysql_storage',
        component: import('@/pages/DataGraph/Graph/Children/mySqlSaveNode'),
        webNodeType: 'mysql_storage',
        width: 600,
        doc: '',
      },
      elastic_storage: {
        iconName: 'icon-elastic_storage',
        component: import('@/pages/DataGraph/Graph/Children/esNode'),
        webNodeType: 'elastic_storage',
        width: 600,
        doc: 'zh/user-guide/dataflow/components/storage/elasticsearch.html',
      },
      tsdb_storage: {
        iconName: 'icon-tsdb_storage',
        component: import('@/pages/DataGraph/Graph/Children/mySqlSaveNode'),
        webNodeType: 'tsdb_storage',
        width: 600,
        doc: '',
      },
      ignite: {
        iconName: 'icon-ignite_storage',
        component: import('@/pages/DataGraph/Graph/Children/IgniteNode'),
        webNodeType: 'ignite',
        width: 600,
        doc: 'zh/user-guide/dataflow/components/storage/ignite.html',
      },
    },
  },
};

class DataflowV2 {
  constructor(nodeConfig = []) {
    this.webNodeConfig = mergeDeep(flowConfig, extendConfig);
    // 节点需要判断当前是否有相关配置
    this.nodeConfig = nodeConfig.reduce((acc, cur) => {
      const groupKey = cur.group_type_name;
      if (!this.webNodeConfig[groupKey]) return acc;

      const curCopy = JSON.parse(JSON.stringify(cur));

      curCopy.instances = cur.instances.filter(instance => {
        const existNodes = Object.keys(this.webNodeConfig[groupKey].instances);
        return existNodes.includes(instance.node_type_instance_name);
      });
      acc.push(curCopy);

      return acc;
    }, []);
  }

  /**
   * handleNodeConfig
   * @param {*} nodeClass
   * @param {*} nodeName
   * @returns
   */
  handleNodeConfig(nodeClass, nodeName) {
    return Object.values(nodeClass.instances).map(nodeConfig => nodeName.push(nodeConfig.component));
  }

  /**
   * getAllWebNodesName
   * @returns
   */
  getAllWebNodesName() {
    const nodeName = [];
    Object.values(this.webNodeConfig).map(nodeClass => this.handleNodeConfig(nodeClass, nodeName));
    return nodeName;
  }

  /**
   * getNodeConfig
   * @returns
   */
  getNodeConfig() {
    this.nodeConfig.forEach((config) => {
      const nodeTypeName = config.group_type_name;
      const nodeWebConfig = this.webNodeConfig[nodeTypeName];
      const { groupTemplate, groupIcon } = nodeWebConfig;
      Object.assign(config, { groupTemplate, groupIcon });
      (config.instances || []).forEach((child) => {
        const instanceName = child.node_type_instance_name;
        const webInstanceConfig = nodeWebConfig.instances[instanceName];
        Object.assign(child, webInstanceConfig);
      });
    });
    return this.nodeConfig;
  }

  /**
   * 根据接口返回内容，设置节点配置
   * @param {*} data nodeConfigV2接口返回配置
   * @returns
   */
  setNodeConfig(data) {
    return Object.assign(data.filter(group => this.webNodeConfig[group.group_type_name]), this.getNodeConfig());
  }

  /**
     * 格式化服务器返回数据
     * @param {*} response ： 接口返回数据
     */
  static formatGraphSchema(response) {
    if (response.result && response.data) {
      response.data.lines = (response.data.lines || []).map((line) => {
        const frontendInfo = line.frontend_info || {};
        const { source = {}, target = {} } = frontendInfo;
        return {
          to_node_id: line.to_node_id,
          from_node_id: line.from_node_id,
          source: Object.assign({}, source, {
            id: `ch_${line.from_node_id}`,
          }),
          target: Object.assign({}, target, {
            id: `ch_${line.to_node_id}`,
          }),
        };
      });

      response.data.locations = (response.data.locations || []).map((location) => {
        const id = { id: `ch_${location.node_id}` };
        return Object.assign(location, location.frontend_info, id);
      });
    }

    return response;
  }
}

export default DataflowV2;

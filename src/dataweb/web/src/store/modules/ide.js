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

/**
 *  ide的状态管理
 */
import { copyObj } from '@/common/js/util';
import { DEBUG_MODE } from '@/pages/DataGraph/Common/constant';

const state = {
  // 全局Loading状态
  loading: {
    main: true,
    graph: true,
  },
  // 补算状态
  complementStatus: {
    status: 'none',
  },
  // 透明loading遮罩
  lucidLoading: false,
  // 调试信息
  debugInfo: {
    id: 0,
    status: DEBUG_MODE.NONE,
    pointTimeInstance: '',
    display: '',
    countdown: '10:00',
  },

  // flow的基础信息
  flowData: {},
  // 画布的显示信息
  graphData: {},
  // 运行信息
  tempNodeConfig: [],
  runningInfo: {
    infoList: [],
    // 当前dataflow是否正在运行
    isRunning: false,
  },
  storageConfig: [],
  flowNodeConfig: [],
};

const getters = {
  getTempNodeConfig(state) {
    return state.tempNodeConfig;
  },
  getNodeConfigByNodeWebType: state => (webNodeType) => {
    const outputConfig = {};
    (state.flowNodeConfig || []).some(config => (config.instances || []).some((instance) => {
      if (instance.webNodeType === webNodeType) {
        Object.assign(outputConfig, instance, {
          node_group_name: config.group_type_name,
          node_group_display_name: config.group_type_alias,
          groupTemplate: config.groupTemplate,
          groupIcon: config.groupIcon,
        });
        return true;
      }
      return false;
    }));
    return outputConfig;
  },
  flowNodeConfig(state) {
    return state.flowNodeConfig;
  },
  flowStatus(state) {
    return (state.flowData && state.flowData.status) || 'no-start';
  },
  /**
   * 根据id获取节点信息
   */
  getNodeDataById: state => (id) => {
    if (!state.graphData || !state.graphData.locations) {
      return null;
    }
    return copyObj(state.graphData.locations.find(location => location.id === id));
  },
  /**
   * 根据nodeId获取节点信息
   */
  getNodeDataByNodeId: state => (nodeId) => {
    if (!state.graphData || !state.graphData.locations) {
      return null;
    }
    // eslint-disable-next-line radix
    const curNodeId = parseInt(nodeId);
    const node = state.graphData.locations.find((location) => {
      if (location.node_id === curNodeId) {
        return location;
      }
      return null;
    });
    return copyObj(node);
  },
  /**
   * 根据id获取节点的父节点信息
   */
  getParentNodeDataById: state => (id) => {
    if (!state.graphData || !state.graphData.lines || !state.graphData.locations) {
      return null;
    }
    const parentIds = [];
    for (const line of state.graphData.lines) {
      if (line.target.id === id) {
        parentIds.push(line.source.id);
      }
    }
    return state.graphData.locations.filter(location => parentIds.includes(location.id));
  },
  /**
   * 根据id获取节点的子节点信息
   */
  getChildrenNodeDataById: state => (id) => {
    if (!state.graphData || !state.graphData.lines || !state.graphData.locations) {
      return null;
    }
    const childrenIds = [];
    for (const line of state.graphData.lines) {
      if (line.source.id === id) {
        childrenIds.push(line.target.id);
      }
    }
    return state.graphData.locations.filter(location => childrenIds.includes(location.id));
  },
  isBeingDebug(state) {
    return state.debugInfo.status === 'normal' && state.debugInfo.status !== DEBUG_MODE.RENORMAL;
  },
  isNormalDebug(state) {
    return state.debugInfo.status === DEBUG_MODE.NORMAL;
  },
  isPointDebug(state) {
    return state.debugInfo.status === DEBUG_MODE.POINT;
  },
  getCanRecalNode(state) {
    const nodeTypes = [
      'tdw_jar_batch',
      'tdw_batch',
      'offline',
      'process_model',
      'model_ts_custom',
      'data_model_batch_indicator',
      'batchv2',
    ];
    return state.graphData.locations !== undefined
      ? state.graphData.locations
        .filter((item) => {
          if (item.result_table_ids) {
            return nodeTypes.includes(item.node_type) && item.status !== 'unconfig';
          }
          return false;
        })
        .map((item) => {
          const displayName = {
            displayName: `${item.result_table_ids[0]}(${item.node_name})`,
          };
          return Object.assign({}, item, displayName);
        })
      : [];
  },
};

const actions = {};

const mutations = {
  setTempNodeConfig(state, status) {
    state.tempNodeConfig = status;
  },
  setComplementStatus(state, status) {
    state.complementStatus.status = status;
  },
  setComplementConfig(state, config) {
    state.complementStatus = config;
  },
  setFlowNodeconfig(state, conf) {
    state.flowNodeConfig = conf;
  },
  updateNodeConfig(state, newData) {
    (state.flowNodeConfig || []).forEach(config => (config.instances || []).forEach((instance) => {
      const data = {
        disable: !newData.some(data => data.operation_id === instance.webNodeType && data.status === 'active'),
      };
      return Object.assign(instance, data);
    }));
  },
  setFlowData(state, flowData) {
    state.flowData = flowData;
  },

  setDebugDisplay(state, display) {
    state.debugInfo.display = display;
  },

  setFlowStaus(state, statu) {
    state.flowData.status = statu;
  },
  changeFlowDataTaskName(state, name) {
    if (state.flowData) {
      state.flowData.flow_name = name;
    }
  },
  setGraphData(state, graphData) {
    state.graphData = graphData;
  },

  setDebugStatus(state, status) {
    state.debugInfo.status = status;
  },

  updateRunningInfoList(state, list = []) {
    state.runningInfo.infoList = list;
  },

  changeRunningStatus(state, isRunning) {
    state.runningInfo.isRunning = isRunning;
  },
  setStorageConfig(state, storageConfig) {
    state.storageConfig = storageConfig;
  },
};

export default {
  namespaced: true,
  state,
  getters,
  actions,
  mutations,
};

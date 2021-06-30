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
 * dataflow的api
 */
import { ajax, bkRequest } from '@/common/js/ajax';

export default {
  namespaced: true,
  state: {},
  mutations: {},
  actions: {
    /**
         * 上传导入dataflow
         */
    uploadDataFlow({}, { params, flowId }) {
      const req = ajax.post(`/v3/dataflow/flow/flows/${flowId}/create/`, params);
      return req;
    },
    /**
         * 获取全部dataflow
         */
    getAllDataflows() {
      return ajax.get('flows/');
    },
    /**
         * 获取单个dataflow
         */
    requestDataflowInfo({}, { flowId }) {
      // if (flowId === demo) {
      //     return EditUtil.hookDemoResult(demoData.flowData)
      // }
      return bkRequest.httpRequest('dataFlow/getFlowInfoByFlowId', {
        params: { fid: flowId },
        query: {
          show_display: 1,
          add_exception_info: 1,
          add_process_status_info: 1,
        },
      });
    },
    /**
         * 修改dataflow信息
         */
    updateDataflowInfo({}, { flowId, param }) {
      return ajax.patch(`v3/dataflow/flow/flows/${flowId}/`, param);
    },
    /**
         * 获取告警列表信息
         */
    getListAlert({}, { flowId, alert_config_id }) {
      return bkRequest.httpRequest('dmonitorCenter/getListAlert', {
        params: { flowId },
        query: { alert_config_id },
      });
    },
    /**
         * 获取操作历史
         */
    getListDeployInfo({}, { flowId }) {
      return bkRequest.httpRequest('dataFlow/getDeployHistory', { params: { flowId } });
    },
    /**
         * 更新图表
         */
    updateGraph({}, { flowId, params }) {
      // if (flowId === 'demo') {
      //     return EditUtil.hookDemoResult({})
      // }
      return ajax.post(`flows/${flowId}/update_graph/`, params);
    },
    requestGraphInfo({}, { flowId }) {
      // if (flowId === demo) {
      //     return EditUtil.hookDemoResult(demoData.graphData)
      // }
      // return bkRequest.httpRequest('dataFlow/getFlowGraph', { params: { flowId: flowId } })
      return bkRequest.httpRequest('dataFlow/getGraphInfo', { params: { fid: flowId } });
    },

    /**
         * 获取连线规则配置
         */
    requsetLinkRulesConfig({}) {
      return bkRequest.httpRequest('dataFlow/getLinkRulesConfig');
    },
    /**
         * 获取执行结果
         */
    requestLatestDeployInfo({}, { flowId }) {
      // if (flowId === 'demo') {
      //     return EditUtil.hookDemoResult(demoData.runningInfo)
      // }
      return ajax.get(`/flows/${flowId}/get_latest_deploy_info/`);
    },
    /**
         * 停止dataflow运行
         */
    stopDataflow({}, { flowId }) {
      return ajax.post(`/v3/dataflow/flow/flows/${flowId}/stop/`);
    },
    /**
         * 运行dataflow
         */
    startDataflow({}, { flowId, consumingMode, clusterGroup }) {
      const options = {
        consuming_mode: consumingMode,
        cluster_group: clusterGroup,
      };
      return ajax.post(`/v3/dataflow/flow/flows/${flowId}/start/`, options);
    },
    /**
         * 重启dataflow
         */
    restartDataflow({}, { flowId, consumingMode, clusterGroup }) {
      const options = {
        consuming_mode: consumingMode,
        cluster_group: clusterGroup,
      };
      return ajax.post(`/v3/dataflow/flow/flows/${flowId}/restart/`, options);
    },
    /**
         * 创建节点
         */
    createNode({}, { params }) {
      return ajax.post(`/v3/dataflow/flow/flows/${params.flow_id}/nodes/`, params);
    },
    /**
         * 更新节点
         */
    updateNode({}, { nodeId, params }) {
      return ajax.put(`/v3/dataflow/flow/flows/${params.flow_id}/nodes/${nodeId}/`, params);
    },
    /**
         * 删除节点
         */
    deleteNode({}, { flowId, nodeId }) {
      return ajax.delete(`/v3/dataflow/flow/flows/${flowId}/nodes/${nodeId}/`);
    },
    /**
         * 监控数据
         */
    dataMonitor({}, { flowId }) {
      // return ajax.get(`flows/${flowId}/data_monitor/`)
      return ajax.get(`/v3/dataflow/flow/flows/${flowId}/monitor_data/`);
    },
    /**
         * 节点具体监控数据
         */
    listMonitorData({}, { flowId, nodeId }) {
      const api = `/v3/dataflow/flow/flows/${flowId}/nodes/${nodeId}/monitor_data/`;
      // return ajax.get(`/nodes/${nodeId}/list_monitor_data/`)
      return ajax.get(api);
    },
    /**
         * 启动调试
         */
    startDebug({}, { flowId }) {
      return ajax.post(`/v3/dataflow/flow/flows/${flowId}/debuggers/start/`);
    },
    /**
         * 停止调试
         */
    stopDebug({}, { flowId, debugId }) {
      return ajax.post(`/v3/dataflow/flow/flows/${flowId}/debuggers/${debugId}/stop/`);
    },
    /**
         * 查询调试信息
         */
    queryDebug({}, { flowId, debugId }) {
      return ajax.get(`/v3/dataflow/flow/flows/${flowId}/debuggers/${debugId}/`);
    },
    listFieldFilter({}, { output }) {
      // if (output === demo + '19700101') {
      //     return EditUtil.hookDemoResult(demoData.listFilter)
      // }
      return ajax.get(`result_tables/${output}/list_field/?filter_time=0`);
    },
    checkGuide({}, { params }) {
      return ajax.get(`tools/check_guide/?${params}`);
    },
    endGuide({}) {
      return ajax.post('tools/end_guide/', { guide: 'dataflow_intro' });
    },
    getClusterList({}, projectId) {
      return Promise.all([
        bkRequest.httpRequest('auth/getClusterGroup', { mock: false, params: { id: projectId } }),
        bkRequest.httpRequest('auth/getStorageClusterConfigs', { mock: false }),
        bkRequest.httpRequest('auth/getClusterGroupConfig', { mock: false }),
      ]);
    },
    getStorageClusterExpiresConfigs({}, { type, name }) {
      return bkRequest.httpRequest('meta/getStorageClusterExpiresConfigsByType', {
        params: {
          cluster_name: name,
          cluster_type: type,
        },
      });
    },
    /**
         * 新增TDW表
         */
    addTdwTable({}, { flowId, params }) {
      return ajax.post(`v3/dataflow/flow/flows/${flowId}/create_tdw_source/`, params);
    },
  },
};

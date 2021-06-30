

<!--
  - Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
  - Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
  - BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
  -
  - License for BK-BASE 蓝鲸基础平台:
  - -------------------------------------------------------------------
  -
  - Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
  - documentation files (the "Software"), to deal in the Software without restriction, including without limitation
  - the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
  - and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
  - The above copyright notice and this permission notice shall be included in all copies or substantial
  - portions of the Software.
  -
  - THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
  - LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
  - NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
  - WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
  - SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
  -->

<template>
  <section class="bk-ide-container">
    <header>
      <FlowHeader :project_id="projectId"
        :graphLoading="isGraphContentLoading" />
    </header>
    <section class="bk-ide-layer">
      <nav id="ide_nav_left"
        :class="{ collapsed: isNaviCollapsed }">
        <GraphNavi @handleCollapsed="(current, pre) => (isNaviCollapsed = current)" />
      </nav>
      <section
        v-bkloading="{ isLoading: isGraphLoading }"
        class="bk-ide-content"
        :class="{ 'with-navi-collapsed': isNaviCollapsed }">
        <div
          v-if="isGraphContentLoading"
          class="loading-container bk-loading"
          :style="{ height: `calc(100% - ${consoleHeight}px)` }">
          <div class="bk-loading-wrapper">
            <div class="bk-loading1">
              <div class="point point1" />
              <div class="point point2" />
              <div class="point point3" />
              <div class="point point4" />
            </div>
            <div class="bk-loading-title" />
          </div>
        </div>
        <section class="bk-ide-body">
          <GraphIndex
            ref="bk_graph_index"
            :class="`bk-graph-${activeAction}`"
            @queryForComplement="loopQueryForComplement" />
        </section>
        <section id="ide_console_panel"
          class="bk-ide-console">
          <GraphConsole v-model="consoleHeight"
            :activeSection.sync="activeConsoleTab"
            :isLoading="isConsoleLoading" />
        </section>
      </section>
    </section>
    <!-- 无权限申请 -->
    <PermissionApplyWindow
      :isOpen.sync="isPermissionShow"
      :objectId="$route.query.project_id"
      :showWarning="true"
      :defaultSelectValue="{
        objectClass: 'project',
      }" />
  </section>
</template>
<script>
const GraphIndex = () => import('./Graph/Graph.index.vue');
const GraphNavi = () => import('./Graph/Components/GraphNavi.vue');
const FlowHeader = () => import('./Graph/Components/Header/flowHeader.vue');
const GraphConsole = () => import('./Graph/Graph.console.vue');
const PermissionApplyWindow = () => import('@/pages/authCenter/permissions/PermissionApplyWindow');

import { OPERATION_ACTION } from './Graph/Config/Toolbar.Config.js';
import { CONSOLE_ACTIVE_TYPE } from './Graph/Config/Console.config.js';
import IDEToolbar from './IDE.Toolbar';
import Bus from '@/common/js/bus.js';
import { mapGetters } from 'vuex';
import { postMethodWarning } from '@/common/js/util';

export default {
  name: 'IDE-index',
  components: { GraphIndex, GraphNavi, FlowHeader, GraphConsole, PermissionApplyWindow },
  provide() {
    const self = this;
    return {
      /**
       * 捕获画布工具栏操作
       * @param { String } action：操作
       * @param { Object } params：参数
       */
      handleFlowToolbarOpation(action, params) {
        return self.flowToolbarOptionHandler(action, params);
      },

      /**
       * 更新画布Loading状态
       */
      updateGraphLoading(isLoading = false) {
        self.isGraphLoading = isLoading;
      },

      /**
       * 激活节点状态（自动定位到节点位置）
       * 用于左侧outline点击节点时，节点高亮并定位
       */
      activeNodeById(nodeId) {
        self.handleNodeActive(nodeId);
      },
    };
  },
  data() {
    return {
      isNaviCollapsed: false,
      toolbarAction: OPERATION_ACTION,
      ToolBar: new IDEToolbar(this.$store, this.flowId, this.bkRequest),
      consoleHeight: 40,
      activeConsoleTab: '',
      loading: {
        [OPERATION_ACTION.START_INSTANCE]: false,
        [OPERATION_ACTION.STOP_INSTANCE]: false,
        [OPERATION_ACTION.START_DEBUG]: false,
        [OPERATION_ACTION.STOP_DEBUG]: false,
        [OPERATION_ACTION.START_MONITORING]: false,
        [OPERATION_ACTION.STOP_MONITORING]: false,
      },
      activeAction: '',
      debugQueryTimer: 0,
      deployQueryTimer: 0,
      complementQueryTimer: 0,
      isGraphLoading: false,
      isPermissionShow: false,
    };
  },
  computed: {
    flowId() {
      return this.$route.params.fid;
    },
    projectId() {
      return this.$route.query.project_id;
    },
    isGraphContentLoading() {
      return (
        [
          this.toolbarAction.START_INSTANCE,
          this.toolbarAction.STOP_INSTANCE,
          this.toolbarAction.START_MONITORING,
        ].includes(this.activeAction) && this.loading[this.activeAction]
      );
    },
    isConsoleLoading() {
      return this.loading[this.activeAction];
    },
    ...mapGetters({
      getCanRecalNode: 'ide/getCanRecalNode',
    }),
  },
  watch: {
    flowId: {
      immediate: true,
      handler(val) {
        this.ToolBar.updateFlowid(val);
        this.initDataFlowInfo(val);
        this.resetGraphStatus();
      },
    },
  },

  async created() {
    this.requestStorageConfig();

    let nodeConf = await this.bkRequest.httpRequest('/dataFlow/getNodeTypeConfigs');
    nodeConf = nodeConf || {};
    if (nodeConf.result) {
      this.$store.commit('ide/setFlowNodeconfig', nodeConf.data);
    } else {
      postMethodWarning(nodeConf.message, 'error');
    }
  },
  methods: {
    /**
     * 设置节点高亮闪烁
     */
    handleNodeActive(nodeId) {
      this.$refs['bk_graph_index'].handleNodeActive(nodeId);
    },

    /**
     * 重置画布状态： 调试、监控、控制台
     */
    resetGraphStatus() {
      this.setLoadingAction(this.activeAction, false);
      this.clearActiveAction();
      this.activeConsoleTab = '';
      this.clearAllTimeout();
      this.$store.commit('ide/changeRunningStatus', false);
      this.$store.commit('ide/updateRunningInfoList', []);
      this.$store.commit('ide/setDebugStatus', 'none');
    },

    clearAllTimeout() {
      if (this.deployQueryTimer) {
        clearTimeout(this.deployQueryTimer);
        this.deployQueryTimer = 0;
      }

      if (this.debugQueryTimer) {
        clearTimeout(this.debugQueryTimer);
        this.debugQueryTimer = 0;
      }
    },

    /**
     * 获取流程信息
     */
    initDataFlowInfo(id) {
      if (id) {
        this.$store.dispatch('api/flows/requestDataflowInfo', { flowId: id }).then(resp => {
          if (this.complementQueryTimer) {
            clearTimeout(this.complementQueryTimer);
            this.complementQueryTimer = null;
            this.loopQueryForComplement(true);
          }
          if (resp.result) {
            this.$store.commit('ide/setFlowData', resp.data);
            this.$store.commit(
              'ide/setComplementConfig',
              resp.data.custom_calculate || { status: 'none', config: {}, message: '' }
            ); // 根据接口更新补算状态
            this.$router.replace({
              query: Object.assign({}, { project_id: resp.data.project_id }, this.$route.query),
            });
          } else {
            this.getMethodWarning(resp.message, resp.code);
            this.isPermissionShow = true;
          }
        });
      } else {
        this.$store.commit('ide/setFlowData', {}); // setFlowStaus
      }
    },

    /**
     * 捕获画布工具栏操作
     * @param { String } action：操作
     * @param { Object } params：参数
     */
    flowToolbarOptionHandler(action, params = {}) {
      this.setLoadingAction(action, true);
      this.setActiveAction(action);
      this.$refs['bk_graph_index'].handleModeChange(true);
      const calcParams = Object.assign({}, params, { callback: this.toolBarCallback });
      return this.ToolBar.handleToolbarAction(action, calcParams)['catch'](_ => {
        this.$refs['bk_graph_index'].triggleGraphMode();
        this.setLoadingAction(action);
        this.clearActiveAction();
        return Promise.reject(_);
      });
    },

    /** 接口回调 */
    toolBarCallback(action, response) {
      switch (action) {
        case OPERATION_ACTION.START_DEBUG:
          this.activeConsoleTab = CONSOLE_ACTIVE_TYPE.RUNNING;
          this.$refs['bk_graph_index'].backupGraphData();
          this.loopQueryDebug();
          break;
        case OPERATION_ACTION.STOP_DEBUG:
          this.debugQueryTimer && clearTimeout(this.debugQueryTimer);
          this.clearActiveAction();
          this.$refs['bk_graph_index'].restoreGraphdata();
          this.$refs['bk_graph_index'].handleModeChange(false);
          break;
        case OPERATION_ACTION.MAGIC_POSITION:
          this.$refs['bk_graph_index'].magicNodePosition();
          // this.$refs['bk_graph_index'].handleModeChange(false)
          this.clearActiveAction();
          break;
        case OPERATION_ACTION.START_INSTANCE:
        case OPERATION_ACTION.STOP_INSTANCE:
        case OPERATION_ACTION.RESTART_INSTANCE:
          this.activeConsoleTab = CONSOLE_ACTIVE_TYPE.RUNNING;
          this.loopQueryFlowInfo(action);
          break;
        case OPERATION_ACTION.START_COMPLEMENT:
          this.activeConsoleTab = CONSOLE_ACTIVE_TYPE.RUNNING;
          this.$refs['bk_graph_index'].handleModeChange(false);
          this.updateComplementHtmlStaus(response);
          this.loopQueryForComplement(false);
          break;
        case OPERATION_ACTION.STOP_COMPLEMENT:
        case OPERATION_ACTION.APPLY_COMPLEMENT:
        case OPERATION_ACTION.CANCEL_COMPLEMENT:
        case OPERATION_ACTION.UPDATE_COMPLEMENT_ICON:
          this.$refs['bk_graph_index'].handleModeChange(false);
          this.updateComplementHtmlStaus(response);
          break;
        case OPERATION_ACTION.START_MONITORING:
          this.$refs['bk_graph_index'].changeAction('monitor');
          this.getMonitorData(response);
          break;
        case OPERATION_ACTION.STOP_MONITORING:
          this.$refs['bk_graph_index'].changeAction('normal');
          this.stopMonitor();
          break;
      }
    },
    /**
     * 补算相关操作成功后，画布节点状态更新
     */
    updateComplementHtmlStaus(response) {
      const updateNodes = Object.keys(response)
        .filter(key => key !== 'callback')
        .map(key => ({
          id: response[key].id,
          ...response[key].content,
        }));
      this.$refs['bk_graph_index'].updateNodeHtmlById(updateNodes, {
        custom_calculate_status: 'custom_calculate_status',
      });
    },

    /** 清空所有节点的补算状态 */
    clearAllComplementStatus() {
      const updateNodes = [];
      this.getCanRecalNode.forEach(node => {
        const nodeId = node.id;
        const status = { custom_calculate_status: 'none' };
        updateNodes.push({
          id: nodeId,
          content: status,
        });
      });
      this.updateComplementHtmlStaus(updateNodes);
    },

    /** 开始补算启动 轮询 */
    loopQueryForComplement(isPolling = false) {
      this.ToolBar.queryDeployInfo().then(info => {
        // 如果是轮询，更新补算进度； 第一次查询不更新
        if (parseInt(info.flow_id) !== parseInt(this.flowId)) return;
        isPolling && Bus.$emit('updateRecalProgress', info.progress);
        this.complementQueryTimer = setTimeout(() => {
          this.$store.commit('ide/setFlowStaus', info.flow_status);
          this.$store.commit('ide/updateRunningInfoList', info.logs);
          if (!info.is_done) {
            this.complementQueryTimer && this.loopQueryForComplement(true);
          } else {
            if (isPolling) {
              this.resetGraphStatus();
              this.clearAllComplementStatus(); // 清空，还原节点的补算状态
              this.$store.commit('ide/setComplementStatus', 'none');
              Bus.$emit('updateRecalProgress', 0); // 补算进度清零
            }
          }
        }, 3000);
      });
    },

    /** 轮训流程启动|停止讯息 */
    loopQueryFlowInfo(action) {
      this.ToolBar.queryDeployInfo().then(info => {
        this.$refs['bk_graph_index'].updateNodeHtml(info.nodes_status, { status: 'status' });
        this.deployQueryTimer = setTimeout(() => {
          this.$store.commit('ide/setFlowStaus', info.flow_status);
          this.$store.commit('ide/updateRunningInfoList', info.logs);
          if (!info.is_done) {
            this.deployQueryTimer && this.loopQueryFlowInfo(action);
          } else {
            if (info.status === 'failure') {
              this.getMethodWarning('执行失败，详细信息请查看执行历史', info.status);
            }
            this.$refs['bk_graph_index'].handleModeChange(false);
            this.setLoadingAction(action);
            this.clearActiveAction();
            this.$store.commit('ide/changeRunningStatus', false);
            Bus.$emit('updateALertCountGraph'); // 重新渲染画布，保证状态稳定
          }
        }, 500);
      });
    },

    /** 查询调试状态 */
    loopQueryDebug() {
      this.ToolBar.queryDebugInfo().then(info => {
        this.$refs['bk_graph_index'].updateNodeHtml(info.nodes_info, { debugInfo: 'all' });
        this.debugQueryTimer = setTimeout(() => {
          if (!info.is_done) {
            this.debugQueryTimer && this.loopQueryDebug();
          } else {
            this.$refs['bk_graph_index'].handleModeChange(false);
            this.$refs['bk_graph_index'].restoreGraphdata();
            this.clearActiveAction();
            this.$store.commit('ide/setDebugStatus', 'none');
          }
          this.$store.commit('ide/setDebugDisplay', info.status_display);
          this.$store.commit('ide/updateRunningInfoList', info.logs);
        }, 5000);
      });
    },

    /** 设置当前Loading状态 */
    setLoadingAction(action, isLoading = false) {
      if (this.loading[action] !== undefined) {
        this.$set(this.loading, action, isLoading);
      }
    },

    clearActiveAction() {
      this.activeAction = '';
    },

    setActiveAction(action) {
      this.activeAction = action;
    },

    getMonitorData(data) {
      const monitorList = [];
      data
        && Object.keys(data).forEach(item => {
          data[item]
            && Object.keys(data[item]).forEach(key => {
              const _item = data[item][key];
              monitorList.push({
                nodeId: key,
                source: {
                  group: item,
                  item: _item,
                },
                formatData: {
                  output_data_count: `${_item.output
                    ? _item.output : _item.output === null
                      ? '-' : 0}${_item.unit}`,
                  input_data_count: `${_item.input
                    ? _item.input : _item.input === null
                      ? '-' : 0}${_item.unit}`,
                  start_time: _item.start_time ? _item.start_time : '-',
                  interval: _item.interval ? _item.interval / 3600 + this.$t('小时') : 'null',
                },
              });
            });
        });

      this.$refs['bk_graph_index'].handleModeChange(true);
      this.$refs['bk_graph_index'].updateMonitor(monitorList);
      this.setLoadingAction(this.toolbarAction.START_MONITORING);
    },

    stopMonitor() {
      this.$refs['bk_graph_index'].removeMonitor();
      this.setLoadingAction(this.toolbarAction.STOP_MONITORING);
    },

    requestStorageConfig() {
      this.bkRequest.httpRequest('dataStorage/getStorageCommon').then(resp => {
        if (resp.result) {
          this.$store.commit('ide/setStorageConfig', resp.data.storage_query);
        }
      });
    },
  },
};
</script>
<style lang="scss" scoped>
$naviWidth: 335px; // 左侧导航宽度
$naviMinWidth: 35px; // 导航收起时宽度
$offsetTop: 60px; // 距离页面顶部高度
$headerHeight: 50px; // 画布顶部高度
$consoleMinHeight: 42px; // 画布控制台输出最小高度

.bk-ide-container {
  // display: flex;
  // flex-direction: column;
  height: 100%;

  header {
    height: 50px;
    line-height: 50px;
    background-color: #efefef;
  }

  .bk-ide-layer {
    display: flex;
    flex-direction: row;
    height: calc(100% - #{$headerHeight});

    nav {
      width: $naviWidth;
      display: flex;
      border-right: 1px solid #ddd;
      border-top: 1px solid #ddd;
      &.collapsed {
        width: $naviMinWidth;
      }
    }

    .bk-ide-content {
      width: calc(100% - #{$naviWidth});
      position: relative;
      &.with-navi-collapsed {
        width: calc(100% - #{$naviMinWidth});
      }

      .bk-ide-body {
        height: 100%;
        .flow-graph-container {
          height: 100%;
        }
      }
      .bk-ide-console {
        min-height: $consoleMinHeight;
        background-color: #fafbfd;
        position: absolute;
        bottom: 0;
        left: 0;
        right: 0;
        z-index: 1;
      }

      .loading-container {
        background: rgba(0, 0, 0, 0.1);
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        bottom: auto;
      }
    }
  }
}
</style>



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
  <div class="flow-header-right-content">
    <ul class="operat-list">
      <flow-tool-item
        v-if="$modules.isActive('data_log')"
        :isDisabled="!flowId"
        :iconContent="'icon-data-query'"
        :content="$t('任务日志')"
        @clickItem="openDatalog" />
      <flow-tool-item
        :isDisabled="!flowId"
        :iconContent="'icon-magic-layout'"
        :content="$t('自动排版')"
        @clickItem="autoPosition" />
      <flow-tool-item
        :isDisabled="isDebugDisabled"
        :iconContent="'icon-monitors'"
        :title="complementStatus.status === 'running' ? $t('补算过程中不支持监控') : ''"
        :content="
          complementStatus.status === 'running'
            ? $t('补算过程中不支持监控')
            : isPointDebug
              ? $t('退出数据流监控')
              : $t('数据流状态')
        "
        @clickItem="doPointDebug" />
      <flow-tool-item
        :isDisabled="!flowId || !(!debugStatus || isNormalDebug) || isRunning"
        :iconContent="beingDebug ? 'icon-exit' : 'icon-debug'"
        :content="isNormalDebug ? $t('退出调试') : $t('启动调试')"
        :extraClass="isNormalDebug ? 'exit-debug' : ''"
        @clickItem="doDebug" />
      <flow-tool-item
        :isDisabled="!flowId"
        :iconContent="'icon-upload-data'"
        :content="$t('导出dataflow')"
        @clickItem="exportAsJson" />
      <flow-tool-item
        :isDisabled="!flowId"
        :iconContent="'icon-download-data'"
        :content="$t('导入dataflow')"
        @clickItem="openUploadDialog" />
    </ul>
    <ul class="operat-list">
      <flow-tool-item
        :isDisabled="recalStoping || !isFlowStarted || debugStatus"
        :iconContent="recalIcon"
        :itemID="'recal-button'"
        :hideTooltip="recalHidetip"
        :content="recalTooltip"
        @clickItem="dataRecalc" />
      <flow-tool-item
        :isDisabled="!flowId"
        :iconContent="'icon-alert'"
        :content="$t('告警配置')"
        @clickItem="isShowAlertConfig = true" />
      <flow-tool-item
        :isDisabled="!flowId || isFlowStarted || isRunning || debugStatus"
        :iconContent="'icon-play-shape'"
        :content="$t('启动')"
        @clickItem="startPrompt" />
      <flow-tool-item
        :isDisabled="!isFlowStarted || isRunning || debugStatus"
        :iconContent="'icon-stop-shape'"
        :content="$t('停止')"
        @clickItem="stopPrompt" />
      <flow-tool-item
        :isDisabled="!flowId || !isFlowStarted || isRunning || debugStatus"
        :iconContent="'icon-restart'"
        :content="$t('重启')"
        @clickItem="restartPrompt" />
    </ul>
    <ul class="operat-list">
      <flow-tool-item
        :isDisabled="!flowId || showFlowDetail"
        :iconContent="'icon-check-detail'"
        :content="$t('任务详情')"
        @clickItem="
          () => {
            showFlowDetail = true;
          }
        " />
      <!-- <flow-tool-item :is-disabled="false"
                @clickItem="showGuider"
                :icon-content="'icon-question-circle'"
                :content="$t('新手帮助')">
            </flow-tool-item> -->
      <!-- <li class="hidden">
                <div class="icon"><i title=''
                        class='bk-icon icon-ellipsis'></i></div>
                <div class="more">
                    <span class='arrow'></span>
                    <ul>
                        <li><i class="bk-icon icon-cog"></i></li>
                        <li><i class="bk-icon icon-calendar"></i></li>
                        <li><i class="bk-icon icon-unlock"></i></li>
                    </ul>
                </div>
            </li> -->
    </ul>
    <bkdata-dialog
      v-model="isShow"
      :extCls="customDisabledConfirmClass"
      :autoClose="false"
      :hasHeader="false"
      :okText="$t('启动')"
      :cancelText="$t('关闭')"
      :closeIcon="true"
      :maskClose="true"
      :height="'210px'"
      :width="654"
      @confirm="operatTask"
      @value-change="dataManageMode = 'continue'">
      <div class="deploy-task">
        <div class="hearder">
          <div class="text fl">
            <p class="title">
              <span class="icon-startup-config" />{{ $t('启动配置') }}
            </p>
            <p>{{ $t('dataflow启动配置_包括dataflow运行的资源组和数据处理模式') }}</p>
          </div>
        </div>
        <div class="text-content">
          <p>{{ $t('重启说明') }}</p>
          <ul>
            <li>{{ $t('仅重启未启动节点_已修改_上次启动异常的节点及其下游所有节点') }}</li>
            <li>
              {{ $t('若重启节点中包含一个实时计算节点_则其相关联的实时计算节点也会被重启所有实时节点在一个任务中') }}
            </li>
            <li>{{ $t('若重启节点中包含存储节点_则其上游节点也会重启') }}</li>
          </ul>
        </div>
        <div class="search-item expire-item">
          <label class="search-label fl">{{ $t('计算集群组') }}</label>
          <div class="search-content">
            <div class="select-wrap">
              <bkdata-selector
                :list="computClusterList"
                :isLoading="isTaskDeployLoading"
                :customStyle="{ width: '315px' }"
                :selected.sync="computClusterSelected"
                :settingKey="'cluster_group_id'"
                :displayKey="'cluster_group_alias'"
                :searchable="true"
                :searchKey="'cluster_group_alias'" />
            </div>
          </div>
        </div>
        <div class="search-item expire-item">
          <label class="search-label">{{ $t('数据处理模式') }}</label>
          <div class="search-content">
            <bkdata-radio-group v-model="dataManageMode">
              <bkdata-radio :value="'continue'">
                {{ $t('继续') }}
              </bkdata-radio>
              <bkdata-radio :value="'from_tail'">
                {{ $t('尾部') }}
              </bkdata-radio>
              <bkdata-radio :value="'from_head'">
                {{ $t('头部') }}
              </bkdata-radio>
            </bkdata-radio-group>
          </div>
        </div>
        <div class="search-item expire-item">
          <p v-if="dataManageMode === 'continue'"
            class="description">
            <span class="icon-info" />{{ $t('任务启动后从上次停止处理位置继续处理') }}
          </p>
          <p v-else-if="dataManageMode === 'from_tail'"
            class="description">
            <span class="icon-info" />{{ $t('任务启动后从最新到达数据开始处理') }}
          </p>
          <p v-else
            class="wearning">
            <span class="icon-info" />{{ $t('任务启动后从平台缓存最久数据开始处理') }}
          </p>
        </div>
      </div>
    </bkdata-dialog>
    <dataflow-upload ref="uploadGraph" />
    <data-recalc-dialog
      ref="recalcdialog"
      @submitSuccess="handleRecalSubmitSuccess"
      @cancelApply="handleCancelApply"
      @applySubmitSuccess="handleRecalApplySubmit" />
    <custom-progess ref="tipMenu"
      :percent="progressNum" />
    <AlertConfig :isShowPopUp="isShowAlertConfig"
      @close="isShowAlertConfig = false" />
    <Datalog ref="datalog"
      :flowId="flowId" />
  </div>
</template>

<script>
import Bus from '@/common/js/bus';
import Vue from 'vue';
import { ajax, bkRequest } from '@/common/js/ajax';
import { mapState, mapGetters } from 'vuex';
import { DEBUG_MODE, GUIDE_STEP } from '@/pages/DataGraph/Common/constant';
import { postMethodWarning, confirmMsg, showMsg } from '@/common/js/util';
import EditUtil from '@/pages/DataGraph/Common/utils';
import FlowToolItem from './flowToolItem';
import DataflowUpload from '@/components/dataflow/dataflowUpload';
import dataRecalcDialog from './dataRecalc';
import customProgess from './customProgress';
import Datalog from '@/pages/DataGraph/Graph/Components/Header/flowToolbar/components/DataLog/Index.vue';
import { OPERATION_ACTION } from '../../../Config/Toolbar.Config.js';
import tippy from 'tippy.js';

export default {
  name: 'flow-toolbar',
  components: {
    FlowToolItem,
    DataflowUpload,
    dataRecalcDialog,
    customProgess,
    Datalog,
    AlertConfig: () => import('@/pages/DataGraph/Graph/Components/Header/flowToolbar/components/AlertConfig.vue'),
  },
  inject: ['handleFlowToolbarOpation'],
  props: {
    showDetail: {
      type: Boolean,
      default: false,
    },
    graphLoading: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      recalHidetip: false,
      progressNum: 5,
      recalTooltipMap: {
        none: this.$t('补算'),
        applying: this.$t('补算审批中'),
        ready: this.$t('补算已审批_待执行'),
        running: this.$t('停止补算'),
      },
      recalStoping: false,
      recalIconMap: {
        none: 'icon-recal-recalable',
        applying: 'icon-recal-pending',
        ready: 'icon-recal-execution',
        running: 'icon-recal-stop',
      },
      stateBackup: null,
      isShow: false,
      dataManageMode: 'continue',
      computClusterList: [],
      computClusterSelected: '',
      isTaskDeployLoading: false,
      isShowAlertConfig: false,
    };
  },
  computed: {
    customDisabledConfirmClass() {
      return this.isTaskDeployLoading ? 'bkdata-dialog flow-tool-confirm-custom' : 'bkdata-dialog';
    },
    recalTooltip() {
      return this.recalTooltipMap[this.complementStatus.status];
    },
    recalIcon() {
      return this.recalIconMap[this.complementStatus.status];
    },
    isDebugDisabled() {
      return (
        !this.flowId
        || !(!this.debugStatus || this.isPointDebug)
        || this.complementStatus.status === 'running'
        || this.graphLoading === true
      );
    },
    showFlowDetail: {
      get() {
        return this.showDetail;
      },
      set(newVal) {
        this.$emit('update:showDetail', newVal);
      },
    },
    ...mapState({
      isRunning: state => state.ide.runningInfo.isRunning,
      debugInfo: state => state.ide.debugInfo,
      graphData: state => state.ide.graphData,
      complementStatus: state => state.ide.complementStatus,
    }),
    ...mapGetters({
      beingDebug: 'ide/isBeingDebug',
      isPointDebug: 'ide/isPointDebug',
      isNormalDebug: 'ide/isNormalDebug',
      getCanRecalNode: 'ide/getCanRecalNode',
      flowStatus: 'ide/flowStatus',
    }),
    debugMacro() {
      return DEBUG_MODE;
    },
    flowId() {
      return this.$route.params.fid;
    },
    isFlowStarted() {
      return this.flowId && this.flowStatus !== 'no-start' && this.flowStatus !== '';
    },

    debugStatus() {
      return this.debugInfo.status === 'point' || this.debugInfo.status === 'normal';
    },
  },
  watch: {
    'complementStatus.status': {
      immediate: true,
      handler(val) {
        if (val === 'running') {
          console.log(this.recaltip, val);
          this.showProgress();
        } else {
          this.recaltip && this.hideProgress();
        }
      },
    },
  },

  mounted() {
    Bus.$on('updateRecalProgress', value => {
      if (value > 5) {
        this.progressNum = parseInt(value);
      } else {
        this.progressNum = 5;
      }
    });
    this.createTippy();
  },
  beforeDestroy() {
    this.recaltip && this.hideProgress();
    this.$store.commit('ide/setComplementStatus', 'none'); // 清楚补算状态
  },
  methods: {
    showProgress() {
      if (!this.recaltip) this.createTippy();
      this.$nextTick(() => {
        this.recaltip[0].show();
        this.recalHidetip = true;
      });
    },
    createTippy() {
      const progess = document.getElementById('customProgressbar');
      this.recaltip = tippy('#recal-button', {
        theme: 'light',
        trigger: 'manual',
        content: progess,
        interactive: true,
        hideOnClick: false,
        arrow: true,
        placement: 'bottom',
        distance: 0,
        zIndex: 2000,
      });
    },
    hideProgress() {
      this.recaltip[0] && this.recaltip[0].hide();
      this.recalHidetip = false;
    },
    openDatalog() {
      this.$refs.datalog.openDatalog();
    },
    exportAsJson() {
      let params = null;
      bkRequest.httpRequest('dataFlow/exportDataFlow', { params: { fid: this.flowId } }).then(res => {
        if (res.result) {
          params = res.data;
          download({ nodes: params.nodes }, `bk_data_${params.flow_name}.json`, 'text/json');
        } else {
          return postMethodWarning(res.message, 'error');
        }
      });

      function download(content, fileName, contentType) {
        if (typeof content === 'object') {
          content = JSON.stringify(content);
        }
        var a = document.createElement('a');
        var file = new Blob([content], { type: contentType });
        a.href = URL.createObjectURL(file);
        a.download = fileName;
        a.click();
      }
    },
    openUploadDialog() {
      this.$refs.uploadGraph.dialogConfig.isShow = true;
    },
    // 停止时清除补算节点状态
    getCompelementNodes() {
      let updateNodes = [];
      this.getCanRecalNode.forEach(node => {
        let nodeId = node.id;
        let status = { custom_calculate_status: 'none' };
        updateNodes.push({ id: nodeId, content: status });
      });
      return updateNodes;
    },
    // 获取计算集群组
    getComputClusterList() {
      this.isTaskDeployLoading = true;
      const options = {
        params: {
          fid: this.$route.params.fid,
        },
      };
      this.bkRequest.httpRequest('dataFlow/getComputClusterList', options).then(res => {
        if (res.result) {
          this.computClusterList = res.data;
          this.computClusterSelected = res.data[0].cluster_group_id;
        } else {
          postMethodWarning(res.message, 'error');
        }
        this.isTaskDeployLoading = false;
      });
    },
    // 操作重启、开始任务
    operatTask() {
      if (this.isTaskDeployLoading) return;
      if (this.flowStatus === 'no-start') {
        this.doStart();
      } else if (this.flowStatus === 'running') {
        this.doRestart();
      }
      this.isShow = false;
    },

    /** 补算启动成功 */
    handleRecalSubmitSuccess(data) {
      this.handleFlowToolbarOpation(OPERATION_ACTION.START_COMPLEMENT, data);
    },
    handleRecalApplySubmit(data) {
      this.handleFlowToolbarOpation(OPERATION_ACTION.APPLY_COMPLEMENT, data);
    },
    handleCancelApply(data) {
      this.handleFlowToolbarOpation(OPERATION_ACTION.CANCEL_COMPLEMENT, data);
    },
    doStopRecalTask() {
      this.recalStoping = true;
      this.handleFlowToolbarOpation(OPERATION_ACTION.STOP_COMPLEMENT, this.getCompelementNodes())
        .then(rep => {
          this.$store.commit('ide/setComplementStatus', 'none');
          this.clearAllStatus();
        })
        ['finally'](() => {
          this.recalStoping = false;
        });
    },
    clearAllStatus() {
      const updateNodes = [];
      this.getCanRecalNode.forEach(node => {
        const nodeId = node.id;
        const status = { custom_calculate_status: 'none' };
        updateNodes.push({
          id: nodeId,
          content: status,
        });
        // 清空画布补算状态
        this.handleFlowToolbarOpation(OPERATION_ACTION.UPDATE_COMPLEMENT_ICON, updateNodes);
      });
    },
    dataRecalc() {
      if (this.complementStatus.status === 'running') {
        let self = this;
        confirmMsg(self.$t('确认停止补算任务'), '', self.doStopRecalTask);
      } else {
        this.$refs.recalcdialog.open();
      }
    },
    autoPosition() {
      this.handleFlowToolbarOpation(OPERATION_ACTION.MAGIC_POSITION);
    },
    startPrompt() {
      this.$route.params.fid && this.getComputClusterList();
      this.isShow = true;
    },
    stopPrompt() {
      let self = this;
      confirmMsg(self.$t('确认停止该流程'), '', self.doStop);
    },
    restartPrompt() {
      this.$route.params.fid && this.getComputClusterList();
      this.isShow = true;
    },

    /**
     * 启动流程
     */
    doStart() {
      this.$store.commit('ide/changeRunningStatus', true);
      this.handleFlowToolbarOpation(OPERATION_ACTION.START_INSTANCE, {
        flowId: this.$route.params.fid,
        consuming_mode: this.dataManageMode,
        cluster_group: this.computClusterSelected,
      })
        .then(res => !res.result && this.$store.commit('ide/changeRunningStatus', false))
        ['catch'](_ => {
          this.$store.commit('ide/changeRunningStatus', false);
        });
      this.dataManageMode = 'continue';
    },
    /**
     * 停止流程
     */
    doStop() {
      this.$store.commit('ide/changeRunningStatus', true);
      this.handleFlowToolbarOpation(OPERATION_ACTION.STOP_INSTANCE, {})
        .then(res => !res.result && this.$store.commit('ide/changeRunningStatus', false))
        ['catch'](_ => {
          this.$store.commit('ide/changeRunningStatus', false);
        });
    },
    /**
     * 重启流程
     */
    doRestart() {
      this.$store.commit('ide/changeRunningStatus', true);
      this.handleFlowToolbarOpation(OPERATION_ACTION.RESTART_INSTANCE, {
        flowId: this.$route.params.fid,
        consuming_mode: this.dataManageMode,
        cluster_group: this.computClusterSelected,
      })
        .then(res => !res.result && this.$store.commit('ide/changeRunningStatus', false))
        ['catch'](_ => {
          this.$store.commit('ide/changeRunningStatus', false);
        });
    },
    /**
     * 启动/停止调试
     */
    async doDebug() {
      if (this.isPointDebug || this.isRunning) return;
      !this.isNormalDebug && this.$store.commit('ide/setDebugDisplay', this.$t('调试中'));
      this.handleFlowToolbarOpation(this.isNormalDebug ? OPERATION_ACTION.STOP_DEBUG : OPERATION_ACTION.START_DEBUG)
        .then(res => this.$store.commit('ide/setDebugStatus', this.isNormalDebug
          ? this.debugMacro.NONE
          : this.debugMacro.NORMAL)
        )
        ['catch'](_ => this.$store.commit('ide/setDebugStatus', this.debugMacro.NONE));
    },
    /**
     * 启动/停止监控
     */
    async doPointDebug() {
      if (this.isNormalDebug) return;
      this.handleFlowToolbarOpation(
        this.isPointDebug ? OPERATION_ACTION.STOP_MONITORING : OPERATION_ACTION.START_MONITORING
      );
      this.$store.commit('ide/setDebugStatus', this.isPointDebug ? this.debugMacro.NONE : this.debugMacro.POINT);
    },
  },
};
</script>

<style lang="scss">
.flow-tool-confirm-custom {
  .footer-wrapper {
    button[name='confirm'] {
      background-color: #dcdee5;
      border-color: #dcdee5;
      color: #fff;
      cursor: not-allowed;
    }
  }
}
.tippy-popper {
  top: 10px;
  left: 190px;
}
.flow-header-right-content {
  display: flex;
  .bk-tooltip {
    position: relative;
    right: 0;
    top: -55px;
  }
  .unit {
    float: left;
    position: absolute;
    border-radius: 2px;
    left: -150px;
    p {
      color: #737987;
      span {
        font-weight: bold;
      }
    }
  }
.operat-list {
  float: left;
  border-right: 1px solid #ddd;
  &:last-of-type {
    border: none;
  }
  > li {
    outline: none;
    position: relative;
    float: left;
    width: 54px;
    text-align: center;
    position: relative;
    cursor: pointer;
    > .icon {
        height: 100%;
        > i {
            line-height: 50px;
        }
    }
    .bk-icon {
      font-size: 18px;
      vertical-align: -2px;
    }
    .icon-play-shape {
      font-size: 16px;
    }
    .icons {
      font-size: 18px;
      vertical-align: -2px;
    }
    .op-btn,
    .more {
      display: none;
    }
    .exit-debug {
      display: block;
    }
    &.op-disabled {
      cursor: not-allowed;
      color: #dedede;
      &:before {
        content: '';
        position: absolute;
        top: 0;
        right: 0;
        bottom: 1px;
        left: 0;
      }
      i {
        color: #dedede;
      }
    }
    &:not(.op-disabled):not(.op-hidetip):hover {
      .icon {
        background-color: #e1ecff;
      }
      .op-btn {
        display: block;
      }
      .more {
        display: block;
        .arrow {
          top: 50px;
          border-color: transparent transparent #ddd transparent;
        }
        li:first-of-type {
          position: relative;
          &:after {
            top: -9px;
            left: 21px;
            position: absolute;
            content: '';
            border: 5px solid;
            border-color: transparent transparent #fafafa transparent;
            z-index: 200;
          }
          &:hover {
            &:after {
              border-color: transparent transparent #e1ecff transparent;
            }
          }
        }
        li:hover {
          background-color: #e1ecff;
        }
      }
    }
    .arrow {
      z-index: 200;
      float: left;
      top: 38px;
      left: 22px;
      position: absolute;
      border: 5px solid #212232;
      border-left-color: transparent;
      border-top-color: transparent;
      border-right-color: transparent;
    }
    .more {
      float: left;
      width: 100%;
      background: #fafafa;
      border: 1px solid #ddd;
      margin: 10px 0 0 0;
    }
    button {
      padding-left: 0px;
      padding-right: 0px;
      text-align: center;
      /*width: 85px;*/
      height: 32px;
      line-height: 1;
      position: absolute;
      top: 48px;
      left: 50%;
      border: none;
      background-color: #212232 !important;
      z-index: 250;
      cursor: default;
      padding: 0 10px;
      transform: translate(-50%, 0);
    }
  }
}
}
.deploy-task {
  .hearder {
    height: 70px;
    background: #23243b;
    position: relative;
    .close {
      display: inline-block;
      position: absolute;
      right: 0;
      top: 0;
      width: 40px;
      height: 40px;
      line-height: 40px;
      text-align: center;
      cursor: pointer;
    }
    .icon {
      font-size: 32px;
      color: #abacb5;
      line-height: 60px;
      width: 142px;
      text-align: right;
      margin-right: 16px;
    }
    .text {
      margin-left: 28px;
      font-size: 12px;
    }
    .title {
      color: #fafafa;
      position: relative;
      margin: 16px 0 3px;
      padding-left: 36px;
      font-size: 18px;
      text-align: left;
      span {
        position: absolute;
        left: 10px;
        top: 2px;
      }
    }
  }
  .text-content {
    color: #63656e;
    font-size: 14px;
    line-height: 32px;
    background: rgba(250, 251, 253, 1);
    position: relative;
    margin-bottom: 30px;
    padding-left: 54px;
    padding-bottom: 15px;
    overflow: hidden;
    p {
      height: 16px;
      font-size: 12px;
      font-family: MicrosoftYaHei;
      color: rgba(74, 74, 74, 1);
      line-height: 16px;
      margin: 17px 0 8px 0;
    }
    ul {
      font-size: 12px;
      padding-right: 25px;
      font-family: MicrosoftYaHei;
      color: rgba(150, 155, 166, 1);
      line-height: 16px;
    }
    li {
      padding-left: 6px;
      position: relative;
      margin-bottom: 8px;
      &::before {
        content: '';
        font-family: MicrosoftYaHei;
        display: block;
        width: 1px;
        height: 1px;
        background: rgba(150, 155, 166, 1);
        position: absolute;
        left: 0;
        top: 7px;
        border-radius: 50%;
      }
    }
  }
  .search-item {
    display: flex;
    line-height: 32px;
    color: #63656e;
    font-size: 14px;
    padding: 0 26px;
    margin-bottom: 10px;
    display: flex;
    align-items: center;
    p {
      span {
        position: absolute;
        top: 50%;
        transform: translateY(-50%);
        left: 11px;
        font-size: 18px;
      }
      position: relative;
      background: rgba(250, 251, 253, 1);
      border-radius: 2px;
      margin-left: 115px;
      margin-bottom: 10px;
      line-height: 16px;
      color: rgba(4, 132, 255, 1);
      padding: 15px 19px 15px 36px;
      font-size: 12px;
      font-family: MicrosoftYaHei;
      display: flex;
      align-items: center;
    }
    .wearning {
      color: #e19f00;
    }
    .search-label {
      height: 19px;
      font-size: 14px;
      font-family: MicrosoftYaHei;
      color: rgba(49, 50, 57, 1);
      line-height: 19px;
      margin-right: 20px;
      width: 98px;
      text-align: right;
    }
    .search-content {
      width: 315px;
      .select-wrap {
        width: 100%;
      }
    }
  }
}
</style>

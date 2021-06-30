

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
  <div class="center-main">
    <transition name="fade"
      enterActiveClass="right-show"
      leaveActiveClass="right-hidden">
      <div :class="['center-main-left', centerExtendCls]">
        <div :id="'center_main_panel' + task.query_id"
          class="center-main-sql">
          <div class="bk-monaco-editor"
            :style="sqlEditorWrapperStyle">
            <div class="bk-monaco-tools data-explore-monaco">
              <div class="header-tool">
                <span class="name">{{ $t('编辑器') }}</span>
                <span class="search-result-tips">
                  <i class="bkdata-icon icon-info" />
                  {{ $t('探索查询tips') }}
                </span>
                <div class="header-tool-right">
                  <div class="control-panel">
                    <bkdata-checkbox v-model="onTypeFormat"
                      class="history-btn">
                      格式化
                    </bkdata-checkbox>
                    <span
                      v-bk-tooltips="fullScreenText"
                      :class="[fullScreenIcon, 'ml20', 'icon']"
                      @click="editorFullScreenClick" />
                  </div>
                </div>
              </div>
            </div>
            <div :id="'sql_editor' + task.query_id"
              :style="sqlEditorStyle" />
            <div class="bk-monaco-foot">
              <bkdata-button
                v-monitor="{ page: 'DataExplore', event: 'click', name: '运行按钮' }"
                :icon="isRun ? 'loading' : 'run-small'"
                :disabled="isRun || !sqlData"
                theme="primary"
                class="run-btn"
                size="small"
                @click.stop="handleExecuteSql">
                {{ runningText }}
              </bkdata-button>
              <template v-if="isRun">
                <div class="stop-button"
                  @click.stop="handleStopRunning">
                  <i class="icon-stop-shape" />
                  <span class="stop-text">停止</span>
                </div>
              </template>
              <div
                v-show="queryStageMsg && runningStatus !== 'running'"
                id="sql_running_msg"
                ref="tooltipsHtml"
                :class="[runningStatus]">
                <i :class="[runningMesPanelClass, 'mr5']" />
                <span class="text-overflow">
                  {{ runningMesContent }}
                </span>
              </div>
            </div>
          </div>
          <div id="query_result_table"
            class="center-main-table"
            :style="queryResultTableStyle">
            <span :class="['drop-button', { 'drop-active': isDrop }]"
              @mousedown="mousedownHandle" />
            <!-- 查询结果 -->
            <div class="search-result">
              <div :id="'resultHeader' + task.query_id"
                class="search-result-head">
                <div class="search-result-progress">
                  <span class="label-title">{{ $t('查询进度') }}：</span>
                  <div class="result-stage-items">
                    <div v-for="(item, index) in stages"
                      :key="index"
                      :class="['result-stage-item', item.stage_status]">
                      <i
                        v-if="getStageItemIcon(item.stage_status)"
                        :class="['stage-item-icon',
                                 'bk-icon',
                                 item.stage_status,
                                 getStageItemIcon(item.stage_status)]" />
                      <img v-else
                        class="stage-item-loading"
                        src="../imgs/data-loading-small.png">
                      <span class="stage-item-text">
                        {{ item.description }}
                        <span v-if="item.total !== null"
                          style="color: #979ba5">
                          {{ item.total }}ms
                        </span>
                      </span>
                      <span class="stage-item-line" />
                    </div>
                  </div>
                </div>
                {{ $t('查询结果') }}：
                <span class="search-result-head-total">
                  {{
                    $t('查询耗时显示', { count: tableData.totalRecords || 0, time: total })
                  }}
                </span>
              </div>
              <bkdata-tab
                class="res-tab-cls"
                type="unborder-card"
                :style="{ '--headHeight': resultHeadHeight + 'px' }"
                :active.sync="resTab"
                @tab-change="handleTabChange">
                <bkdata-tab-panel
                  v-bkloading="{ isLoading: tableLoading }"
                  name="list"
                  :label="$t('查询结果')"
                  :visible="runningStatus !== 'error'">
                  <main-table
                    ref="refMainTable"
                    :tableData="tableData"
                    :task="task"
                    @option-changed="handleOptionChanged"
                    @on-download-result="showDownLoad"
                    @panel-chart-update="handlePanelChartUpdate" />
                </bkdata-tab-panel>
                <bkdata-tab-panel name="error"
                  :label="$t('错误信息')"
                  :visible="runningStatus === 'error'">
                  <div class="error-details">
                    <i class="icon-exclamation-circle" />
                    <span>{{ queryStageMsgDetails || queryStageMsg }}</span>
                  </div>
                </bkdata-tab-panel>
                <bkdata-tab-panel name="running"
                  :label="$t('执行详情')"
                  :disabled="true">
                  <div>{{ $t('执行详情') }}</div>
                </bkdata-tab-panel>
                <bkdata-tab-panel name="history"
                  :label="$t('查询历史')"
                  renderDirective="if">
                  <History
                    ref="history"
                    :visible.sync="showHistory"
                    :data-list="historyList"
                    :isLoading="historyLoading"
                    :total="historyTotal"
                    @pageChange="historyPageChange" />
                </bkdata-tab-panel>
              </bkdata-tab>
            </div>
          </div>
        </div>
      </div>
    </transition>
    <!-- 右栏 -->
    <i class="bk-icon icon-indent list-icon"
      @click="handleRightExpend" />

    <transition name="fade"
      enterActiveClass="fade-in"
      leaveActiveClass="fade-out">
      <div v-show="!isHideRight"
        class="center-main-right">
        <RightMenu :projectId="projectId"
          :isCommon="isCommon"
          :projectName="projectName"
          @setSQL="setSQL" />
      </div>
    </transition>
    <!-- 确认下载 -->
    <bkdata-dialog
      v-model="isShowDownLoad"
      :loading="isDownLoading"
      :hasFooter="false"
      :okText="$t('确定')"
      :cancelText="$t('取消')"
      theme="warning"
      :title="$t('请确认是否下载数据')"
      @confirm="downLoadData"
      @cancel="isShowDownLoad = false">
      <div class="downLoad-explain">
        {{ $t('数据探索下载数据提示') }}
      </div>
    </bkdata-dialog>
  </div>
</template>
<script lang='ts'>
import { once, on, off } from '@/common/js/events.js';
import RightMenu from '../components/RightMenu';
import { mapState } from 'vuex';
import MainTable from './CenterMainRT.vue';
import History from '../components/History';
import '@/common/monaco/main';
import axios from 'axios';
import { fullScreen, exitFullscreen, isFullScreen, debounce, readBlobRespToJson } from '@/common/js/util.js';
class RequestStore {}
// eslint-disable-next-line no-proto
RequestStore.__proto__.requestList = [];
// eslint-disable-next-line no-proto
RequestStore.__proto__.push = function (key) {
  this.requestList.push(key);
};
// eslint-disable-next-line no-proto
RequestStore.__proto__.remove = function (key) {
  const tidIndex = this.requestList.findIndex(tid => tid === key);
  this.requestList.splice(tidIndex, 1);
};
// eslint-disable-next-line no-proto
RequestStore.__proto__.every = function (callFn) {
  return this.requestList.every(callFn);
};
let CancelToken = null;
let cancelFunc = null;
const generatorStages = () => {
  return [
    {
      description: '提交查询',
      stage_status: 'default',
      total: null,
    },
    {
      description: '连接存储',
      stage_status: 'default',
      total: null,
    },
    {
      description: '执行查询',
      stage_status: 'default',
      total: null,
    },
    {
      description: '写入缓存',
      stage_status: 'default',
      total: null,
    },
    {
      description: '数据拉取',
      stage_status: 'default',
      total: null,
    },
  ];
};
export default {
  components: {
    RightMenu,
    MainTable,
    History,
  },
  provide() {
    return {
      setSQL: this.setSQL,
    };
  },
  props: {
    sqlData: String,
    projectId: Number,
    projectName: {
      type: String,
      default: '',
    },
    taskList: {
      type: Array,
      default: () => [],
    },
    task: {
      type: Object,
      default: () => ({}),
    },
    isCommon: {
      type: Boolean,
      default: true,
    },
    isDataAnalyst: {
      type: Boolean,
      default: true,
    },
  },
  data() {
    return {
      worker: null,
      isEditorFullScreen: false,
      queryTimer: null,
      executeTime: 0,
      startExecuteTime: 0,
      historyLoading: false,
      historyList: [],
      historyTotal: 0,
      showHistory: false,
      isHideRight: false,
      isRun: false,
      tableLoading: false,
      isDrop: false,
      startY: 0,
      buttonList: [
        { key: 0, name: 'SQL' },
        { key: 1, name: '结果集' },
      ],
      tabKey: 0,
      activeItem: {
        scope_config: {
          content: '',
        },
      },
      /**
       * 编辑器模块扩展Class
       */
      centerExtendCls: '',
      monacoConfig: {
        wordWrap: true,
        selectOnLineNumbers: true,
        roundedSelection: false,
        readOnly: false,
        cursorStyle: 'line',
        automaticLayout: true,
        glyphMargin: false,
        fontSize: 14,
        fontFamily: 'SourceCodePro, Menlo, Monaco, Consolas, "Courier New", monospace',
        lineNumbers: 'on',
        lineDecorationsWidth: '0px',
        lineNumbersMinChars: 3,
        scrollBeyondLastLine: false,
        quickSuggestions: true,
        wordBasedSuggestions: true,
        formatOnType: true,
        theme: 'vs-dark',
        formatontype: true,
        minimap: {
          enabled: false,
        },
      },
      codeChangeEmitter: null,
      editorInstance: null,
      pagination: {
        current: 1,
        count: 100,
        limit: 15,
      },
      tableData: {},
      total: '',
      offsetHeight: null,
      movedHeight: 0,
      queryStageTimer: 0,
      queryStageStoped: false,
      stages: generatorStages(),
      lastStagesPosition: 0,
      sourceTagList: '',
      queryStageMsg: '',
      queryStageMsgDetails: '',
      runningStatus: '',
      requestCancelSource: null,
      showMsgDetails: false,
      isShowDownLoad: false,
      isDownLoading: false,
      // showStageProgress: true,
      onTypeFormat: true,
      resultHeadHeight: 50,
      occupyTimer: null,
      occupied: false,
      occupier: '',
      resTab: 'list',
      errorInfo: {
        text: '',
        decorations: [],
        position: [],
      },
      observer: null,
      requestList: [],
    };
  },
  computed: {
    ...mapState({
      userName: state => state.common.userName,
    }),
    fullScreenText() {
      return this.isEditorFullScreen ? this.$t('恢复') : this.$t('全屏');
    },
    fullScreenIcon() {
      return this.isEditorFullScreen ? 'icon-un-full-screen' : 'icon-full-screen';
    },
    runningText() {
      const time = ((this.executeTime - this.startExecuteTime) / 1000).toFixed(1);
      return this.runningStatus === 'running' ? this.$t('正在执行') + ` ${time}s` : this.$t('执行');
    },
    runningMesPanelClass() {
      const statuIcon = {
        running: 'icon-point-loading',
        success: 'icon-check-line',
        error: 'icon-exclamation-circle-delete',
      };
      return [statuIcon[this.runningStatus], 'running-icon'];
    },
    queryResultTableStyle() {
      return {
        minHeight: '90px',
        height: `calc(40% + ${this.movedHeight}px)`,
      };
    },
    sqlEditorWrapperStyle() {
      return {
        minHeight: '90px',
        height: `calc(60% - ${this.movedHeight + 40}px)`,
      };
    },

    sqlEditorStyle() {
      return {
        minHeight: '90px',
        height: 'calc(100% - 60px)',
      };
    },
    runningMesContent() {
      return this.queryStageMsg.replace(/^【数据平台探索模块】/, '');
    },

    leftStyle() {
      return {
        width: `calc(100% - ${this.isHideRight ? '0px' : '320px'})`,
      };
    },
    notRun() {
      return this.isDataAnalyst && this.isCommon;
    },
    runBtnTips() {
      return {
        content: this.notRun ? this.$t('项目观察员无权限执行') : this.$t('xx正在编辑中', { name: this.occupier }),
        width: 400,
      };
    },
  },
  watch: {
    sqlData(val) {
      // this.handleOptionChanged(null)
      // this.handleChangePanelOption(this.$route.query.query)
      this.handleSqlChange(val);
    },
    projectId: {
      immediate: true,
      handler(val) {
        if (val) {
          this.resTab = 'list';
          this.disposeEditor();
          this.$nextTick(() => {
            this.initMonacoClient();
            this.handleSqlChange(this.sqlData);
          });
        }
      },
    },
    runningStatus(val) {
      this.resTab = val === 'error' ? 'error' : 'list';
      // this.$emit('updateRunStatus', val)
    },
    onTypeFormat(val, old) {
      if (val !== old) {
        this.resetSqlFormat();
      }
    },
    task: {
      immediate: true,
      deep: true,
      handler(task, old) {
        if ((task && task.query_id) !== (old && old.query_id)) {
          this.resTab = 'list';
        }
        this.occupyTimer && clearInterval(this.occupyTimer);
        this.occupied = false;
        if (task.query_id && this.isCommon) {
          this.handleGetOccupyStatus();
          this.handleOccupyPolling();
        }
      },
    },
    '$route.query.query': {
      immediate: true,
      handler(id, old) {
        if (id && RequestStore.every(tid => tid !== id)) {
          RequestStore.push(id);
          cancelFunc && cancelFunc();
          this.tableLoading = true;
          this.tableData = {};
          this.$nextTick(() => {
            this.getLastQueryResultList(id)
              .then(res => {
                if (res.result) {
                  this.tableData = res.data || {};
                  this.tableData.list
                    && this.tableData.list.length
                    && this.$nextTick(() => {
                      this.movedHeight = 0;
                      this.handleChangePanelOption(id);
                    });
                }

                setTimeout(() => {
                  this.tableLoading = false;
                  RequestStore.remove(id);
                }, 300);
              })
              ['catch'](() => {
                this.tableLoading = false;
                RequestStore.remove(id);
              });
          });
        }
      },
    },
  },
  mounted() {
    CancelToken = axios.CancelToken;
    window.addEventListener('resize', this.handleResize);
    this.setResultHeadObserver();
    this.workerInit();
  },
  beforeDestroy() {
    this.occupyTimer && clearInterval(this.occupyTimer);
    this.disposeEditor();
    window.removeEventListener('resize', this.handleResize);
    if (this.observer) {
      this.observer.disconnect();
      this.observer = null;
    }
    this.worker && this.worker.postMessage({ type: 'close' });
    this.worker = null;
  },
  methods: {
    handlePanelChartUpdate() {
      const cfgJson = this.handleChangePanelOption(this.$route.query.query, false);
      cfgJson && this.changeMovedHeight(cfgJson.chart.height);
    },
    changeMovedHeight(chartHeight = 0) {
      const elHeight = this.$el.offsetHeight;
      const height = elHeight * 0.4;
      const panelHeihgt = chartHeight + 180;
      if (panelHeihgt > height && height > 0) {
        const offsetHeight = panelHeihgt - height;
        const diffHeight = this.movedHeight + offsetHeight;
        if (elHeight * 0.6 - diffHeight > 200) {
          this.movedHeight = diffHeight;
        } else {
          this.movedHeight = 200;
        }
      }
    },
    handleChangePanelOption(id, updateOpt = true) {
      const activeItem = (this.taskList || []).find(item => item.query_id === id) || {};
      let cfgJson = null;
      if (activeItem.chart_config) {
        cfgJson = JSON.parse(activeItem.chart_config);
        cfgJson.chart = {
          width: 460,
          height: 500,
        };

        if ((this.tableData.list || []).length === 0) {
          cfgJson.bkChartsActiveTabIndex = 0;
        }
        updateOpt && this.$refs.refMainTable && this.$refs.refMainTable.updateChartPanelOptions(cfgJson);
      }
      return cfgJson;
    },
    handleOptionChanged(option) {
      const activeItem = (this.taskList || []).find(item => item.query_id === this.$route.query.query);
      if (activeItem) {
        activeItem.chart_config = JSON.stringify(option || { bkChartsActiveTabIndex: 0, bkChartsFormData: {} });
      }
    },
    editorFullScreenClick() {
      const el = document.getElementsByClassName('bk-monaco-editor')[0];
      this.isEditorFullScreen ? exitFullscreen() : fullScreen(el);
      this.isEditorFullScreen = !this.isEditorFullScreen;
    },
    getStageItemIcon(status) {
      const icon = {
        finished: 'icon-check-line',
        failed: 'icon-circle',
        default: 'icon-circle',
      };
      return icon[status] || '';
    },
    handleRightExpend() {
      this.isHideRight = !this.isHideRight;
      this.centerExtendCls = (this.isHideRight && 'right-hidden') || 'right-show';
    },
    showDownLoad() {
      this.isShowDownLoad = true;
    },
    downLoadData() {
      this.isDownLoading = true;
      this.bkRequest
        .httpRequest('dataExplore/getDownLoadSecret', {
          params: { query_id: this.$route.query.query },
        })
        .then(res => {
          if (res.result) {
            window.open(res.data);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isShowDownLoad = false;
          this.isDownLoading = false;
        });
    },
    handleShowMsgDetails() {
      this.$refs.tooltipsHtml._tippy.hide();
      this.showMsgDetails = !this.showMsgDetails;
      this.$nextTick(() => {
        this.$refs.tooltipsHtml._tippy.show();
      });
    },
    disposeEditor() {
      if (this.editorInstance) {
        this.editorInstance.dispose();
        this.editorInstance = null;
      }
    },
    isDevEnviro() {
      return /^local|dev|webdoc|127/i.test(window.location.hostname);
    },
    initMonacoClient() {
      const devUri = window.BKBASE_Global.langServerDevWS;
      const proUri = window.BKBASE_Global.langServerProdWS;
      const wsUri = this.isDevEnviro() ? devUri : proUri;
      this.editorInstance = new window.MonacoClient(
        {
          initializationOptions: {
            project_id: this.projectId,
            user_name: this.$store.getters.getUserName,
            on_type_format: this.onTypeFormat,
          },
        },
        'json',
        wsUri
      );
      this.editorInstance.init('sql_editor' + this.task.query_id, this.monacoConfig);
      const changeEvent = () => this.codeChangeHandler(this.editorInstance.editor, event);
      this.editorInstance.editor.onDidChangeModelContent(event => changeEvent());
    },
    codeChangeHandler: function (editor) {
      if (this.codeChangeEmitter) {
        this.codeChangeEmitter(editor);
      } else {
        this.codeChangeEmitter = debounce(function (editor) {
          this.editorChange(editor.getValue());
        }, 200);
        this.codeChangeEmitter(editor);
      }
    },
    handleSqlChange(sql, isTabChange) {
      let oldVal = this.editorInstance.editor.getValue();
      if (!this.compareStr(sql, oldVal)) {
        const position = this.editorInstance.editor.getPosition();
        this.editorInstance.editor.setValue([sql].join('\n'));
        this.editorInstance.editor.setPosition(position);
        if (isTabChange) return;
        this.runningStatus = 'ready';
        this.queryStageMsg = '';
        this.queryStageMsgDetails = '';
        this.tableData = {};
        this.total = 0;
        this.$set(this, 'stages', generatorStages());
      }
    },
    compareStr(str1, str2) {
      if (str1 && str2) {
        if (str1.length !== str2.length) {
          return false;
        } else {
          let isEques = true;
          for (let i = 0; i < str1.length; i++) {
            if (str1.charCodeAt(i) !== str2.charCodeAt(i)) {
              isEques = false;
              break;
            }
          }

          return isEques;
        }
      } else {
        return false;
      }
    },
    handlePageChange(page) {
      this.pagination.current = page;
    },
    // 鼠标拖拽
    mousedownHandle({ y: startY }) {
      this.isDrop = true;
      const moveEle = document.querySelector('#center_main_panel' + this.task.query_id);
      const maxUpHeight = moveEle.offsetHeight * 0.6 - 200;
      const maxDownHeight = moveEle.offsetHeight * 0.4 - 200;
      const startHeight = this.movedHeight;
      const mouseMoveHandle = ({ y: mouseY }) => {
        setTimeout(() => {
          const movingHeight = startY - mouseY + startHeight;
          if ((movingHeight > 0 && movingHeight < maxUpHeight)
                    || (movingHeight < 0 && -movingHeight < maxDownHeight)) {
            this.movedHeight = movingHeight;
          }
        }, 60);
      };
      on(document, 'mousemove', mouseMoveHandle);
      once(document, 'mouseup', () => {
        this.isDrop = false;
        off(document, 'mousemove', mouseMoveHandle);
      });
    },
    // 编辑器修改
    editorChange(sql) {
      this.handleRemoveDecorations();
      this.$emit('update:sqlData', sql);
    },
    setSQL(sql) {
      this.$emit('update:sqlData', sql);
    },
    // 执行SQL
    handleExecuteSql() {
      const that = this;
      this.isRun = true;
      this.queryStageStoped = false;
      this.runningStatus = 'running';
      this.queryStageMsg = 'running';
      this.tableLoading = true;
      this.tableData = {};
      this.total = 0;
      this.$set(this, 'stages', generatorStages());
      // 修改第一个状态为running
      this.stages.splice(
        0,
        1,
        Object.assign(this.stages[0], {
          stage_status: 'running',
        })
      );
      // this.stages = []
      this.startExecuteTime = +new Date();
      this.executeTime = this.startExecuteTime;
      this.worker && this.worker.postMessage({ type: 'start', time: this.executeTime });
      // this.queryTimer = setTimeout(function count() {
      //     that.executeTime += 100
      //     that.queryTimer = setTimeout(count, 100)
      // }, 100)
      this.bkRequest
        .httpRequest('dataExplore/updateTask', {
          params: { sql: this.sqlData, query_id: this.task.query_id },
          ext: {
            cancelToken: new CancelToken(function executor(c) {
              cancelFunc = c;
            }),
            responseType: 'blob',
          },
        })
        .then(response => {
          readBlobRespToJson(response)
            .then(res => {
              this.$emit('reLoad', this.task.query_id);
              if (res.result) {
                this.$emit('updateOriginSql', this.sqlData);
                this.getResultList(this.task.query_id);
              } else {
                this.isRun = false;
                this.runningStatus = 'error';
                this.queryStageMsg = res.message;
                this.queryStageMsgDetails = res.errors ? res.errors.error : '';
                this.tableLoading = false;
                this.clearQueryTimer();
                // 修改状态
                this.stages.splice(
                  0,
                  1,
                  Object.assign(this.stages[0], {
                    stage_status: 'failed',
                  })
                );
              }
            })
            ['catch'](e => {
              this.isRun = false;
              this.tableLoading = false;
              this.clearQueryTimer();
            });
        })
        ['catch'](e => {
          this.isRun = false;
          this.tableLoading = false;
          this.clearQueryTimer();
        });
    },
    clearQueryTimer() {
      this.queryTimer && clearTimeout(this.queryTimer);
      this.queryTimer = null;
      this.startExecuteTime = 0;
      this.worker && this.worker.postMessage({ type: 'stop' });
      this.executeTime = 0;
    },
    handleStopRunning() {
      this.queryStageStoped = true;
      clearTimeout(this.queryStageTimer);
      this.clearQueryTimer();
      this.queryStageMsg = '已停止';
      this.queryStageMsgDetails = '';
      this.runningStatus = '';
      this.isRun = false;
      this.tableLoading = false;
      cancelFunc && cancelFunc();
      const last = this.stages[this.lastStagesPosition];
      last.stage_status !== 'finished'
        && this.stages.splice(
          this.lastStagesPosition,
          1,
          Object.assign(last, {
            stage_status: 'failed',
          })
        );
      this.lastStagesPosition = 0;
    },

    getLastQueryResultList(id) {
      this.tableData = {};
      return this.bkRequest.httpRequest('dataExplore/getResultList', {
        params: { query_id: id },
        query: { limit: true },
        ext: {
          cancelToken: new CancelToken(function executor(c) {
            cancelFunc = c;
          }),
        },
      });
    },

    // 获取查询结果
    getResultList(id) {
      if (this.queryStageStoped) {
        this.tableLoading = false;
        this.isRun = false;
        return;
      }
      // this.queryStageMsg = ''
      return this.getQueryStage(id, data => {
        this.bkRequest
          .httpRequest('dataExplore/getResultList', {
            params: { query_id: id },
            query: { limit: true },
            ext: {
              cancelToken: new CancelToken(function executor(c) {
                cancelFunc = c;
              }),
            },
          })
          .then(res => {
            let status = 'finished';
            if (res.result && res.data) {
              this.runningStatus = 'success';
              const { timetaken, totalRecords } = res.data;
              this.tableData = res.data;
              this.total = timetaken.timetaken;
              this.queryStageMsg = `SQL ${this.$t('执行成功')}`;
              this.queryStageMsgDetails = '';
              this.pagination.count = totalRecords;
              status = 'finished';
            } else {
              status = 'failed';
              if (!res.result) {
                this.tableData = {};
                this.queryStageMsg = res.message;
                this.queryStageMsgDetails = res.errors ? res.errors.error : '';
                this.runningStatus = 'error';
                this.tableLoading = false;
              }
            }
            this.setResultListStage({ stage_status: status }, true);
          })
          ['catch'](e => {
            this.setResultListStage({ stage_status: 'failed' }, true);
          })
          ['finally'](() => {
            this.tableLoading = false;
            this.isRun = false;
            this.clearQueryTimer();
          });
      });
    },
    // SQL作业轨迹
    getQueryStage(id, callback) {
      if (this.queryStageStoped) {
        this.queryStageTimer && clearTimeout(this.queryStageTimer);
        this.tableLoading = false;
        this.isRun = false;
        return;
      }
      this.bkRequest
        .httpRequest('dataExplore/getTaskStage', {
          params: { query_id: id },
          ext: {
            cancelToken: new CancelToken(function executor(c) {
              cancelFunc = c;
            }),
          },
        })
        .then(res => {
          if (res.result) {
            const formatData = res.data.stages;
            formatData.forEach((item, index) => {
              this.stages.splice(index, 1, item);
            });
            // 记录最后执行位置
            this.lastStagesPosition = formatData.length - 1;
            if (!this.isComplete(res.data)) {
              this.queryStageTimer = setTimeout(() => {
                this.getQueryStage(id, callback);
              }, 800);
            } else {
              // 前端添加获取列表stage
              this.setResultListStage({
                stage_status: 'running',
                startTime: new Date().getTime(),
              });
              callback(res);
            }
          } else {
            this.tableLoading = false;
            this.isRun = false;
            this.queryStageMsg = res.message;
            this.queryStageMsgDetails = res.errors ? res.errors.error : '';

            this.runningStatus = 'error';

            this.clearQueryTimer();
            // 修改上次执行的状态
            const last = this.stages[this.lastStagesPosition];
            this.stages.splice(
              this.lastStagesPosition,
              1,
              Object.assign(last, {
                stage_status: 'failed',
              })
            );
            this.lastStagesPosition = 0;
          }
        })
        ['catch'](_ => {
          this.clearQueryTimer();
        });
    },
    setResultListStage(stage = {}, isEnd) {
      const res = this.stages[this.stages.length - 1];
      Object.assign(res, stage);
      if (isEnd) {
        const listStage = this.stages[this.stages.length - 1];
        const startTime = listStage.startTime;
        const endTime = new Date().getTime();
        res.total = endTime - startTime;
        // 查询结果时间加上数据拉取时间
        this.total += res.total;
      }
      this.stages.splice(-1, 1, res);
    },
    isComplete(data) {
      return data.status === 'finished';
    },
    // 查看历史记录
    showHistoryTab() {
      this.showHistory = true;
      this.getHistory();
    },
    // 获取历史记录
    getHistory(page = 1, pageSize = 10) {
      this.historyLoading = true;
      this.bkRequest
        .httpRequest('dataExplore/getHistory', {
          params: { query_id: this.task.query_id },
          query: { page: page, page_size: pageSize },
        })
        .then(res => {
          this.historyLoading = false;
          if (res.data) {
            const { results, count } = res.data;
            this.historyList = results;
            this.historyTotal = count;
          }
        })
        ['catch'](e => {
          this.historyLoading = false;
        });
    },
    // 历史记录分页
    historyPageChange(page, pageSize) {
      this.getHistory(page, pageSize);
    },
    resetSqlFormat: debounce(function () {
      this.disposeEditor();
      this.$nextTick(() => {
        this.initMonacoClient();
        const position = this.editorInstance.editor.getPosition();
        this.editorInstance.editor.setValue([this.sqlData].join('\n'));
        this.editorInstance.editor.setPosition(position);
      });
    }, 200),
    handleResize() {
      this.isEditorFullScreen = isFullScreen();
    },
    handleOccupyPolling() {
      this.occupyTimer = setInterval(this.handleGetOccupyStatus, 30000);
    },
    handleGetOccupyStatus() {
      this.bkRequest
        .httpRequest('dataExplore/getQueryLock', {
          params: { query_id: this.task.query_id },
        })
        .then(res => {
          if (res.result) {
            this.occupier = (res.data || {}).lock_user;
            this.occupied = (res.data || {}).lock_status;
          }
        });
    },
    handleSetSqlError() {
      if (!this.queryStageMsgDetails) return;
      if (this.editorInstance) {
        // 添加错误修饰
        const position = this.errorInfo.position;
        const rowStart = Number(position[0]);
        const rowEnd = Number(position[2]);
        const colStart = Number(position[1]);
        const colEnd = Number(position[3]) + 1;
        const decorations = [
          {
            range: new window.monaco.Range(rowStart, colStart, rowEnd, colEnd),
            options: {
              inlineClassName: 'word-error-highlight',
            },
          },
          {
            range: new window.monaco.Range(rowStart, 1, rowEnd, 10000),
            options: {
              isWholeLine: true,
              className: 'line-error-highlight',
              marginClassName: 'line-error-highlight',
            },
          },
        ];
        this.errorInfo.decorations = this.editorInstance.editor.deltaDecorations([], decorations);
        // 光标定位且聚焦
        this.editorInstance.editor.setPosition({
          lineNumber: rowStart,
          column: colEnd,
        });
        this.editorInstance.editor.focus();
        // 滚动到特定行且视图位于编辑器中心
        this.editorInstance.editor.revealLineInCenter(rowStart);
      }
    },
    handleRemoveDecorations() {
      if (this.editorInstance && this.errorInfo.decorations.length) {
        // 移除所有行修饰
        this.editorInstance.editor.deltaDecorations(this.errorInfo.decorations, []);
      }
    },
    handleTabChange(name) {
      if (name === 'history') {
        this.showHistoryTab();
      }
    },
    setResultHeadObserver() {
      const that = this;
      const targetNode = document.getElementById('resultHeader' + this.task.query_id);
      this.observer = new ResizeObserver(entries => {
        const headHeight = parseInt(window.getComputedStyle(targetNode).getPropertyValue('height'));
        if (that.resultHeadHeight === headHeight) return;
        that.resultHeadHeight = headHeight - 6;
      });
      this.observer.observe(targetNode);
    },
    workerInit() {
      this.worker = new Worker('./static/dist/web_worker/bundle.worker.dataExplore.js');
      this.worker.addEventListener('message', e => {
        this.executeTime = event.data;
      });
    },
  },
};
</script>
<style lang="scss" scoped>
@keyframes fade-in {
  0% {
    opacity: 0;
    width: 0;
  }

  100% {
    opacity: 1;
    width: 320px;
  }
}

@keyframes fade-out {
  0% {
    opacity: 1;
    width: 320px;
  }

  100% {
    opacity: 0;
    width: 0;
  }
}

@keyframes slide-show {
  0% {
    width: calc(100% - 320px);
  }

  100% {
    width: calc(100% - 0px);
  }
}

@keyframes slide-hide {
  0% {
    width: calc(100% - 0);
  }

  100% {
    width: calc(100% - 320px);
  }
}

@keyframes loading-rotate {
  0% {
    transform: rotate(0);
  }
  100% {
    transform: rotate(360deg);
  }
}

.center-main {
  width: 100%;
  height: 100%;
  display: flex;

  .right-hidden {
    width: calc(100%);
    animation: slide-show 0.7s;
  }

  .right-show {
    width: calc(100% - 320px);
    animation: slide-hide 0.5s;
  }

  .fade-in {
    animation: fade-in 0.7s;
  }
  .fade-out {
    animation: fade-out 0.5s;
  }

  .slide-fade-enter-active {
    transition: all 0.9s ease;
  }
  .slide-fade-leave-active {
    transition: all 1.5s cubic-bezier(1, 0.5, 0.8, 1);
  }
  .slide-fade-enter,
  .slide-fade-leave-to {
    transform: translateY(10px);
    opacity: 0;
  }
  .center-main-left {
    height: 100%;
    flex: 1;
    background: #ccc;
    position: relative;
    width: calc(100% - 320px);
    .center-main-sql {
      .bk-monaco-editor {
        height: 100%;
        position: relative;
        #sql_running_msg {
          display: flex;
          align-items: center;
          max-width: 300px;
          min-width: 100px;
          background: #313238;
          border-radius: 3px;
          line-height: 26px;
          padding: 0 15px;
          cursor: default;
          &.error {
            background: #ffeded;
            color: #63656e;
            border: 1px solid #ffd2d2;
            i {
              color: #ea3636;
            }
          }

          &.success {
            color: #3fc06d;
          }

          &.running {
            background: #2dcb56;
            i.running-icon {
              display: inline-block;
              animation: donut-spin 3s linear infinite;
            }
          }
          &:focus {
            outline: none;
          }

          @keyframes donut-spin {
            0% {
              transform: rotate(0deg);
            }
            100% {
              transform: rotate(360deg);
            }
          }
        }
      }
      .bk-monaco-tools {
        &.data-explore-monaco {
          line-height: 40px;
          height: 40px;
          display: flex;
          align-items: center;
          padding-left: 16px;
        }
      }
      .bk-monaco-foot {
        height: 60px;
        padding: 17px 20px;
        display: flex;
        ::v-deepbutton {
          margin-right: 10px;
          div {
            display: flex;
            align-items: center;
          }
          .icon-loading {
            margin-right: 5px;
            &::before {
              content: '';
            }
          }
        }
        .stop-button {
          margin-right: 10px;
          padding-left: 15px;
          display: flex;
          border: 1px solid #979797;
          border-radius: 2px;
          height: 26px;
          width: 82px;
          align-items: center;
          line-height: 26px;
          color: #c4c6cc;
          cursor: pointer;
          .stop-text {
            margin-left: 8px;
          }
        }
      }
    }
  }
  .center-main-right {
    width: 320px;
    height: 100%;
    border-left: 1px solid #dcdee5;
  }
  .center-main-sql {
    width: 100%;
    height: 100%;
    background: #171719;
    color: #fff;
    .bk-scroll-y {
      overflow-y: inherit;
    }
    .bk-monaco-tools {
      background: #171719;
      position: relative;
    }
    .data-explore-monaco {
      .header-tool {
        width: 100%;
        position: relative;
        height: 40px;
        line-height: 40px;
        .search-result-tips {
          color: #979ba5;
          margin-left: 10px;
          font-size: 12px;
        }
        .name {
          font-size: 14px;
          color: #c4c6cc;
        }
        .run-btn {
          height: 28px;
          line-height: 28px;
          font-size: 14px;
          margin-right: 5px;
          padding: 0 18px 0 14px;
          > div {
            display: flex;
            align-items: center;
            justify-content: center;
            height: 20px;
            line-height: 20px;
            .bk-icon {
              margin-right: 4px;
              height: 18px;
              line-height: 15px;
              &.icon-right-shape {
                font-size: 16px;
              }
            }
            .icon-loading {
              margin-right: 2px;
              line-height: 21px;
              &::before {
                content: '';
              }
            }
          }
        }
        .tips-btn {
          display: inline-block;
          min-width: 68px;
          height: 28px;
          line-height: 28px;
          font-size: 14px;
          margin-right: 5px;
          padding: 0 18px 0 14px;
          background-color: #dcdee5;
          border-color: #dcdee5;
          color: #ffffff;
          cursor: not-allowed;
          border-radius: 2px;
          .icon-right-shape {
            font-size: 16px;
            margin-right: 4px;
            height: 18px;
            line-height: 15px;
          }
        }
        .header-tool-right {
          position: absolute;
          right: 20px;
          top: 0;
        }
        .control-panel {
          display: inline-block;
          color: #979ba5;
          line-height: 19px;
          .history-btn {
            font-size: 14px;
            letter-spacing: 0;
            cursor: pointer;
            margin-left: 10px;
            position: relative;
            &:first-child {
              &::before {
                content: '';
                display: inline-block;
                width: 1px;
                height: 12px;
                background: #555;
                position: absolute;
                right: -8px;
                top: 2px;
              }
            }
            &.bk-form-checkbox {
              margin-right: 0;
              ::v-deep .bk-checkbox {
                width: 14px;
                height: 14px;
                border: none;
                background-color: #63656e;
                color: #17171a;
                &::after {
                  top: 1px;
                  left: 4px;
                  border: 2px solid #17171a;
                  border-left: 0;
                  border-top: 0;
                }
              }
              .bk-checkbox-text {
                color: #979ba5;
              }
            }
          }
        }
        .disabled {
          color: #777 !important;
        }
      }
    }
  }
  .center-main-table {
    width: 100%;
    height: 40%;
    background: #fff;
    position: absolute;
    bottom: 0;
    .drop-button {
      display: block;
      width: 100%;
      height: 12px;
      position: absolute;
      background: no-repeat center/40px url('../imgs/drag.png');
      z-index: 2;
      left: 50%;
      top: -5px;
      transform: translateX(-50%);
      cursor: row-resize;
    }
    .drop-active {
      background: no-repeat center/40px url('../imgs/drag-active.png');
    }
  }
  .search-result {
    width: 100%;
    height: 100%;
    font-size: 12px;
    .search-result-head {
      position: relative;
      width: 100%;
      padding: 6px 20px;
      color: #313238;
      border-bottom: 1px solid #dcdee5;
      .search-result-head-total {
        color: #63656e;
      }

      .search-result-progress {
        display: flex;
        padding-bottom: 6px;
        .label-title {
          flex: 0 0 62px;
          line-height: 24px;
        }
        .result-stage-items {
          display: flex;
          align-items: center;
          flex-wrap: wrap;
          .result-stage-item {
            display: flex;
            align-items: center;
            .stage-item-icon {
              font-size: 20px;
              transform: scale(0.5);
              margin: 0 -5px;
              color: #c4c6cc;
              &.failed {
                color: #ea3636;
              }
              &.finished {
                font-size: 24px;
                font-weight: bold;
                margin: 0 -6px;
              }
              &.finished {
                font-size: 24px;
                font-weight: bold;
                margin: 0 -6px;
              }
            }
            .stage-item-loading {
              width: 10px;
              height: 10px;
              animation: loading-rotate 1s infinite linear;
            }
            .stage-item-loading {
              width: 10px;
              height: 10px;
              animation: loading-rotate 1s infinite linear;
            }
            .stage-item-text {
              color: #63656e;
              padding-left: 4px;
              white-space: nowrap;
            }
            .stage-item-line {
              width: 28px;
              height: 1px;
              background: #dcdee5;
              margin: 0 8px;
            }
            &:last-child {
              margin-right: 0;
              .stage-item-line {
                display: none;
              }
            }
          }
        }
      }
    }
    .res-tab-cls {
      height: calc(100% - var(--headHeight));
      ::v-deep {
        .bk-tab-header-setting {
          padding-right: 22px;
          .icon-download-3 {
            font-size: 16px;
            color: #979ba5;
            cursor: pointer;
          }
        }
        .bk-tab-label-list {
          padding-left: 11px;
        }
        .bk-tab-label-item {
          min-width: unset;
          padding: 0 9px;
          &.active::after {
            width: calc(100% - 18px);
            left: 9px;
          }
        }
        .bk-tab-label {
          font-size: 12px;
        }
        .bk-tab-section {
          height: calc(100% - 42px);
          padding: 0;
        }
        .bk-tab-content {
          height: 100%;
          color: #63656e;
        }
      }
    }
    .error-details {
      color: #63656e;
      padding: 30px 0 0 40px;
      .icon-exclamation-circle {
        color: #ea3636;
        font-size: 16px;
        margin-right: 2px;
      }
      .error-text {
        margin: 8px 0 0 20px;
        ::v-deep .error-tips {
          position: relative;
          &::after {
            content: '^';
            position: absolute;
            left: 50%;
            top: 12px;
            transform: translateX(-50%);
            color: #979ba5;
            font-size: 14px;
          }
        }
      }
    }
    .text-popup {
      padding: 0 5px;
    }
  }
  ::v-deep {
    .line-error-highlight {
      background-color: #382322;
    }
    .word-error-highlight {
      background-color: #66231f;
    }
  }
}
</style>

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
  <DialogWrapper :dialog="dialogConfig"
    extCls="data-log"
    icon="icon-data-query"
    :title="$t('任务日志')"
    :subtitle="$t('查询当前画布中的计算任务日志_目前支持查看实时计算和离线计算节点的日志信息')">
    <template #subtitle-right>
      <div class="right-content">
        <span class="help-icon icon-help-fill" /><a :href="$store.getters['docs/getDocsUrl'].dataLog"
          target="_blank"
          class="document">
          {{ $t('帮助文档') }}
        </a>
      </div>
    </template>
    <template #content>
      <div class="content-wrapper">
        <div class="button-row">
          <TippyInfo :text="$t('运行在老计算引擎中任务暂时不支持查询任务日志')"
            type="small"
            style="flex: 1" />
        </div>
        <div class="button-row">
          <bkdata-badge :theme="'#3a84ff'"
            :visible="!panelShow"
            :val="searchNum"
            extCls="log-badge">
            <bkdata-button :iconRight="searchIcon"
              :title="$t('高级搜索')"
              @click="panelShow = !panelShow">
              {{ $t('高级搜索') }}
            </bkdata-button>
          </bkdata-badge>
        </div>
        <transition name="fold">
          <div v-show="panelShow"
            class="config-row">
            <div class="first-row row">
              <div class="first-col col">
                <label class="label">{{ $t('计算任务') }}</label>
                <div class="standard-form">
                  <bkdata-selector
                    :selected.sync="config.calcTask"
                    :isLoading="loading.taskListLoading"
                    :hasChildren="true"
                    :optionTip="true"
                    :toolTipConfig="{
                      maxWidth: 'none',
                    }"
                    :toolTipTpl="getTaskInfo"
                    :list="calcTaskList" />
                </div>
                <span v-bk-tooltips="getTooltipContent('task')"
                  class="icon-info ml8 icon" />
              </div>
              <div class="second-col col">
                <label class="label">{{ $t('任务阶段') }}</label>
                <div class="standard-form">
                  <bkdata-selector :selected.sync="config.taskStage"
                    :list="taskStageList" />
                </div>
                <span v-bk-tooltips="getTooltipContent('stage')"
                  class="icon-info ml8 icon" />
              </div>
              <div v-if="taskType === 'batch'"
                class="third-col col">
                <label class="label">{{ $t('执行记录') }}</label>
                <div class="standard-form">
                  <bkdata-selector :selected.sync="config.executeId"
                    :isLoading="loading.executionLoading"
                    settingKey="execute_id"
                    displayKey="name"
                    :list="recordList" />
                </div>
                <span v-if="isRecordFailed"
                  id="record-list-tip"
                  v-bk-tooltips="getTooltipContent('recordList')"
                  class="icon-info ml8 icon" />
              </div>
            </div>
            <template v-if="!isSubmit">
              <div class="second-row row">
                <div class="first-col col">
                  <label class="label">{{ $t('计算角色') }}</label>
                  <div class="standard-form">
                    <bkdata-selector :selected.sync="config.role"
                      :list="roleList"
                      @change="getContainer" />
                  </div>
                  <span v-bk-tooltips="getTooltipContent('role')"
                    class="icon-info ml8 icon" />
                </div>
                <div class="second-col col">
                  <label class="label">{{ $t('日志类型') }}</label>
                  <div class="standard-form">
                    <bkdata-selector :selected.sync="config.logType"
                      :list="logTypeList"
                      @change="getContainer" />
                  </div>
                  <span v-bk-tooltips="getTooltipContent('type')"
                    class="icon-info ml8 icon" />
                </div>
                <div class="third-col col">
                  <label class="label">{{ $t('运行容器') }}</label>
                  <div class="standard-form">
                    <bkdata-selector :selected.sync="config.container"
                      :isLoading="loading.containerLoading"
                      :list="containerList"
                      :customExtendCallback="getContainer">
                      <div slot="bottom-option"
                        class="bkSelector-bottom-options">
                        <div class="bkSelector-bottom-option">
                          <i class="text"
                            style="font-size: 12px">
                            {{ $t('重新拉取运行容器') }}
                          </i>
                        </div>
                      </div>
                    </bkdata-selector>
                  </div>
                  <span v-bk-tooltips="getTooltipContent('container')"
                    class="icon-info ml8 icon" />
                </div>
              </div>
            </template>
            <div class="third-row row">
              <div class="col">
                <label class="label">{{ $t('日志内容') }}</label>
                <div class="long-form">
                  <bkdata-input v-model="config.logContent"
                    :clearable="true" />
                </div>
                <bkdata-button :theme="'primary'"
                  :disabled="!searchAvailable"
                  @click="searchHandle">
                  {{ $t('查询') }}
                </bkdata-button>
              </div>
            </div>
            <div class="forth-row row">
              <div class="first-col col">
                <label class="label">{{ $t('查询方式') }}</label>
                <div class="standard-form">
                  <bkdata-radio-group v-model="queryMode"
                    @change="searchHandle">
                    <bkdata-radio :value="'from-start'">
                      头部开始
                    </bkdata-radio>
                    <bkdata-radio :value="'from-end'"
                      :disabled="config.logType === 'exception'">
                      尾部开始
                    </bkdata-radio>
                  </bkdata-radio-group>
                </div>
                <span v-bk-tooltips="getTooltipContent('mode')"
                  class="icon-info icon"
                  style="margin-left: -44px" />
              </div>
              <div class="second-col col">
                <label class="label">{{ $t('自动刷新') }}</label>
                <div class="standard-form">
                  <bkdata-selector :selected.sync="autoConfig"
                    :autoSort="false"
                    :list="refreshConfigList" />
                </div>
              </div>
            </div>
          </div>
        </transition>
        <div class="table-content">
          <logTable ref="logTable"
            :loading="logLoading"
            :maxHeight="tableHeight"
            :data.sync="logData"
            :search="config.logContent" />
        </div>
      </div>
    </template>
  </DialogWrapper>
</template>

<script>
import TippyInfo from '@/components/TipsInfo/TipsInfo.vue';
import DialogWrapper from '@/components/dialogWrapper/index.vue';
import tableAction from './tableAction';
import logTable from '@/components/logTable/index.vue';
import constSetting from './constSetting';
import { showMsg, scrollToPosition } from '@/common/js/util.js';
import _ from 'underscore';
import axios from 'axios';
let CancelToken = null;
let cancelFunc = null;
export default {
  components: {
    DialogWrapper,
    TippyInfo,
    logTable,
  },
  mixins: [tableAction, constSetting],
  props: {
    flowId: {
      type: [Number, String],
      default: '',
    },
  },
  data() {
    return {
      getLogFlag: false,
      dataSizeQuery: 32768, // 一次请求数据的长度
      queryMode: 'from-start',
      canGetAppID: true, // 防止多次触发api请求，防抖用
      containerDebounce: true,
      logLengthData: null,
      submitLogSize: 0,
      logStatus: null,
      logPosInfo: {},
      logLoading: false,
      loading: {
        taskListLoading: false,
        executionLoading: false,
        containerLoading: false,
      },
      autoConfig: '0',
      dialogConfig: {
        isShow: false,
        width: 1300,
        quickClose: false,
        loading: false,
      },
      config: {
        calcTask: '',
        taskStage: 'run',
        executeId: '',
        role: '',
        logType: '',
        container: '',
        logContent: '',
        deployMode: '',
      },
      calcTaskList: [],
      recordList: [],
      containerList: [],
      panelShow: true,
      timer: null,
      appId: null,
    };
  },
  computed: {
    tableHeight() {
      return this.panelShow ? 436 : 660;
    },
    isRecordFailed() {
      if (!this.config.executeId) return false;
      const executeId = this.config.executeId;
      const status = this.recordList.find(item => item.execute_id === parseInt(executeId)).status;
      return status === 'failed';
    },
    searchDirection() {
      return this.queryMode === 'from-start' ? 'head' : 'tail';
    },
    canGetContainer() {
      const basicRunningConfig = this.config.taskStage && this.config.role && this.config.logType;
      if (this.taskType === 'stream') {
        return basicRunningConfig;
      } else {
        return basicRunningConfig && this.config.executeId;
      }
    },
    currentLogSize() {
      if (this.isSubmit) {
        return this.submitLogSize;
      } else {
        if (!this.config.container) return 0;
        const logType = this.config.logType;
        const worker = this.config.container;
        return this.config.deployMode === 'k8s'
          ? Infinity : this.logLengthData[worker][logType]; // 处理，这里k8s的logLengthData可能是[]
      }
    },
    searchAvailable() {
      if (this.taskType === null) return false;

      let result = true;
      const searchConfig = this.config;
      const configMap = this.isSubmit ? this.submitLogField : this.runLogField;

      result = configMap[this.taskType].every(key => {
        return searchConfig[key] !== '';
      });

      return result;
    },
    isSubmit() {
      return this.config.taskStage === 'submit';
    },
    taskType() {
      const taskId = this.config.calcTask;
      let type = null;
      this.calcTaskList.some(group => {
        group.children.some(item => {
          if (item.id === taskId) {
            type = item.type;
            this.config.deployMode = item.deployMode;
          }
        });
      });
      return type;
    },
    /** 查询条件个数统计
     * 提交/运行 以及 离线/实时 日志的查询keys值有所不同，需做区分处理
     */
    searchNum() {
      let num = 0;
      const conditions = this.config;
      const submitKeys = ['calcTask', 'taskStage', 'executeId', 'logContent'];
      const runKeys = Object.keys(conditions);

      const targetKeys = this.isSubmit ? submitKeys : runKeys;
      const executeKeyIndex = targetKeys.findIndex(key => key === 'executeId');

      this.taskType === 'stream' && targetKeys.splice(executeKeyIndex, 1);
      for (const [key, value] of Object.entries(conditions)) {
        targetKeys.includes(key) && value && num++;
      }
      return num;
    },
    autoRefreshText() {
      return this.textMap[this.autoConfig] + ' ' + this.$t('自动刷新');
    },
    searchIcon() {
      return this.panelShow ? 'icon-angle-double-up' : 'icon-angle-double-down';
    },
    queryConfig() {
      const taskType = this.taskType;
      return taskType === 'stream'
        ? {
          query: {
            job_id: this.config.calcTask,
          },
        }
        : {
          query: {
            job_id: this.config.calcTask,
            execute_id: this.config.executeId,
          },
        };
    },
  },
  watch: {
    /** 首次进入自动触发日志查询 */
    searchAvailable(val, oldValue) {
      if (val && !this.getLogFlag) {
        this.searchHandle();
      }
    },
    'dialogConfig.isShow'(val) {
      if (val) {
        this.clearConfig();
        this.getTaskList(this.flowId);
      } else {
        this.logData = [];
        this.getLogFlag = false;
        clearTimeout(this.timer);
        cancelFunc && cancelFunc();
      }
    },
    'config.logType'(val) {
      if (val === 'exception') {
        this.queryMode = 'from-start';
      }
    },
    'config.executeId'(val) {
      val && !this.isSubmit && this.getContainer();
    },
    /** 切换计算任务，当为【离线&&运行】任务时获取执行记录 */
    'config.calcTask'(val) {
      this.taskType === 'batch' && this.getHistoryList();
    },
    'config.taskStage'(val, oldVal) {
      if (val === 'run' && oldVal === 'submit') {
        this.getContainer();
      }
    },
    taskType(val) {
      if (val === 'stream') {
        this.config.role = 'taskmanager';
        this.config.logType = 'log';
      } else if (val === 'batch') {
        this.config.role = 'executor';
        this.config.logType = 'stdout';
      }
    },
    autoConfig(val) {
      clearTimeout(this.timer);
      if (val === 0) return;
      this.timer = this.intervalScroll;
      this.intervalScroll(val * 1000);
    },
  },
  mounted() {
    const el = document.querySelector('.bk-table-body-wrapper');
    el.addEventListener('mousewheel', _.throttle(this.onScrollHandle, 200));
    CancelToken = axios.CancelToken;
  },
  methods: {
    getTooltipContent(name) {
      let content;
      let showOnInit = false;
      switch (name) {
        case 'stage':
          content = '<p>提交：将计算任务提交到计算引擎的过程日志</p><p>运行：计算任务运行的日志</p>';
          break;
        case 'task':
          content = `<p>通过画布中的<span>结果数据表</span>查询对应的计算任务：</p>
                                <p>1.所有实时计算节点对应一个计算任务</p>
                                <p>2.一个离线计算节点对应一个计算任务</p>`;
          break;
        case 'role':
          content = '任务执行过程中具有相同功能的角色集合';
          break;
        case 'type':
          content = '标准输入/标准输出/程序日志';
          break;
        case 'container':
          content = '运行某一计算角色的容器';
          break;
        case 'mode':
          content = '从头或从尾部查看日志';
          break;
        case 'recordList':
          content = '提交失败的任务只能查询提交日志';
          showOnInit = true;
      }
      return {
        placement: 'right',
        content: content,
        theme: 'light',
        showOnInit: showOnInit,
        boundary: 'window',
      };
    },
    resetConfig() {
      this.config.calcTask = this.calcTaskList[0].children[0].id;
      this.config.taskStage = 'run';
      this.logType = this.taskType === 'stream' ? 'log' : 'stdout';
    },
    clearConfig() {
      this.config = {
        calcTask: '',
        taskStage: 'run',
        executeId: '',
        role: '',
        logType: '',
        container: '',
        logContent: '',
      };
      this.containerList = [];
      this.recordList = [];
      this.clearTableText();
    },
    /**
     * 清除“已到底部”文字
     */
    clearTableText() {
      while (document.getElementById('table-bottom-info')) {
        const node = document.getElementById('table-bottom-info');
        const parent = node.parentElement;
        parent.removeChild(node);
      }
    },
    searchHandle() {
      this.logPosInfo = {};
      document.querySelector('.bk-table-body-wrapper').scrollTop = 1; // 滚动条置位
      if (this.isSubmit) {
        const query = this.queryConfig;
        return this.bkRequest.httpRequest('dataLog/getSubmitLogSize', query).then(res => {
          if (res.result) {
            this.submitLogSize = res.data.file_size;
            this.logStatus = res.data.status;
            this.config.executeId = res.data.execute_id;
            this.searchForLog()
              .then(res => {
                this.getLogIfNoScroll();
              })
              ['catch'](res => {
                showMsg(res, 'warning');
              });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
      }
      this.searchForLog()
        .then(res => {
          this.getLogIfNoScroll();
        })
        ['catch'](res => {
          showMsg(res, 'warning');
        });
    },
    /** 判断日志是否满屏，出现滚动条，如果没有且日志没到底，继续加载 */
    getLogIfNoScroll() {
      const contentWrapper = document.getElementsByClassName('bk-table-body-wrapper')[0];
      if (contentWrapper.scrollHeight === contentWrapper.clientHeight
                && this.logPosInfo.process_end < this.currentLogSize - 1) {
        this.getLogData('forward').then(res => {
          this.processLogDataRes(res, false, true);
          this.$nextTick(() => {
            this.getLogIfNoScroll();
          });
        });
      } else if (this.taskType === 'stream' && this.logData.length < 1) {
        const num = 10 - this.logData.length;
        for (let i = 0; i < num; i++) {
          this.logData.push({
            time: '——',
            level: '——',
            origin: '——',
            log: '——',
          });
        }
      }
    },
    processLogDataRes(res, start = true, end = true) {
      this.logData.push(...res.data.formatLog);
      this.checkAllLogData();
    },
    /** 主动拉取日志 */
    searchForLog() {
      if (this.currentLogSize === 0) {
        return Promise.reject(this.$t('暂无日志'));
      }
      this.logData = [];

      const direction = this.searchDirection === 'head' ? 'forward' : 'backward';
      return this.getLogData(direction).then(res => {
        this.processLogDataRes(res);
        return Promise.resolve(true);
      });
    },
    getLogData(direction = 'forward') {
      this.logLoading = true;
      let url, query, containerInfo;
      if (this.isSubmit) {
        url = 'dataLog/getSubmitLog';
        query = {
          job_id: this.config.calcTask,
          execute_id: this.config.executeId,
          pos_info: JSON.stringify(this.logPosInfo),
          log_info: JSON.stringify({
            log_length: this.currentLogSize || -1,
          }),
          search_info: JSON.stringify({
            search_direction: this.searchDirection,
            search_words: this.config.logContent.trim(),
            scroll_direction: direction,
          }),
        };
      } else {
        if (this.config.deployMode === 'yarn') {
          url = 'dataLog/getYarnRunLog';
          containerInfo = {
            app_id: this.appId,
            job_name: this.config.calcTask,
            container_id: this.config.container.split('@')[1],
            container_host: this.config.container.split('@')[0],
          };
        } else {
          url = 'dataLog/getK8sRunLog';
          containerInfo = {
            container_id: this.config.container.split('@')[1],
            container_hostname: this.config.container.split('@')[0],
          };
        }
        url = this.config.deployMode === 'yarn' ? 'dataLog/getYarnRunLog' : 'dataLog/getK8sRunLog';
        query = {
          container_info: JSON.stringify(containerInfo),
          log_info: JSON.stringify(
            Object.assign(
              {
                log_component_type: this.config.role,
                log_type: this.config.logType,
              },
              this.config.deployMode === 'yarn'
                ? {
                  log_length: this.currentLogSize || -1,
                }
                : {}
            )
          ),
          pos_info: JSON.stringify(this.logPosInfo),
          search_info: JSON.stringify({
            search_direction: this.searchDirection,
            search_words: this.config.logContent.trim(),
            scroll_direction: direction,
          }),
        };
      }

      return this.bkRequest
        .httpRequest(url, {
          query: query,
          ext: {
            cancelToken: new CancelToken(function executor(c) {
              cancelFunc = c;
            }),
          },
        })
        .then(res => {
          if (res.result) {
            const logs = this.formatLogData(res.data.log_data[0].inner_log_data);
            res.data.formatLog = logs;

            if (Object.prototype.hasOwnProperty.call(this.logPosInfo, 'process_end')) {
              /** k8s 与 yarn 日志的返回结构不同 */
              if (this.config.deployMode === 'yarn') {
                this.logPosInfo.process_end = Math.max(this.logPosInfo.process_end,
                  res.data.log_data[0].pos_info.process_end);
                this.logPosInfo.process_start = Math.min(this.logPosInfo.process_start,
                  res.data.log_data[0].pos_info.process_start);
              } else {
                if (res.data.log_data[0].pos_info) {
                  this.logPosInfo.process_end = this.compareK8sPos(this.logPosInfo.process_end,
                    res.data.log_data[0].pos_info.process_end, 'max');
                  this.logPosInfo.process_start = this.compareK8sPos(this.logPosInfo.process_start,
                    res.data.log_data[0].pos_info.process_start, 'min');
                }
              }
            } else {
              this.logPosInfo = res.data.log_data[0].pos_info; // 更新和获取pos_info
            }
            return Promise.resolve(res);
          } else {
            this.getMethodWarning(res.message, res.code);
            return Promise.reject(false);
          }
        })
        ['finally'](() => {
          this.getLogFlag = true;
          this.logLoading = false;
        });
    },
    compareK8sPos(a, b, type = 'max') {
      const pos = {
        max: null,
        min: null,
      };

      if (a.dtEventTimeStamp === b.dtEventTimeStamp) {
        pos.max = +a.gseindex > +b.gseindex ? a : b;
        pos.min = +a.gseindex < +b.gseindex ? a : b;
      }
      pos.max = a.dtEventTimeStamp > b.dtEventTimeStamp ? a : b;
      pos.min = a.dtEventTimeStamp < b.dtEventTimeStamp ? a : b;

      return pos[type];
    },
    formatLogData(logData) {
      const logs = logData.map(log => {
        return {
          time: log.log_time || '——',
          level: log.log_level || '——',
          origin: log.log_source || '——',
          log: log.log_content || '——',
        };
      });
      return logs;
    },
    /** 检查是否已经拉去所有日志 */
    checkAllLogData() {
      this.clearTableText();
      if (this.taskType === 'stream') return; // 实时任务不设限制，没有文件底部
      const table = document.querySelector('.bk-table-body-wrapper tbody');
      if (this.logStatus !== 'running'
                && this.logPosInfo.process_end >= this.currentLogSize - 1
                && this.logData.length > 0) {
        const div = document.createElement('div');
        div.id = 'table-bottom-info';
        div.innerText = '已到达文件底部';
        this.$nextTick(() => {
          table.append(div);
        });
      }
    },
    getContainer() {
      if (!this.canGetContainer) return;
      this.containerList = [];
      this.config.container = '';
      const task = this.recordList.find(item => item.execute_id === this.config.executeId);
      const taskStatus = task && task.status ? task.status : '';
      if (taskStatus === 'failed') return;
      this.canGetAppID
        && this.getAppId().then(appId => {
          if (appId) {
            this.appId = appId;
            this.containerList = [];
            this.config.container = '';
            this.bkRequest
              .httpRequest('dataLog/getContainer', {
                query: {
                  app_id: appId,
                  job_name: this.config.calcTask,
                  log_component_type: this.config.role,
                  log_type: this.config.logType,
                  deploy_mode: this.config.deployMode,
                },
              })
              .then(res => {
                if (res.result) {
                  const containerList = [];
                  for (const [key, value] of Object.entries(res.data.container_dict)) {
                    const container = {
                      id: value,
                      name: key,
                    };
                    containerList.push(container);
                  }
                  this.containerList = containerList.sort((a, b) => {
                    let aId = a.name.split('worker')[1];
                    let bId = b.name.split('worker')[1];
                    return aId - bId;
                  });
                  this.config.container = this.containerList.length && this.containerList[0].id;
                  this.logLengthData = res.data.log_length_data;
                  this.logStatus = res.data.log_status;
                } else {
                  this.getMethodWarning(res.message, res.code);
                }
              })
              ['finally'](() => {
                this.canGetAppID = true;
                this.loading.containerLoading = false;
              });
          }
        });
    },
    getAppId() {
      if (this.config.deployMode === 'k8s') {
        this.canGetAppID = false;
        return Promise.resolve('k8s');
      }
      const query = this.queryConfig;
      this.loading.containerLoading = true; // 获取AppID即为获取运行容器的前置动作
      this.canGetAppID = false;
      return this.bkRequest
        .httpRequest('dataLog/getAppId', query)
        .then(res => {
          if (res.result) {
            return Promise.resolve(res.data.app_id);
          } else {
            this.getMethodWarning(res.message, res.code);
            return Promise.reject(res.message);
          }
        })
        ['finally'](() => {
          this.canGetAppID = true;
        });
    },
    getTaskList(flowId) {
      this.calcTaskList = [];
      this.config.calcTask = '';
      this.loading.taskListLoading = true;
      this.bkRequest
        .httpRequest('dataLog/getTaskList', {
          params: {
            fid: this.flowId,
          },
        })
        .then(res => {
          if (res.result) {
            this.calcTaskList = res.data.list;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.loading.taskListLoading = false;
          // 默认选中第一个任务
          if (this.calcTaskList.length) {
            this.config.calcTask = this.calcTaskList[0].children[0].id;
            this.initConfig();
          }
        });
    },
    initConfig() {
      if (this.taskType === 'stream') {
        this.getContainer();
      }
    },
    getHistoryList() {
      const statusMap = {
        failed: this.$t('失败'),
        finished: this.$t('成功'),
        failed_succeeded: this.$t('提交成功，无数据'),
      };
      this.recordList = [];
      this.config.executeId = '';
      this.loading.executionLoading = true;
      this.bkRequest
        .httpRequest('dataLog/getHistoryList', {
          query: {
            job_id: this.config.calcTask,
          },
        })
        .then(res => {
          if (res.result) {
            this.recordList = res.data.submit_history_list.map(item => {
              item.name = item.status === 'failed'
                ? `${item.schedule_time}(${this.$t('提交失败')})`
                : item.schedule_time;
              return item;
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.loading.executionLoading = false;
          // 默认选中第一个任务
          if (this.recordList.length) {
            const defaultRecord = this.recordList.find(item => item.status !== 'failed');
            this.config.executeId = defaultRecord.execute_id;
          }
        });
    },
    getTaskInfo(option) {
      const list = option.name.split(',');
      let content = '';
      list.forEach(item => {
        content += `<p>${item}</p>`;
      });
      return content;
    },
    openDatalog() {
      this.dialogConfig.isShow = true;
    },
    intervalScroll(time) {
      this.timer = setTimeout(() => {
        this.autoScroll();
        this.intervalScroll(time);
      }, time);
    },
    autoScroll() {
      if (this.logLoading) return;

      const el = document.querySelector('.bk-table-body-wrapper');
      const position = el.scrollHeight - el.clientHeight;
      scrollToPosition(el, position).then(() => {
        if (this.logPosInfo.process_end < this.currentLogSize - 1 || this.taskType === 'stream') {
          this.getLogData('forward').then(res => {
            this.logData.push(...res.data.formatLog);
            this.checkAllLogData();
          });
        } else {
          showMsg(this.$t('已无更多日志，关闭自动刷新'), 'warning');
          this.autoConfig = 0;
        }
      });
    },
  },
};
</script>

<style lang="scss" scoped>
::v-deep .data-log {
  .highlight {
    background: #feed93;
  }
  .bk-dialog-content {
    top: -100px;
    .footer {
      display: none;
    }
  }
  .right-content {
    cursor: pointer;
    .help-icon {
      color: #c4c6cc;
      margin-right: 6px;
    }
    .document {
      color: #c4c6cc;
    }
  }
  .content-wrapper {
    padding: 20px 24px 20px 24px;
    overflow: hidden;
    .log-badge .bk-badge {
      border-color: #3a84ff;
    }
    .fold-enter-active,
    .fold-leave-active {
      transition: all 0.2s ease;
      max-height: 180px;
      margin-bottom: 10px;
    }
    .fold-enter,
    .fold-leave-to {
      max-height: 0;
      opacity: 0;
      margin-bottom: 0;
    }
    .button-row {
      display: flex;
      justify-content: space-between;
      margin-bottom: 10px;
      .bk-button .icon-auto-refresh {
        font-size: 19px;
        color: #c4c6cc;
      }
      .tippy-tooltip {
        min-width: 100px;
        padding: 0;
      }
    }
    .config-row {
      padding: 0 20px;
      background: #fafbfd;
      border: 1px solid #f0f1f5;
      border-radius: 2px;
      overflow: hidden;
      .row {
        display: flex;
        position: relative;
        height: 32px;
        margin-bottom: 20px;
        &:first-child {
          margin-top: 20px;
        }
        .second-col {
          position: absolute;
          left: 340px;
          top: 0px;
        }
        .third-col {
          position: absolute;
          left: 709px;
          top: 0px;
        }
        .col {
          display: flex;
          align-items: center;
          .label {
            display: inline-block;
            width: 56px;
            margin-right: 20px;
            text-align: right;
          }
          .standard-form {
            width: 240px;
            .bk-date-picker {
              width: 100%;
            }
          }
          .icon-info {
            cursor: pointer;
            z-index: 99;
            outline: none;
          }
          .long-form {
            width: 980px;
            margin-right: 10px;
          }
          .ml8 {
            margin-left: 8px;
          }
          .icon {
            font-size: 16px;
            color: #c4c6cc;
          }
          &:not(:first-child) {
            margin-left: 30px;
          }
        }
      }
    }
  }
  .config-row {
    margin-bottom: 10px;
  }
}
</style>

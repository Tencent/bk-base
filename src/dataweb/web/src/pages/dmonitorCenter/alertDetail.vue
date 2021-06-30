

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
  <Layout
    class="dmonitor-center"
    :crumbName="[
      { name: $t('我的告警'), to: '/user-center?tab=alert' },
      { name: `${$t('告警详情')}(${$route.params.alertId})` },
    ]">
    <div class="alert-detail">
      <!-- 告警屏蔽 start -->
      <div>
        <alert-shield-modal ref="alertShieldModal"
          @closeAlertShieldModal="isAlertShieldModalShow = false" />
      </div>
      <div class="alert-detail-tool">
        <div class="back-list-button alert-detail-button">
          <bkdata-button theme="primary"
            class="mb10"
            @click="linkAlerts()">
            {{ $t('返回列表') }}
          </bkdata-button>
        </div>
        <div class="alert-shield-button alert-detail-button">
          <bkdata-button theme="primary"
            class="mb10"
            @click="openAlertShieldModal(alertInfo)">
            {{ $t('屏蔽告警') }}
          </bkdata-button>
        </div>
      </div>
      <div class="alert-detail-content">
        <div v-bkloading="{ isLoading: isLoading }"
          class="alert-detail-left">
          <div class="alert-detail-left-content">
            <div class="shadows alert-info pb10">
              <div class="info data">
                <div class="type">
                  <span>
                    {{ $t('告警详情') }}
                  </span>
                </div>

                <div class="info-detail">
                  <form v-if="alertInfo !== null"
                    class="bk-form">
                    <div class="bk-form-item">
                      <label class="bk-label info-left"> {{ $t('告警对象') }}</label>
                      <div
                        class="bk-form-content info-right"
                        :title="alertInfo.alert_target_alias">
                        <a
                          href="javascript:;"
                          :title="getAlertTargetTitle(alertInfo)"
                          class="operation-button text-overflow"
                          @click="linkAlertTarget(alertInfo)">
                          {{ getAlertTargetTitle(alertInfo) }}
                        </a>
                      </div>
                    </div>
                    <div class="bk-form-item">
                      <label class="bk-label info-left"> {{ $t('告警时间') }}</label>
                      <div class="bk-form-content info-right"
                        :title="alertInfo.alert_time">
                        {{ alertInfo.alert_time }}
                      </div>
                    </div>
                    <div class="bk-form-item">
                      <label class="bk-label info-left"> {{ $t('告警类型') }}</label>
                      <div
                        class="bk-form-content info-right"
                        :title="getAlertTypeDisplay(alertInfo)">
                        {{ getAlertTypeDisplay(alertInfo) }}
                      </div>
                    </div>
                    <div class="bk-form-item">
                      <label class="bk-label info-left"> {{ $t('告警级别') }}</label>
                      <div
                        class="bk-form-content info-right"
                        :title="getAlertLevelDisplay(alertInfo)">
                        <span :class="`status status-${alertInfo.alert_level}`">
                          {{ getAlertLevelDisplay(alertInfo) }}
                        </span>
                      </div>
                    </div>
                    <div class="bk-form-item">
                      <label class="bk-label info-left"> {{ $t('告警信息') }}</label>
                      <div
                        class="bk-form-content info-right break-message"
                        :title="
                          $i18n.locale === 'en'
                            ? alertInfo.full_message_en
                            : alertInfo.full_message
                        ">
                        {{
                          $i18n.locale === 'en'
                            ? alertInfo.full_message_en
                            : alertInfo.full_message
                        }}
                      </div>
                    </div>
                    <div class="bk-form-item">
                      <label class="bk-label info-left"> {{ $t('告警状态') }}</label>
                      <div
                        v-if="
                          alertInfo.alert_status === 'alerting' &&
                            alertInfo.alert_send_status === 'init'
                        "
                        class="bk-form-content info-right"
                        :title="getAlertStatusDisplay(alertInfo)">
                        <span class="status status-primary">
                          {{ getAlertStatusDisplay(alertInfo) }}
                        </span>
                        <span
                          v-bk-tooltips.right="
                            $t('为了避免骚扰用户_告警会隔一段时间才发一次')
                          "
                          class="cursor-pointer alert-rule-tooltip ml5">
                          <i class="bk-icon icon-info-circle-shape" />
                        </span>
                      </div>
                      <div
                        v-if="
                          alertInfo.alert_status === 'shielded' ||
                            alertInfo.alert_status === 'converged'
                        "
                        class="bk-form-content info-right"
                        :title="getAlertStatusDisplay(alertInfo)">
                        <span class="status status-primary">
                          {{ getAlertStatusDisplay(alertInfo) }}
                        </span>
                        <span
                          v-if="alertInfo.description"
                          v-bk-tooltips.right="alertInfo.description"
                          class="cursor-pointer alert-rule-tooltip ml5">
                          <i class="bk-icon icon-info-circle-shape" />
                        </span>
                      </div>
                      <div
                        v-if="
                          alertInfo.alert_status === 'alerting' &&
                            alertInfo.alert_send_status !== 'init'
                        "
                        class="bk-form-content info-right"
                        :title="getAlertStatusDisplay(alertInfo)">
                        <span class="status status-primary">
                          {{ getAlertStatusDisplay(alertInfo) }}
                        </span>
                      </div>
                    </div>
                    <div class="bk-form-item">
                      <label class="bk-label info-left"> {{ $t('告警通知情况') }}</label>
                      <div
                        class="bk-form-content info-right"
                        :title="alertInfo.alert_send_status">
                        <span
                          v-if="
                            alertInfo.alert_status === 'alerting' &&
                              alertInfo.alert_send_status !== 'init'
                          ">
                          <bkdata-popover
                            v-for="(sendInfo, username) in alertInfo.alert_send_error"
                            :key="username"
                            class="alert-send-tooltip"
                            placement="bottom">
                            <span
                              :class="`status status-send status-${getSendStatus(
                                sendInfo
                              )}`">
                              {{ username }}
                            </span>
                            <div slot="content">
                              <div class="send-content">
                                <div class="send-time">
                                  {{ $t('通知时间') }}:
                                  {{ alertInfo.alert_send_time }}
                                </div>
                                <br>
                                <div
                                  v-for="(notifyInfo, notifyWay) in sendInfo"
                                  :key="notifyWay"
                                  class="notify-way-detail">
                                  <span>
                                    {{
                                      notifyWayMappings[notifyWay]
                                        ? notifyWayMappings[notifyWay]
                                          .notify_way_alias
                                        : notifyWay
                                    }}:
                                  </span>
                                  <span
                                    v-if="notifyInfo.status === 'init'"
                                    :class="[
                                      'status',
                                      `status-${notifyInfo.status}`,
                                      'status-send-detail',
                                    ]">
                                    {{ $t('未通知') }}
                                  </span>
                                  <span
                                    v-else
                                    :class="[
                                      'status',
                                      `status-${notifyInfo.status}`,
                                      'status-send-detail',
                                    ]">
                                    {{ notifyInfo.message }}
                                  </span>
                                </div>
                              </div>
                            </div>
                          </bkdata-popover>
                        </span>
                        <span v-else> - </span>
                      </div>
                    </div>
                  </form>
                </div>
              </div>
              <div class="info data">
                <div class="type">
                  <span>
                    {{ $t('告警配置') }}
                  </span>

                  <div class="total-count fr">
                    <i
                      :title="$t('编辑')"
                      class="bk-icon icon-edit"
                      @click="linkAlertConfig(alertInfo)" />
                  </div>
                </div>

                <div class="info-detail">
                  <form v-if="alertInfo !== null"
                    class="bk-form">
                    <div class="bk-form-item">
                      <label class="bk-label info-left"> {{ $t('告警策略') }}</label>
                      <div
                        class="bk-form-content info-right"
                        :title="getAlertCodeDisplay(alertInfo)">
                        <span class="mr5">{{ getAlertCodeDisplay(alertInfo) }}</span>
                        <span
                          v-bk-tooltips.right="alertRuleDescription[alertInfo.alert_code]"
                          class="alert-rule-tooltip">
                          <i class="bk-icon icon-info-circle-shape" />
                        </span>
                      </div>
                    </div>
                    <div class="bk-form-item">
                      <label class="bk-label info-left"> {{ $t('当前配置') }}</label>
                      <div
                        class="bk-form-content info-right"
                        :title="getAlertRuleDisplay(alertInfo)">
                        {{ getAlertRuleDisplay(alertInfo) }}
                      </div>
                    </div>
                  </form>
                </div>
              </div>
            </div>
          </div>
        </div>
        <div class="alert-detail-right">
          <div class="pre-container">
            <div class="pre-view">
              <bkdata-tab
                v-bkloading="{ isLoading: alertInfo === null || isLoading }"
                :active.sync="activeTabName">
                <bkdata-tab-panel :key="1"
                  name="dataMetric"
                  :label="$t('数据质量指标')">
                  <div v-if="alertInfo !== null"
                    style="padding: 0 15px">
                    <template v-if="activeTabName === 'dataMetric'">
                      <no-data-chart
                        v-if="alertInfo.alert_code === 'no_data'"
                        :alertInfo="alertInfo" />
                      <data-trend-chart
                        v-if="alertInfo.alert_code === 'data_trend'"
                        :alertInfo="alertInfo" />
                      <data-drop-chart
                        v-if="alertInfo.alert_code === 'data_drop'"
                        :alertInfo="alertInfo" />
                      <data-time-delay-chart
                        v-if="alertInfo.alert_code === 'data_time_delay'"
                        :alertInfo="alertInfo" />
                      <process-time-delay-chart
                        v-if="alertInfo.alert_code === 'process_time_delay'"
                        :alertInfo="alertInfo" />
                      <data-interrupt-chart
                        v-if="alertInfo.alert_code === 'data_interrupt'"
                        :alertInfo="alertInfo" />
                      <task-exception-chart
                        v-if="alertInfo.alert_code === 'task'"
                        :alertInfo="alertInfo" />
                      <batch-delay-chart
                        v-if="alertInfo.alert_code === 'batch_delay'"
                        :alertInfo="alertInfo" />
                    </template>
                  </div>
                </bkdata-tab-panel>
                <bkdata-tab-panel :key="2"
                  name="alertTask"
                  :label="$t('影响任务')">
                  <div v-bkloading="{ isLoading: isUseTaskLoading }">
                    <template v-if="activeTabName === 'alertTask'">
                      <use-task
                        v-if="alertInfo !== null"
                        :resultTableInfo="resultTableInfo"
                        :data-params="useTaskParams" />
                    </template>
                  </div>
                </bkdata-tab-panel>
                <bkdata-tab-panel
                  v-if="alertInfo !== null && flowRecoverTips[alertInfo.alert_code].length > 0"
                  :key="3"
                  name="alertRecover"
                  :label="$t('数据流恢复')">
                  <div class="data-access-component">
                    <template v-if="activeTabName === 'alertRecover'">
                      <h5>{{ $t('可能原因') }}：</h5>
                      <ul>
                        <li
                          v-for="(message, index) in flowRecoverTips[alertInfo.alert_code]"
                          :key="index">
                          {{ index + 1 }}.{{ message }}
                        </li>
                      </ul>
                    </template>
                  </div>
                </bkdata-tab-panel>
              </bkdata-tab>
            </div>
          </div>
        </div>
      </div>
    </div>
  </Layout>
</template>
<script>
import Bus from '@/common/js/bus.js';
import Layout from '../../components/global/layout';
import { postMethodWarning, showMsg } from '@/common/js/util.js';
import {
  alertTypeMappings,
  alertLevelMappings,
  alertCodeMappings,
  flowRecoverTips,
  alertStatusMappings,
  alertSendStatusMappings,
  alertRuleDescription,
  alertRuleDisplay,
} from '@/common/js/dmonitorCenter.js';
import useTask from '@/pages/datamart/DataDict/components/children/UseTask';
import alertShieldModal from './components/alertShieldModal';
import noDataChart from './components/alertChart/noDataChart';
import dataTrendChart from './components/alertChart/dataTrendChart';
import dataTimeDelayChart from './components/alertChart/dataTimeDelayChart';
import processTimeDelayChart from './components/alertChart/processTimeDelayChart';
import dataDropChart from './components/alertChart/dataDropChart';
import dataInterruptChart from './components/alertChart/dataInterruptChart';
import taskExceptionChart from './components/alertChart/taskExceptionChart';
import batchDelayChart from './components/alertChart/batchDelayChart';

export default {
  components: {
    Layout,
    alertShieldModal,
    useTask,
    noDataChart,
    dataTrendChart,
    dataDropChart,
    dataTimeDelayChart,
    processTimeDelayChart,
    dataInterruptChart,
    taskExceptionChart,
    batchDelayChart,
  },
  data() {
    return {
      isLoading: false,
      alertInfo: null,
      resultTableInfo: {},
      useTaskParams: {},
      activeTabName: 'dataMetric',
      notifyWayMappings: {},
      alertRuleDescription: alertRuleDescription,
      alertRuleDisplay: alertRuleDisplay[this.$i18n.locale],
      flowRecoverTips: flowRecoverTips,
      isUseTaskLoading: false,
      isAlertShieldModalShow: false,
    };
  },
  mounted() {
    this.eventListen();
    this.init();
  },
  methods: {
    init() {
      // 组件加载后默认显示告警
      this.getAlertDetail();
      this.getNotifyWayList();
      this.activeTabName = this.$route.params.tabid || this.activeTabName;
    },
    eventListen() {
      // 监听message事件
      window.addEventListener(
        'message',
        event => {
          if (event.data.msg === 'closeModel') {
            this.handleCancel();
          }
        },
        false
      );
    },
    /**
         * 跳转到告警对象详情页
         */
    linkAlertTarget(item) {
      if (item.alert_target_type === 'rawdata') {
        this.$router.push(`/data-access/data-detail/${item.alert_target_id}/`);
      } else if (item.alert_target_type === 'dataflow') {
        this.$router.push(`/dataflow/ide/${item.alert_target_id}/`);
      }
    },
    /**
         * 跳转到告警详情
         */
    linkAlert(item) {
      this.$router.push(`/dmonitor-center/alert/${item.alert_id}/`);
    },
    /**
         * 跳转到告警列表
         */
    linkAlerts() {
      this.$router.push({
        path: '/user-center',
        query: {
          tab: 'alert',
        },
      });
    },
    /**
         * 跳转到产生改告警的告警配置
         */
    linkAlertConfig(item) {
      this.$router.push(`/dmonitor-center/alert-config/${item.alert_config_id}/`);
    },
    /**
         * 打开告警屏蔽模态框
         */
    openAlertShieldModal(item) {
      this.isAlertShieldModalShow = true;
      this.$nextTick(() => {
        this.$refs.alertShieldModal.openModal(item);
      });
    },
    /**
         * 生成数据AlertTarget的title
         */
    getAlertTargetTitle(alertItem) {
      if (alertItem.alert_target_type === 'dataflow') {
        return `[${this.$t('数据开发任务')}] ${alertItem.alert_target_alias}`;
      } else if (alertItem.alert_target_type === 'rawdata') {
        return `[${this.$t('数据源')}] ${alertItem.alert_target_alias}`;
      } else {
        return alertItem.alert_target_type_alias;
      }
    },
    /**
         * 告警分类展示
         */
    getAlertTypeDisplay(alertItem) {
      if (alertItem.alert_type === 'task_monitor') {
        return this.$t('任务监控');
      } else if (alertItem.alert_type === 'data_monitor') {
        return this.$t('数据监控');
      } else {
        return this.$t('未知类型');
      }
    },
    /**
         * 告警策略展示
         */
    getAlertCodeDisplay(alertItem) {
      return alertCodeMappings[alertItem.alert_code] || this.$t('未知类型');
    },
    /**
         * 告警级别展示
         */
    getAlertLevelDisplay(alertItem) {
      return alertLevelMappings[alertItem.alert_level] || this.$t(`未知告警级别${alertItem.alert_level}`);
    },
    /**
         * 告警推送状态
         */
    getSendStatus(sendInfo) {
      for (let notifyWay in sendInfo) {
        if (sendInfo[notifyWay].status === 'init') {
          return 'init';
        }
        if (sendInfo[notifyWay].status !== 'success') {
          return 'error';
        }
      }
      return 'success';
    },
    /**
         * 告警策略展示
         */
    getAlertRuleDisplay(alertItem) {
      return this.alertRuleDisplay[alertItem.alert_code](alertItem.monitor_config);
    },
    /**
         * 告警状态展示
         */
    getAlertStatusDisplay(alertItem) {
      if (alertItem.alert_status === 'alerting') {
        return alertSendStatusMappings[alertItem.alert_send_status] || '';
      } else {
        return alertStatusMappings[alertItem.alert_status] || '';
      }
    },
    /**
         * 获取告警详情
         */
    getAlertDetail() {
      this.isLoading = true;
      const options = {
        params: {
          alert_id: this.$route.params.alertId,
        },
      };
      this.bkRequest.httpRequest('dmonitorCenter/getAlertDetail', options).then(res => {
        if (res.result) {
          this.alertInfo = res.data;
          this.alertInfo.alert_send_error = JSON.parse(this.alertInfo.alert_send_error);
          const dimensions = this.alertInfo.dimensions;
          if (dimensions.hasOwnProperty('raw_data_id')) {
            this.useTaskParams.dataType = 'raw_data';
            this.useTaskParams.data_id = dimensions.raw_data_id;
            this.useTaskParams.bk_biz_id = dimensions.bk_biz_id;
          } else {
            this.useTaskParams.dataType = 'result_table';
            if (dimensions.hasOwnProperty('result_table_id')) {
              this.useTaskParams.result_table_id = dimensions.result_table_id;
            } else if (dimensions.hasOwnProperty('data_set_id')) {
              this.useTaskParams.result_table_id = dimensions.data_set_id;
            } else if (dimensions.hasOwnProperty('downstreams')) {
              const result_table_ids = dimensions.downstreams.split(',');
              if (result_table_ids.length >= 1) {
                this.useTaskParams.result_table_id = result_table_ids[0];
              }
              if (result_table_ids.length > 1) {
                this.useTaskParams.result_table_ids = result_table_ids;
              }
            }
          }
          this.getDetailInfo();
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        this.isLoading = false;
      });
    },
    getNotifyWayList() {
      this.bkRequest.httpRequest('dmonitorCenter/getNotifyWayList').then(res => {
        if (res.result) {
          for (let notifyWay of res.data) {
            this.notifyWayMappings[notifyWay.notify_way] = notifyWay;
          }
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      });
    },
    safeValue(value) {
      return value || this.$t('无');
    },
    getDetailInfo() {
      let key = 'result_table_id';
      if (this.useTaskParams.dataType === 'raw_data') {
        key = 'data_id';
      }
      this.isUseTaskLoading = true;
      const options = {
        query: {
          [key]: this.useTaskParams[key],
          data_set_type: this.useTaskParams.dataType,
        },
      };
      this.bkRequest.httpRequest('dataDict/getResultInfoDetail', options).then(res => {
        if (res.result) {
          if (!Object.keys(res.data).length) {
            postMethodWarning(this.$t('数据平台不存在该数据'), 'warning');
          }
          this.resultTableInfo = res.data;
          this.isHasPermission = this.resultTableInfo.has_permission;
          this.isPermissionProcessing = this.resultTableInfo.ticket_status
            ? this.resultTableInfo.ticket_status === 'processing'
            : false;
          if (this.useTaskParams.dataType !== 'result_table') {
            this.rawInfo = {
              title: this.$t('来源信息'),
              list: [
                {
                  value: this.safeValue(this.resultTableInfo.data_scenario_alias),
                  name: this.$t('接入类型'),
                },
                {
                  value: this.safeValue(this.resultTableInfo.bk_app_code_alias),
                  name: this.$t('接入渠道'),
                },
                {
                  value: this.resultTableInfo.data_source_alias,
                  name: this.$t('数据来源'),
                },
                {
                  value: this.safeValue(this.resultTableInfo.data_encoding),
                  name: this.$t('字符集编码'),
                },
              ],
            };
          }
          const basic = {
            title: this.$t('基本信息'),
            list: [
              {
                value:
                                    this.useTaskParams.dataType === 'result_table'
                                      ? this.safeValue(this.resultTableInfo.result_table_id)
                                      : this.safeValue(this.resultTableInfo.id),
                name: this.$t('数据ID'),
              },
              {
                value: this.safeValue(
                  this.resultTableInfo.result_table_name || this.resultTableInfo.raw_data_name
                ),
                name: this.$t('数据名称'),
              },
              {
                value: this.safeValue(
                  this.resultTableInfo.result_table_name_alias || this.resultTableInfo.raw_data_alias
                ),
                name: this.$t('中文名称'),
              },
              {
                value: this.safeValue(this.resultTableInfo.description),
                name:
                                    this.useTaskParams.dataType === 'result_table'
                                      ? this.$t('数据描述')
                                      : this.$t('数据源描述'),
              },
              {
                value:
                                    this.useTaskParams.dataType === 'result_table'
                                      ? this.resultTableInfo.processing_type_alias
                                        ? `${this.resultTableInfo.processing_type_alias}${this.$t('结果表')}`
                                        : this.$t('结果表')
                                      : this.$t('数据源'),
                name: this.$t('类型'),
              },
              {
                value:
                                    (this.resultTableInfo.bk_biz_id ? `【${this.resultTableInfo.bk_biz_id}】` : '')
                                    + `${this.resultTableInfo.bk_biz_name ? this.resultTableInfo.bk_biz_name : ''}`,
                name: this.$t('所属业务'),
              },
              {
                value:
                                    this.useTaskParams.dataType === 'result_table'
                                      ? (this.resultTableInfo.project_id
                                        ? `【${this.resultTableInfo.project_id}】`
                                        : '')
                                          + `${
                                              this.resultTableInfo.project_name ? this.resultTableInfo.project_name : ''
                                          }`
                                      : `【4】${this.resultTableInfo.project_name}`,
                name: this.$t('所属项目'),
              },
              {
                value: this.safeValue(this.resultTableInfo.data_category_alias),
                name:
                                    this.useTaskParams.dataType === 'result_table'
                                      ? this.$t('数据分类')
                                      : this.$t('数据源分类'),
              },
              {
                value: this.safeValue(this.resultTableInfo.tag_list),
                name: this.$t('标签'),
              },
            ],
          };
          const lifeCycle = {
            title: this.$t('生命周期'),
            list: [
              {
                value:
                                    this.useTaskParams.dataType === 'raw_data'
                                      ? this.safeValue(this.resultTableInfo.expires) + this.$t('天')
                                      : '',
                name: this.$t('存储天数'),
              },
              {
                value: this.resultTableInfo.created_at,
                name: this.$t('创建时间'),
              },
              {
                value: this.safeValue(this.resultTableInfo.updated_at),
                name: this.$t('最近修改时间'),
              },
              {
                value: this.safeValue(this.resultTableInfo.updated_by),
                name: this.$t('最近修改人'),
              },
            ],
          };
          const permission = {
            title: this.$t('数据权限'),
            list: [
              {
                value: '',
                name: this.$t('数据管理员'),
              },
              {
                value: '',
                name: this.$t('数据观察员'),
              },
              {
                value: this.resultTableInfo.created_by,
                name: this.$t('创建者'),
              },
              {
                value: this.safeValue(this.resultTableInfo.sensitivity),
                name: this.$t('数据敏感度'),
              },
              {
                // 1-有 2-无，权限正在申请中 3-无，申请
                value: this.resultTableInfo.has_permission
                  ? 1
                  : this.resultTableInfo.ticket_status === 'processing'
                    ? 2
                    : 3,
                name: this.$t('权限'),
              },
            ],
          };
          this.paramsData = [this.rawInfo, basic, lifeCycle, permission];
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        this.isUseTaskLoading = false;
      });
    },
    parseLogicalKeys(logicalKeys) {
      const logicalKeyList = logicalKeys.split(',');
      let data_sets = [];
      for (let logicalKey of logicalKeyList) {
        // 把形式为{data_set_id}_{storage_key}的logical_key展开为实际的内容
        const logicalItems = logicalKey.split('_');
        const splitIndex = logicalItems.length - 2;
        const dataSetId = logicalItems.slice(0, splitIndex).join('_');
        data_sets.push({
          data_set_id: dataSetId,
          data_set_type: 'result_table',
        });
      }
      return data_sets;
    },
  },
};
</script>
<style lang="scss">
.alert-shield-dialog-button {
    margin: 10px 0 30px;
    display: flex;
    justify-content: space-evenly;
    text-align: center;
    .bk-button {
        margin-top: 20px;
        width: 120px;
        margin-right: 15px;
    }
}

.status {
    background: #737987;
    color: #fff;
    border-radius: 2px;
    text-align: center;
    display: inline-block;
    padding: 1px 8px;
    height: 22px;
    line-height: 20px;
    &-running {
        background: #9dcb6b;
    }
    &-exception {
        background: #fe771d;
    }
    &-danger {
        background: #ff5656;
    }
    &-warning {
        background: #ffb848;
    }
    &-primary {
        background: #699df4;
    }
    &-send {
        padding: 1px 8px;
        height: 26px;
        cursor: pointer;
        border: 2px solid;
    }
    &-send-detail {
        padding: 1px 5px;
        height: 20px;
        line-height: 16px;
        border: 1px solid;
        background: transparent !important;
        margin-left: auto;
    }
    &-success {
        background: #dcffe2;
        border-color: #45e35f;
        color: #45e35f;
    }
    &-error {
        background: #ffdddd;
        border-color: #ff5656;
        color: #ff5656;
    }
    &-init {
        background: #cfd3db;
        color: #787d88;
        border-color: #787d88;
    }
}

.send-content {
    display: flex;
    flex-direction: row;
    flex-wrap: wrap;
    font-size: 14px;

    .send-time {
        width: 100%;
        margin-bottom: 3px;
    }

    .notify-way-detail {
        width: 100%;
        display: flex;
        margin-bottom: 2px;

        .status-send-detail {
            margin-left: auto;
        }
    }
}

.alert-detail {
    display: flex;
    min-width: 1600px;
    flex-wrap: wrap;

    .alert-detail-tool {
        display: flex;
        width: 100%;
        margin: auto;

        .alert-detail-button {
            width: 90px;
        }

        .back-list-button {
            margin-right: auto;
        }

        .alert-shield-button {
            margin-left: auto;
        }
    }

    .alert-detail-content {
        display: flex;
        flex-wrap: nowrap;
        width: 100%;

        .alert-detail-right {
            display: flex;
            width: calc(100% - 400px);
            .pre-container {
                width: 100%;
            }
        }

        .alert-detail-left {
            display: flex;
            margin-bottom: 20px;
            min-width: 400px;
            .right-info {
                display: none;
                width: calc(100% - 415px);
                .info-detail-con {
                    padding: 0 14px 14px;
                }
                .detail-box {
                    width: 33.333%;
                    float: left;
                    border: 1px solid #dbe1e7;
                }
                .detail-box + .detail-box {
                    border-left: none;
                }
                .info-title {
                    width: 91px;
                    background: #efefef;
                    padding: 10px;
                    border-right: 1px solid #dbe1e7;
                }
                .info-content {
                    width: calc(100% - 91px);
                    padding: 10px;
                    color: #737987;
                    overflow: hidden;
                    text-overflow: ellipsis;
                    white-space: nowrap;
                }
                .info-detail-raw {
                    div {
                        border-top: none;
                    }
                }
            }
            .alert-detail-left-content {
                width: 400px;
                margin-right: 15px;

                .alert-info {
                    width: 100%;
                    .info {
                        height: auto;
                        &-detail {
                            display: flex;
                            flex-direction: row;
                            margin: 0px 15px;
                            .info-left div {
                                border: 1px solid #d9dfe5;
                                border-right: none;
                                border-top: none;
                                box-sizing: border-box;
                                padding: 12px 20px;
                                width: 148px;
                                background: #efefef;
                                white-space: nowrap;
                                &:first-of-type {
                                    border-top: 1px solid #d9dfe5;
                                }
                            }
                        }
                        .info-right {
                            display: inherit;
                            width: calc(100% - 148px);
                            color: #737987;

                            > div {
                                width: 100%;
                                background: #fff;
                                border: 1px solid #efefef;
                                border-left: none;
                                border-top: none;
                                box-sizing: border-box;
                                padding: 12px 20px;
                                overflow: hidden;
                                text-overflow: ellipsis;
                                white-space: nowrap;
                                &:first-of-type {
                                    border-top: 1px solid #d9dfe5;
                                }
                            }
                        }
                        .info-detail {
                            form {
                                width: 100%;
                                border: 1px solid #d9dfe5;
                                .bk-form-item {
                                    margin-top: 0;
                                    border-top: 1px solid #d9dfe5;

                                    &:first-child {
                                        border-top: none;
                                    }
                                }
                                .bk-form-item {
                                    background: #efefef;
                                }
                                label.info-left {
                                    width: 148px;
                                    background: #efefef;
                                    white-space: nowrap;
                                }

                                .info-right {
                                    display: inherit;
                                    line-height: 24px;
                                    white-space: normal;
                                    min-height: 34px;
                                    margin-left: 148px;
                                    background: #fff;
                                    padding-left: 15px;
                                    overflow: hidden;
                                    text-overflow: ellipsis;

                                    .alert-rule-tooltip {
                                        display: flex;
                                    }

                                    .alert-send-tooltip {
                                        padding: 1px;
                                    }
                                }

                                span.name {
                                    padding: 2px 5px;
                                    border-radius: 2px;
                                    background: #f7f7f7;
                                    border: 1px solid #dbe1e7;
                                    display: inline-block;
                                    margin-right: 3px;
                                    color: #737987;
                                    line-height: 25px;
                                }
                            }
                        }
                        .des {
                            color: #232232;
                        }
                        .break-message {
                            word-wrap: break-word;
                            word-break: break-all;
                        }
                    }
                    .msg {
                        margin-bottom: 10px;
                    }
                    .auth {
                        .auto-detail {
                            margin: 0px 15px;
                        }
                    }

                    .type {
                        height: 55px;
                        line-height: 55px;
                        padding: 0px 15px;
                        font-weight: bold;
                        &::before {
                            content: '';
                            width: 2px;
                            height: 19px;
                            background: #3a84ff;
                            display: inline-block;
                            margin-right: 15px;
                            position: relative;
                            top: 4px;
                        }
                        .name {
                            display: inline-block;
                            min-width: 58px;
                            height: 24px;
                            color: white;
                            line-height: 24px;
                            background-color: #737987;
                            border-radius: 2px;
                            text-align: center;
                            margin-top: 16px;
                            padding: 0 5px;
                        }
                        .bk-icon {
                            color: #3a84ff;
                            cursor: pointer;
                        }
                    }

                    .detail {
                        color: #3a84ff;
                        cursor: pointer;
                    }
                    .is-disabled {
                        color: #666;
                        opacity: 0.6;
                        cursor: not-allowed;
                    }
                    /*样式覆盖*/
                    .bk-dialog {
                        .bk-dialog-style {
                            width: 100%;
                        }
                        .content-from {
                            margin: 0 auto;
                            width: 590px;
                        }
                    }
                    .bk-dialog-footer {
                        height: 126px;
                    }
                    .bk-dialog-footer .bk-dialog-outer {
                        text-align: center;
                        background: #fff;
                        padding: 18px 0 47px;
                    }
                    .bk-dialog-body {
                        padding: 20px 20px 60px;
                        overflow-y: auto;
                        &::-webkit-scrollbar {
                            width: 6px;
                            height: 5px;
                        }

                        &::-webkit-scrollbar-thumb {
                            border-radius: 20px;
                            background-color: #a5a5a5;
                        }
                    }
                    // min-width: 1200px;
                    .header {
                        background: #f2f4f9;
                        padding: 19px 75px;
                        color: #1a1b2d;
                        font-size: 16px;
                        p {
                            line-height: 36px;
                            padding-left: 22px;
                            position: relative;
                            &:before {
                                content: '';
                                width: 4px;
                                height: 20px;
                                position: absolute;
                                left: 0px;
                                top: 8px;
                                background: #3a84ff;
                            }
                        }
                    }
                    .bk-loading {
                        margin-top: 0px;
                    }
                    .sideslider-wrapper {
                        box-shadow: none !important;
                    }
                }

                .shadows {
                    box-shadow: 2px 3px 5px 0px rgba(33, 34, 50, 0.15);
                    border-radius: 2px;
                    border: solid 1px rgba(195, 205, 215, 0.6);
                }
            }
        }
    }

    .open {
        background: #9dcb6b;
    }
    .sensitive {
        background: #ff5555;
    }
    .private {
        background: #f6ae00;
    }
}
</style>



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
  <Layout class="dmonitor-center"
    :crumbName="[{ name: $t('我的告警'), to: '/user-center?tab=alert' },
                 { name: $t('告警配置列表'), to: '/dmonitor-center/alert-config/' },
                 { name: alertConfigId ? `${$t('告警配置详情')}(${alertConfigId})` : $t('新建告警配置') }]">
    <div class="alert-config-detail">
      <div class="alert-config-detail-tool">
        <div class="back-list-button alert-config-detail-button">
          <bkdata-button theme="primary"
            class="mb10"
            @click="linkAlertConfigList()">
            {{ $t('返回列表') }}
          </bkdata-button>
        </div>
        <div v-if="alertConfigId"
          class="watch-alert-button alert-config-detail-button">
          <bkdata-button theme="primary"
            class="mb10"
            @click="linkAlerts()">
            {{ $t('查看告警') }}
          </bkdata-button>
        </div>
      </div>
      <div v-bkloading="{ isLoading: isLoading }"
        class="alert-config-detail-content">
        <alert-config-row :isActive="true"
          :isOpen="true"
          :step="1"
          :stepName="$t('告警对象')"
          class="bkdata-alert row step-one">
          <div v-if="alertConfigId">
            <div v-if="alertConfigInfo !== null && alertConfigInfo.bk_biz_id">
              <alert-config-form v-if="alertConfigInfo !== null"
                :formTitle="$t('业务')"
                class="bk-access-define mb10">
                <div :title="`[${alertConfigInfo.bk_biz_id}]${alertConfigInfo.bk_biz_name}`">
                  {{ alertConfigInfo.bk_biz_name }}
                  ({{ $t('业务ID') }}: {{ alertConfigInfo.bk_biz_id }})
                </div>
              </alert-config-form>
            </div>
            <div v-if="alertConfigInfo !== null && alertConfigInfo.project_id">
              <alert-config-form v-if="alertConfigInfo !== null"
                :formTitle="$t('项目')"
                class="bk-access-define mb10">
                <div v-if="alertConfigInfo.project_id"
                  :title="`[${alertConfigInfo.project_id}]${alertConfigInfo.project_alias}`">
                  {{ alertConfigInfo.project_alias }}
                  ({{ $t('项目ID') }}: {{ alertConfigInfo.project_id }})
                </div>
              </alert-config-form>
            </div>
            <alert-config-form v-if="alertConfigInfo !== null"
              :formTitle="getAlertTargetTitle(alertConfigInfo)"
              class="bk-access-define mb10">
              {{ alertConfigInfo.alert_target_alias }}
              <i class="bk-icon icon-link-to ml5 cursor-pointer"
                :title="$t('跳转到告警对象详情页')"
                @click="linkAlertTarget(alertConfigInfo)" />
            </alert-config-form>
          </div>
          <div v-if="alertConfigId === null"
            class="bkdata-alert-config-target">
            <div class="inquire">
              <div class="target-type-select">
                <bkdata-selector :selected.sync="filterForm.alertTargetType"
                  :list="filterForm.targetTypeList"
                  :settingKey="'key'"
                  :placeholder="$t('请选择告警对象类型')"
                  :displayKey="'name'"
                  :allowClear="true" />
              </div>
            </div>
            <div v-if="filterForm.alertTargetType === 'rawdata'"
              class="inquire mt5">
              <div class="business-select">
                <bkdata-selector :selected.sync="filterForm.bkBizId"
                  :filterable="true"
                  :searchable="true"
                  :placeholder="filterForm.bizHoder"
                  :list="filterForm.bizList"
                  :settingKey="'bk_biz_id'"
                  :displayKey="'bk_biz_name'"
                  :allowClear="true"
                  searchKey="bk_biz_name" />
              </div>
            </div>
            <div v-if="filterForm.alertTargetType === 'dataflow'"
              class="inquire mt5">
              <div class="project-select">
                <bkdata-selector :selected.sync="filterForm.projectId"
                  :filterable="true"
                  :searchable="true"
                  :placeholder="filterForm.projectHoder"
                  :list="filterForm.projectList"
                  :settingKey="'project_id'"
                  :displayKey="'project_alias'"
                  :allowClear="true"
                  searchKey="project_alias" />
              </div>
            </div>
            <div v-if="filterForm.alertTargetType === 'dataflow'
                   || filterForm.alertTargetType === 'rawdata'"
              class="inquire mt5">
              <div class="alert-target-select">
                <bkdata-selector :filterable="true"
                  :searchable="true"
                  :displayKey="'name'"
                  :allowClear="true"
                  :disabled="filterForm.alertTargetLoading"
                  :list="filterForm.alertTargetList"
                  :placeholder="filterForm.alertTargetHoder"
                  :selected.sync="filterForm.alertTarget"
                  :settingKey="'key'" />
              </div>
            </div>
          </div>
        </alert-config-row>
        <alert-config-row :isActive="alertConfigInfo !== null"
          :isOpen="alertConfigInfo !== null"
          :collspan="alertConfigInfo === null"
          :step="2"
          :stepName="$t('告警策略')"
          class="bkdata-alert row">
          <div class="bkdata-alert-config-switch">
            <bkdata-switcher v-if="alertConfigInfo !== null"
              v-model="alertConfigInfo.active"
              :showText="true"
              :onText="$t('已启用')"
              :offText="$t('未启用')" />
          </div>
          <div class="alert-type-title">
            <span>
              {{ $t('数据监控') }}
            </span>
          </div>
          <no-data-form v-if="alertConfigInfo !== null"
            v-model="alertConfigInfo.monitor_config.no_data"
            class="alert-rule-form"
            :disabled="!alertConfigInfo.active" />
          <data-drop-form v-if="alertConfigInfo !== null"
            v-model="alertConfigInfo.monitor_config.data_drop"
            class="alert-rule-form"
            :disabled="!alertConfigInfo.active" />
          <data-trend-form v-if="alertConfigInfo !== null"
            v-model="alertConfigInfo.monitor_config.data_trend"
            class="alert-rule-form"
            :disabled="!alertConfigInfo.active" />
          <data-time-delay-form v-if="alertConfigInfo !== null"
            v-model="alertConfigInfo.monitor_config.data_time_delay"
            class="alert-rule-form"
            :disabled="!alertConfigInfo.active" />
          <process-time-delay-form v-if="alertConfigInfo !== null"
            v-model="alertConfigInfo.monitor_config.process_time_delay"
            class="alert-rule-form"
            :disabled="!alertConfigInfo.active" />
          <data-interrupt-form v-if="alertConfigInfo !== null"
            v-model="alertConfigInfo.monitor_config.data_interrupt"
            class="alert-rule-form"
            :disabled="!alertConfigInfo.active" />
          <div class="alert-type-title">
            <span>
              {{ $t('任务监控') }}
            </span>
          </div>
          <task-exception-form v-if="alertConfigInfo !== null"
            v-model="alertConfigInfo.monitor_config.task"
            class="alert-rule-form"
            :disabled="!alertConfigInfo.active" />
          <batch-delay-form v-if="alertConfigInfo !== null"
            v-model="alertConfigInfo.monitor_config.batch_delay"
            class="alert-rule-form"
            :disabled="!alertConfigInfo.active" />
        </alert-config-row>
        <alert-config-row :isActive="alertConfigInfo !== null"
          :isOpen="alertConfigInfo !== null"
          :step="3"
          :stepName="$t('收敛策略')"
          class="bkdata-alert row">
          <alert-trigger-form v-if="alertConfigInfo !== null"
            v-model="alertConfigInfo.trigger_config"
            class="alert-rule-form" />
          <alert-convergence-form v-if="alertConfigInfo !== null"
            v-model="alertConfigInfo.convergence_config"
            class="alert-rule-form" />
        </alert-config-row>
        <alert-config-row :isActive="alertConfigInfo !== null"
          :isOpen="alertConfigInfo !== null"
          :step="4"
          :stepName="$t('告警通知')"
          class="bkdata-alert row">
          <alert-config-form v-if="alertConfigInfo !== null"
            :formTitle="$t('通知方式')"
            class="bk-access-define mb30">
            <notify-way-form v-model="alertConfigInfo.notify_config" />
          </alert-config-form>
          <alert-config-form v-if="alertConfigInfo !== null"
            :formTitle="$t('接收人')"
            class="bk-access-define mb30">
            <receiver-form v-model="alertConfigInfo.receivers"
              :roles="getPermissionRoles(alertConfigInfo)" />
          </alert-config-form>
          <!-- 暂时用户端无法配置接受群组 -->
          <!-- <alert-config-form
                        v-if="alertConfigInfo !== null"
                        :form-title="$t('接收群组')"
                        class="bk-access-define">
                        <receive-group-form v-model="alertConfigInfo.receivers">
                        </receive-group-form>
                    </alert-config-form> -->
        </alert-config-row>
      </div>
    </div>
    <div v-if="alertConfigInfo !== null"
      class="submit">
      <bkdata-button class="update-alert-config-button"
        :loading="saveLoading"
        theme="primary"
        @click="saveAlertConfig">
        {{ alertConfigInfo.updated_by !== null ? $t('修改') : $t('保存') }}
      </bkdata-button>
      <bkdata-button class="update-alert-config-button"
        theme="default"
        @click="linkAlertConfigList()">
        {{ $t('取消') }}
      </bkdata-button>
    </div>
  </Layout>
</template>
<script>
import Layout from '../../components/global/layout';
import { userPermScopes } from '@/common/api/auth';
import { postMethodWarning, showMsg } from '@/common/js/util.js';
import { alertTargetTypeMappings } from '@/common/js/dmonitorCenter.js';
import alertConfigRow from './components/alertConfigRow';
import alertConfigForm from './components/alertConfigForm';
import noDataForm from './components/alertRuleForm/noDataForm';
import dataDropForm from './components/alertRuleForm/dataDropForm';
import dataTrendForm from './components/alertRuleForm/dataTrendForm';
import dataTimeDelayForm from './components/alertRuleForm/dataTimeDelayForm';
import processTimeDelayForm from './components/alertRuleForm/processTimeDelayForm';
import dataInterruptForm from './components/alertRuleForm/dataInterruptForm';
import taskExceptionForm from './components/alertRuleForm/taskExceptionForm';
import batchDelayForm from './components/alertRuleForm/batchDelayForm';
import alertConvergenceForm from './components/convergenceForm/alertConvergenceForm';
import alertTriggerForm from './components/convergenceForm/alertTriggerForm';
import notifyWayForm from './components/notifyForm/notifyWayForm';
import receiverForm from './components/notifyForm/receiverForm';

export default {
  components: {
    Layout,
    alertConfigRow,
    alertConfigForm,
    noDataForm,
    dataDropForm,
    dataTrendForm,
    dataTimeDelayForm,
    processTimeDelayForm,
    dataInterruptForm,
    taskExceptionForm,
    batchDelayForm,
    alertConvergenceForm,
    alertTriggerForm,
    notifyWayForm,
    receiverForm,
  },
  data() {
    return {
      isLoading: false,
      alertConfigId: this.$route.params.alertConfigId || null,
      saveLoading: false,
      alertConfigInfo: null,
      alertConfigForm: {
        selectedAlertTarget: '',
        notifyWayList: [],
        alertTargetList: [],
        notifyConfig: {},
      },

      filterForm: {
        bkBizId: '',
        bizList: [],
        bizHoder: '',
        projectId: '',
        projectList: [],
        projectHoder: '',
        targetTypeList: [
          {
            name: this.$t('数据开发任务'),
            key: 'dataflow',
          },
          {
            name: this.$t('数据源'),
            key: 'rawdata',
          },
        ],
        alertTargetType: '',
        alertTargetList: [],
        alertTarget: '',
        alertTargetHoder: this.$t('请选择告警对象'),
        alertTargetLoading: false,
      },
    };
  },
  watch: {
    'filterForm.alertTargetType': {
      handler(alertTargetType) {
        if (!alertTargetType) {
          this.alertConfigInfo = null;
        }
      },
    },
    'filterForm.bkBizId': {
      handler(bkBizId) {
        this.updateAlertTargetList();
      },
    },
    'filterForm.projectId': {
      handler(projectId) {
        this.updateAlertTargetList();
      },
    },
    'filterForm.alertTarget': {
      handler(alertTarget) {
        if (!alertTarget) {
          this.alertConfigInfo = null;
          return;
        }
        this.getAlertConfigDetailByTarget(this.filterForm.alertTargetType, alertTarget);
      },
    },
  },
  mounted() {
    this.eventListen();
    this.init();
  },
  methods: {
    init() {
      // 组件加载后默认显示告警
      if (this.alertConfigId === 'add') {
        this.alertConfigId = null;
      }
      if (this.alertConfigId) {
        this.getAlertConfigDetail();
      } else {
        this.getProjectList();
        this.getBizList();
        this.updateAlertTargetList();
      }
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
     * 跳转到告警详情
     */
    linkAlerts() {
      let flow_id = null;
      if (this.alertConfigInfo.alert_target_type === 'rawdata') {
        flow_id = `rawdata${this.alertConfigInfo.alert_target_id}`;
      } else {
        flow_id = this.alertConfigInfo.alert_target_id;
      }
      this.$router.push({
        path: '/user-center',
        query: {
          tab: 'alert',
          alert_target_id: this.alertConfigInfo.alert_target_id,
          alert_target_type: this.alertConfigInfo.alert_target_type,
          alert_target_alias: this.alertConfigInfo.alert_target_alias,
        },
      });
    },
    /**
     * 跳转到告警配置列表页
     */
    linkAlertConfigList() {
      this.$router.push('/dmonitor-center/alert-config/');
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
        return this.$t('数据开发任务');
      } else if (alertItem.alert_target_type === 'rawdata') {
        return this.$t('数据源');
      } else {
        return this.$t('未知类型任务');
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
     * 获取告警详情
     */
    getAlertConfigDetail() {
      this.isLoading = true;
      const options = {
        params: {
          alert_config_id: this.$route.params.alertConfigId,
        },
      };
      this.bkRequest.httpRequest('dmonitorCenter/getAlertConfig', options).then(res => {
        if (res.result) {
          this.alertConfigInfo = res.data;
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        this.isLoading = false;
      });
    },
    /**
     * 根据告警对象获取告警配置详情
     */
    getAlertConfigDetailByTarget(alertTargetType, alertTargetId) {
      this.alertConfigInfo = null;
      this.$nextTick(() => {
        this.isLoading = true;
        const options = {
          params: {
            alert_target_type: alertTargetType,
            alert_target_id: alertTargetId,
          },
        };
        this.bkRequest.httpRequest('dmonitorCenter/getAlertConfigByTarget', options).then(res => {
          if (res.result) {
            this.alertConfigInfo = res.data;
            if (this.alertConfigInfo.active === true) {
              this.$bkMessage({
                message: `该${alertTargetTypeMappings[alertTargetType]}已配置监控告警，请直接进行修改`,
                delay: 10000,
              });
            }
          } else {
            this.getMethodWarning(res.message, res.code);
          }
          this.isLoading = false;
        });
      });
    },
    updateAlertTargetList() {
      this.filterForm.alertTargetHoder = this.$t('数据加载中');
      this.filterForm.alertTargetLoading = true;
      let options = {
        query: {
          base: 'alert_config',
        },
      };
      if (this.filterForm.bkBizId) {
        options.query.bk_biz_id = this.filterForm.bkBizId;
      }
      if (this.filterForm.projectId) {
        options.query.project_id = this.filterForm.projectId;
      }
      if (this.filterForm.alertTargetType) {
        options.query.alert_target_type = this.filterForm.alertTargetType;
      }
      this.bkRequest.httpRequest('dmonitorCenter/getMineAlertTargetList', options).then(res => {
        if (res.result) {
          this.filterForm.alertTargetList = res.data;
          this.filterForm.alertTargetList.map(alertTarget => {
            Object.assign(alertTarget, this.formatAlertTargetOption(alertTarget));
            return alertTarget;
          });
        } else {
          this.getMethodWarning(res.message, 'error');
        }
        this.filterForm.alertTargetLoading = false;
        this.filterForm.alertTargetHoder = this.$t('请选择告警对象');
      });
    },
    /**
     * 格式化告警对象选项
     */
    formatAlertTargetOption(alertTarget) {
      if (alertTarget.alert_target_type === 'dataflow') {
        return {
          name: `[${alertTarget.alert_target_id}] ${alertTarget.alert_target_alias}`,
          key: alertTarget.alert_target_id,
        };
      } else {
        return {
          name: `[${alertTarget.alert_target_id}] ${alertTarget.alert_target_alias}`,
          key: `${alertTarget.alert_target_id}`,
        };
      }
    },
    /**
     * 保存告警配置
     */
    saveAlertConfig() {
      this.saveLoading = true;
      const options = {
        params: this.alertConfigInfo,
      };
      options.params.alert_config_id = this.alertConfigInfo.id;
      this.bkRequest.httpRequest('dmonitorCenter/updateAlertConfig', options).then(res => {
        if (res.result) {
          this.alertConfigInfo.updated_at = res.data.updated_at;
          showMsg(this.$t('保存告警配置成功'), 'success');
        } else {
          postMethodWarning(res.message, 'error');
        }
        this.saveLoading = false;
      });
    },
    getBizList() {
      this.filterForm.bizHoder = this.$t('数据加载中');
      const options = {
        params: {
          action_id: 'raw_data.update',
          dimension: 'bk_biz_id',
        },
      };
      this.bkRequest
        .httpRequest('dmonitorCenter/getMineAuthScopeDimension', options)
        .then(res => {
          if (res.result) {
            this.filterForm.bizList = res.data;
            this.filterForm.bizList.map(biz => {
              biz.bk_biz_id = Number(biz.bk_biz_id);
              biz.bk_biz_name = `[${biz.bk_biz_id}]${biz.bk_biz_name}`;
              return biz;
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.filterForm.bizHoder = this.$t('请选择业务');
        });
    },
    getProjectList() {
      this.filterForm.projectHoder = this.$t('数据加载中');

      userPermScopes({
        show_display: true,
        action_id: 'project.manage_flow',
      })
        .then(res => {
          if (res.result) {
            this.filterForm.projectList = res.data.filter(project => {
              if (project.project_id === undefined) {
                console.log(project);
                return false;
              }
              return true;
            });
            this.filterForm.projectList.map(project => {
              project.project_id = Number(project.project_id);
              project.project_alias = project.project_name;
              return project;
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.filterForm.projectHoder = this.$t('请选择项目');
        });
    },
    getPermissionRoles(alertConfigInfo) {
      let roles = [];
      if (alertConfigInfo.alert_target_type === 'rawdata') {
        roles = [
          {
            objectClass: 'biz',
            scopeId: alertConfigInfo.bk_biz_id,
          },
          {
            objectClass: 'raw_data',
            scopeId: alertConfigInfo.alert_target_id,
          },
        ];
      } else {
        roles = [
          {
            objectClass: 'project',
            scopeId: alertConfigInfo.project_id,
          },
        ];
      }
      return roles;
    },
  },
};
</script>
<style lang="scss">
.update-alert-config-button {
  width: 120px;
  margin: auto;
  display: flex;
  justify-content: center;
}

.alert-config-detail {
  display: flex;
  min-width: 1600px;
  flex-wrap: wrap;

  .cursor-pointer {
    cursor: pointer;
  }

  .alert-config-detail-tool {
    display: flex;
    width: 80%;
    margin: auto;

    .alert-config-detail-button {
      width: 90px;
    }

    .back-list-button {
      margin-right: auto;
    }

    .watch-alert-button {
      margin-left: auto;
    }
  }

  .alert-config-detail-content {
    display: flex;
    width: 80%;
    margin: auto;
    flex-wrap: wrap;

    .bkdata-alert {
      &.row {
        margin-bottom: 15px;
      }
      width: 100%;
      min-width: 100%;
      margin: auto;

      .bkdata-alert-config-switch {
        margin-left: 14px;

        .bk-switcher.show-label {
          width: 80px;
        }

        .bk-switcher .switcher-label {
          width: 36px;
          margin-left: 30px;
        }
        .bk-switcher.is-checked .switcher-label {
          margin-left: 10px;
        }
      }

      .bkdata-alert-config-target {
        width: 300px;
        margin-right: 20px;
      }

      .alert-rule-form {
        display: flex;
        margin-left: 36px;
        margin-bottom: 5px;

        .alert-rule-content {
          color: #737987;
        }
      }

      .alert-type-title {
        height: 55px;
        line-height: 55px;
        padding: 0px 15px;
        font-weight: bold;
        font-size: 16px;
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

      .alert-instructions {
        width: 200px;
        position: absolute;
        top: 0;
        border: 1px solid #ddd;
        border-radius: 5px;
        transform: translate(calc(100% + 10px), 0);
        padding: 10px;
        line-height: 20px;
        box-shadow: 0px 0px 10px #ddd;
        &:after {
          content: '';
          border: 10px solid;
          border-color: transparent #fff transparent transparent;
          position: absolute;
          left: 0;
          top: 30px;
          transform: translate(-100%, 0);
        }
        &:before {
          content: '';
          border: 10px solid;
          border-color: transparent #3a84ff transparent transparent;
          position: absolute;
          left: 0;
          top: 30px;
          transform: translate(-100%, 0);
        }
        .instructions {
          margin: 20px 0;
        }
        .info {
          font-size: 12px;
          min-height: 50px;
        }
        .instructions-image {
          width: 100%;
          height: 100px;
          background: #ccc;
          img {
            width: 100%;
            height: 100%;
          }
        }
      }
      .alert-types-container {
        min-height: 32px;
        .bkdata-alert-types {
          align-items: center;
          min-height: 32px;
          &.common {
            font-size: 15px;
          }

          &.diaplay-mode {
            align-items: flex-start;
          }

          &.other {
            margin-bottom: 0;
          }
          .bk-form-items {
            display: flex;
            flex-wrap: wrap;
            min-height: 32px;

            &.common {
              .bk-form-radio {
                min-width: 70px;
                .bk-radio-text {
                  font-size: 14px;
                  padding: 7px;
                }
              }
            }

            &.display {
              span {
                background: #3a84ff;
                color: #fff;
                padding: 5px 8px;
                border-radius: 2px;
                font-size: 16px;
              }
            }
            .bk-form-radio {
              border-radius: 2px;
              background: #eee;
              padding: 0;
              display: flex;
              flex-direction: column;
              align-items: center;
              justify-content: center;
              min-width: 60px;
              cursor: pointer;
              .bk-radio-text {
                cursor: pointer;
              }
              &.active {
                background: #3a84ff;
                color: #fff;
              }

              i {
                padding: 5px;
              }
              &[disabled='disabled'] {
                &:hover {
                  cursor: not-allowed;
                }

                i {
                  &:hover {
                    cursor: not-allowed;
                  }
                }
              }
              .hide-input {
                width: 1px;
                height: 0;
                border: none;
              }
            }
          }
        }
      }

      .bkdata-alert-filter {
        .bk-form-items {
          display: flex;
          flex-direction: column;
        }
      }

      .bk-item-des {
        color: #a5a5a5;
        padding: 1px;
        display: inline-block;
      }
      .data-auth {
        font-weight: bold;
        color: #3a84ff;
        padding-left: 5px;
        cursor: pointer;
      }
      .bkdata-alert-define {
        align-items: flex-start;
      }

      .bkdata-alert-method {
        .bk-form-items {
          display: flex;
          flex-direction: column;
        }
      }
    }
    .step-one {
      .bk-collapse-item-detail {
        display: flex;
        flex-direction: column;
      }
    }
  }
}

.submit {
  display: flex;
  width: 280px;
  margin: auto;
}
</style>

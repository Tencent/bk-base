

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
    v-bkloading="{ isLoading: isFirstLoading }"
    :collspan="true"
    :crumbName="$t('监控配置')"
    :headerBackground="'inherit'"
    :headerMargin="false"
    :isOpen="isOpen"
    :withMargin="false"
    class="container-with-shadow"
    height="auto">
    <div class="content-item mb10 pl10">
      <alert-config-form :formTitle="$t('是否启用')"
        class="bk-access-define">
        <bkdata-switcher v-model="alertConfigValue.active"
          size="large"
          @change="val => switchChange(val)" />
      </alert-config-form>
    </div>
    <div>
      <div class="content-item">
        <div class="collapse-title">
          <div
            :class="['cont-left', (params.data_monitor.collspanOpened && 'coll-selected') || '']"
            @click="handCollspanClick('data_monitor')">
            {{ $t('数据监控') }}
            <span class="desc">{{ $t('对数据进行监控，包括监控数据的数据量_延迟情况_有效率等') }}</span>
          </div>
        </div>
        <transition name="slide">
          <div class="config-list f13"
            :class="{ 'config-list-padding': !params.data_monitor.collspanOpened }">
            <no-data-form
              v-model="alertConfigValue.monitor_config.no_data"
              class="alert-rule-form"
              :disabled="!alertConfigInfo.active" />
            <data-drop-form
              v-model="alertConfigValue.monitor_config.data_drop"
              class="alert-rule-form"
              :disabled="!alertConfigInfo.active" />
            <data-trend-form
              v-model="alertConfigValue.monitor_config.data_trend"
              class="alert-rule-form"
              :disabled="!alertConfigInfo.active" />
            <data-time-delay-form
              v-model="alertConfigValue.monitor_config.data_time_delay"
              class="alert-rule-form"
              :disabled="!alertConfigInfo.active" />
            <process-time-delay-form
              v-model="alertConfigValue.monitor_config.process_time_delay"
              class="alert-rule-form"
              :disabled="!alertConfigInfo.active" />
            <data-interrupt-form
              v-model="alertConfigValue.monitor_config.data_interrupt"
              class="alert-rule-form"
              :disabled="!alertConfigInfo.active" />
          </div>
        </transition>
      </div>
      <div class="content-item">
        <div class="collapse-title">
          <div
            :class="['cont-left', (params.task_monitor.collspanOpened && 'coll-selected') || '']"
            @click="handCollspanClick('task_monitor')">
            {{ $t('任务监控') }}
            <span class="desc">{{ $t('对任务进行监控，包括接入_清洗_入库_计算等任务') }}</span>
          </div>
        </div>
        <transition name="slide">
          <div class="config-list f13"
            :class="{ 'config-list-padding': !params.task_monitor.collspanOpened }">
            <task-exception-form
              v-model="alertConfigValue.monitor_config.task"
              class="alert-rule-form"
              :disabled="!alertConfigInfo.active" />
            <batch-delay-form
              v-model="alertConfigValue.monitor_config.batch_delay"
              class="alert-rule-form"
              :disabled="!alertConfigInfo.active" />
          </div>
        </transition>
      </div>
      <div class="content-item">
        <div class="collapse-title">
          <div
            :class="['cont-left', (params.convergence_config.collspanOpened && 'coll-selected') || '']"
            @click="handCollspanClick('convergence_config')">
            {{ $t('收敛策略') }}
            <span class="desc">{{ $t('对告警进行收敛') }}</span>
          </div>
        </div>
        <transition name="slide">
          <div class="config-list f13"
            :class="{ 'config-list-padding': !params.convergence_config.collspanOpened }">
            <alert-trigger-form v-model="alertConfigValue.trigger_config"
              class="alert-rule-form" />
            <alert-convergence-form v-model="alertConfigValue.convergence_config"
              class="alert-rule-form" />
          </div>
        </transition>
      </div>
      <div class="content-item pt10 pl10">
        <alert-config-form :formTitle="$t('通知方式')"
          class="bk-access-define">
          <notify-way-form v-model="alertConfigValue.notify_config"
            :disabled="!alertConfigInfo.active" />
        </alert-config-form>
      </div>
      <div class="content-item pt10 pl10">
        <alert-config-form :formTitle="$t('接收人')"
          class="bk-access-define">
          <receiver-form v-model="alertConfigValue.receivers"
            :roles="getPermissionRoles(alertConfigInfo)" />
        </alert-config-form>
      </div>
    </div>
    <div class="config-btn-box">
      <bkdata-button :loading="submitLoading"
        :title="$t('确认提交')"
        theme="primary"
        @click="handleSubmitClick">
        {{ $t('确认提交') }}
      </bkdata-button>
    </div>
  </Layout>
</template>

<script>
import Layout from '@/components/global/layout';
import { showMsg } from '@/common/js/util.js';
import noDataForm from '@/pages/dmonitorCenter/components/alertRuleForm/noDataForm';
import dataDropForm from '@/pages/dmonitorCenter/components/alertRuleForm/dataDropForm';
import dataTrendForm from '@/pages/dmonitorCenter/components/alertRuleForm/dataTrendForm';
import dataTimeDelayForm from '@/pages/dmonitorCenter/components/alertRuleForm/dataTimeDelayForm';
import processTimeDelayForm from '@/pages/dmonitorCenter/components/alertRuleForm/processTimeDelayForm';
import dataInterruptForm from '@/pages/dmonitorCenter/components/alertRuleForm/dataInterruptForm';
import taskExceptionForm from '@/pages/dmonitorCenter/components/alertRuleForm/taskExceptionForm';
import batchDelayForm from '@/pages/dmonitorCenter/components/alertRuleForm/batchDelayForm';
import notifyWayForm from '@/pages/dmonitorCenter/components/notifyForm/notifyWayForm';
import receiverForm from '@/pages/dmonitorCenter/components/notifyForm/receiverForm';
import alertConfigForm from '@/pages/dmonitorCenter/components/alertConfigForm';
import alertConvergenceForm from '@/pages/dmonitorCenter/components/convergenceForm/alertConvergenceForm';
import alertTriggerForm from '@/pages/dmonitorCenter/components/convergenceForm/alertTriggerForm';

export default {
  name: 'MonitoringConfig',
  components: {
    Layout,
    noDataForm,
    dataDropForm,
    dataTrendForm,
    dataTimeDelayForm,
    processTimeDelayForm,
    dataInterruptForm,
    taskExceptionForm,
    batchDelayForm,
    notifyWayForm,
    receiverForm,
    alertConfigForm,
    alertConvergenceForm,
    alertTriggerForm,
  },
  props: {
    isRefresh: {
      type: Boolean,
      default: true,
    },
    alertConfigInfo: {
      type: Object,
      default: () => ({}),
    },
    isFirstLoading: {
      type: Boolean,
      default: false,
    },
    submitLoading: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      validata: {
        status: false,
        errorMsg: window.$t('该值不能为空'),
      },
      params: {
        data_monitor: {
          collspanOpened: false,
        },
        task_monitor: {
          collspanOpened: false,
        },
        convergence_config: {
          collspanOpened: false,
        },
      },
    };
  },
  computed: {
    alertConfigValue: {
      get() {
        return this.alertConfigInfo;
      },
      set(val) {
        Object.assign(this.alertConfigInfo, val);
      },
    },
    isOpen() {
      return this.$route.query.from !== 'pre';
    },
  },
  methods: {
    handCollspanClick(objName) {
      this.params[objName].collspanOpened = !this.params[objName].collspanOpened;
      // 在告警配置弹窗里，打开具体的配置项需要改变弹窗在窗口的高度
      let count = 0;
      for (const obj in this.params) {
        if (this.params[obj].collspanOpened) {
          count++;
        }
      }
      this.$emit('changeUseStatus', count > 2 || this.params.data_monitor.collspanOpened);
    },

    switchChange(val) {
      if (!this.isFirstLoading) {
        this.alertConfigValue.active = val;
        this.params.task_monitor.collspanOpened = this.alertConfigInfo.active;
        this.params.data_monitor.collspanOpened = this.alertConfigInfo.active;
        this.params.convergence_config.collspanOpened = this.alertConfigInfo.active;
      }
      this.$emit('changeUseStatus', val);
    },

    checkFormate() {
      let processFlag = false;
      if (!this.alertConfigInfo.receivers.length && this.alertConfigInfo.active) {
        this.validata.status = true;
        processFlag = true;
      } else {
        this.validata.status = false;
      }
      return processFlag;
    },

    /**
     * 确认提交
     * */
    handleSubmitClick() {
      if (this.checkFormate()) {
        showMsg(this.$t('告警接收人不能为空'), 'warning');
        return;
      }
      const options = {
        params: this.alertConfigInfo,
      };
      options.params.alert_config_id = this.alertConfigInfo.id;
      this.$emit('submit', options);
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
<style lang="scss" scoped>
::v-deep .layout-content {
  margin-left: 8px;
}
</style>
<style scoped lang="scss">
/* slide动画 */
.slide-enter-active {
  transition: all 0.3s ease-in-out;
  overflow: hidden;
}
.slide-leave-active {
  transition: all 0.3s ease-in-out;
  height: 0;
  overflow: hidden;
}
.slide-enter,
.slide-leave {
  height: 0;
  opacity: 0;
}

.bk-form-input {
  padding: 0;
}

.ml20 {
  margin-left: 20px;
}
.ml4 {
  margin-left: 4px;
}
.container-with-shadow {
  .content-item + .content-item {
    .collapse-title {
      border-top: none;
    }
  }

  .cont-right {
    margin-left: auto;
    margin-bottom: 5px;
  }

  ::v-deep .layout-content {
    flex-direction: column;
  }

  .collapse-title {
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 14px;
    font-weight: bold;
    height: 40px;
    line-height: 40px;
    border: 1px solid #dde3e8;
    background: #fafafa;
    .cont-left {
      margin-left: 10px;
      width: 100%;
      color: #979ba5;
      cursor: pointer;
      .desc {
        margin-left: 10px;
        color: #979ba5;
        font-weight: normal;
      }
    }
    .cont-right {
      margin-right: 10px;
    }
  }
  .config-list {
    color: #606266;
    border: 1px solid #dde3e8;
    border-top: none;
    padding: 10px 0 10px 20px;

    .alert-rule-form {
      display: flex;
      margin-left: 10px;
      margin-bottom: 6px;
    }

    .content-item {
      display: flex;
      padding: 10px 0;
      .monitor-title {
        display: flex;
        justify-content: flex-end;
        align-items: flex-start;
        width: 65px;
        margin: 7px 0;
      }
      .config-list-item {
        display: flex;
        align-items: center;
        width: 100%;
        margin: 0 15px;
        ::v-deep .bk-tag-selector .bk-tag-input .tag-list > li {
          margin: 4px;
        }
        .el-checkbox + .el-checkbox {
          margin-left: 15px;
        }
        ::v-deep .bk-tag-selector {
          width: 100%;
          .bk-tag-input {
            height: 100px;
          }
        }
        .config-cont {
          display: flex;
          align-items: center;
          &.disabled {
            color: #ccc;
          }
        }
        .bk-form-input {
          width: 69px;
          height: 20px;
        }
        .bk-checkbox-text {
          font-style: normal;
          font-weight: normal;
          cursor: pointer;
          vertical-align: middle;
        }
      }
      .config-list-item + .config-list-item {
        margin-bottom: 0;
      }
    }
  }
  .config-list-padding {
    height: 0;
    padding: 0;
    opacity: 0;
    border: none;
    pointer-events: none;
  }
  .config-btn-box {
    display: flex;
    justify-content: center;
    margin-top: 30px;
  }
}

::v-deep .input-align-right {
  width: 80px;
  input {
    text-align: right;
    padding-right: 15px;
  }
}
.formate-error {
  margin-left: 76px;
  color: red;
}
</style>

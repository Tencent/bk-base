

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
  <div>
    <ConfigPopUp
      :isShowPopUp="isShowPopUp"
      :isShowFooter="false"
      :position="position"
      :isHiddenConfirmBtn="true"
      :cancelText="$t('关闭')"
      :title="$t('告警配置')"
      :width="1000"
      :icon="'icon-alert'"
      @close="close">
      <div :class="[isScroll ? 'bk-scroll-y' : '', 'alert-config-container']">
        <MonitoringConfig
          :submitLoading="submitLoading"
          :isFirstLoading="isFirstLoading"
          :alertConfigInfo="alertConfigInfo"
          @changeUseStatus="changeUseStatus"
          @submit="alertConfigSubmit" />
      </div>
    </ConfigPopUp>
  </div>
</template>

<script>
import { postMethodWarning, showMsg } from '@/common/js/util.js';
import ConfigPopUp from '@/components/dataflow/configPopUp.vue';
import MonitoringConfig from '@/pages/DataAccess/Details/components/Monitoring/MonitoringConfig';

export default {
  components: {
    ConfigPopUp,
    MonitoringConfig,
  },
  props: {
    isShowPopUp: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      alertConfigInfo: {
        active: false,
        monitor_config: {
          no_data: {},
          data_drop: {},
          data_trend: {},
          data_time_delay: {},
          process_time_delay: {},
          data_interrupt: {},
          task: {},
        },
        trigger_config: {
          alert_threshold: 1,
          duration: 1,
        },
        convergence_config: {},
        notify_config: [],
        receivers: [],
      },
      isFirstLoading: false,
      submitLoading: false,
      position: {
        top: 200,
      },
      isScroll: false,
    };
  },
  watch: {
    '$route.params.fid': {
      immediate: true,
      handler(val) {
        if (val) {
          this.getAlertConfigByFlowID();
        }
      },
    },
  },
  methods: {
    changeUseStatus(val) {
      this.isScroll = val;
      this.position.top = this.isScroll ? 123 : 200;
    },
    alertConfigSubmit(options) {
      this.submitLoading = true;
      this.bkRequest
        .httpRequest('dmonitorCenter/updateAlertConfigByFlowID', options)
        .then(res => {
          if (res.result) {
            showMsg(this.$t('保存告警配置成功'), 'success', { delay: 2000 });
            this.getAlertConfigByFlowID();
          } else {
            postMethodWarning(res.message, res.code, { delay: 2000 });
          }
        })
        ['finally'](() => {
          this.submitLoading = false;
          this.close();
        });
    },
    close() {
      this.$emit('close');
    },
    getAlertConfigByFlowID() {
      this.isFirstLoading = true;
      const params = {
        flow_id: this.$route.params.fid,
      };
      return this.bkRequest.httpRequest('dmonitorCenter/getAlertConfigByFlowID', { params }).then(res => {
        if (res && res.result) {
          this.alertConfigInfo = res.data;
        } else {
          res && this.getMethodWarning(res.message, res.code);
        }
        this.isFirstLoading = false;
      });
    },
  },
};
</script>

<style lang="scss" scoped>
.alert-config-container {
  max-height: 600px;
}
</style>

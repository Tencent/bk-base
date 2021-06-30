

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
  <div class="alert-data-form">
    <div class="alert-switch-container-line">
      <bkdata-checkbox v-model="monitorStatusValue"
        :trueValue="'on'"
        :falseValue="'off'"
        :disabled="disabled">
        <span>{{ $t('任务执行异常') }}</span>
      </bkdata-checkbox>
    </div>
    <div class="alert-sub-rule">
      <div class="alert-switch-container">
        <span>- {{ $t('通用任务异常') }}</span>
        <span v-bk-tooltips.right="alertRuleDescription['task']"
          class="alert-rule-tooltip">
          <i class="bk-icon icon-info-circle-shape" />
        </span>
      </div>
      <div class="alert-rule-content">
        <i18n path="通用任务持续"
          class="flex"
          tag="div">
          <bkdata-input v-model="noMetricsInterval"
            place="minute"
            style="width: 100px"
            type="number"
            :max="100080"
            :min="1"
            :disabled="disabled" />
        </i18n>
      </div>
    </div>
    <div class="alert-sub-rule">
      <div class="alert-switch-container">
        <span>- {{ $t('离线任务异常') }}</span>
        <span v-bk-tooltips.right="alertRuleDescription['batch_exception']"
          class="alert-rule-tooltip">
          <i class="bk-icon icon-info-circle-shape" />
        </span>
      </div>
      <div class="alert-rule-content">
        <span>{{ $t('离线任务在调度或执行过程中出现如下状态') }}:</span>
        <bkdata-selector style="width: 160px"
          :displayKey="'name'"
          :list="batchStatusList"
          :settingKey="'key'"
          :placeholder="$t('任务状态')"
          :selected.sync="value.batch_exception_status"
          :disabled="disabled"
          :optionTip="true"
          :toolTipTpl="formatBatchStatus"
          :multiSelect="true" />
      </div>
    </div>
  </div>
</template>

<script>
import { alertRuleDescription } from '@/common/js/dmonitorCenter.js';
import mixins from '../alert.mixin.js';
export default {
  mixins: [mixins],
  data() {
    return {
      alertRuleDescription: alertRuleDescription,
      noMetricsInterval: 0,
      batchStatusList: [
        {
          name: this.$t('任务禁用'),
          key: 'disabled',
          description: this.$t('前置节点执行失败'),
        },
        {
          name: this.$t('任务失败'),
          key: 'failed',
          description: this.$t('任务执行过程中出现异常'),
        },
        {
          name: this.$t('任务部分成功'),
          key: 'failed_succeeded',
          description: this.$t('任务无输出_前序任务没有数据或当前任务计算结果无数据'),
        },
        {
          name: this.$t('任务无效'),
          key: 'skipped',
          description: this.$t('前序任务不满足执行条件_请等下一个周期'),
        },
      ],
    };
  },
  watch: {
    noMetricsInterval: {
      handler(value) {
        // eslint-disable-next-line vue/no-mutating-props
        this.value.no_metrics_interval = value * 60;
        this.$emit('input', this.value);
      },
    },
    value: {
      immediate: true,
      handler(val) {
        if (!val.batch_exception_status) {
          this.value.batch_exception_status = [];
        }
        this.noMetricsInterval = parseInt(this.value.no_metrics_interval / 60);
      },
    },
  },
  mounted() {
    this.bkRequest.httpRequest('dmonitorCenter/getJobStatusConfigs').then(res => {
      if (res.result) {
        let status_mappings = {};
        for (let status_config of res.data) {
          status_mappings[status_config.status_id] = status_config;
        }
        for (let status of this.batchStatusList) {
          if (status.key in status_mappings) {
            status.name = status_mappings[status.key].status_alias;
            status.description = status_mappings[status.key].description;
          }
        }
      }
    });
  },
  methods: {
    formatBatchStatus(option) {
      return `${option.description}`;
    },
  },
};
</script>

<style lang="scss" scoped>
@import './scss/alertRuleForm';
</style>

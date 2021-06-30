

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
    <div class="alert-switch-container">
      <bkdata-checkbox v-model="monitorStatusValue"
        :trueValue="'on'"
        :falseValue="'off'"
        :disabled="disabled">
        <span>{{ $t('离线任务延迟') }}</span>
      </bkdata-checkbox>
      <span v-bk-tooltips.right="alertRuleDescription['batch_delay']"
        class="alert-rule-tooltip">
        <i class="bk-icon icon-info-circle-shape" />
      </span>
    </div>
    <div class="alert-rule-content">
      <i18n path="离线任务延迟超过hour小时minute分钟"
        class="flex"
        tag="div">
        <bkdata-input v-model="delayTimeHour"
          place="hour"
          style="width: 60px; margin: 0 5px"
          type="number"
          :max="24"
          :min="1"
          :disabled="disabled"
          @change="changeExecuteDelay(...arguments, 'hour')" />
        <bkdata-input v-model="delayTimeMinute"
          place="minute"
          style="width: 60px; margin: 0 5px"
          type="number"
          :max="59"
          :min="0"
          :disabled="disabled"
          @change="changeExecuteDelay(...arguments, 'min')" />
      </i18n>
    </div>
  </div>
</template>

<script>
import { alertRuleDescription } from '@/common/js/dmonitorCenter.js';
import mixins from '../alert.mixin.js';
export default {
  mixins: [mixins],
  model: {
    prop: 'value',
    event: 'change',
  },
  data() {
    return {
      alertRuleDescription: alertRuleDescription,
      delayTimeHour: 0,
      delayTimeMinute: 0,
    };
  },
  watch: {
    value: {
      immediate: true,
      handler(val) {
        const delayTime = parseInt((this.value.schedule_delay + this.value.execute_delay) / 60);
        this.delayTimeHour = parseInt(delayTime / 60);
        this.delayTimeMinute = delayTime % 60;
      },
    },
  },
  methods: {
    changeExecuteDelay(value, event, type) {
      const difference = 3600;
      const executeDelay = type === 'hour'
        ? value * 3600 + this.delayTimeMinute * 60 - difference
        : this.delayTimeHour * 3600 + value * 60 - difference;

      this.value.execute_delay = executeDelay;
      this.$emit('change', this.value);
    },
  },
};
</script>

<style lang="scss" scoped>
@import './scss/alertRuleForm';
</style>



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
        <span>{{ $t('数据源无数据') }}</span>
      </bkdata-checkbox>
      <span v-bk-tooltips.right="alertRuleDescription['no_data']"
        class="alert-rule-tooltip">
        <i class="bk-icon icon-info-circle-shape" />
      </span>
    </div>
    <div class="alert-rule-content">
      <i18n path="数据源持续分钟没有输入数据"
        class="flex"
        tag="div">
        <bkdata-input v-model="noDataInterval"
          place="m"
          style="width: 100px; margin: 0 5px"
          type="number"
          :max="100080"
          :min="1"
          :disabled="disabled" />
      </i18n>
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
      noDataInterval: 0,
    };
  },
  watch: {
    noDataInterval: {
      handler(value) {
        // eslint-disable-next-line vue/no-mutating-props
        this.value.no_data_interval = value * 60;
        this.$emit('input', this.value);
      },
    },
    value: {
      immediate: true,
      handler(val) {
        this.noDataInterval = parseInt(this.value.no_data_interval / 60);
      },
    },
  },
};
</script>

<style lang="scss" scoped>
@import './scss/alertRuleForm';
</style>

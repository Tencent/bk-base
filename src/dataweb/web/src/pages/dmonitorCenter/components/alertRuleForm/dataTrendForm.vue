

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
        <span>{{ $t('数据波动') }}</span>
      </bkdata-checkbox>
      <span v-bk-tooltips.right="alertRuleDescription['data_trend']"
        class="alert-rule-tooltip">
        <i class="bk-icon icon-info-circle-shape" />
      </span>
    </div>
    <div class="alert-rule-content">
      <i18n path="数据相比小时前同时刻"
        class="flex"
        tag="div">
        <bkdata-input v-model="diffPeriod"
          place="a"
          style="width: 100px; margin: 0 5px"
          type="number"
          :max="100080"
          :min="1"
          :disabled="disabled" />
        <bkdata-selector place="b"
          style="width: 80px; margin: 0 5px"
          :displayKey="'name'"
          :list="trendList"
          :settingKey="'key'"
          :placeholder="$t('趋势')"
          :selected.sync="value.diff_trend"
          :disabled="disabled" />
        <bkdata-input v-model="diffCount"
          place="c"
          style="width: 80px; margin: 0 5px"
          type="number"
          :max="maxPercent"
          :min="minPercent"
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
      diffCount: 0,
      diffPeriod: 0,
      trendList: [
        {
          name: this.$t('减少'),
          key: 'decrease',
        },
        {
          name: this.$t('增长'),
          key: 'increase',
        },
        {
          name: this.$t('变化'),
          key: 'both',
        },
      ],
    };
  },
  computed: {
    maxPercent() {
      if (this.value.diff_trend === 'decrease') {
        return 100;
      } else {
        return 10000;
      }
    },
    minPercent() {
      return 1;
    },
  },
  watch: {
    diffCount: {
      handler(value) {
        this.value.diff_count = parseInt(value);
        this.$emit('input', this.value);
      },
    },
    diffPeriod: {
      handler(value) {
        this.value.diff_period = parseInt(value);
        this.$emit('input', this.value);
      },
    },
    value: {
      immediate: true,
      handler(val) {
        this.diffCount = this.value.diff_count;
        this.diffPeriod = this.value.diff_period;
        if (!val.diff_trend) {
          this.value.diff_trend = '';
        }
      },
    },
  },
};
</script>

<style lang="scss" scoped>
@import './scss/alertRuleForm';
</style>

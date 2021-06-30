

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
      <span>{{ $t('触发条件') }}</span>
      <span v-bk-tooltips.right="$t('解决瞬时抖动导致的告警泛滥')"
        class="alert-rule-tooltip">
        <i class="bk-icon icon-info-circle-shape" />
      </span>
    </div>
    <div class="alert-rule-content">
      <i18n :path="desLan"
        class="flex"
        tag="div">
        <bkdata-input v-model="duration"
          place="b"
          :disabled="disabled"
          style="width: 100px; margin: 0 5px"
          type="number"
          :max="100080"
          :min="1" />
        <bkdata-input v-model="alertThreshold"
          place="a"
          :disabled="disabled"
          style="width: 100px; margin: 0 5px"
          type="number"
          :max="100080"
          :min="1" />
      </i18n>
    </div>
  </div>
</template>

<script>
export default {
  props: {
    value: {
      type: Object,
    },
    disabled: {
      type: Boolean,
      default: false,
    },
    desLan: {
      type: String,
      default: '分钟内满足次检测算法',
    },
  },
  data() {
    return {
      duration: this.value.duration,
      alertThreshold: this.value.alert_threshold,
    };
  },
  watch: {
    duration: {
      handler(value) {
        // eslint-disable-next-line vue/no-mutating-props
        this.value.duration = parseInt(value);
        this.$emit('input', this.value);
      },
    },
    alertThreshold: {
      handler(value) {
        // eslint-disable-next-line vue/no-mutating-props
        this.value.alert_threshold = parseInt(value);
        this.$emit('input', this.value);
      },
    },
  },
};
</script>

<style lang="scss" scoped>
@import '../alertRuleForm/scss/alertRuleForm';
</style>

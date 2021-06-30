

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
  <div class="flink-basic-wrapper"
    style="width: 100%">
    <WindowForm
      ref="fixedWindow"
      v-model="paramsForWindow"
      :isShowDependencyConfig="isShowDependencyConfig"
      :parentConfig="parentConfig"
      :isSetDisable="isSetDisable"
      :selfConfig="selfConfig" />
  </div>
</template>

<script>
import WindowForm from '../OfflineNode/Fixed.window';

export default {
  components: {
    WindowForm,
  },
  props: {
    params: {
      type: Object,
      default: () => ({}),
    },
    parentConfig: {
      type: [Object, Array],
      default: () => ({}),
    },
    selfConfig: {
      type: Object,
      default: () => ({}),
    },
    isShowDependencyConfig: {
      type: Boolean,
      default: true,
    },
    isSetDisable: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      paramsForWindow: {},
    };
  },
  watch: {
    paramsForWindow(value, old) {
      if (value !== old) {
        this.$emit('update: params', value);
      }
    },
  },
  created() {
    this.$set(this.paramsForWindow, 'config', this.params);
  },
  methods: {
    validateForm() {
      return this.$refs.fixedWindow.validateForm().then(
        validator => {
          return true;
        },
        validator => {
          return false;
        }
      );
    },
  },
};
</script>

<style lang="scss" scoped>
.flink-basic-wrapper {
  ::v-deep .bk-form-item {
    width: 100% !important;
  }
}
</style>

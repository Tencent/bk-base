

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
  <div v-bkloading="{ isLoading: isLoading }"
    class="edit-input-wrap">
    <div class="bk-form-item pl0">
      <div v-if="!isShowInput"
        class="bk-form-content text-overflow"
        :title="inputValue">
        {{ inputValue }}
      </div>
      <div v-else
        class="bk-form-content text-overflow">
        <bkdata-input ref="input"
          v-model="value"
          :maxlength="50"
          class="editing"
          @focus="value = inputValue"
          @blur="changeValue" />
      </div>
    </div>
    &nbsp;&nbsp;
    <i v-if="!isShowInput"
      v-bk-tooltips="toolTipsContent"
      class="bk-icon icon-edit"
      :class="{ 'is-disabled': !isEdit }"
      @click="showInput" />
  </div>
</template>
<script>
import Bus from '@/common/js/bus.js';

export default {
  name: 'EditInput',
  props: {
    inputValue: {
      type: String,
      default: '',
    },
    rowData: {
      type: Object,
      default: () => ({}),
    },
    toolTipsContent: {
      type: String,
      default: '',
    },
    isEdit: {
      type: Boolean,
      default: true,
    },
    contentType: {
      type: String,
      default: '',
    },
    isCancelFieldLoading: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      isShowInput: false,
      value: '',
      isLoading: false,
    };
  },
  watch: {
    inputValue(val) {
      if (val === this.value) {
        this.isLoading = false;
      }
    },
    isCancelFieldLoading(val) {
      if (val) {
        this.isLoading = false;
      }
    },
  },
  methods: {
    showInput() {
      if (!this.isEdit) return;
      this.isShowInput = true;
      this.$nextTick(() => {
        this.$refs.input.focus();
      });
    },
    changeValue() {
      // value未修改或者value为空，提前返回
      // if (this.value === this.inputValue || !this.value.length) {
      if (this.value === this.inputValue) {
        this.isShowInput = false;
        return;
      }
      this.isShowInput = false;
      this.isLoading = true;
      this.$emit('change', this.value, this.rowData, this.contentType);
    },
  },
};
</script>
<style lang="scss" scoped>
.edit-input-wrap {
  display: flex;
  align-items: center;
  .bk-form-item {
    width: calc(100% - 34px);
    .bk-form-content {
      margin-left: 0;
    }
  }
  .icon-edit {
    cursor: pointer;
    color: #3a84ff;
  }
}
.is-disabled {
  color: #666 !important;
  opacity: 0.6 !important;
  cursor: not-allowed !important;
}
</style>

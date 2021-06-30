

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
  <bkdata-popover
    ref="textarea"
    class="textarea-popover"
    placement="right-end"
    theme="light package-input"
    trigger="manual"
    :zIndex="1001"
    :tippyOptions="{
      interactive: true,
      hideOnClick: false,
    }">
    <bkdata-input
      v-model="localValue"
      type="textarea"
      :class="{ isError: reg.isError }"
      :placeholder="$t('请输入库的名称，例如 pydotplus，一行一个库\r\n如需指定版本，请输入pydotplus==2.0.2')"
      @focus="handleFocus"
      @blur="handleBlur" />
    <p v-if="reg.isError"
      class="error-tips">
      {{ reg.tips }}
    </p>
    <!--eslint-disable vue/no-v-html-->
    <div
      slot="content"
      v-html="$t('请输入库的名称，例如 pydotplus，一行一个库<br>如需指定版本，请输入pydotplus==2.0.2')" />
  </bkdata-popover>
</template>

<script>
export default {
  props: {
    value: {
      type: [String, Number],
      default: '',
    },
    reg: {
      type: Object,
      default: () => ({
        isError: false,
        tips: '',
      }),
    },
  },
  data() {
    return {
      localValue: '',
    };
  },
  watch: {
    value(val) {
      this.localValue = val;
    },
    localValue(val) {
      const instance = this.$refs.textarea.instance;
      const state = instance.state;
      if (!state.isVisible && val) {
        instance.show();
      } else if (!val && state.isVisible) {
        instance.hide();
      }
      this.$emit('input', val);
    },
  },
  methods: {
    handleFocus() {
      this.localValue && this.$refs.textarea.instance.show();
    },
    handleBlur() {
      this.$refs.textarea.instance.hide();
    },
  },
};
</script>

<style lang="scss" scoped>
.textarea-popover {
  ::v-deep {
    .isError {
      font-size: 20px;
      .bk-textarea-wrapper {
        border-color: #ea3636 !important;
      }
    }
    .bk-form-textarea {
      min-height: 60px;
    }
  }
  .error-tips {
    font-size: 12px;
    color: #ea3636;
    padding-top: 2px;
  }
}
</style>

<style lang="scss">
.package-input-theme {
  top: -16px;
}
</style>

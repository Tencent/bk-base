

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
  <bkdata-popconfirm ref="customPop"
    trigger="click"
    confirmText=""
    cancelText=""
    v-bind="$attrs">
    <div slot="content"
      class="custom-pop-content">
      <slot name="content" />
      <div class="btns">
        <bkdata-button
          class="mr10"
          theme="primary"
          size="small"
          :disabled="confirmLoading"
          :loading="confirmLoading"
          @click="handleConfirm">
          {{ $t('确定') }}
        </bkdata-button>
        <bkdata-button theme="default"
          size="small"
          @click="handleCancel">
          {{ $t('取消') }}
        </bkdata-button>
      </div>
    </div>
    <slot />
  </bkdata-popconfirm>
</template>

<script>
export default {
  props: {
    confirmFn: {
      type: Function,
      default: () => true,
    },
    cancelFn: {
      type: Function,
      default: () => true,
    },
    confirmLoading: {
      type: Boolean,
      default: false,
    },
    isEmit: {
      type: Boolean,
      default: false,
    },
  },
  methods: {
    async handleConfirm() {
      if (this.isEmit) {
        this.$emit('confirm');
        this.hideHandler();
      } else {
        let close = true;
        if (typeof this.confirmFn === 'function') {
          close = await this.confirmFn();
        }
        close && this.hideHandler();
      }
    },
    handleCancel() {
      typeof this.cancelFn === 'function' && this.cancelFn();
      this.hideHandler();
    },
    hideHandler() {
      const customPop = this.$refs.customPop;
      customPop && customPop.cancel();
    },
  },
};
</script>

<style lang="scss" scoped>
.custom-pop-content {
  .btns {
    font-size: 0;
    text-align: right;
    margin-bottom: 9px;
    .bk-button {
      min-width: 60px;
      width: 60px;
      height: 24px;
      line-height: 22px;
    }
  }
}
</style>



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
  <bkdata-popover ref="operateBtnPopover"
    placement="top"
    trigger="manual"
    theme="light">
    <span class="operator-btn operator-tool"
      @click="openWin()">
      <slot />
    </span>
    <div slot="content">
      <div :class="{ 'has-message': hasMessage }"
        class="confirm-window">
        <textarea v-if="hasMessage"
          v-model="message"
          class="bk-form-textarea mb5"
          :placeholder="$t('请输入理由')" />

        <bkdata-button theme="primary"
          size="small"
          class="ml10"
          @click="confirm()">
          {{ $t('确定') }}
        </bkdata-button>
        <bkdata-button theme="default"
          size="small"
          class="ml10"
          @click="closeWin()">
          {{ $t('取消') }}
        </bkdata-button>
      </div>
    </div>
  </bkdata-popover>
</template>

<script>
export default {
  props: {
    id: {
      required: true,
      type: String,
      default: '',
    },
    hasMessage: {
      type: Boolean,
      default: false,
    },
    defaultValue: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      showWindow: false,
      message: this.defaultValue,
    };
  },
  methods: {
    closeAllWin() {
      for (let confirmWindow of this.$parent.$children) {
        if (confirmWindow.$el.className.includes('auth-operate-btn')) {
          confirmWindow.showWindow = false;
        }
      }
    },
    openWin() {
      // 打开前先关闭其他窗口
      this.closeAllWin();
      this.showWindow = true;
      this.$refs.operateBtnPopover.instance.show();
    },
    closeWin() {
      this.showWindow = false;
      this.$refs.operateBtnPopover.instance.hide();
    },
    confirm() {
      this.showWindow = false;
      this.$refs.operateBtnPopover.instance.hide();
      this.$emit('confirm', { id: this.id, message: this.message });
    },
  },
};
</script>
<style lang="scss">
.operator-btn {
  &.operator-tool {
    margin-left: 15px;
    color: #3a84ff;
    cursor: pointer;
  }
}
</style>

<style lang="scss" scoped>
.auth-operate-btn {
  position: relative;
  cursor: pointer;

  .confirm-window {
    position: absolute;
    z-index: 100;
    top: -48px;
    width: 145px;
    padding: 8px 10px;
    border: 1px solid #ccc;
    box-shadow: 1px 1px 9px 1px #6968b8;
    background: #fff;
    text-align: center;

    &.has-message {
      top: -102px;
      left: -150px;
      width: 300px;
    }

    .bk-form-textarea {
      min-height: 50px;
    }
  }

  .confirm-window:after {
    position: absolute;
    top: 100%;
    left: 50%;
    content: '';
    border: 5px solid #fff;
    border-color: #fff transparent transparent transparent;
  }
}
</style>

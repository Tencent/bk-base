

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
  <bkdata-dialog
    v-model="isShow"
    :extCls="`${customClass} ${isHiddenConfirmBtn ? 'hidden-confirm-btn' : ''}`"
    :position="position"
    :autoClose="false"
    :hasHeader="false"
    :okText="okText"
    :cancelText="cancelText"
    :closeIcon="true"
    :maskClose="false"
    :showFooter="isShowFooter"
    :height="height"
    :width="width"
    @confirm="confirm"
    @cancel="cancel">
    <div class="hearder">
      <div class="text fl">
        <p class="title">
          <span :class="[icon]" />{{ title }}
        </p>
        <p>{{ des }}</p>
      </div>
    </div>
    <slot />
  </bkdata-dialog>
</template>

<script>
export default {
  props: {
    title: {
      type: String,
      default: '',
    },
    icon: {
      type: String,
      default: '',
    },
    des: {
      type: String,
      default: '',
    },
    customClass: {
      type: String,
      default: '',
    },
    okText: {
      type: String,
      default: window.$t('确定'),
    },
    cancelText: {
      type: String,
      default: window.$t('取消'),
    },
    isHiddenConfirmBtn: {
      type: Boolean,
      default: false,
    },
    height: {
      type: [Number, String],
      default: 'auto',
    },
    width: {
      type: [Number, String],
      default: 400,
    },
    position: {
      type: Object,
      default: () => ({}),
    },
    isShowFooter: {
      type: Boolean,
      default: true,
    },
    isShowPopUp: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      isShow: false,
    };
  },
  watch: {
    isShowPopUp: {
      immediate: true,
      handler(val) {
        this.isShow = val;
      },
    },
  },
  methods: {
    confirm() {},
    cancel() {
      this.$emit('close');
    },
  },
};
</script>

<style lang="scss" scoped>
::v-deep .hidden-confirm-btn {
  .footer-wrapper {
    button[name='confirm'] {
      display: none;
    }
  }
}
::v-deep .bk-dialog {
  .bk-dialog-body {
    padding: 0 !important;
    margin-top: -30px;
    display: inline-block;
    width: 100%;
  }
}
.hearder {
  height: 70px;
  background: #23243b;
  position: relative;
  .close {
    display: inline-block;
    position: absolute;
    right: 0;
    top: 0;
    width: 40px;
    height: 40px;
    line-height: 40px;
    text-align: center;
    cursor: pointer;
  }
  .icon {
    font-size: 32px;
    color: #abacb5;
    line-height: 60px;
    width: 142px;
    text-align: right;
    margin-right: 16px;
  }
  .text {
    margin-left: 28px;
    font-size: 12px;
  }
  .title {
    margin: 16px 0 3px 0;
    padding-left: 36px;
    position: relative;
    color: #fafafa;
    font-size: 18px;
    text-align: left;
    height: 20px;
    line-height: 20px;
    border-left: 4px solid #3a84ff;
    span {
      position: absolute;
      left: 10px;
      top: 2px;
    }
  }
}
</style>

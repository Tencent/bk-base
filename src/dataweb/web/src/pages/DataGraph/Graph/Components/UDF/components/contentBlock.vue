

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
  <div class="content-block-wrapper">
    <div class="block-left">
      <p>{{ header }}</p>
      <p v-if="headRequired"
        class="head-required">
        *
      </p>
    </div>
    <div class="block-content"
      :style="`padding-bottom: ${paddingBottom}; padding-top: ${noPadding ? 0 : '25px'}`">
      <slot />
    </div>
  </div>
</template>
<script>
export default {
  props: {
    header: {
      type: String,
      default: '',
    },
    headRequired: {
      type: Boolean,
      default: false,
    },
    noPadding: {
      type: Boolean,
      default: false,
    },
    paddingBottom: {
      type: String,
      default: '0px',
    },
  },
};
</script>
<style lang="scss" scoped>
.content-block-wrapper {
  width: 883px;
  display: flex;
  justify-content: flex-start;
  margin-bottom: 15px;
  .block-left {
    width: 120px;
    background: rgba(250, 251, 253, 1);
    border: 1px solid rgba(220, 222, 229, 1);
    display: flex;
    justify-content: center;
    align-items: center;
    .head-required {
      font-size: 14px;
      font-family: MicrosoftYaHeiUI-Bold;
      font-weight: bold;
      color: rgba(234, 54, 54, 1);
      line-height: 18px;
    }
  }
  .block-content {
    width: 763px;
    background: rgba(255, 255, 255, 1);
    border: 1px solid rgba(220, 222, 229, 1);
    border-left: 0;
    padding-top: 25px;
    .block-item {
      display: flex;
      justify-content: flex-start;
      align-items: center;
      margin-bottom: 19px;
      span {
        font-size: 12px;
        font-family: MicrosoftYaHeiUI;
        color: rgba(151, 155, 165, 1);
        line-height: 15px;
      }
      .desp {
        flex: 114px 0 0;
        text-align: right;
        margin-right: 16px;
        width: 56px;
        height: 18px;
        font-size: 14px;
        font-family: MicrosoftYaHeiUI;
        color: rgba(99, 101, 110, 1);
        line-height: 18px;
      }
      .required::after {
        content: '*';
        color: red;
        display: inline-block;
        vertical-align: middle;
      }
      .pub-desp {
        flex: 146px 0 0;
        text-align: right;
        margin-right: 20px;
        width: 56px;
        height: 18px;
        font-size: 14px !important;
        font-family: MicrosoftYaHeiUI !important;
        color: rgba(99, 101, 110, 1) !important;
        line-height: 18px;
        &.lh28 {
          line-height: 28px;
        }
      }
      .content {
        font-size: 14px;
        font-family: MicrosoftYaHeiUI;
        color: rgba(49, 50, 56, 1);
        line-height: 18px;
      }
    }
    .block-verticle-item {
      display: block;
    }
  }
}
</style>

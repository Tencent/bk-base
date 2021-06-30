

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
  <div class="header-type-container clearfix"
    :class="{ padding: isPaddding }">
    <div class="type"
      :class="{ 'left-border': isShowLeft }">
      <div class="title-container">
        <slot name="title">
          <span>
            {{ title }}
            <i v-if="tipsContent"
              v-bk-tooltips="tipsContent"
              class="icon-question-circle color-blue" />
          </span>
          <div class="title-right">
            <slot name="title-right" />
          </div>
        </slot>
      </div>
    </div>
    <div class="content-container">
      <slot />
    </div>
  </div>
</template>

<script>
export default {
  props: {
    title: {
      type: String,
      default: '',
    },
    tipsContent: {
      type: String,
      default: '',
    },
    isShowLeft: {
      type: Boolean,
      default: true,
    },
    isPaddding: {
      type: Boolean,
      default: true,
    },
  },
};
</script>

<style lang="scss" scoped>
.padding {
  padding: 0 20px 20px;
}
.header-type-container {
  .type {
    display: flex;
    align-items: center;
    height: 55px;
    line-height: 55px;
    font-weight: 700;
    .title-container {
      width: calc(100% - 15px);
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
  }
  .left-border {
    &::before {
      content: '';
      position: relative;
      width: 2px;
      height: 19px;
      background: #3a84ff;
      display: inline-block;
      margin-right: 15px;
    }
  }
  .color-blue {
    color: #3a84ff;
  }
  .content-container {
    box-shadow: 0 0 10px 0 rgba(33, 34, 50, 0.05);
    padding: 15px;
    .chart-container {
      position: relative;
      display: flex;
      justify-content: space-around;
    }
  }
}
</style>

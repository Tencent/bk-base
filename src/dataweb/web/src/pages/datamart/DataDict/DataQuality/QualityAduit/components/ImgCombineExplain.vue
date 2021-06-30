

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
  <div class="container">
    <span class="icon-wrapper">
      <i :class="[`icon-${iconClass}`]" />
    </span>
    <div class="explain">
      <div class="text-word text-overflow">
        {{ explain }}
      </div>
      <div :title="count"
        class="text-number text-overflow">
        {{ count }}
      </div>
    </div>
  </div>
</template>

<script>
export default {
  props: {
    count: {
      type: [Number, String],
      default: 100,
    },
    explain: {
      type: String,
      default: '',
    },
    iconClass: {
      type: String,
      default: '',
    },
  },
};
</script>

<style lang="scss" scoped>
.container {
  display: flex;
  align-items: flex-start;
  justify-content: space-around;
  width: 150px;
  .icon-wrapper {
    width: 80px;
    height: 80px;
    border: 2px solid #ddd;
    border-radius: 50%;
    display: flex;
    align-items: center;
    background-color: rgb(235, 235, 235);
    [class^='icon-'] {
      display: block;
      width: 80px;
      font-size: 60px;
      transform: scale(0.8);
    }
  }
  .explain {
    display: flex;
    flex-direction: column;
    justify-content: space-around;
    width: calc(100% - 50px);
    height: 100%;
    padding-left: 5px;
    font-size: 13px;
    font-weight: 530;
  }
}
</style>

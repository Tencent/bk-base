

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
  <div :class="['bk-spin-loading', styleMap[type], sizeMap[size]]">
    <div class="rotate rotate1" />
    <div class="rotate rotate2" />
    <div class="rotate rotate3" />
    <div class="rotate rotate4" />
    <div class="rotate rotate5" />
    <div class="rotate rotate6" />
    <div class="rotate rotate7" />
    <div class="rotate rotate8" />
  </div>
</template>

<script>
export default {
  props: {
    size: {
      type: String,
      default: 'normal',
    },
    type: {
      type: String,
      default: 'primary',
    },
  },
  data() {
    return {
      sizeMap: {
        mini: 'bk-spin-loading-mini',
        small: 'bk-spin-loading-small',
        normal: '',
        large: 'bk-spin-loading-large',
      },
      styleMap: {
        primary: 'bk-spin-loading-primary',
        success: 'bk-spin-loading-success',
        danger: 'bk-spin-loading-danger',
        warning: 'bk-spin-loading-warning',
        white: 'bk-spin-loading-white',
      },
    };
  },
};
</script>

<style lang="scss" scoped>
.bk-spin-loading {
  margin-bottom: 1px;
}
</style>

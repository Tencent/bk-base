

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
  <div class="bar-wrap">
    <div ref="inner"
      :style="{ width: innerValue + 'px' }"
      class="inner-bar" />
  </div>
</template>
<script>
export default {
  props: {
    innerValue: {
      type: Number,
      default: 20,
    },
  },
  data() {
    return {
      innerWidth: 0,
    };
  },
  watch: {
    innerValue(val) {
      const barWidth = 120;
      if (val / barWidth < 0) {
        this.innerWidth = (barWidth * val) / barWidth;
      } else {
        this.innerWidth = barWidth;
      }
    },
  },
};
</script>
<style lang="scss" scoped>
.bar-wrap {
  width: 120px;
  background: #dddddd;
  height: 20px;
  border-radius: 5px;
  .inner-bar {
    background: #3a84ff;
    height: 100%;
  }
}
</style>

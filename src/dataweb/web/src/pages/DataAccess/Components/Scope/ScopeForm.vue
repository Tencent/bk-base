

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
  <div :class="[(showWrapper && 'bk-access-scope') || '']"
    :style="calcStyle">
    <div v-if="showWrapper"
      class="bk-mark-corner bk-mark-left">
      <p class="coner-mark">
        {{ step }}
      </p>
    </div>
    <slot />
  </div>
</template>
<script>
export default {
  props: {
    showWrapper: {
      type: Boolean,
      default: true,
    },
    step: {
      type: Number,
      default: 1,
    },
    width: {
      type: [String, Number],
      default: 789,
    },

    height: {
      type: [String, Number],
      default: 100,
    },
  },
  computed: {
    calcWidth() {
      return Number.isFinite(this.width) ? `${this.width}px` : this.width;
    },
    calcHeight() {
      return Number.isFinite(this.height) ? `${this.height}px` : this.height;
    },
    calcStyle() {
      return (
        (this.showWrapper && {
          width: this.calcWidth,
          height: this.calcHeight,
        })
        || {}
      );
    },
  },
};
</script>
<style lang="scss" scoped>
.bk-access-scope {
  background: #f5f5f5;
  position: relative;
  padding: 15px 15px 15px 0px;
  min-height: 90px;

  .bk-mark-corner {
    &.bk-mark-left {
      left: 0;
      top: 0;
    }
  }
}
</style>

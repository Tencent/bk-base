/** * 画布右侧缩放工具栏 */


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
  <div class="auxiliary-tools">
    <div class="tools-list clearfix">
      <ul class="list">
        <li :title="$t('放大')"
          @click="zoomIn('in')">
          <i class="bk-icon icon-plus-circle" />
          <span>
            {{ value.toFixed(2) }}
          </span>
        </li>
        <li :title="$t('缩小')"
          @click="zoomIn('out')">
          <i class="bk-icon icon-minus-circle" />
          <span>
            {{ value.toFixed(2) }}
          </span>
        </li>
        <li :title="$t('还原')"
          @click="zoomIn()">
          <i class="bk-icon icon-full-screen" />
        </li>
      </ul>
    </div>
  </div>
</template>

<script>
export default {
  name: 'flow-dock',
  props: {
    zoomValue: {
      type: Number,
      default: 0,
    },
  },
  computed: {
    value: {
      get() {
        return this.zoomValue;
      },
      set(newVal) {
        this.$emit('update:zoomValue', newVal);
      },
    },
  },
  methods: {
    zoomIn(type) {
      let el = 'canvas';
      if (type === 'in') {
        if (this.value > 1.3) return;
        this.value = this.value + 0.1;
      } else if (type === 'out') {
        if (this.value < 0.7) return;
        this.value = this.value - 0.1;
      } else {
        this.value = 1;
      }
      this.$emit('notifyZoom', el);
    },
  },
};
</script>

<style></style>

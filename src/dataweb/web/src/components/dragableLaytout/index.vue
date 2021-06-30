

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
  <div
    class="split-panel"
    :class="{ dragging: dragging, vert: layoutType == 'vertical', hori: layoutType == 'horizontal' }"
    @mousemove.stop.prevent="dragMove"
    @mouseup.stop.prevent="dragStop">
    <div class="split-panel-part"
      :style="layoutType == 'horizontal' ? { width: panelSize1 } : { height: panelSize1 }">
      <slot name="1" />
    </div>
    <div
      :class="['split-panel-gutter', { 'is-disabled': disabled }]"
      :style="splitPanelGutterStyle"
      @mousedown.stop.prevent="dragStart" />
    <div class="split-panel-part"
      :style="layoutType == 'horizontal' ? { width: panelSize2 } : { height: panelSize2 }">
      <slot name="2" />
    </div>
  </div>
</template>

<script>
export default {
  props: {
    /** 布局方式： horizontal / vertical */
    layout: {
      type: String,
      default: 'horizontal',
    },
    /** 分隔栏宽度， Number类型，单位px */
    gutter: {
      type: Number,
      default: 8,
    },
    /** 第一栏的初始宽/高度， 单位% */
    init: {
      type: Number,
      default: 50,
    },
    /** 第一栏的最小宽/高度 */
    min: {
      type: Number,
      default: 0,
    },
    /** 第一栏的最大宽/高度 */
    max: {
      type: Number,
      default: 100,
    },
    disabled: {
      type: Boolean,
      default: false,
    },
    splitColor: {
      type: String,
    },
  },
  data() {
    return {
      layoutType: this.layout,
      gutterSize: this.gutter,
      percent: this.init,
      minPercent: this.min,
      maxPercent: this.max,
      dragging: false,
      startPos: null,
    };
  },
  computed: {
    gutterSizeString() {
      return `${this.gutterSize}px`;
    },
    panelSize1() {
      if (this.percent < this.minPercent) {
        // eslint-disable-next-line vue/no-side-effects-in-computed-properties
        this.percent = this.minPercent;
      }
      return `calc(${this.percent}% - ${this.gutterSize}px)`;
    },
    panelSize2() {
      if (this.percent > this.maxPercent) {
        // eslint-disable-next-line vue/no-side-effects-in-computed-properties
        this.percent = this.maxPercent;
      }
      return `${100 - this.percent}%`;
    },
    splitPanelGutterStyle() {
      const style = {};
      if (this.layoutType === 'horizontal') {
        style.width = this.gutterSizeString;
      } else {
        style.height = this.gutterSizeString;
      }
      this.splitColor && (style.borderColor = this.splitColor);
      return style;
    },
  },
  methods: {
    dragStart(e) {
      if (this.disabled) return;
      this.dragging = true;
      this.startPos = this.layoutType === 'horizontal' ? e.pageX : e.pageY;
      this.startSplit = this.percent;
    },
    dragMove(e) {
      if (this.dragging) {
        const dx = (this.layoutType === 'horizontal' ? e.pageX : e.pageY) - this.startPos;
        const totalSize = this.layoutType === 'horizontal' ? this.$el.offsetWidth : this.$el.offsetHeight;
        this.percent = this.startSplit + Math.round((dx / totalSize) * 100);
      }
    },
    dragStop(e) {
      if (this.dragging) {
        this.dragging = false;
      }
    },
  },
};
</script>

<style lang="scss" scoped>
.split-panel {
  width: 100%;
  height: 100%;
  .split-panel-part {
    display: inline-block;
    float: left;
    overflow: auto;
  }
  .split-panel-gutter {
    display: inline-block;
    background: rgba(0, 0, 0, 0.2);
    z-index: 1;
    float: left;
    background-clip: padding-box;
    &.is-disabled {
      cursor: default !important;
    }
  }
  &.hori {
    .split-panel-part {
      height: 100%;
    }
    .split-panel-gutter {
      height: 100%;
      cursor: col-resize;
      border-left: 1px solid rgba(0, 0, 0, 0.2);
      border-right: 1px solid rgba(0, 0, 0, 0.2);
      &:hover,
      &:focus {
        border-left: 1px solid rgba(0, 0, 0, 0.8);
        border-right: 1px solid rgba(0, 0, 0, 0.8);
      }
    }
    .dragging {
      cursor: col-resize;
    }
  }
  &.vert {
    .split-panel-part {
      width: 100%;
    }
    .split-panel-gutter {
      width: 100%;
      cursor: row-resize;
      border-top: 1px solid rgba(0, 0, 0, 0.2);
      border-bottom: 1px solid rgba(0, 0, 0, 0.2);
      &:hover,
      &:focus {
        border-top: 1px solid rgba(0, 0, 0, 0.8);
        border-bottom: 1px solid rgba(0, 0, 0, 0.8);
      }
    }
    .dragging {
      cursor: row-resize;
    }
  }
}
</style>

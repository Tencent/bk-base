

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
  <div :class="columnClass"
    :style="columnStyle">
    <div :class="['vtable-cell-content', (textEllipsis && 'ellipsis') || '']"
      :title="title"
      :style="contentStyles">
      <slot />
    </div>
  </div>
</template>
<script lang="ts">
import { Component, Prop, Vue, Watch, Inject } from 'vue-property-decorator';

@Component({})
export default class BkDataVColumn extends Vue {
  name: string = 'BkDataVColumn';
  @Prop({ default: 'auto' }) width: string | undefined;

  @Prop({ default: '20px' }) minWidth: string | undefined;

  /** 放大倍数，默认为1，表示平均分配剩余空间，0表示不作缩放，固定宽度 */
  @Prop({ default: 1 }) grow: Number | undefined;

  /** 超出文本省略显示 */
  @Prop({ default: false }) textEllipsis: boolean | undefined;

  /** 用于设置超长文本Title */
  @Prop({ default: '' }) title: string | undefined;

  /** 高度 */
  @Prop({ default: '30px' }) height: string | undefined;

  /** 当前Cell是否被选中 */
  @Prop({ default: false }) selected: boolean | undefined;

  @Prop({ default: () => ({}) }) contentStyle: Object | undefined;

  get columnStyle() {
    return {
      width: this.width,
      height: this.height,
      lineHeight: this.height,
    };
  }

  get contentStyles() {
    return Object.assign({}, this.columnStyle, this.contentStyle || {});
  }

  get columnClass() {
    return `bkdata-vtable-cell grow${this.grow} ${(this.selected && 'selected') || ''}`;
  }
}
</script>
<style lang="scss">
.bkdata-vtable-cell {
  position: relative;
  height: 28px;
  line-height: 28px;
  font-size: 12px;
  // border-bottom: 1px solid #e8ecf2;

  // &.selected {
  //     background: #F5F9FF;
  //     border: 1px solid #F5F9FF;
  // }
  .vtable-cell-content {
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    height: 100%;
    background: transparent;
    padding: 0 10px;
    &.ellipsis {
      overflow: hidden;
      text-overflow: ellipsis;
    }
  }

  flex-grow: 1;

  $class-slug: grow !default;
  @for $i from 5 through 0 {
    &.#{$class-slug}#{$i} {
      flex-grow: $i;
    }
  }
}
</style>



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
    v-bkloading="{ isLoading: isLoading }"
    :class="['bkdata-v-table', (colBorder && 'col-border') || '', showColumnShadow && 'column-shadow']"
    :style="containerStyle">
    <template v-if="showHeader && !isEmpty">
      <div
        ref="headerRef"
        class="bkdata-vtable-header"
        :class="[(showColumnShadow && 'shadow-column') || '']"
        :style="headerStyle">
        <slot name="header" />
      </div>
    </template>
    <div
      ref="bkdataTableBody"
      class="bkdata-vtable-body"
      :class="bodyClass"
      :style="bodyStyle"
      @scroll="handleBodyScroll">
      <div v-show="!isEmpty"
        class="bkdata-vtable-content">
        <slot />
      </div>
      <div v-if="isEmpty"
        class="bk-vtable-empty">
        <i class="bk-table-empty-icon bk-icon icon-empty" />
        <div>{{ emptyText }}</div>
      </div>
    </div>
    <div v-show="showFooter"
      class="bkdata-vtable-footer"
      :style="footerStyle">
      <div class="footer-row">
        <slot name="footer" />
      </div>
    </div>
    <div
      v-if="isColumnShadow && showColumnShadow"
      class="column-hightlight-border"
      :style="calcAComunHightlightStyle" />
  </div>
</template>
<script lang="ts">
import { Component, Prop, Vue, Watch, Ref } from 'vue-property-decorator';
import BkDataVRow from './bkdata-v-row.vue';

@Component({ components: { BkDataVRow } })
export default class BkDataVTable extends Vue {
  name = 'BkDataVTable';

  /** 横向滚动offset left */
  bodyScrollLeft = 0;

  /** 选中列的高亮事件 */
  clomunHightlightStyle = {
    width: 0,
    left: 0,
    height: 'auto',
  };

  @Prop({ default: true }) highLightHoverRow: boolean;

  @Prop({ default: 'auto' }) height: string;

  /** slot使用了 virtualScroll 同步内部横轴位置*/
  @Prop({ default: 0 }) virtualScrolllLeft: number;

  /** Table body overflow Y 模式 */
  @Prop({ default: 'auto' }) overflowY: string;

  @Prop({ default: false }) isEmpty: boolean;

  @Prop({ default: window.$t('暂无数据') }) emptyText: string;

  @Prop({ default: false }) isLoading: boolean;

  @Prop({ default: true }) showHeader: boolean;

  @Prop({ default: false }) showFooter: boolean;

  @Prop({ default: 'auto' }) minWidth: string;

  @Prop({ default: false }) colBorder: boolean;

  @Prop({ default: '30px' }) headerHeight: string;

  @Prop({ default: '30px' }) footerHeight: string;

  /** 表格内容X轴偏移，用于高亮列 */
  @Prop({ default: 0 }) bodyOffsetX: number;

  /** 表格内容横向滚动偏离X */
  // @Prop({ default: 0 }) bodyScrollOffsetX: number

  /** 表格内容部分X轴是否滚动 */
  @Prop({ default: true }) scrollX: boolean;

  /** 表格内容部分Y轴是否滚动 */
  @Prop({ default: true }) scrollY: boolean;

  /** 是否高亮阴影列 */
  @Prop({ default: false }) showColumnShadow: boolean;

  /**
   *  选中的列序号
   *  只有当此数值大于等于0才会生效
   */
  @Prop({ default: null }) activeColumnIndex: number | null;

  @Watch('activeColumnIndex')
  handleActiveColumnChanged(val: null | number) {
    this.$nextTick(() => {
      this.computedHighlightColumnPosition(val);
    });
  }

  get isColumnShadow() {
    return this.activeColumnIndex !== null && this.activeColumnIndex >= 0;
  }

  get rowClass() {
    return `bkdata-vtable-row ${(this.highLightHoverRow && 'hover-row hover-highlight') || ''}`;
  }

  get bodyClass() {
    return `overflow-y-${this.overflowY} ${(this.showColumnShadow && 'shadow-column') || ''} ${
      (this.showHeader && 'show-header') || ''
    } ${(this.scrollX && 'bk-scroll-x') || ''} ${(this.scrollY && 'bk-scroll-y') || ''}`;
  }

  get bodyStyle() {
    return {
      height: `calc(100% - ${(this.showHeader && this.headerHeight) || '0px'} - ${
        (this.showColumnShadow && '17px') || '0px'
      } - ${(this.showFooter && this.footerHeight) || '0px'})`,
    };
  }

  get containerStyle() {
    return {
      height: this.height,
      minWidth: this.minWidth,
    };
  }

  get footerStyle() {
    return {
      height: this.footerHeight,
    };
  }
  /** 存在虚拟滚动 取虚拟滚动坐标  */
  get scrollLeft() {
    return this.virtualScrolllLeft !== 0 ? this.virtualScrolllLeft : this.bodyScrollLeft;
  }
  get headerStyle() {
    return {
      transform: `translateX(-${this.scrollLeft}px)`,
    };
  }

  get calcAComunHightlightStyle() {
    const { height, width, left } = this.clomunHightlightStyle;
    return {
      height: height,
      width: width + 'px',
      transform: `translateX(${left - this.scrollLeft}px)`,
    };
  }

  computedHighlightColumnPosition(index) {
    const row = this.$el.querySelector('.bkdata-vtable-header .bkdata-vtable-row');
    const tablebody = this.$refs.bkdataTableBody;
    if (row) {
      // const column = row.children[index] as HTMLElement;
      const column = row.children[index];
      if (column) {
        const left = column.offsetLeft + this.bodyOffsetX;
        const width = column.offsetWidth;
        const height = `${tablebody.clientHeight - 15}px`;
        this.clomunHightlightStyle = { height, width, left };
      }
    }
  }

  handleBodyScroll(e) {
    this.bodyScrollLeft = e.target.scrollLeft;
  }

  updateScrollOffsetX(offsetX = 0) {
    this.bodyScrollLeft = offsetX;
  }

  mounted() {
    this.isColumnShadow && this.computedHighlightColumnPosition(this.activeColumnIndex);
  }
}
</script>
<style lang="scss" scoped>
.bkdata-v-table {
  position: relative;
  overflow: hidden;
  border-top: 1px solid #e8ecf2;

  .bkdata-v-table-layer {
    position: absolute;
    width: 100%;
  }

  &.col-border {
    ::v-deep .bkdata-vtable-cell {
      border-right: solid 1px #e8ecf2;
    }
  }

  .column-hightlight-border {
    background: transparent;
    pointer-events: none;
    box-shadow: 0px 0px 15px 6px rgba(15, 25, 77, 0.14);
    position: absolute;
    top: 15px;
    left: 0px;
  }
}
.bkdata-vtable-header {
  &.shadow-column {
    padding: 15px 15px 0px;
  }

  .bkdata-vtable-row {
    background-color: white !important;
    color: #313239;
  }
  .bkdata-vtable-cell {
    .content {
      font-size: 12px;
    }
  }
}

.bkdata-vtable-body {
  .bkdata-vtable-content {
    height: 100%;
  }

  &.overflow-y-auto {
    overflow-y: auto !important;
  }

  &.overflow-y-hidden {
    overflow-y: hidden !important;
  }

  &.overflow-y-scroll {
    overflow-y: scroll !important;
  }

  &.shadow-column {
    padding: 15px;

    &.show-header {
      padding-top: 0;
    }
  }
  // height: calc(100% - 50px);

  .bk-vtable-empty {
    display: flex;
    justify-content: center;
    height: 100%;
    align-items: center;
    flex-direction: column;
    .bk-table-empty-icon {
      font-size: 65px;
      color: #c3cdd7;
    }
  }
}

.bkdata-vtable-footer {
  width: 100%;
  position: absolute;
  bottom: 0;
  .footer-row {
    width: 100%;
    display: inline-block;
    padding: 5px;
    position: relative;
  }
}
</style>

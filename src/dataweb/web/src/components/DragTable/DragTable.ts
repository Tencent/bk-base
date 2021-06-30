/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
 */

import { debounce } from '@/common/js/util.js';
import EmptyView from '@/components/emptyView/EmptyView';
import { Component, Emit, Prop, PropSync, Vue, Watch } from 'vue-property-decorator';
import draggable from 'vuedraggable';
import DragTableColumn from './DragTableColumn';
import DragTableRow from './DragTableRow';
@Component({
  components: { draggable, DragTableRow, DragTableColumn, EmptyView },
})
export default class DragTable extends Vue {
  @PropSync('data', { default: () => ({ columns: [], list: [] }) })
  public syncData!: object;
  @Prop({ default: false })
  public isEmpty!: boolean;
  @Prop({ default: '' })
  public readonly dragHandle!: string;
  @Prop({ default: true })
  public readonly showHeader!: boolean;

  @Prop({ default: 'auto' })
  public bodyHeight!: number | string;

  @Prop({ default: false })
  public isResizeColumn!: boolean;

  public isDragging = false;
  public delayedDragging = false;
  public editable = true;
  public offsetWidth: number = 0;
  public observer = null;
  get dragOptions() {
    return {
      animation: 0,
      group: 'description',
      disabled: !this.editable,
      ghostClass: 'ghost',
    };
  }
  get columnsWidth() {
    return `${100 / this.syncData.columns.length}%`;
  }

  @Emit('drag-end')
  public handleDragEnd() {
    this.isDragging = false;
    return [JSON.parse(JSON.stringify(this.syncData.list)), ...arguments];
  }

  @Watch('isDragging')
  public handleIsDragging(newValue) {
    if (newValue) {
      this.delayedDragging = true;
      return;
    }
    this.$nextTick(() => {
      this.delayedDragging = false;
    });
  }
  public start() {
    this.isDragging = true;
  }

  public orderList() {
    this.syncData.list = this.syncData.list.sort((one, two) => {
      return one.order - two.order;
    });
  }
  public onMove({ relatedContext, draggedContext }) {
    const relatedElement = relatedContext.element;
    const draggedElement = draggedContext.element;
    return (!relatedElement || relatedElement.isDrag) && draggedElement.isDrag;
  }
  public handleResetOffsetWidth() {
    this.offsetWidth = document.querySelector('.drag-table-body').offsetWidth - 40;
  }
  public handleResizeBody() {
    const self = this;
    this.observer = new ResizeObserver(entries => {
      debounce(self.handleResetOffsetWidth, 200)();
    });
    this.observer.observe(document.querySelector('.drag-table-body'));
  }
  public mounted() {
    this.isResizeColumn && this.handleResizeBody();
    this.offsetWidth = document.querySelector('.drag-table-body').offsetWidth - 40;
  }
  public beforeDestroy() {
    if (this.observer) {
      this.observer.disconnect();
      this.observer = null;
    }
  }
  get widthNum() {
    return this.offsetWidth / this.syncData.columns.length + 'px';
  }
}

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

import { Component, Emit, Prop, Ref, Watch } from 'vue-property-decorator';
import { VNode } from 'vue/types/umd';
import { DataModelManageBase } from '../../Controller/DataModelManageBase';
import { debounce } from '@/common/js/util.js';
import { EmptyView } from '@/pages/DataModelManage/Components/Common/index';
import { bkException } from 'bk-magic-vue';

@Component({
  components: {
    EmptyView,
    bkException,
  },
})
export default class IndexTree extends DataModelManageBase {
  @Prop({ required: true }) data!: any[];
  @Prop({ default: false }) showAddBtns!: boolean;

  @Ref() public readonly tree!: VNode;

  get isEmpty() {
    return !this.data.length;
  }

  public isFirstRender = true;
  public filter = '';
  public handleFilter: Function = () => ({});
  public isLoading = false;

  @Watch('data', { immediate: true, deep: true })
  handleRenderTree() {
    this.$nextTick(() => {
      if (this.data.length && this.tree) {
        this.tree.setData(this.data);
        // 初次渲染默认选中 rootNode
        this.isFirstRender && this.handleSetTreeSelected(this.data[0].id);
        this.isFirstRender = false;
      }
    });
  }

  @Watch('filter')
  handleChangeFilter() {
    !this.filter && (this.isLoading = true);
    this.handleFilter();
  }

  @Emit('on-selected-node')
  public handleSelectedNode(node: object) {
    return node;
  }

  @Emit('on-add-caliber')
  public handleAddCaliber(data: object) {
    return data;
  }

  @Emit('on-add-indicator')
  public handleAddIndicator(data: object) {
    return data;
  }

  public getBtnTips(text: string) {
    return {
      content: text,
      boundary: document.body,
      interactive: false,
    };
  }

  public handleSetTreeSelected(id: number | string) {
    this.tree.setSelected(id, { emitEvent: true });
  }

  public handleGetNodeById(id: number | string) {
    return this.tree.getNodeById(id);
  }

  public handleFilterMethod(keyword: string, node: object) {
    const { alias, name } = node.data;
    const key = String(keyword).toLowerCase();
    return (
      String(alias)
        .toLowerCase()
        .includes(key)
            || String(name)
              .toLowerCase()
              .includes(key)
    );
  }

  public created() {
    this.handleFilter = debounce(() => {
      this.isLoading = true;
      this.tree && this.tree.filter(this.filter);
      const timer = setTimeout(() => {
        this.isLoading = false;
        clearTimeout(timer);
      }, 500);
    }, 300);
  }
}

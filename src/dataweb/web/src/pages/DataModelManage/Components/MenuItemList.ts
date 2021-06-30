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

import { Component, Emit, Prop, Vue, Watch } from 'vue-property-decorator';
import { DataModelManageBase } from '../Controller/DataModelManageBase';
import { generateId } from '@/common/js/util';

@Component
export default class MenuItemList extends DataModelManageBase {
  @Prop({ default: 'urge-fill' }) public type: string | 'urge' | 'urge-fill';
  @Prop() public list: any[];
  @Prop({ default: false }) public isShowIcon: boolean;
  @Prop({ default: false }) public isShowDelete: boolean;
  /** 点击选中 */
  @Emit('onClick')
  public click(item) {
    return item;
  }
  /** 置顶&取消置顶 */
  @Emit('topChanged')
  public handleModelTopChanged(item) {
    return item;
  }
  @Emit('onDelete')
  public handleSubmitDelete(item) {
    return item;
  }
  get iconTips() {
    return this.type === 'urge' ? '置顶' : '取消置顶';
  }

  public modelType = {
    fact_table: '事实表数据模型',
    dimension_table: '维度表数据模型',
  };

  public status = {
    developing: '未发布',
    published: '已发布',
    're-developing': '发布有修改',
  };

  public getGenerateModelId(modelId: number) {
    return generateId('model_list_id_' + modelId + '_');
  }

  public getModelInfoTooltips(modelId) {
    return {
      boundary: document.body,
      allowHtml: true,
      distance: 0,
      theme: 'light data-model-tooltips',
      placement: 'right-start',
      content: `#model_${modelId}`,
    };
  }

  public deleteIconTips(item) {
    const contents = [];
    if (item.is_instantiated) {
      contents.push(this.$t('该数据模型在DataFlow中已被应用，无法删除'));
    }
    if (item.is_quoted) {
      contents.push(this.$t('数据模型中的指标统计口径被引用，无法删除'));
    }
    if (item.is_related) {
      contents.push(this.$t('该维度模型已被其他数据模型引用，无法删除'));
    }
    return {
      placement: 'top',
      interactive: false,
      html: contents.join('<br />') || this.$t('删除'),
    };
  }

  public handleDeleteModel(item) {
    if (item.is_instantiated || item.is_quoted || item.is_related) return;
    this.handleSubmitDelete(item);
  }
}

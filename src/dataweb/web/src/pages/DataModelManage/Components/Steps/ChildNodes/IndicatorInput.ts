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

import { Component, Inject, Prop, Vue, Watch } from 'vue-property-decorator';

/*** 指标输入 */
@Component({
  components: {},
})
export default class IndicatorInput extends Vue {
  // 当前模型的相关信息
  @Inject('activeTabItem')
  activeTabItem!: function;

  @Prop({ default: () => [] }) fieldInfo: Array<object>;
  @Prop({ default: false }) isChildIndicator!: boolean;

  iconNameMap = {
    measure: 'measure-line',
    dimension: 'dimens',
    primary_key: 'key',
  };

  activeInput = [];

  get mainTableName() {
    if (this.isChildIndicator && this.fieldInfo.length && this.fieldInfo[0].key) {
      return this.fieldInfo[0].key;
    }
    const { displayName, name } = this.activeTabItem();
    return `${displayName}（${name}）`;
  }

  get typeName() {
    return this.isChildIndicator ? this.$t('指标') : this.$t('数据模型的主表');
  }

  @Watch('fieldInfo', { immediate: true })
  public handleFieldInfoChanged(val: object[]) {
    if (val.length && val[0].key) {
      this.activeInput.push(val[0].key);
    }
  }
}

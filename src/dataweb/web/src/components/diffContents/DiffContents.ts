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

import { Vue, Component, Prop, PropSync, Watch } from 'vue-property-decorator';
// import { generateId } from '@/common/js/util'
import SingleCollapse from '@/components/singleCollapse/SingleCollapse.vue';
import EmptyView from '../emptyView/EmptyView.vue';

@Component({
  components: { SingleCollapse, EmptyView },
})
export default class DiffContents extends Vue {
  /** DiffContents
   * @param data 对比数据
   */
  @PropSync('isLoading', { type: Boolean, default: false }) localIsLoading!: Boolean;
  @PropSync('onlyShowDiff', { type: Boolean, default: true }) localOnlyShowDiff!: Boolean;
  @Prop({ default: () => [] }) readonly data!: [];
  @Prop({ default: () => ({}) }) readonly diffResult!: Object;
  @Prop({ default: true }) readonly showDiffResult!: Boolean;

  list: any[] = [];

  @Watch('data', { immediate: true, deep: true })
  handleSetData(data: []) {
    this.list = data.map((item, index) => Object.assign({}, item, { collapsed: index === 0 }));
  }

  getDisplayText(data: object = {}, key = '') {
    if (!key) return '--';
    const paths = key.split('.');
    const empty = [undefined, null, ''];
    if (paths.length === 1) return !empty.includes(data[key]) ? data[key] : '--';
    let value;
    for (let i = 0; i <= paths.length; i++) {
      const path = paths[i];
      if (i === 0) {
        value = data[path];
        continue;
      }
      if (typeof value === 'object') {
        value = value[path];
      }
    }
    return !empty.includes(value) ? value : '--';
  }

  getItemStatus(data: object = {}, key = '') {
    return (data.diffKeys || []).includes(key);
  }
}

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

import { Component, Vue, Prop, Watch } from 'vue-property-decorator';
import DataTable from '@/pages/datamart/common/components/DataTable.vue';
import { BKHttpResponse } from '@/common/js/BKHttpResponse';
import { submitCorrectSql, getCorrectSqlTask } from '@/pages/datamart/Api/DataQuality';
import { ISubmitCorrectSql } from '@/pages/datamart/InterFace/DataDict/DataQualityFix.ts';
import Bus from '@/common/js/bus.js';

let copyCorrectConfig: any;

@Component({
  components: {
    DataTable,
  },
})
export default class FixResultPreview extends Vue {
  @Prop({ type: Boolean, default: true }) isShowDebug: boolean;
  @Prop({ type: String, default: '' }) sql: string;
  isLoading = false;

  isCorrectLoading = false;

  tableData: any[] = [];

  calcPageSize = 10;

  title = '使用真实数据运行，确认输出的数据是否符合预期';

  alertType = 'info'; // error

  percent = 0.7;
  width = '100px';
  config = {
    strokeWidth: 10,
    bgColor: '#f0f1f5',
    activeColor: '#2dcb56',
  };

  correctResult = [];

  tableColumKeys: string[] = [];

  moreWidthKeys = ['wrong_field2', 'time'];

  timer: any;

  // 值为true表示修正调试成功
  // 默认修正调试失败
  debugFlag = false;

  get correctFields() {
    return this.$attrs.correct_configs.length ? this.$attrs.correct_configs.map(item => item.field) : [];
  }

  get correctFieldsMap() {
    if (!this.$attrs.correct_configs.length || !this.$attrs.correctFieldList.length) return {};
    const target = {};
    const correctConfigList = this.$attrs.correct_configs.map(item => item.field);
    this.$attrs.correctFieldList.forEach(item => {
      if (correctConfigList.includes(item.field_name)) {
        target[item.field_name] = item.field_alias;
      }
    });
    return target;
  }

  get debugResultField() {
    if (!Object.keys(this.correctFieldsMap).length) return '';
    let target = '';
    Object.values(this.correctFieldsMap).forEach(item => {
      target += item + '、';
    });
    return target.substring(0, target.length - 1);
  }

  @Watch('$attrs.correct_configs', { deep: true })
  onCorrectConfigChange(val: []) {
    this.debugFlag = false;
    // if (!copyCorrectConfig) {
    //     copyCorrectConfig = JSON.stringify(val)
    //     return
    // }
    // this.debugFlag = copyCorrectConfig === JSON.stringify(val)
    this.$nextTick(() => {
      Bus.$emit('changeCorrectFixStatus', this.debugFlag);
    });
  }

  renderHeader(h, { column }) {
    if (column.label in this.correctFieldsMap) {
      return (
        <div class="table-head-field">
          {column.label}
          <i class="icon-data-amend"></i>
        </div>
      );
    } else {
      return column.label;
    }
  }

  submitCorrectSql() {
    this.isCorrectLoading = true;
    this.alertType = 'info';
    this.title = '使用真实数据运行，确认输出的数据是否符合预期';
    submitCorrectSql(
      this.$attrs.sourceDataSetId,
      this.$attrs.dataSetId,
      this.$attrs.sourceSql,
      this.$attrs.correct_configs
    ).then(res => {
      if (res.result) {
        this.getCorrectSqlTask(res.data.debug_request_id);
      } else {
        this.alertType = 'error';
        this.title = res.message;
        this.isCorrectLoading = false;
      }
    });
  }

  pollQuery(id: string) {
    this.timer = setTimeout(() => {
      this.getCorrectSqlTask(id);
    }, 3000);
  }

  getCorrectSqlTask(id: string) {
    this.correctResult = [];
    this.tableColumKeys = [];
    getCorrectSqlTask(id).then(res => {
      if (res.result) {
        if (res.data.output.data.length) {
          this.correctResult = res.data.output.data;
          this.tableColumKeys = Object.keys(this.correctResult[0]);
        }
        if (res.data.state !== 'available') {
          this.debugFlag = false;
          this.pollQuery(id);
        } else {
          if (res.data.output.status === 'error') {
            this.debugFlag = false;
            this.alertType = 'error';
            this.title = res.data.output.error.message;
          } else {
            this.debugFlag = true;
          }
          this.isCorrectLoading = false;
          clearTimeout(this.timer);
        }
      } else {
        this.isCorrectLoading = false;
        this.debugFlag = false;
        this.alertType = 'error';
        this.title = res.message;
        clearTimeout(this.timer);
      }
      // 数据修正调试不通过，禁止修改节点配置
      Bus.$emit('changeCorrectFixStatus', this.debugFlag);
    });
  }

  beforeDestroy() {
    clearTimeout(this.timer);
  }
}

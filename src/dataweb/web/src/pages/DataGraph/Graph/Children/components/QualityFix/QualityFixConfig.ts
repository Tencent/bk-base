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

import { Component, Vue, Prop } from 'vue-property-decorator';
// 请求方法
import { getFixCorrectConditions, getFixFillTempList } from '@/pages/datamart/Api/DataQuality';
// 数据接口
import {
  IQualityFixCorrectConditions,
  IQualityFixFillTemp,
  IQualityFixCorrectConditionsData,
  IQualityFixFillTempData,
  IResultInfoData,
} from '@/pages/datamart/InterFace/DataDict/DataQualityFix';
import { } from '@/pages/datamart/InterFace/DataQuality';
import { BKHttpResponse } from '@/common/js/BKHttpResponse';
import OperateRow from '@/pages/dataManage/cleanChild/components/OperateRow.vue';

const configParams = {
  correct_config_item_id: '',
  field: '',
  correct_config_detail: {
    rules: [
      {
        condition: {
          condition_name: 'custom_sql_condition',
          condition_type: 'custom',
          condition_value: '',
        },
        handler: {
          handler_name: 'fixed_filling',
          handler_type: 'fixed',
          handler_value: 100,
        },
      },
    ],
    output: {
      generate_new_field: false,
      new_field: '',
    },
  },
  correct_config_alias: '',
  created_by: 'admin',
  created_at: '2020-07-27 10:30:00',
  updated_by: 'admin',
  updated_at: '2020-07-27 10:31:00',
  description: '',
};

@Component({
  components: {
    OperateRow,
  },
})
export default class QualityFixConfig extends Vue {
  @Prop({ default: 'normal-table' }) tableSize: string;
  @Prop({ default: () => [] }) correct_configs: any;
  @Prop({ default: () => [] }) correctFieldList: IResultInfoData[];

  isLoading = false;

  calcPageSize = 10;

  dropDownWidth = 200;

  tableData: Array<any> = [];

  correctConditionList: IQualityFixCorrectConditionsData[] = [];

  correctFillingsList: IQualityFixFillTempData[] = [];

  popoverOptions = {
    boundary: document.body,
  };

  timer: any;

  correctConditionTarget: any;

  created() {
    // 修正条件下拉列表
    this.getFixCorrectConditions();
    // 修正算子下拉列表
    this.getFixFillTempList();
  }

  getCorrectOperatorTarget(child) {
    this.correctOperatorChange.child = child;
  }

  correctOperatorChange(name, data) {
    this.correctOperatorChange.child.handler_type = data.handler_template_type;
    delete this.correctOperatorChange.child;
  }

  getCorrectTarget(data, child) {
    this.correctConditionChange.data = data;
    this.correctConditionTarget = child;
  }

  correctConditionChange(name, data) {
    this.correctConditionTarget.condition_type = data.condition_template_type;
    if (data.condition_template_config.condition_value_template) {
      this.correctConditionTarget.condition_value = data.condition_template_config.condition_value_template
        .replace(
          '${field}',
          this.correctConditionChange.data.field
        );
    } else {
      this.correctConditionTarget.condition_value = '';
    }
    this.correctConditionTarget = null;
    delete this.correctConditionChange.data;
  }

  correctInputChange() {
    if (this.timer) return;
    this.timer = setTimeout(() => {
      this.correctConfigChange();
    }, 500);
  }

  getFixCorrectConditions() {
    this.isLoading = true;
    getFixCorrectConditions({}).then(res => {
      console.log(res);
      if (res.result && res.data) {
        const instance = new BKHttpResponse<IQualityFixCorrectConditions>(res, false);
        instance.setData(this, 'correctConditionList');
      }
      this.isLoading = false;
    });
  }

  getFixFillTempList() {
    this.isLoading = true;
    getFixFillTempList({}).then(res => {
      console.log(res);
      if (res.result) {
        const instance = new BKHttpResponse<IQualityFixFillTemp>(res, false);
        instance.setData(this, 'correctFillingsList');
      }
      this.isLoading = false;
    });
  }

  addCondition(index: number, list: any[]) {
    console.log(list);
    list.splice(index, 0, {
      condition: {
        condition_name: 'custom_sql_condition',
        condition_type: 'custom',
        condition_value: '',
      },
      handler: {
        handler_name: 'fixed_filling',
        handler_type: 'fixed',
        handler_value: 100,
      },
    });
  }

  deleteParams(index: number, list: any[]) {
    if (list.length === 1) return;
    list.splice(index, 1);
  }

  addRule(list: any[], index: number) {
    const item = JSON.parse(JSON.stringify(configParams));
    item.correct_config_item_id = null;
    list.splice(index, 0, item);
  }

  correctConfigChange() {
    this.timer = null;
  }

  getBoundary() {
    return {
      boundary: document.body,
    };
  }

  changeSqlFeild() {
    this.correct_configs.forEach(item => {
      item.correct_config_detail.rules.forEach(child => {
        child.condition.condition_value = child.condition.condition_value.replace(/\(\S*\,/, `(${item.field},`);
      });
    });
  }
}

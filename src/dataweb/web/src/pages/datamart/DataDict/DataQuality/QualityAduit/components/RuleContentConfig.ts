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

import { Component, Prop, Vue, Watch } from 'vue-property-decorator';
import Mixin from './../Mixin';
// api请求函数
import {
  queryQualityRuleFunc,
  queryQualityRuleIndex,
  queryQualityRuleTemplates,
  createQualityRules,
  getResultTables,
} from '@/pages/datamart/Api/DataQuality';
// 数据接口
import {
  IQualityRuleFunc,
  IQualityRuleIndex,
  IQualityRuleTemplate,
  ICreateRuleConfigField,
  IRuleFuncData,
  IRuleIndexData,
  IRuleTemplateData,
  IRtFieldsList,
  IRtFieldsData,
} from '@/pages/datamart/InterFace/DataQuality';
import { BKHttpResponse } from '@/common/js/BKHttpResponse';
import { bkCascade } from 'bk-magic-vue';

interface CascadeList {
  id: string;
  name: string;
  disabled: boolean;
}

interface SimpleIndexRule {
  id: string;
  name: string;
  type: string;
  children?: {}[];
}
const rowData: ICreateRuleConfigField = {
  metric: {
    metric_type: '',
    metric_name: '',
  },
  function: '',
  constant: {
    constant_type: 'float',
    constant_value: 0,
  },
  operation: 'and',
};

@Component({
  mixins: [Mixin],
  components: {
    bkCascade,
  },
})
export default class RuleContentConfig extends Vue {
  // 区分创建规则和更新规则
  @Prop() isCoveredConfig: boolean;
  @Prop() disabled: boolean;
  @Prop({ default: () => [], required: true }) ruleList: ICreateRuleConfigField;
  @Prop({ default: () => [] }) limitMetrics: string[];

  isRuleFuncLoading = false;
  isRuleIndexLoading = false;
  selectedFunc = '';
  active = 'easyConfig';
  selectedIndex = [];
  selectedThreshold = '';
  ruleFuncList: IRuleFuncData[] = [];
  rtFieldsList: IRtFieldsData[] = [];

  symbolList = [
    {
      id: 'and',
      name: 'AND',
      alias: '&',
    },
    {
      id: 'or',
      name: 'OR',
      alias: '|',
    },
  ];

  ruleIndexList = [
    {
      id: '',
      name: '',
      children: [],
    },
  ];

  cascadeValues: Array<string>[] = [[]];

  @Watch('ruleList', { immediate: true, deep: true })
  onRuleListChange(val: any[]) {
    // 级联菜单存在二级菜单时，需要补全两个值，才会正常显示
    val.forEach(item => {
      if (item.metric.metric_type === 'data_profiling' && !item.metric.metric_field) {
        item.metric.metric_field = 'wrong_field2';
      }
    });
    this.changeRuleContent(val);
  }

  @Watch('limitMetrics', { immediate: true })
  onLimitMetricsChange(val: string[]) {
    this.changeOptionStatus();
  }

  @Watch('ruleIndexList')
  onRuleIndexListChange(val: []) {
    this.changeOptionStatus();
  }

  @Watch('isCoveredConfig')
  onIsCoveredConfigChange() {
    if (this.isCoveredConfig) {
      this.fillCascadeValue();
      this.$emit('update:isCoveredConfig', false);
    }
  }

  async mounted() {
    this.queryQualityRuleFunc();
    await this.getResultTables();
    this.queryQualityRuleIndex();
  }

  changeOptionStatus() {
    // 重置disabled状态
    this.ruleIndexList.forEach(item => {
      this.$set(item, 'disabled', false);
      if (item.children.length) {
        item.children.forEach(child => {
          this.$set(child, 'disabled', false);
        });
      }
    });
    if (this.limitMetrics.length) {
      this.ruleIndexList.forEach(item => {
        if (item.children.length) {
          item.children.forEach(child => {
            if (!this.limitMetrics.includes(child.id)) {
              this.$set(child, 'disabled', true);
            }
          });
        }
        if (item.children.every(child => child.disabled)) {
          this.$set(item, 'disabled', true);
        }
      });
    }
  }

  changeRuleContent(val: ICreateRuleConfigField) {
    let ruleContent = '';
    val.forEach((item, index) => {
      const ruleType = this.ruleIndexList
        .find(child => child.id === item.metric.metric_type)
        ?.children.find(child => child.id === item.metric.metric_name)?.name || '';
      const fieldAlias = this.rtFieldsList
        .find(child => child.fieldName === item.metric.metric_field)?.fieldAlias;
      const ruleName = item.metric.metric_name && fieldAlias ? `（${fieldAlias}）` : '';
      const func = this.ruleFuncList.find(child => child.functionName === item['function'])?.functionAlias;
      const constantValue = item.constant?.constant_value;
      const symbol = this.symbolList.find(child => child.id === item.operation)?.alias || '';
      const isComplete = [ruleType, func].every(child => child);
      if (isComplete) {
        ruleContent += `（${ruleType}${ruleName} ${func} ${constantValue}）${index === val.length - 1
          ? '' : symbol}\n`;
      }
    });

    this.$emit('update:ruleContent', ruleContent);
  }

  onCascadeChange(data: string[], index: number) {
    const copyData = JSON.parse(JSON.stringify(data));
    const type = copyData.shift();

    if (!copyData.length) return;
    if (copyData.length > 1) {
      this.ruleList[index].metric.metric_field = copyData[1];
    }
    this.ruleList[index].metric.metric_type = type;
    this.ruleList[index].metric.metric_name = copyData[0];
  }

  // 获取规则配置函数列表
  queryQualityRuleFunc() {
    this.isRuleFuncLoading = true;
    queryQualityRuleFunc(this.DataId).then(res => {
      const instance = new BKHttpResponse<IQualityRuleFunc>(res);
      instance.setData(this, 'ruleFuncList');
      this.changeRuleContent(this.ruleList);

      this.isRuleFuncLoading = false;
    });
  }

  // 获取规则配置指标列表
  queryQualityRuleIndex() {
    this.isRuleIndexLoading = true;
    this.ruleIndexList = [];
    return queryQualityRuleIndex(this.DataId).then(res => {
      if (res.result && res.data.length) {
        const metricTypeMap = {
          data_flow: this.$t('数据埋点'),
          data_profiling: this.$t('数据剖析'),
        };
        this.ruleIndexList = [
          {
            id: 'data_flow',
            name: metricTypeMap.data_flow,
            children: [],
          },
          {
            id: 'data_profiling',
            name: metricTypeMap.data_profiling,
            children: [],
          },
        ];
        const children = this.rtFieldsList.map(item => ({
          id: item.fieldName,
          name: `${item.fieldAlias}（${item.fieldName}）`,
        }));
        res.data.forEach(item => {
          this.ruleIndexList[item.metric_type === 'data_flow' ? 0 : 1].children?.push({
            id: item.metric_name,
            name: item.metric_alias,
            type: item.metric_type,
            children: item.metric_type === 'data_profiling' ? children : [],
          });
        });

        this.changeRuleContent(this.ruleList);

        this.$nextTick(() => {
          this.fillCascadeValue();
        });
      }
      this.isRuleIndexLoading = false;
    });
  }

  fillCascadeValue() {
    this.$refs.cascadeInstance && this.$refs.cascadeInstance.forEach(item => item.clearData());
    this.cascadeValues = [];
    this.ruleList.forEach((item: object, index: number) => {
      let cascadeValue = this.cascadeValues[index];
      if (!cascadeValue || !cascadeValue.length) {
        this.$nextTick(() => {
          this.$set(this.cascadeValues, index, [
            item.metric.metric_type,
            item.metric.metric_name,
            item.metric.metric_field,
          ]);
        });
      }
    });
  }

  // 获取结果表字段
  getResultTables() {
    return getResultTables(this.DataId).then(res => {
      res.data.shift();
      const instance = new BKHttpResponse<IRtFieldsList>(res);
      instance.setData(this, 'rtFieldsList');
    });
  }

  add(index: number) {
    if (this.disabled) return;
    if (!this.ruleList[this.ruleList.length - 1].operation) {
      this.ruleList[this.ruleList.length - 1].operation = 'and';
    }
    this.ruleList.splice(index + 1, 0, JSON.parse(JSON.stringify(rowData)));
    this.cascadeValues.splice(index + 1, 0, []);
  }
  del(index: number) {
    if (this.disabled) return;
    if (this.ruleList.length === 1) return;
    this.ruleList.splice(index, 1);
    this.cascadeValues.splice(index, 1);
  }

  upMove(index: number) {
    if (this.disabled) return;
    if (index !== 0) {
      this.changeArrMethod(index, index - 1, this.ruleList);

      this.changeArrMethod(index, index - 1, this.cascadeValues);
    }
  }

  downMove(index: number) {
    if (this.disabled) return;
    if (index !== this.ruleList.length - 1) {
      this.changeArrMethod(index, index + 1, this.ruleList);

      this.changeArrMethod(index, index + 1, this.cascadeValues);
    }
  }

  changeArrMethod(index: number, posIndex: number, list: any) {
    const target = list.splice(index, 1)[0];
    list.splice(posIndex, 0, target);
  }
}

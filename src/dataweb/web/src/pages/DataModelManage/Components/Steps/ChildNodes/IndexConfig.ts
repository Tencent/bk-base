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

import Monaco from '@/components/monaco/index.vue';
import Layout from '@/pages/DataGraph/Graph/Children/components/TribleColumnLayout.vue';
import { IDataModelManage } from '@/pages/DataModelManage/Interface/index';
import { Component, Prop, Ref, Vue, Watch } from 'vue-property-decorator';
import { VNode } from 'vue/types/umd';
import { ICalculationAtomsResults, ICreateIndexParams } from '../../../Interface/indexDesign';
import { getNodeConfig } from '@/pages/DataGraph/Graph/Children/config/nodeConfig.js';

let streamParams = {
  // 滑动窗口
  window_type: 'scroll',
  count_freq: 30,
  window_time: 10,
  waiting_time: 0,
  expired_time: 0,
  window_lateness: {
    lateness_count_freq: 60,
    allowed_lateness: false,
    lateness_time: 1,
  },
  session_gap: 0,
};

let batchParams = {
  window_type: 'fixed', // 必须传以下参数
  dependency_config_type: 'unified',
  fallback_window: 1, // 窗口长度
  fixed_delay: 0, // 延迟时间
  // window_type=accumulate_by_hour 必须传以下参数
  data_start: 0, // 数据起点
  data_end: 23, // 数据终点
  delay: 0, // 延迟时间
  count_freq: 1, // 统计频率的值，固定，不能修改
  schedule_period: 'day', // 统计频率的单位，固定，不能修改
  delay_period: 'hour', //统计延迟单位
  unified_config: {
    window_size: 1,
    window_size_period: 'day', // 窗口长度的单位
    window_delay: 0,
    dependency_rule: 'all_finished',
  },
  custom_config: {},
  advanced: {
    start_time: '',
    recovery_enable: false,
    recovery_times: 1,
    recovery_interval: '5m',
  },
};

const paramsMap = {
  stream: streamParams,
  batch: batchParams,
};

@Component({
  components: {
    Layout,
    Monaco,
  },
})
export default class IndexConfig extends Vue {
  @Prop({ default: () => [] }) calculationAtomsList: ICalculationAtomsResults[];
  @Prop({ default: () => [] }) aggregateFieldList: IDataModelManage.IMasterTableField[];
  @Prop({ default: '' }) calculationFormula: string;
  @Prop({ default: 'stream' }) nodeType: string;
  @Prop({ default: () => ({}) }) params: ICreateIndexParams;
  @Prop({ default: false }) isSaveNode: boolean;
  @Prop({ default: '' }) initNodeType: string;
  @Prop({ default: false }) isChildIndicator!: boolean;
  @Prop({ default: false }) isRestrictedEdit!: boolean;
  @Prop({ default: () => ({}) }) indexInfo: object;

  @Ref() public readonly monacoEditor!: VNode;

  rules = {
    calculation_atom_name: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
    ],
    parent_indicator_name: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
    ],
    filter_formula: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
    ],
    aggregation_fields: [
      {
        validator: function (val: string[]) {
          return val.length >= 1;
        },
        message: $t('请选择聚合字段'),
        trigger: 'blur',
      },
    ],
  };

  nodeConfig = {};

  initNodeParams: any = null;

  // 是否增加指标
  isAddIndicator = false;

  activeName = '';

  get tabConfig() {
    const nodeTypeMap = {
      batch: 'offline',
      stream: 'realtime',
    };
    // 离线计算和实时计算下都需要过滤掉执行记录
    return (this.nodeConfig[nodeTypeMap[this.nodeType]].tabConfig || []).filter(item => item.name !== 'execute-record');
  }

  get parentIndicatorDisplayName() {
    if (this.isSaveNode && this.isChildIndicator) {
      return `${this.indexInfo.parentIndicatorAlias} (${this.indexInfo.parentIndicatorName})`;
    }
    return `${this.indexInfo.indicatorAlias} (${this.indexInfo.indicatorName})`;
  }

  @Watch('isSaveNode', { immediate: true })
  handleNodeTypeChanged() {
    if (this.isSaveNode) {
      if (this.initNodeType === this.nodeType && !this.initNodeParams) {
        this.initNodeParams = Object.assign(
          {},
          JSON.parse(JSON.stringify(paramsMap[this.nodeType])),
          this.params.scheduling_content
        );
        this.params.scheduling_content = this.initNodeParams;
      }
    }
  }

  async created() {
    this.nodeConfig = await getNodeConfig();
  }
  mounted() {
    this.initParams(this.nodeType);
  }

  isPanelDisabled(condition) {
    if (typeof condition === 'boolean') return condition;
    return false;
  }

  initParams(nodeType: string) {
    this.$set(this.params, 'scheduling_content', JSON.parse(JSON.stringify(paramsMap[nodeType])));
  }

  changeCode(sql: string) {
    this.params.filter_formula = sql;
  }

  changeNodeType(type: string) {
    this.$emit('update:nodeType', type);
    // 编辑状态,避免覆盖接口参数
    if (this.isSaveNode) {
      if (this.initNodeType === type) {
        this.params.scheduling_content = this.initNodeParams;
        return;
      }
      this.params.scheduling_content = paramsMap[type];
    } else {
      // 新建状态
      this.initParams(type);
    }
  }

  validateFormData() {
    return this.$refs.baseForm.validate().then(
      () => {
        return true;
      },
      () => {
        return false;
      }
    );
  }

  beforeDestroy() {
    this.initNodeParams = null;
  }
}

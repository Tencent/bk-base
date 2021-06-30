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

import { CreateElement } from 'vue';
import { Component, Watch } from 'vue-property-decorator';
import ProcessFrame from '../NodesComponents/ProcessFrame';
import SplitLayout from '../NodesComponents/SplitLayout';
import Basic from './Basic';
import Advanced from './Advanced.vue';
import ExecuteRecord from '@/pages/DataGraph/Graph/Children/components/OfflineNode/ExecutionRecord.vue';
import Chart from '@/components/taskTimelineChart/PreviewChart';
import { timeHour, timeDay, timeMonday, timeMonth, timeYear } from 'd3-time';
import Bus from '@/common/js/bus.js';
import bkStorage from '@/common/js/bkStorage.js';
import { WindowInfoEntity, IChartPanel } from '../../../interface/INodeParams';

@Component({})
export default class OfflineProcess extends SplitLayout {
  panels = [
    { name: 'basic', label: window.$t('基础配置'), component: Basic, props: {} },
    { name: 'advanced', label: window.$t('高级配置'), component: Advanced, props: {} },
    {
      name: 'debug',
      label: window.$t('执行记录'),
      component: ExecuteRecord,
      props: {
        params: this.params,
        loading: false,
        nodeId: 0,
        isSelfDepEnabled: this.params.dedicated_config.self_dependence.self_dependency,
      },
      disabled: false,
    },
  ];

  public timeUnitMap: object = {
    hour: timeHour,
    day: timeDay,
    week: timeMonday,
    month: timeMonth,
    year: timeYear,
  };
  public chart: object = {};
  public previewChart: IChartPanel = {
    isInit: false,
    render: () => { },
    updateChart: () => { },
  };

  @Watch('selfConfig', { deep: true })
  selfConfigChange(val: object) {
    this.panels[2].disabled = !val.hasOwnProperty('node_id');
    if (!this.panels[2].disabled) {
      this.panels[2].props.nodeId = val.node_id;
    }
  }

  @Watch('params.dedicated_config.self_dependence.self_dependency', { deep: true })
  selfDependenceChange(val: boolean) {
    this.panels[2].props.isSelfDepEnabled = val;
  }

  @Watch('params.dedicated_config.schedule_config', { deep: true, immediate: true })
  scheduleChange(val: object) {
    this.previewChart && this.previewChart.isInit && this.previewChart.updateChart(this.params.window_info, val);
  }

  @Watch('params.window_info', { deep: true })
  inputsChange(val: WindowInfoEntity) {
    this.previewChart.isInit && this.previewChart.updateChart(val);
  }

  mounted() {
    const width = document.querySelector('.chart-main')?.offsetWidth;
    this.previewChart = new Chart({
      layout: { width: width },
      container: '.chart-main',
      domain: this.getPreviewChartDomain(),
      startTime: this.params.dedicated_config.schedule_config.start_time,
      schedulingPeriod: this.params.dedicated_config.schedule_config,
      data: this.params.window_info,
    });

    Bus.$on('offlineNodeHelp', (helpMode: boolean) => {
      this.$refs['top-bottom-split'] && this.$refs['top-bottom-split'].setCollapse(!helpMode);
    });

    Bus.$on('nodeConfigBack', () => {
      switch (this.params.window_info?.length) {
        case 1:
          this.upperSectionMaxHeight = 136;
          break;
        case 2:
          this.upperSectionMaxHeight = 172;
          break;
      }
      this.upperPanelHeight = this.upperSectionMaxHeight + 2;

      /** 回填后重置图表高度，nexttick根据帮助模式决定是否收起图表 */
      this.$nextTick(() => {
        const helpMode = bkStorage.get('nodeHelpMode');
        !helpMode && this.$refs['top-bottom-split'].setCollapse(!helpMode);
      });

      this.previewChart.renderChart(this.params.window_info, this.params.dedicated_config.schedule_config);
    });
  }

  getMainProcessAdapter(h: CreateElement) {
    return (
      <ProcessFrame
        params={this.params}
        tabConfig={this.panels}
        bizList={this.bizList}
        selfConfig={this.selfConfig}
      ></ProcessFrame>
    );
  }

  getPreviewChartDomain() {
    const startTime = this.params.dedicated_config.schedule_config.start_time;
    const scheduleUnit = this.timeUnitMap[this.params.dedicated_config.schedule_config.schedule_period];
    const count = this.params.dedicated_config.schedule_config.count_freq;

    return [scheduleUnit.offset(startTime, -count), scheduleUnit.offset(startTime, 5 * +count)];
  }
}

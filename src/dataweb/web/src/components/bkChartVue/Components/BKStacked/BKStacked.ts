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
import BaseSettings from '../BaseSettings';
import BKChart from '../../libs/index';
import { toolTip } from '@/common/js/util';
@Component()
export default class BKStacked extends BaseSettings {
  @Watch('series', { immediate: true })
  handleDataChanged(val) {
    this.$nextTick(() => {
      if (!this.instance) {
        this.renderInstance();
      } else {
        this.updateOptions();
      }
    });
  }
  defaultDataSet: any = {
    label: '',
    backgroundColor: '#3a84ff',
    barThickness: 20,
    data: [],
  };
  defaultTooltips: any = {
    caretSize: 5,
    displayColors: false,
    cornerRadius: 2,
    // callbacks: {
    //   title: function (tooltipItem, data) {
    //     return null;
    //   },
    //   label: function (tooltipItem, data) {
    //     return tooltipItem.label || 0;
    //   },
    // },
  };
  defaultScales: any = {
    x: {
      stacked: true,
      offset: true,
      gridLines: {
        display: false,
      },
    },
    y: {
      ticks: {
        stepSize: 2,
      },
      stacked: true,
    },
  };

  get chartConfig() {
    return {
      labels: this.labels || [],
      datasets: this.series.map(ds => Object.assign({}, this.defaultDataSet, ds)),
    };
  }

  get titleConfig() {
    return typeof this.title !== 'object' ? { display: true, text: '' } : this.title;
  }

  get legendConfig() {
    const labels = {
      boxWidth: 12,
    };
    return Object.assign({}, { display: false, labels: labels, align: 'end' }, this.legend);
  }

  get animationConfig() {
    return Object.assign({}, { animateScale: true, animateRotate: true }, this.animation);
  }
  get tooltipsConfig() {
    return Object.assign({}, { animateScale: true, animateRotate: true }, this.defaultTooltips);
  }
  get scalesConfig() {
    return Object.assign({}, {}, this.defaultScales);
  }
  mounted() {
    this.type = 'bar';
    this.init(this.$el);
  }

  updateOptions() {
    this.instance.clear();
    this.instance.destroy();
    this.instance = null;
    this.renderInstance();
    // this.update(this.chartConfig.datasets, this.localOptions)
  }

  renderInstance() {
    this.renderChart(this.chartConfig, {
      responsive: this.responsive,
      scales: this.scalesConfig,
      animation: this.animationConfig,
      onClick: this.handleClick,
      plugins: {
        legend: this.legendConfig,
        title: this.titleConfig,
        tooltip: this.tooltipsConfig,
      },
    });
  }
  handleClick(evt, data) {
    const activePoint = this.instance.getElementsAtEventForMode(evt, 'nearest', { intersect: true }, true)[0];
    const datasetIndex = activePoint.datasetIndex;
    const dataItem = this.instance.data.datasets[datasetIndex] || {};
    this.$emit('click', activePoint, dataItem, datasetIndex);
  }
  renderChart(chartConfig, opts) {
    this.instance = new BKChart(this.context, {
      type: this.type,
      data: chartConfig,
      options: opts,
    });
  }
}

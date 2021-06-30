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

@Component()
export default class BKVTree extends BaseSettings {
  @Watch('series', { immediate: true, deep: true })
  handleDataChanged(val) {
    this.$nextTick(() => {
      if (!this.instance) {
        this.renderInstance();
      } else {
        this.update(this.chartConfig.datasets, this.localOptions);
      }
    });
  }

  @Watch('options')
  handleOptionsChanged() {
    this.$nextTick(() => {
      this.update(this.chartConfig.datasets, this.localOptions);
    });
  }

  get defaultDataSet() {
    return {
      label: '',
      backgroundColor: this.color('#3a84ff')
        .alpha(0.5)
        .rgbString(),
      borderColor: '#3a84ff',
      borderWidth: 1,
      data: [],
    };
  }

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
    return Object.assign({}, { position: 'right' }, this.legend);
  }

  get localOptions() {
    return {
      elements: {
        rectangle: {
          borderWidth: 2,
        },
      },
      responsive: this.responsive,
      legend: this.legendConfig,
      title: this.titleConfig,
      tooltips: this.tooltips,
      scales: this.scales,
    };
  }

  renderInstance() {
    this.renderChart(this.chartConfig, this.localOptions);
  }

  renderChart(chartConfig, opts) {
    this.instance = new this.BKChart(this.context, {
      type: this.chartType || this.type,
      data: chartConfig,
      options: Object.assign({}, opts, this.options || {}),
    });
  }

  mounted() {
    this.type = 'tree';
    this.init(this.$el);
  }
}

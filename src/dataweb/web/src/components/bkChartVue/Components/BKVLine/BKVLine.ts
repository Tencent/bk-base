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
@Component({})
export default class BKVLine extends BaseSettings {
  /**
   * line tension (factor for the bezier control point as distance between the nodes)
   * @default 0.4
   */
  @Prop({ default: 0.4 }) lineTension: number;
  @Watch('series', { immediate: true, deep: true })
  handleDataChanged(val) {
    this.$nextTick(() => {
      if (!this.instance) {
        this.renderInstance();
      } else {
        this.updateOptions();
      }
    });
  }

  get defaultDataSet() {
    return {
      label: '',
      backgroundColor: this.color('#3a84ff')
        .alpha(0.5)
        .rgbString(),
      borderColor: '#3a84ff',
      borderWidth: 2,
      pointRadius: 0,
      data: [],
      fill: false,
    };
  }

  get chartConfig() {
    return {
      labels: this.labels || [],
      datasets: (this.series || []).map(ds => Object.assign({}, this.defaultDataSet, ds)),
    };
  }

  get titleConfig() {
    return typeof this.title !== 'object' ? { display: true, text: '' } : this.title;
  }

  get legendConfig() {
    return Object.assign({}, { position: 'right', labels: { boxWidth: 12 } }, this.legend);
  }

  get localOptions() {
    return {
      responsive: this.responsive,
      plugins: {
        legend: this.legendConfig,
        title: this.titleConfig,
        tooltip: this.tooltips,
      },
    };
  }

  /**
   * 更新图表配置
   * @param forceUpdate 是否强制更新 默认：false
   */
  updateOptions(forceUpdate = false) {
    /** 禁止自动更新 */
    if (!this.autoUpdate && !forceUpdate) {
      return;
    }

    if (this.instance) {
      this.instance.clear();
      this.instance.destroy();
      this.instance = null;
    }
    this.renderInstance();
    // this.update(this.chartConfig.datasets, this.localOptions)
  }

  updateConfig(force = false) {
    this.update(this.chartConfig.datasets, this.localOptions, force);
  }

  renderInstance() {
    this.renderChart(this.chartConfig, this.localOptions);
  }

  renderChart(chartConfig: any, opts: any) {
    const targetOption = Object.assign({}, opts, this.options || {});
    if (this.webwork) {
      this.renderByWebworker({
        type: this.chartType || this.type,
        data: chartConfig,
        options: targetOption,
      });
    } else {
      this.instance = new this.BKChart(this.context, {
        type: this.chartType || this.type,
        data: chartConfig,
        options: targetOption,
      });
      this.$emit('init', this.instance);
    }
  }

  mounted() {
    this.type = 'line';
    this.init(this.$el);
  }
}

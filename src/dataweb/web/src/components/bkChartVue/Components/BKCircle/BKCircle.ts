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

import { Component, Watch } from 'vue-property-decorator';
import BaseSettings from '../BaseSettings';
@Component()
export default class BKCircle extends BaseSettings {
  get defaultDataSet() {
    return {
      label: '',
      backgroundColor: this.color('#3a84ff')
        .alpha(0.5)
        .rgbString(),
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
    return typeof this.title !== 'object' ? { display: false, text: '' } : this.title;
  }

  get legendConfig() {
    return Object.assign({}, { display: false }, this.legend);
  }

  get animationConfig() {
    return Object.assign({}, { animateScale: true, animateRotate: true }, this.animation);
  }
  get tooltipsConfig() {
    return Object.assign({}, { animateScale: true, animateRotate: true }, this.defaultTooltips);
  }
  public defaultTooltips: any = {
    caretSize: 0,
    displayColors: false,
    cornerRadius: 2,
    callbacks: {
      label(tooltipItem: any) {
        return tooltipItem.formattedValue || 0;
      },
    },
  };
  @Watch('series', { immediate: true })
  public handleDataChanged() {
    this.$nextTick(() => {
      if (!this.instance) {
        this.renderInstance();
      } else {
        this.updateOptions(true);
      }
    });
  }

  /**
   * 更新图表配置
   * @param forceUpdate 是否强制更新 默认：false
   */
  public updateOptions(forceUpdate = false) {
    /** 禁止自动更新 */
    if (!this.autoUpdate && !forceUpdate) {
      return;
    }

    this.instance.clear();
    this.instance.destroy();
    this.instance = null;
    this.renderInstance();
    // this.update(this.chartConfig.datasets, this.localOptions)
  }

  public renderInstance() {
    this.renderChart(this.chartConfig, {
      responsive: this.responsive,
      cutoutPercentage: 80,
      circumference: Math.PI * 1.6,
      rotation: -Math.PI * 1.3,
      // animation: this.animationConfig,
      plugins: {
        tooltip: this.tooltipsConfig,
        legend: this.legendConfig,
        title: this.titleConfig,
      },
    });
  }

  public renderChart(chartConfig, opts) {
    this.instance = new this.BKChart(this.context, {
      type: this.type,
      data: chartConfig,
      options: Object.assign({}, opts, this.options || {}),
    });
  }

  public mounted() {
    this.type = 'doughnut';
    this.init(this.$el);
  }
}

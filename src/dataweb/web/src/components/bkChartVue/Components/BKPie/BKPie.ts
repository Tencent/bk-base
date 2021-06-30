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

import { deepAssign } from '@/common/js/util';
import { Component, Prop, Vue, Watch } from 'vue-property-decorator';
import BKChart from '../../libs/index';
import BaseSettings from '../BaseSettings';
@Component()
export default class BKPie extends BaseSettings {
  get chartConfig() {
    return {
      labels: this.labels || [],
      datasets: this.series.map((ds: any) => Object.assign({}, this.defaultDataSet, ds)),
    };
  }

  get titleConfig() {
    return typeof this.title !== 'object' ? { display: false, text: '' } : this.title;
  }

  get legendConfig() {
    return Object.assign({}, { display: false });
  }
  @Prop({ default: {} }) public series: Record<string, any>;
  public defaultDataSet: any = {
    label: '',
    backgroundColor: '#3a84ff',
    data: [],
  };
  // @Prop({ default: {} }) legend: Object
  @Watch('series', { immediate: true })
  public handleDataChanged(val) {
    this.$nextTick(() => {
      if (!this.instance) {
        this.renderInstance();
      } else {
        this.update(this.chartConfig.datasets);
      }
    });
  }

  public renderInstance() {
    this.renderChart(this.chartConfig, {
      responsive: this.responsive,
      plugins: {
        legend: this.legendConfig,
        title: this.titleConfig,
      },
    });
  }

  public renderChart(chartConfig: any, opts: any) {
    const pieConfig = deepAssign({}, [opts, this.options || {}]);
    this.instance = new BKChart(this.context, {
      type: this.chartType || this.type,
      data: chartConfig,
      options: pieConfig,
    });
  }

  public mounted() {
    this.type = 'pie';
    this.init(this.$el);
  }
}

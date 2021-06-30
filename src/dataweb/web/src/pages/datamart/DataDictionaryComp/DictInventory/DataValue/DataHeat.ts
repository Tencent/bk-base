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

import Chart from '@/components/bkChartVue/Components/BaseChart.vue';
import { getQueryData, getRelyonNodeData, getScoreDistribution } from '@/pages/datamart/Api/DataInventory';
import HeaderType from '@/pages/datamart/common/components/HeaderType';
import { Component, Inject, Mixins } from 'vue-property-decorator';
import ChartPart from './components/ChartPart.vue';
import PieWidthTable from './components/PieWithTable';
import dataValue from './mixin/dataValue';
import bubbleChartOption from './mixin/mixin';

@Component({
  mixins: [dataValue],
  components: {
    HeaderType,
    Chart,
    ChartPart,
    PieWidthTable,
  },
})
export default class DataHeat extends bubbleChartOption {
  @Inject()
  public getParams!: Function;

  public isRequested = false;

  public loading = {
    isHeatRateLoading: false,
    isDataQueryLoading: false,
    isDataRelyonNodeLoading: false,
  };

  public heatChartData = {
    labels: [],
    datasets: [
      {
        label: '',
        data: [],
      },
    ],
  };

  public heatQuaryData = {
    labels: [],
    datasets: [
      {
        label: '',
        data: [],
      },
    ],
  };

  public relyonNodeData = {
    labels: [],
    datasets: [
      {
        label: '',
        data: [],
      },
    ],
  };

  public relyonNodeDataOptions = {};

  public heatQueryOptions = {};

  public heatChartOptions = {};

  public initData() {
    this.isRequested = false;
  }

  public getData() {
    if (this.isRequested) {
      return;
    }
    this.getScoreDistribution();
    this.getQueryData();
    this.getRelyonNodeData();
    this.$nextTick(() => {
      this.$refs.heatDistributed.initData();
    });
    this.isRequested = true;
  }

  public getRelyonNodeData() {
    this.loading.isDataRelyonNodeLoading = true;
    getRelyonNodeData(this.getParams()).then(res => {
      if (res.result && res.data) {
        this.relyonNodeData.labels = res.data.x;
        this.relyonNodeData.datasets = this.getChartDataSet(res);
        this.relyonNodeDataOptions = this.getChartOptions(this.$t('后继依赖节点个数'));
      }
      this.loading.isDataRelyonNodeLoading = false;
    });
  }

  public getQueryData() {
    this.loading.isDataQueryLoading = true;
    getQueryData(this.getParams()).then(res => {
      if (res.result && res.data) {
        this.heatQuaryData.labels = res.data.x;
        this.heatQuaryData.datasets = this.getChartDataSet(res);
        this.heatQueryOptions = this.getChartOptions(this.$t('查询次数'));
      }
      this.loading.isDataQueryLoading = false;
    });
  }

  public getChartOptions(xLabel: string) {
    return {
      responsive: true,
      animation: false,
      maintainAspectRatio: false,
      scales: {
        x: {
          scaleLabel: {
            display: true,
            labelString: xLabel,
          },
        },
        y: {
          scaleLabel: {
            display: true,
            labelString: this.$t('数据表个数（个）'),
          },
          type: 'linear',
          display: true,
          position: 'left',
        },
        y1: {
          scaleLabel: {
            display: true,
            labelString: this.$t('数据表占比（%）'),
          },
          type: 'linear',
          display: true,
          position: 'right',
          min: 0,
          max: 1,
          gridLines: {
            drawOnChartArea: false,
          },
        },
      },
    };
  }

  public getScoreDistribution() {
    this.loading.isHeatRateLoading = true;
    this.heatChartData = {
      labels: [],
      datasets: [
        {
          label: '',
          data: [],
        },
      ],
    };
    getScoreDistribution('heat', this.getParams()).then(res => {
      if (res.result && res.data) {
        this.$refs.heatRate.initChart(
          this.getChartData(res, 0.25),
          this.getBubbleOption(
            res,
            this.$t('热度评分'),
            this.$t('数据个数'),
            this.$t('热度评分（100分制）'),
            this.$t('数据累计占比（%）')
          )
        );
      }

      this.loading.isHeatRateLoading = false;
    });
  }
}

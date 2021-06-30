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

import { Component, Inject, Prop } from 'vue-property-decorator';
import HeaderType from '@/pages/datamart/common/components/HeaderType.vue';
import {
  getScoreDistribution,
  getApplyNodeData,
  getApplyProjData,
  getApplyAppData,
} from '@/pages/datamart/Api/DataInventory';
// 数据接口
import Chart from '@/components/bkChartVue/Components/BaseChart.vue';
import ChartPart from './components/ChartPart.vue';
import PieWidthTable from './components/PieWithTable';
import bubbleChartOption from './mixin/mixin';
import dataValue from './mixin/dataValue';

@Component({
  mixins: [dataValue],
  components: {
    HeaderType,
    Chart,
    ChartPart,
    PieWidthTable,
  },
})
export default class DataRange extends bubbleChartOption {
  @Inject()
  getParams!: Function;

  @Prop({ default: () => ({}) }) commonChartOptions: Record<string, any>;

  isRequested = false;

  loading = {
    isRangeRateLoading: true,
    isAppBizLoading: true,
    isAppProjLoading: true,
    isAppLoading: true,
  };

  applyAppData = {
    labels: [],
    datasets: [
      {
        data: [],
      },
    ],
  };

  applyProjData = {
    labels: [],
    datasets: [
      {
        data: [],
      },
    ],
  };

  applyNodeData = {
    labels: [],
    datasets: [
      {
        data: [],
      },
    ],
  };

  applyProjDataOptions = {};

  applyNodeDataOptions = {};

  applyAppDataOptions = {};

  initData() {
    this.isRequested = false;
  }

  getData() {
    if (this.isRequested) return;
    this.getApplyNodeData();
    this.getApplyProjData();
    this.getApplyAppData();
    this.getScoreDistribution();
    this.$nextTick(() => {
      this.$refs.rangeDistributed.initData();
    });
    this.isRequested = true;
  }

  getScoreDistribution() {
    this.loading.isRangeRateLoading = true;
    getScoreDistribution('range', this.getParams()).then(res => {
      if (res.result && res.data) {
        this.$refs.rangeRate.initChart(
          this.getChartData(res, 0.3),
          this.getBubbleOption(
            res,
            this.$t('广度评分'),
            this.$t('数据个数'),
            this.$t('广度评分（100分制）'),
            this.$t('数据累计占比（%）')
          )
        );
      }
      this.loading.isRangeRateLoading = false;
    });
  }

  getApplyNodeData() {
    this.loading.isAppBizLoading = true;
    this.applyNodeData = {
      labels: [],
      datasets: [
        {
          data: [],
        },
      ],
    };
    getApplyNodeData(this.getParams()).then(res => {
      if (res.result && res.data) {
        this.applyNodeData.labels = res.data.x;
        this.applyNodeData.datasets = this.getChartDataSet(res);
        this.applyNodeDataOptions = Object.assign({}, this.commonChartOptions, {
          title: {
            display: false,
          },
          scales: Object.assign({}, this.commonChartOptions.scales, {
            x: {
              display: true,
              scaleLabel: {
                display: true,
                labelString: this.$t('业务个数'),
              },
            },
          }),
        });
      }
      this.loading.isAppBizLoading = false;
    });
  }

  getApplyProjData() {
    this.loading.isAppProjLoading = true;
    this.applyProjData = {
      labels: [],
      datasets: [
        {
          data: [],
        },
      ],
    };
    getApplyProjData(this.getParams()).then(res => {
      if (res.result && res.data) {
        this.applyProjData.labels = res.data.x;
        this.applyProjData.datasets = this.getChartDataSet(res);
        this.applyProjDataOptions = Object.assign({}, this.commonChartOptions, {
          title: {
            display: false,
          },
          scales: Object.assign({}, this.commonChartOptions.scales, {
            x: {
              display: true,
              scaleLabel: {
                display: true,
                labelString: this.$t('项目个数'),
              },
            },
          }),
        });
      }
      this.loading.isAppProjLoading = false;
    });
  }

  getApplyAppData() {
    this.loading.isAppLoading = true;
    this.applyAppData = {
      labels: [],
      datasets: [
        {
          data: [],
        },
      ],
    };
    getApplyAppData(this.getParams()).then(res => {
      if (res.result && res.data) {
        this.applyAppData.labels = res.data.x;
        this.applyAppData.datasets = this.getChartDataSet(res);
        this.applyAppDataOptions = Object.assign({}, this.commonChartOptions, {
          title: {
            display: false,
          },
          scales: Object.assign({}, this.commonChartOptions.scales, {
            x: {
              display: true,
              scaleLabel: {
                display: true,
                labelString: this.$t('APP个数'),
              },
            },
          }),
        });
      }
      this.loading.isAppLoading = false;
    });
  }
}

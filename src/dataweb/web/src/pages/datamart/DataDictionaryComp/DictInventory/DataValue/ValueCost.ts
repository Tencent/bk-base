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

// 数据接口
import Chart from '@/components/bkChartVue/Components/BaseChart.vue';
import {
  getScoreDistribution,
  getStorageCapacityDistribution,
  getStorageCapacityTrend,
} from '@/pages/datamart/Api/DataInventory';
import HeaderType from '@/pages/datamart/common/components/HeaderType';
import { chartColors } from '@/pages/datamart/common/config';
import { Component, Inject } from 'vue-property-decorator';
import ChartPart from './components/ChartPart.vue';
import PieWidthTable from './components/PieWithTable';
import bubbleChartOption from './mixin/mixin';

@Component({
  components: {
    HeaderType,
    Chart,
    ChartPart,
    PieWidthTable,
  },
})
export default class ValueCost extends bubbleChartOption {
  @Inject()
  public getParams!: Function;

  public loading = {
    valueRateLoading: true,
    valueCostLoading: false,
    valueCostLevelLoading: false,
    storageTrendLoading: false,
  };

  public storageTrendData = {
    labels: [],
    datasets: [],
  };

  public storageTrendOption = {
    plugins: {
      legend: {
        display: false,
      },
    },
  };

  public storageDistributionData = {
    labels: [],
    datasets: [
      {
        data: [],
        backgroundColor: [],
      },
    ],
  };

  // 总储存成本
  public totalStorageCapacity = '';

  get isShowPie() {
    // data数组里都为0时，隐藏饼图
    return !this.storageDistributionData.datasets[0].data.every(item => !item);
  }

  get isStorageLoading() {
    return this.loading.storageTrendLoading || this.loading.valueCostLevelLoading;
  }

  public created() {
    this.initData();
  }

  public initData() {
    this.getData();
  }

  public getData() {
    this.getScoreDistribution();
    this.getStorageCapacityDistribution();
    this.getStorageCapacityTrend();
    this.$nextTick(() => {
      this.$refs.valueLevel.initData();
      this.$refs.valueCostRatio.initData();
    });
  }

  public getStorageCapacityDistribution() {
    this.loading.valueCostLevelLoading = true;
    getStorageCapacityDistribution(this.getParams()).then(res => {
      if (res.result && res.data && res.data.capacity_list) {
        this.storageDistributionData.labels = res.data.label;
        this.storageDistributionData.datasets = [
          {
            data: res.data.capacity_list,
            backgroundColor: chartColors.slice(1, res.data.capacity_list.length + 1),
          },
        ];
        this.totalStorageCapacity = res.data.sum_capacity.toFixed(1) + res.data.unit;
      }
      this.loading.valueCostLevelLoading = false;
    });
  }
  public getStorageCapacityTrend() {
    this.loading.storageTrendLoading = true;
    this.storageTrendData = {
      labels: [],
      datasets: [
        {
          label: '',
          data: [],
          type: '',
          backgroundColor: '',
        },
      ],
    };
    getStorageCapacityTrend(this.getParams()).then(res => {
      if (res.result && res.data && res.data.unit) {
        const unit = res.data.unit;
        this.storageTrendData.labels = res.data.time;
        this.storageTrendData.datasets = [
          {
            label: 'total',
            data: res.data.total_capacity,
            type: 'line',
            backgroundColor: chartColors[0],
            fill: false,
            yAxisID: 'y1',
            borderColor: chartColors[0],
            borderWidth: 1,
          },
        ];
        const keys = Object.keys(res.data).filter(item => !['time', 'unit', 'total_capacity'].includes(item));
        keys.forEach((item, index) => {
          this.storageTrendData.datasets.push({
            label: item.split('_')[0],
            data: res.data[item],
            type: 'bar',
            backgroundColor: chartColors[index + 1],
            yAxisID: 'y',
          });
        });
        this.storageTrendOption = {
          responsive: true,
          maintainAspectRatio: false,
          animation: false,
          plugins: {
            title: {
              display: true,
              text: this.$t('最近7日 | 变化趋势'),
              fontStyle: 450,
              fontColor: '#737987',
              padding: {
                left: 0,
              },
              fontStyle: 'normal',
            },
            legend: {
              display: false,
            },
          },
          scales: {
            y: {
              scaleLabel: {
                display: true,
                labelString: this.$t('存储成本') + `（${unit}）`,
              },
              position: 'left',
            },
            y1: {
              scaleLabel: {
                display: true,
                labelString: this.$t('总存储成本') + `（${unit}）`,
              },
              position: 'right',
            },
          },
        };
      }
      this.loading.storageTrendLoading = false;
    });
  }

  public getScoreDistribution() {
    this.loading.valueRateLoading = true;

    getScoreDistribution('asset_value', this.getParams()).then(res => {
      if (res.result && res.data) {
        this.$refs.valueRate.initChart(
          this.getChartData(res),
          this.getBubbleOption(
            res,
            this.$t('价值评分'),
            this.$t('数据个数'),
            this.$t('价值评分（100分制）'),
            this.$t('数据累计占比（%）')
          )
        );
      }
      this.loading.valueRateLoading = false;
    });
    this.loading.valueCostLoading = true;
    getScoreDistribution('assetvalue_to_cost', this.getParams()).then(res => {
      if (res.result && res.data) {
        const chartOptions = this.getBubbleOption(
          res,
          this.$t('收益比（价值/成本）'),
          this.$t('数据个数'),
          this.$t('收益比（价值/成本）')
        );
        const options = Object.assign({}, chartOptions, {
          scales: Object.assign({}, chartOptions.scales, {
            y: {
              scaleLabel: {
                display: true,
                labelString: this.$t('收益比（价值/成本）'),
              },
              min: 0,
              max: 1,
              stepSize: 0.2,
            },
          }),
        });
        this.$refs.valueCost.initChart(this.getChartData(res), options);
      }
      this.loading.valueCostLoading = false;
    });
  }
}

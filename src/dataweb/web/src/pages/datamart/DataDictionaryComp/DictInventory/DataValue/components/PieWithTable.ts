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
import { getScoreDistributionLevelTrend } from '@/pages/datamart/Api/DataInventory';
import { chartColors } from '@/pages/datamart/common/config';
import { handlerPercentText, tranNumber } from '@/pages/datamart/common/utils.js';
// 数据接口
import { IValueLevelDistributionData } from '@/pages/datamart/InterFace/DataInventory';
import { Component, Inject, Prop, Vue, Watch } from 'vue-property-decorator';
import ChartPart from './ChartPart.vue';

const valueLevels = [$t('低'), $t('中'), $t('高'), $t('超高')];

@Component({
  components: {
    Chart,
    ChartPart,
  },
})
export default class PieWithTable extends Vue {
  @Prop() public levelDistributionType: string;
  @Prop() public levelScoreType: string;
  @Prop() public title: string;
  @Inject()
  public getParams!: Function;

  public innerText = 0;

  public valueCostChartData = {
    labels: [],
    datasets: [
      {
        data: [],
      },
    ],
  };

  public chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: false,
      },
    },
    layout: {
      padding: {
        left: 0,
        right: 0,
        top: 0,
        bottom: 0,
      },
    },
  };

  public valueCostChartOption = {};

  public valueTrendData = [];

  public pieChartData = {
    labels: [''],
    datasets: [
      {
        data: [],
        backgroundColor: [''],
      },
    ],
  };

  public isLoading = true;

  public levelDistributionList: IValueLevelDistributionData;

  get chartColors() {
    return chartColors;
  }

  get isShowPie() {
    // data数组里都为0时，隐藏饼图
    return !this.pieChartData.datasets[0].data.every(item => !item);
  }

  get tranNumber() {
    return tranNumber;
  }

  public initData() {
    this.innerText = 0;
    this.getScoreDistributionLevelTrend();
  }

  public getScoreDistributionLevelTrend() {
    this.isLoading = true;
    this.pieChartData = {
      labels: [],
      datasets: [
        {
          data: [],
          backgroundColor: [''],
        },
      ],
    };
    this.valueTrendData = [];
    getScoreDistributionLevelTrend(this.levelScoreType, this.getParams()).then(res => {
      if (res.result && res.data) {
        this.valueTrendData = res.data.score_trend.map((item, index) => {
          return {
            tips: `${this.$t('价值评分区间是')}${item.score_level}`,
            index,
            nums: 0,
            percent: 0,
            level: valueLevels[index],
            trendData: {
              labels: item.time,
              datasets: [
                {
                  data: item.num,
                  fill: false,
                  borderColor: chartColors[0],
                  backgroundColor: chartColors[0],
                  borderWidth: 1,
                },
              ],
            },
            options: {
              plugins: {
                legend: {
                  display: false,
                },
                tooltip: {
                  callbacks: {
                    title() {
                      return '';
                    },
                    beforeLabel(tooltipItem) {
                      return `${$t('日期')}：${tooltipItem.label}`;
                    },
                    label(tooltipItem) {
                      return `${$t('数据个数')}：${tooltipItem.formattedValue}`;
                    },
                  },
                  // backgroundColor: '#FFF',
                  titleFontSize: 16,
                  titleFontColor: '#0066ff',
                  bodyFontColor: 'white',
                  bodyFontSize: 12,
                  displayColors: false,
                },
              },
              layout: {
                padding: {
                  left: 10,
                  right: 10,
                  top: 10,
                  bottom: 10,
                },
              },
              maintainAspectRatio: false,
              scales: {
                y: {
                  display: false,
                },
                x: {
                  display: false,
                },
              },
            },
          };
        });
        this.levelDistributionList = res.data.level_distribution;
        this.getValueNums();
      }
      this.isLoading = false;
    });
  }

  public getValueNums() {
    if (this.levelDistributionList && Object.keys(this.levelDistributionList).length) {
      const { x, y, z, sum_count } = this.levelDistributionList;
      this.pieChartData.labels = x.map((item, index) => valueLevels[index]);
      this.pieChartData.datasets = [
        {
          data: y,
          backgroundColor: chartColors.slice(0, y.length),
        },
      ];
      if (this.valueTrendData.length) {
        y.forEach((num, index) => {
          this.valueTrendData[index].nums = num;
          this.valueTrendData[index].percent = `${handlerPercentText(z[index], 7)}%`;
        });
      }
      this.innerText = sum_count.toFixed(0);
    }
  }
}

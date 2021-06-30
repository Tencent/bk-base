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

import BKChart from '@blueking/bkcharts';
import { Component, Vue } from 'vue-property-decorator';
import BubbleChart from '../components/BubbleChart.vue';

@Component({
  components: {
    BubbleChart,
  },
})
export default class bubbleChartOption extends Vue {
  public initChart(canvas: Element, chartData: any, options: any) {
    return new BKChart(canvas, {
      type: 'bubble',
      data: chartData,
      options,
    });
  }

  public getChartData(res: any, multiple?= 0) {
    const chartData = {
      labels: [],
      datasets: [
        {
          data: [],
        },
      ],
    };
    res.data.x.forEach((item, index) => {
      chartData.datasets[0].data.push({
        x: item,
        y: res.data.y[index],
        r: res.data.cnt[index] / 2,
      });
    });
    if (!multiple) {
      chartData.labels = JSON.parse(JSON.stringify(res.data.x)).reverse();
    } else {
      const labelMax = Math.max(null, ...res.data.x);
      // 防止气泡图和colorbar重叠，需要增加x轴的宽度，热度和广度评分气泡图暂定25%
      const len = labelMax + Math.ceil(labelMax * multiple);
      for (let index = 0; index < len; index++) {
        chartData.labels.push(index);
      }
    }
    return chartData;
  }

  public getPercent(number: number, isAddSymbol = true) {
    if (!number) {
      return '';
    }
    return Number((number * 100).toFixed(3)) + (isAddSymbol ? '%' : '');
  }

  public getBubbleOption(
    res: any,
    scoreLabel: string = $t('热度评分'),
    dataCountLabel: string = $t('数据个数'),
    yName: string = this.$t('价值评分（100分制）'),
    xName: string = this.$t('数据累计占比（%）')
  ) {
    const that = this;
    const colorBarMax = Math.max(null, ...res.data.y);
    return {
      responsive: true,
      animation: false,
      maintainAspectRatio: false,
      scales: {
        y: {
          scaleLabel: {
            display: true,
            labelString: yName,
          },
          min: -20,
          max: 100,
        },
        x: {
          type: 'category',
          ticks: {
            callback(value) {
              return that.getPercent(JSON.parse(JSON.stringify(res.data.cum_perc))
                .reverse()[value - 1], false);
            },
            min: '0',
            max: '100',
          },
          scaleLabel: {
            display: true,
            labelString: xName,
          },
        },
      },
      plugins: {
        legend: {
          display: false,
        },
        colorBar: {
          enabled: true,
          title: {
            content: '',
          },
          width: 60,
          height: 25,
          padding: 15,
          position: 'right',
          min: 0,
          max: colorBarMax,
        },
        tooltip: {
          callbacks: {
            beforeLabel(tooltipItem) {
              const score = tooltipItem.formattedValue
                .slice(1, tooltipItem.formattedValue.length - 1)
                .split(',')[1];
              return `${scoreLabel}：${score}`;
            },
            label(tooltipItem) {
              const count = res.data.z[tooltipItem.datasetIndex];
              const perc = res.data.perc[tooltipItem.datasetIndex];
              return `${dataCountLabel}：${count}（${that.getPercent(perc)}）`;
            },
            afterLabel(tooltipItem) {
              const score = tooltipItem.formattedValue
                .slice(1, tooltipItem.formattedValue.length - 1)
                .split(',')[1];
              const cumPerc = res.data.cum_perc[tooltipItem.datasetIndex];
              return `累计${that.getPercent(cumPerc)}的数据${scoreLabel}>=${score}`;
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
    };
  }
}

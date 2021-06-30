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

import { Component, Watch, Prop, Vue } from 'vue-property-decorator';
import { lookQualityIndex } from '@/pages/datamart/Api/DataQuality';
import chart from '@/components/bkChartVue/Components/BaseChart.vue';
import moment from 'moment';
import { chartColors } from '@/pages/datamart/common/config';
import NoData from '@/pages/datamart/DataDict/components/children/chartComponents/NoData.vue';
import PlotlyChart from '@/components/plotly/index.vue';
@Component({
  components: {
    chart,
    NoData,
    PlotlyChart,
  },
})
export default class QualityIndexChart extends Vue {
  @Prop() ruleId: string;
  @Prop() startTime: string;
  @Prop() endTime: string;

  plotlyChartData = [];

  plotlyChartConfig = {
    operationButtons: ['resetScale2d', 'select2d'],
  };

  plotlyChartLayout = {
    hovermode: 'closest',
    width: 450,
    height: 270,
    xaxis: {
      autorange: true,
      showgrid: true, // 是否显示x轴线条
      showline: true, // 是否绘制出该坐标轴上的直线部分
      zeroline: true,
      gridcolor: 'rgba(145,164,237,0.2)',
      tickcolor: '#3a84ff',
      tickangle: 45,
      tickformat: '%H:%M:%S',
      tickfont: {
        // color: '#b2bac0'
      },
      linecolor: '#3a84ff',
    },
    yaxis: {
      showticklabels: true,
      autorange: true,
      rangemode: 'tozero',
    },
    showlegend: true,
    legend: {
      y: 1.2,
      x: 0.5,
      orientation: 'h',
      font: {
        color: '#b2bac0',
      },
    },
    margin: {
      l: 30,
      r: 50,
      b: 70,
      t: 60,
    },
  };

  isLoading = false;

  dialogSetting = {
    isShow: false,
  };

  chartData = {};

  chartOptions = [];

  plotlyLayout = [];

  get momentStartTime() {
    return moment(Number(this.startTime) * 1000).format('YYYY-MM-DD HH:mm:ss');
  }

  get momentEndTime() {
    return moment(Number(this.endTime) * 1000).format('YYYY-MM-DD HH:mm:ss');
  }

  @Watch('ruleId', { immediate: true })
  onChartDataChanged(val: string) {
    if (val) {
      this.lookQualityIndex();
    }
  }

  cancel() {
    this.$nextTick(() => {
      this.$emit('close');
    });
  }

  // 获取规则配置函数列表
  lookQualityIndex() {
    this.isLoading = true;
    lookQualityIndex(this.ruleId, this.startTime, this.endTime).then(res => {
      console.log(res);

      if (res.result && res.data) {
        const fieldsMap = {
          today: this.$t('今日'),
          last_day: this.$t('昨日'),
          last_week: this.$t('上周'),
        };
        const timeDays = {
          today: 0,
          last_day: 1,
          last_week: 7,
        };
        Object.keys(res.data).forEach((key: string, idx: number) => {
          const chartData = [];

          Object.keys(fieldsMap).forEach((item, index) => {
            const fieldData = {
              x: [],
              y: [],
              type: 'scatter',
              name: fieldsMap[item],
              line: {
                color: chartColors[index],
              },
            };
            const oneDayTimes = 24 * 60 * 60 * 1000;
            // 经过处理后，图表数据的第一和最后一个数据的x轴数据可能相同，避免图上出现一道横线，需要去掉最后一个数据
            res.data[key][item].pop();
            res.data[key][item].forEach(child => {
              const yName = res.data[key].metric_name;
              fieldData.x
                .push(moment(Number(child.time) * 1000 + timeDays[item] * oneDayTimes).format('HH:mm'));
              fieldData.y.push(child[yName]);
            });
            chartData.push(fieldData);
          });
          this.plotlyChartData.push(chartData);

          this.plotlyLayout.push(
            Object.assign({}, this.plotlyChartLayout, {
              title: {
                text: res.data[key].metric_alias,
                font: {
                  size: 14,
                },
                y: 0.99,
              },
            })
          );
        });
      }
      this.isLoading = false;
    });
  }
}

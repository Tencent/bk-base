

<!--
  - Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
  - Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
  - BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
  -
  - License for BK-BASE 蓝鲸基础平台:
  - -------------------------------------------------------------------
  -
  - Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
  - documentation files (the "Software"), to deal in the Software without restriction, including without limitation
  - the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
  - and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
  - The above copyright notice and this permission notice shall be included in all copies or substantial
  - portions of the Software.
  -
  - THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
  - LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
  - NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
  - WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
  - SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
  -->

<template>
  <div class="alert-chart">
    <div class="data-interrupt-chart">
      <div class="upstream-data">
        <div class="type">
          <span>
            {{ $t('上游数据量') }}
          </span>
        </div>
        <div v-for="upstream in upstreamList"
          :key="upstream">
          <PlotlyChart v-if="allChartData[upstream].loaded"
            :chartData="allChartData[upstream].chartData"
            :chartConfig="allChartData[upstream].chartConfig"
            :chartLayout="allChartData[upstream].chartLayout" />
        </div>
      </div>
      <hr class="mb10">
      <div class="downstream-data">
        <div class="type">
          <span>
            {{ $t('下游数据量') }}
          </span>
        </div>
        <div v-for="downstream in downstreamList"
          :key="downstream">
          <PlotlyChart v-if="allChartData[downstream].loaded"
            :chartData="allChartData[downstream].chartData"
            :chartConfig="allChartData[downstream].chartConfig"
            :chartLayout="allChartData[downstream].chartLayout" />
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import Vue from 'vue';
import moment from 'moment';
import PlotlyChart from '@/components/plotly';
export default {
  components: {
    PlotlyChart,
  },
  props: {
    alertInfo: {
      type: Object,
      required: true,
    },
  },
  data() {
    return {
      dataInterruptLoading: false,
      alertTime: moment(this.alertInfo.alert_time).unix(),
      dimensions: this.alertInfo.dimensions,
      allChartData: {},
      upstreamList: [],
      downstreamList: [],
      minTime: 9999999999999,
      maxTime: 0,
      chartData: {
        type: 'scatter',
        mode: 'lines',
        name: this.$t('数据量'),
        line: {
          color: 'rgba(133, 142, 200, 1)',
        },
        x: [],
        y: [],
        fillcolor: 'rgba(133, 142, 200, .7)',
      },
      alertData: {
        type: 'lines',
        name: this.$t('告警'),
        x: [],
        y: [0, 0],
        line: {
          color: '#fe621d',
          width: 2,
          dash: 'dash',
        },
        hovertemplate: this.$t('告警'),
      },
      chartLayout: {
        title: this.$t('数据量'),
        showlegend: true,
        xaxis: {
          type: 'date',
          range: null,
          autorange: false,
          showgrid: true, // 是否显示x轴线条
          showline: true, // 是否绘制出该坐标轴上的直线部分
          zeroline: true,
          gridcolor: 'rgba(145,164,237,0.2)',
          tickcolor: '#3a84ff',
          tickangle: 0,
          tickformat: '%H:%M:%S',
          tickfont: {
            color: '#b2bac0',
          },
          title: {
            text: this.$t('时间'),
          },
          linecolor: '#3a84ff',
        },
        yaxis: {
          type: 'linear',
          rangemode: 'tozero',
          showticklabels: true,
          title: {
            text: this.$t('数据条数'),
          },
        },
      },
    };
  },
  computed: {
    maxTimeStr() {
      return new Date(this.maxTime * 1000);
    },
    minTimeStr() {
      return new Date(this.minTime * 1000);
    },
  },
  mounted() {
    const dimensions = this.alertInfo.dimensions;
    this.upstreamList = dimensions.upstreams.split(',');
    this.downstreamList = dimensions.downstreams.split(',');
    // 添加告警曲线
    this.alertData.x.push(this.alertInfo.alert_time);
    this.alertData.x.push(this.alertInfo.alert_time);
    this.initChartData(this.upstreamList);
    this.initChartData(this.downstreamList);
    this.getAlertData();
  },
  methods: {
    initChartData(logicalKeyList) {
      for (let logicalKey of logicalKeyList) {
        // 把形式为{data_set_id}_{storage_key}的logical_key展开为实际的内容
        const logicalItems = logicalKey.split('_');
        const splitIndex = logicalItems.length - 2;
        const dataSetId = logicalItems.slice(0, splitIndex).join('_');
        const storageKey = logicalItems.slice(splitIndex).join('_');
        Vue.set(this.allChartData, logicalKey, {
          dataSetId: dataSetId,
          storageKey: storageKey,
          chartData: [Object.assign({}, this.alertData), Object.assign({}, this.chartData)],
          chartLayout: Object.assign({}, this.chartLayout),
          loading: false,
          loaded: false,
        });
      }
    },
    getAlertData() {
      for (let logicalKey in this.allChartData) {
        (logicalKey => {
          let chartInfo = this.allChartData[logicalKey];
          chartInfo.loading = true;
          const options = {
            params: {
              measurement: 'output_count',
            },
            query: {
              data_set_ids: [chartInfo.dataSetId],
              conditions: JSON.stringify({ storage: chartInfo.storageKey }),
              start_time: `${this.alertTime - 3600 * 6}s`,
              end_time: `${this.alertTime + 3600 * 6}s`,
              fill: 'null',
            },
          };
          this.bkRequest.httpRequest('dmonitorCenter/getDmonitorMetrics', options).then(res => {
            if (res.result) {
              for (let data of res.data) {
                if (data.data_set_id === chartInfo.dataSetId) {
                  let dataSeries = [];
                  let timeSeries = [];
                  let textSeries = [];
                  for (let metric of data.series) {
                    let timestr = moment(metric.time * 1000).format('YYYY-MM-DD HH:mm:ss');
                    dataSeries.push(metric.output_count);
                    timeSeries.push(timestr);
                    if (metric.output_count > chartInfo.chartData[0].y[1]) {
                      chartInfo.chartData[0].y[1] = metric.output_count;
                    }
                    if (metric.time < this.minTime) {
                      this.minTime = metric.time;
                    }
                    if (metric.time > this.maxTime) {
                      this.maxTime = metric.time;
                    }
                  }
                  chartInfo.chartData[1].x = timeSeries;
                  chartInfo.chartData[1].y = dataSeries;
                  chartInfo.chartLayout.title = this.$t(`${data.data_set_id}(${data.storage})数据量`);
                }
              }
            } else {
              this.getMethodWarning(res.message, res.code);
            }
            if (res.data.length === 0) {
              chartInfo.chartLayout.title = this.$t(`${chartInfo.dataSetId}数据量`);
            }
            chartInfo.chartLayout.xaxis.range = [this.minTimeStr, this.maxTimeStr];
            chartInfo.loaded = true;
            chartInfo.loading = false;
          });
        })(logicalKey);
      }
    },
  },
};
</script>

<style lang="scss">
.alert-chart {
  .type {
    height: 30px;
    line-height: 30px;
    padding: 0px 15px;
    font-weight: bold;
    &::before {
      content: '';
      width: 2px;
      height: 19px;
      background: #3a84ff;
      display: inline-block;
      margin-right: 15px;
      position: relative;
      top: 4px;
    }
    .name {
      display: inline-block;
      min-width: 58px;
      height: 24px;
      color: white;
      line-height: 24px;
      background-color: #737987;
      border-radius: 2px;
      text-align: center;
      margin-top: 16px;
      padding: 0 5px;
    }
    .bk-icon {
      color: #3a84ff;
      cursor: pointer;
    }
  }
}
</style>

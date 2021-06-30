

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
    <div class="data-trend-chart">
      <div class="data-trend">
        <div class="type">
          <span>
            {{ $t('数据同比趋势') }}
          </span>
        </div>
        <PlotlyChart v-if="loaded && lastLoaded"
          :chartData="chartData"
          :chartLayout="chartLayout" />
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
    const dimensions = this.alertInfo.dimensions;
    return {
      alertTime: moment(this.alertInfo.alert_time).unix(),
      loaded: false,
      lastLoaded: false,
      chartData: [
        {
          type: 'lines',
          name: this.$t('告警'),
          x: [this.alertInfo.alert_time, this.alertInfo.alert_time],
          y: [0, 4],
          line: {
            color: '#fe621d',
            width: 2,
            dash: 'dash',
          },
          hovertemplate: this.$t('告警'),
        },
        {
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
        {
          type: 'scatter',
          mode: 'lines',
          name: this.$t('同比周期数据量'),
          line: {
            color: 'rgba(133, 142, 200, 1)',
            dash: 'dash',
          },
          x: [],
          y: [],
          fillcolor: 'rgba(133, 142, 200, .7)',
        },
      ],
      flowId: dimensions.flow_id,
      nodeId: dimensions.node_id,
      dataSetId: dimensions.data_set_id,
      chartLayout: {
        showlegend: true,
        xaxis: {
          type: 'date',
          range: [this.minTimeStr, this.maxTimeStr],
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
      return new Date(this.alertTime * 1000 + 3600000 * 6);
    },
    minTimeStr() {
      return new Date(this.alertTime * 1000 - 3600000 * 6);
    },
  },
  mounted() {
    this.getAlertData();
    this.getLastAlertData();
  },
  methods: {
    getAlertData() {
      this.loading = true;
      const options = {
        params: {
          measurement: 'output_count',
        },
        query: {
          flow_ids: [this.flowId],
          start_time: `${this.alertTime - 3600 * 6}s`,
          end_time: `${this.alertTime + 3600 * 6}s`,
          fill: 'null',
        },
      };
      this.bkRequest.httpRequest('dmonitorCenter/getDmonitorMetrics', options).then(res => {
        if (res.result) {
          for (let data of res.data) {
            if (data.data_set_id === this.dataSetId) {
              let dataSeries = [];
              let timeSeries = [];
              let textSeries = [];
              for (let metric of data.series) {
                let timestr = moment(metric.time * 1000).format('YYYY-MM-DD HH:mm:ss');
                dataSeries.push(metric.output_count);
                timeSeries.push(timestr);
                if (metric.output_count > this.chartData[0].y[1]) {
                  this.chartData[0].y[1] = metric.output_count;
                }
                if (metric.time < this.minTime) {
                  this.minTime = metric.time;
                }
                if (metric.time > this.maxTime) {
                  this.maxTime = metric.time;
                }
              }
              this.chartData[1].x = timeSeries;
              this.chartData[1].y = dataSeries;
            }
          }
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        this.chartLayout.xaxis.range = [this.minTimeStr, this.maxTimeStr];
        this.loaded = true;
        this.loading = false;
      });
    },
    getLastAlertData() {
      this.lastLoading = true;
      const hours = this.alertInfo.monitor_config.diff_period;
      const options = {
        params: {
          measurement: 'output_count',
        },
        query: {
          flow_ids: [this.flowId],
          start_time: `${this.alertTime - 3600 * 6 - hours * 3600}s`,
          end_time: `${this.alertTime + 3600 * 6 - hours * 3600}s`,
          fill: 'null',
        },
      };
      this.bkRequest.httpRequest('dmonitorCenter/getDmonitorMetrics', options).then(res => {
        if (res.result) {
          for (let data of res.data) {
            if (data.data_set_id === this.dataSetId) {
              let dataSeries = [];
              let timeSeries = [];
              let textSeries = [];
              for (let metric of data.series) {
                let timestr = moment((metric.time + hours * 3600) * 1000).format('YYYY-MM-DD HH:mm:ss');
                dataSeries.push(metric.output_count);
                timeSeries.push(timestr);
              }
              this.chartData[2].x = timeSeries;
              this.chartData[2].y = dataSeries;
            }
          }
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        this.chartLayout.xaxis.range = [this.minTimeStr, this.maxTimeStr];
        this.lastLoaded = true;
        this.lastLoading = false;
      });
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

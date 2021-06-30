

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
  <div>
    <div class="type">
      <span>{{ $t('统计分布') }}</span>
    </div>
    <div class="struc-container">
      <DataChartStruc v-bkloading="{ isLoading: isHeatChartLoading }">
        <template slot="explain">
          {{ `${$t('该数据当前热度评分')}：` }}
          <span class="score-color"> {{ heatScore }} </span>
          ({{ $t('一百分制') }})，{{ $t('大于等于平台') }}
          <span class="score-color"> {{ heatRanking }} </span>
          {{ $t('的') }} {{ $t('数据') }}。
        </template>
        <template slot="leftChart">
          <p class="chart-explain">
            {{ $t('数据热度变化趋势') }}<i v-bk-tooltips="rateToolText[$t('热度评分')]"
              class="icon-question-circle" />
          </p>
          <PlotlyChart v-if="heatChartData[0].x.length"
            :chartData="heatChartData"
            :chartConfig="config"
            :chartLayout="dataHeatLayout" />
          <NoData v-else />
        </template>
        <template slot="rightChart">
          <div class="chart-explain">
            {{ $t('数据查询趋势') }}
          </div>
          <PlotlyChart v-if="queryChartData[0].x.length"
            :chartData="queryChartData"
            :chartConfig="config"
            :chartLayout="queryTimeConfig" />
          <NoData v-else />
        </template>
      </DataChartStruc>
      <DataChartStruc v-bkloading="{ isLoading: isRangeChartLoading }">
        <template slot="explain">
          {{ `${$t('该数据当前广度评分')}：` }}
          <span class="score-color">{{ rangeScore }}</span>
          ({{ $t('一百分制') }})，{{ $t('大于等于平台') }}
          <span class="score-color">{{ rangeRanking }}</span> {{ $t('的') }} {{ $t('数据') }}。
        </template>
        <template slot="leftChart">
          <p class="chart-explain">
            {{ $t('数据广度变化趋势') }}<i v-bk-tooltips="rateToolText[$t('广度评分')]"
              class="icon-question-circle" />
          </p>
          <PlotlyChart v-if="rangeChartData[0].x.length"
            :chartData="rangeChartData"
            :chartConfig="config"
            :chartLayout="dataRangeLayout" />
          <NoData v-else />
        </template>
        <template slot="rightChart">
          <p class="chart-explain">
            {{ $t('数据应用分布') }}
          </p>
          <PlotlyChart v-if="chartData[0].x.length"
            :chartData="chartData"
            :chartConfig="config"
            :chartLayout="nodeCount" />
          <NoData v-else />
        </template>
      </DataChartStruc>
    </div>
  </div>
</template>
<script>
import DataChartStruc from '@/pages/datamart/DataDict/components/children/chartComponents/DataChartStruc';
import PlotlyChart from '@/components/plotly';

export default {
  components: {
    DataChartStruc,
    PlotlyChart,
    NoData: () => import('@/components/global/NoData.vue'),
  },
  props: {
    correctHeatScore: {
      type: Number,
      default: 0,
    },
    correctRangeScore: {
      type: Number,
      default: 0,
    },
  },
  data() {
    return {
      isHeatChartLoading: false,
      isRangeChartLoading: false,
      rateToolText: {
        [this.$t('热度评分')]: this.$t('热度评分tips'),
        [this.$t('广度评分')]: this.$t('广度评分tips'),
      },
      layout: {
        showlegend: true,
        legend: {
          orientation: 'h',
          x: 0.1,
          y: 1.2,
          font: {
            size: 10,
          },
        },
        height: 240,
        width: 280,
        xaxis: {
          title: {
            // text: '时间',
            font: {
              size: 12,
            },
          },
          zeroline: true,
          showgrid: false,
          showline: true,
          autorange: true,
          tickfont: { size: 10 },
          tickangle: 30,
        },
        margin: {
          l: 40,
          r: 20,
          b: 40,
          t: 20,
        },
        yaxis: {
          automargin: true,
          rangemode: 'nonnegative',
          side: 'right',
          dtick: 1,
        },
        yaxis2: {
          automargin: true,
          titlefont: { color: 'rgb(148, 103, 189)' },
          overlaying: 'y',
          rangemode: 'nonnegative',
        },
        titlefont: { size: 12 },
      },
      config: {
        // operationButtons: ['resetScale2d', 'zoomIn2d', 'select2d']
        operationButtons: ['autoScale2d', 'resetScale2d'],
        // responsive: true
      },
      chartData: [
        {
          x: [],
        },
      ],
      heatChartData: [
        {
          x: [],
        },
      ],
      queryChartData: [
        {
          x: [],
        },
      ],
      rangeChartData: [
        {
          x: [],
        },
      ],
      queryTimeConfig: null,
      nodeCount: null,
      nowRangeScore: 13.1,
      nowHeatScore: 0,
      heatRanking: '0%',
      rangeRanking: '0%',
      dataHeatLayout: null,
      dataRangeLayout: null,
    };
  },
  computed: {
    heatScore() {
      return this.correctHeatScore || this.nowHeatScore;
    },
    rangeScore() {
      return this.correctRangeScore || this.nowRangeScore;
    },
  },
  mounted() {
    this.getChartData();
    this.getHeatTrendData();
    this.getDictScore();
  },
  methods: {
    getRangeTdick(max, maxValue, isShowMax) {
      // 获取图表Y轴最大值及Y轴间距大小
      max = max <= maxValue ? maxValue : max;
      let tdick = max <= maxValue ? 2 : null;
      if (isShowMax) {
        max += 2;
      }
      return {
        yMax: max,
        tdick: tdick,
      };
    },
    getDictScore() {
      this.bkRequest
        .httpRequest('dataDict/getDictScore', {
          query: {
            dataset_id: this.$route.query.result_table_id || this.$route.query.data_id,
          },
        })
        .then(res => {
          if (res.result) {
            if (!res.data.length) return;
            this.nowHeatScore = res.data[0].heat_score;
            this.heatRanking = (res.data[0].heat_ranking * 100).toFixed(2) + '%';
            this.nowRangeScore = res.data[0].range_score;
            this.rangeRanking = (res.data[0].range_ranking * 100).toFixed(2) + '%';
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isRangeChartLoading = false;
        });
    },
    getChartData() {
      this.isRangeChartLoading = true;
      const newTime = new Date().getTime();
      this.bkRequest
        .httpRequest('dataDict/getRangeTrendData', {
          query: {
            dataset_id: this.$route.query.result_table_id || this.$route.query.data_id,
            dataset_type: this.$route.query.dataType,
          },
        })
        .then(res => {
          if (res.result) {
            const time = res.data.time.map(item => {
              return item.replace(/-/, '/');
            });

            let { yMax: rangeMax, tdick: rangeTdick } = this.getRangeTdick(
              Math.max.apply(Math, res.data.score), 10, true
            );
            if (rangeMax < 16) {
              // 广度评分默认为13.1，为了和右边的项目和业务图对应，把Y轴最大值增加一倍
              rangeMax = rangeMax * 2;
            }

            this.rangeChartData = [
              {
                x: time,
                y: res.data.score,
                name: '数据广度变化趋势',
                type: 'scatter',
                line: {
                  color: '#3a84ff',
                },
                yaxis: 'y2',
              },
            ];
            this.dataRangeLayout = Object.assign({}, this.layout, {
              showlegend: false,
              yaxis2: {
                automargin: true,
                title: {
                  text: this.$t('广度评分'),
                  font: {
                    size: 12,
                  },
                },
                tickfont: {
                  color: 'rgb(148, 103, 189)',
                  size: 11,
                },
                rangemode: 'tozero',
                range: [0, rangeMax + 10],
                dtick: rangeTdick,
              },
            });
            this.chartData = [];
            this.chartData.push({
              x: time,
              y: res.data.proj_count,
              name: this.$t('项目'),
              type: 'bar',
              marker: {
                color: 'rgb(142,124,195)',
              },
            });
            this.chartData.push({
              x: time,
              y: res.data.biz_count,
              name: this.$t('业务'),
              type: 'bar',
            });
            this.chartData.push({
              x: time,
              y: res.data.app_code_count,
              name: this.$t('应用APP'),
              type: 'bar',
            });
            this.chartData.push({
              x: time,
              y: res.data.node_count,
              name: this.$t('应用节点'),
              type: 'scatter',
              line: {
                color: '#3a84ff',
              },
              yaxis: 'y2',
            });

            // 项目或业务的柱形图的y轴最大值设定
            let maxQueryTime = Math.max.apply(
              Math,
              [res.data.proj_count, res.data.biz_count, res.data.app_code_count].map(item => Math.max.apply(Math, item))
            );
            let projCountDtick = null; // 项目柱形图的y轴间隔

            if (maxQueryTime / 10 >= 2) {
              maxQueryTime = (maxQueryTime / 10 + 1) * 10;
              projCountDtick = 10;
            } else if (maxQueryTime / 5 >= 2) {
              maxQueryTime = (maxQueryTime / 5 + 1) * 5;
              projCountDtick = 5;
            } else if (maxQueryTime / 2 >= 2) {
              maxQueryTime = (maxQueryTime / 2 + 1) * 2;
              projCountDtick = 2;
            } else {
              maxQueryTime = maxQueryTime + 1;
              projCountDtick = 1;
            }

            let cusmParams = {
              automargin: true,
              range: [0, maxQueryTime],
              title: {
                text: this.$t('项目_业务个数'),
                font: {
                  size: 12,
                },
              },
              dtick: projCountDtick,
              rangemode: 'nonnegative',
              tickfont: {
                color: 'rgb(148, 103, 189)',
                size: 11,
              },
            };
            let applyNodeDtick = null; // 应用节点间隔
            let maxApplyNode = Math.max.apply(Math, res.data.node_count); // 应用节点数最大值
            if (maxApplyNode > 0) {
              const intervals = maxQueryTime / projCountDtick; // 间隔数量
              // 为了图的最大值不被legend遮挡，Y轴最大值需要手动加1.5/intervals的高度
              maxApplyNode = Math.ceil((maxApplyNode * (intervals + 1.5)) / intervals);
              applyNodeDtick = maxApplyNode / intervals; // 重新计算每个间隔的高度
              if (applyNodeDtick + '.0' !== applyNodeDtick) {
                // 如果间隔是浮点数，就取整，再重新计算最大高度
                applyNodeDtick = Math.ceil(applyNodeDtick);
                maxApplyNode = applyNodeDtick * intervals;
              }
            } else {
              maxApplyNode = maxQueryTime;
              applyNodeDtick = projCountDtick;
            }

            this.nodeCount = Object.assign({}, this.layout, {
              hoverlabel: {
                font: {
                  size: 11,
                },
              },
              width: 290,
              legend: {
                orientation: 'h',
                x: 0,
                y: 1.05,
                font: {
                  size: 10,
                },
              },
              margin: {
                l: 40,
                r: 50,
                b: 40,
                t: 20,
              },
              yaxis: cusmParams,
              yaxis2: {
                automargin: true,
                dtick: applyNodeDtick,
                titlefont: { color: 'rgb(148, 103, 189)' },
                tickfont: {
                  color: 'rgb(148, 103, 189)',
                  size: 11,
                },
                overlaying: 'y',
                rangemode: 'nonnegative',
                range: [0, maxApplyNode],
                title: {
                  text: this.$t('应用节点个数'),
                  font: {
                    size: 12,
                  },
                },
                side: 'right',
              },
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isRangeChartLoading = false;
        });
    },
    getHeatTrendData() {
      this.isHeatChartLoading = true;
      this.bkRequest
        .httpRequest('dataDict/getHeatTrendData', {
          query: {
            dataset_id: this.$route.query.result_table_id || this.$route.query.data_id,
            dataset_type: this.$route.query.dataType,
          },
        })
        .then(res => {
          if (res.result) {
            if (!Object.keys(res.data).length) return;

            const time = res.data.time.map(item => {
              return item.replace(/-/, '/');
            });
            this.heatChartData = [
              {
                x: time,
                y: res.data.score,
                name: '数据热度变化趋势',
                type: 'scatter',
                line: {
                  color: '#3a84ff',
                },
                yaxis: 'y2',
              },
            ];

            let { yMax: scoreMax, tdick: scoreTdick } = this.getRangeTdick(Math.max.apply(Math, res.data.score), 10);

            this.dataHeatLayout = Object.assign({}, this.layout, {
              showlegend: false,
              yaxis2: {
                automargin: true,
                title: {
                  text: this.$t('热度评分'),
                  font: {
                    size: 12,
                  },
                },
                tickfont: {
                  color: 'rgb(148, 103, 189)',
                  size: 11,
                },
                range: [0, scoreMax + 2], // Y轴最大值加2是使图形上的最高点显示完全
                tdick: scoreTdick,
                rangemode: 'tozero',
              },
            });
            this.queryChartData = [
              {
                x: time,
                y: res.data.day_query_count,
                name: this.$t('数据查询次数'),
                type: 'scatter',
                line: {
                  color: '#3a84ff',
                },
                yaxis: 'y2',
              },
            ];

            let { yMax: maxQueryTime, tdick: queryTdick } = this.getRangeTdick(
              Math.max.apply(Math, res.data.day_query_count), 10
            );

            let cusmParams = {
              showlegend: false,
              yaxis2: {
                automargin: true,
                range: [0, maxQueryTime * 1.5], // 使图形上的最高点显示完全
                title: {
                  text: this.$t('查询次数'),
                  font: {
                    size: 12,
                  },
                },
                tickfont: {
                  color: 'rgb(148, 103, 189)',
                  size: 11,
                },
                rangemode: 'tozero',
                tdick: queryTdick,
              },
            };
            this.queryTimeConfig = Object.assign({}, this.layout, cusmParams);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isHeatChartLoading = false;
        });
    },
  },
};
</script>
<style lang="scss" scoped>
.score-color {
  color: #3a84ff;
}
.type {
  height: 55px;
  line-height: 55px;
  padding: 0 15px;
  font-weight: 700;
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
}
.struc-container {
  display: flex;
  background-color: #fafafa;
  margin: 0 15px;
  .data-chart-wrap:first-child {
    margin-right: 20px;
  }
  .chart-explain {
    text-align: center;
    height: 16px;
    line-height: 16px;
    color: rgb(68, 68, 68);
    font-size: 12px;
    .icon-question-circle {
      text-indent: 0;
      margin-left: 5px;
    }
  }
}
</style>

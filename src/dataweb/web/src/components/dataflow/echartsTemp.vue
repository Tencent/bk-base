

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
  <div
    id="charts"
    v-bkloading="{ isLoading: charts.types.length === 0 && charts.location.type !== 'algorithm_model' }"
    class="charts"
    :class="{ 'is-hidden': isloading }"
    @mouseenter="$emit('mouseenter')"
    @mouseleave="!isSelectorOpened && $emit('mouseleave')">
    <div class="charts-header clearfix">
      <div class="fl clearfix tips"
        :title="charts.location.output + '(' + charts.location.name + ')'">
        <bkdata-selector :list="rtdList"
          :selected.sync="activeRtd"
          @visible-toggle="handleVisibleToggle" />
        <!-- {{ charts.location.output}} ({{charts.location.name}}) -->
      </div>
      <span class="fr time clearfix">
        {{ $t('最近24小时') }}
        <i class="bk-icon icon-clock" />
      </span>
    </div>
    <div class="charts-content">
      <div v-if="charts.types.length === 0"
        class="charts-contents-nodata">
        <i class="bk-icon icon-no-data" />
        <p>{{ $t('暂无数据') }}</p>
      </div>
      <div v-show="warningmsg"
        class="charts-content-warning"
        :title="warningmsg">
        <i class="bk-icon icon-exclamation-circle" />
        {{ warningmsg }}
      </div>
      <bkdata-tab :active.sync="activeName"
        type="unborder-card"
        @tab-change="handleClick">
        <bkdata-tab-panel v-if="charts.types.indexOf('hasTrend') > -1"
          name="dataTrend"
          :label="$t('数据量趋势')">
          <div id="charts-trend-content">
            <div v-if="!charts.isInit.hasTrend"
              class="charts-trend-nodata">
              <div>
                <i class="bk-icon icon-no-data" />
                <p>{{ $t('暂无数据') }}</p>
              </div>
            </div>
            <PlotlyChart
              v-else
              :eventListen="['plotly_relayout', 'onmousedown']"
              :chartData="trendChartInfo.data"
              :chartConfig="trendChartInfo.config"
              :chartLayout="trendChartInfo.layout"
              @onmousedown="handleChartMourseDown" />
          </div>
        </bkdata-tab-panel>
        <bkdata-tab-panel v-if="charts.types.indexOf('hasDelay') > -1"
          name="dataDelay"
          :label="$t('计算延迟_单位_秒')">
          <!-- <template> -->
          <div id="charts-delay-content">
            <div v-if="!charts.isInit.hasDelay"
              class="charts-delay-nodata">
              <div>
                <i class="bk-icon icon-no-data" />
                <p>{{ $t('暂无数据') }}</p>
              </div>
            </div>
            <PlotlyChart
              v-else
              :eventListen="['plotly_relayout', 'onmousedown']"
              :chartData="delayChartInfo.data"
              :chartConfig="delayChartInfo.config"
              :chartLayout="delayChartInfo.layout"
              @onmousedown="handleChartMourseDown" />
          </div>
          <!-- </template> -->
        </bkdata-tab-panel>

        <bkdata-tab-panel v-if="charts.types.indexOf('loss') > -1"
          name="dataLoss"
          :label="$t('数据丢失')">
          <!-- <template> -->
          <div id="charts-loss-content">
            <div v-if="!charts.isInit.hasLoss"
              class="charts-loss-nodata">
              <div>
                <i class="bk-icon icon-no-data" />
                <p>{{ $t('暂无数据') }}</p>
              </div>
            </div>
            <PlotlyChart
              v-else
              :eventListen="['plotly_relayout', 'onmousedown']"
              :chartData="lossChartInfo.data"
              :chartConfig="lossChartInfo.config"
              :chartLayout="lossChartInfo.layout"
              @onmousedown="handleChartMourseDown" />
          </div>
          <!-- </template> -->
        </bkdata-tab-panel>
        <bkdata-tab-panel v-if="charts.types.indexOf('offline') > -1"
          name="offline"
          :label="$t('执行记录')">
          <!-- <template> -->
          <div class="history-do">
            <div class="history-do-record">
              <div v-if="charts.histroryLists && charts.histroryLists.length"
                class="clearfix">
                <div
                  v-for="item in charts.histroryLists"
                  :key="item.execute_id"
                  :title="(item.status === 'warning' && $t('查看错误信息')) || $t('执行记录')"
                  class="clearfix history-info"
                  @click="expandErr(item)">
                  <div class="history-info-item">
                    <span
                      class="status-label-success"
                      :class="{
                        'status-label-failure': item.status === 'fail',
                        'status-label-warning': item.status === 'warning',
                      }">
                      {{ item.status_str }}
                    </span>
                    <span class="mr10">
                      {{ item.period_start_time }}
                    </span>
                  </div>
                  <transition name="slide">
                    <div v-show="item && item.isShow"
                      class="history-container">
                      <div class="history-time">
                        <span>{{ $t('开始时间') }}：</span>
                        <span class="flex5">{{ `${item.start_time}` }}</span>
                      </div>
                      <div class="history-time">
                        <span>{{ $t('结束时间') }}：</span>
                        <span class="flex5"
                          :class="{ 'text-center': !endTimeStatus.includes(item.status) }">
                          {{ endTimeStatus.includes(item.status)
                            ? `${item.end_time}`
                            : '--' }}
                        </span>
                      </div>
                      <template v-if="isWarning(item)">
                        <div
                          :class="{
                            'warning-color': item.status === 'warning',
                            'fail-color': item.status === 'fail',
                          }"
                          class="clearfix error-msg">
                          {{ `${$t('错误信息')}：${item.err_msg}（${$t('错误码')}${item.err_code}）` }}
                        </div>
                      </template>
                    </div>
                  </transition>
                </div>
              </div>
              <template v-else>
                <p style="height: 40px">
                  <span>
                    {{ $t('无历史执行记录') }}
                  </span>
                </p>
              </template>
            </div>
          </div>
          <!-- </template> -->
        </bkdata-tab-panel>
      </bkdata-tab>
    </div>
  </div>
</template>
<script>
import Vue from 'vue';
import PlotlyChart from '@/components/plotly';
import * as moment from 'moment';

export default {
  components: {
    PlotlyChart,
  },
  props: {
    zoom: {
      type: Number,
      default: 1,
    },
    chartsData: {
      type: Object,
    },
  },
  data() {
    return {
      isSelectorOpened: false,
      activeRtd: '',
      activeChartOption: {},
      sss: true,
      activeName: 'dataTrend', // 当前激活图表的名称
      isLeftShow: '', // 离线内容是否左边显示
      warningmsg: '',
      isloading: true,
      arrowPosition: {
        isTop: true,
        isLeft: false,
        isRight: false,
        isBottom: false,
      },
      charts: {
        isAllReady: false,
        isInit: {
          hasTrend: false,
          hasDelay: false,
          hasOffline: false,
        },
        data: [],
        types: [],
        option: {
          data_delay_max: {
            delay_max: [],
            time: [],
          },
          data_trend: {
            input: [],
            output: [],
            alert_list: [],
            time: [],
          },
        },
        nodeId: '',
        location: {}, // 节点的配置信息，包括位置信息
        histroryLists: [],
      },
      lossChartInfo: {
        data: [],
        config: {
          operationButtons: ['resetScale2d', 'zoomIn2d', 'select2d'],
        },
        layout: {
          hovermode: 'closest',
          width: 450,
          height: 270,
          xaxis: {
            autorange: true,
            showgrid: true, // 是否显示x轴线条
            showline: true, // 是否绘制出该坐标轴上的直线部分
            tickwidth: 1,
            zeroline: true,
            gridcolor: 'rgba(145,164,237,0.2)',
            tickcolor: '#3a84ff',
            tickangle: 0,
            tickfont: {
              color: '#b2bac0',
            },
            linecolor: '#3a84ff',
          },
          yaxis: {
            showticklabels: false,
            autorange: true,
          },
          showlegend: true,
          legend: {
            xanchor: 'center',
            yanchor: 'top',
            y: -0.2,
            x: 0.5,
            orientation: 'h',
            font: {
              color: '#b2bac0',
            },
          },
          margin: {
            l: 40,
            r: 40,
            b: 60,
            t: 20,
          },
          paper_bgcolor: '#2c2d3c',
          plot_bgcolor: '#2c2d3c',
        },
      },
      trendChartInfo: {
        data: [],
        config: {
          operationButtons: ['resetScale2d', 'zoomIn2d', 'select2d'],
        },
        layout: {
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
            tickangle: 0,
            tickformat: '%H:%M:%S',
            tickfont: {
              color: '#b2bac0',
            },
            linecolor: '#3a84ff',
          },
          yaxis: {
            showticklabels: true,
            autorange: true,
          },
          showlegend: true,
          legend: {
            xanchor: 'center',
            yanchor: 'top',
            y: -0.2,
            x: 0.5,
            orientation: 'h',
            font: {
              color: '#b2bac0',
            },
          },
          margin: {
            l: 40,
            r: 40,
            b: 60,
            t: 20,
          },
          paper_bgcolor: '#2c2d3c',
          plot_bgcolor: '#2c2d3c',
        },
      },
      delayChartInfo: {
        data: [
          {
            type: 'lines',
            name: '111',
            line: {
              color: '#fe621d',
            },
            x: [],
            y: [],
            text: [],
          },
        ],
        config: {
          operationButtons: ['resetScale2d'],
        },
        layout: {
          width: 450,
          height: 270,
          xaxis: {
            autorange: true,
            showgrid: true, // 是否显示x轴线条
            showline: true, // 是否绘制出该坐标轴上的直线部分
            // tickwidth: 1,
            dtick: 100,
            gridcolor: 'rgba(145,164,237,0.2)',
            tickcolor: '#3a84ff',
            tickangle: 0,
            tickformat: '%H:%M:%S',
            tickfont: {
              color: '#b2bac0',
            },
            linecolor: '#3a84ff',
            type: 'category',
            showticklabels: true,
            zeroline: false,
          },
          yaxis: {
            showticklabels: true,
            autorange: true,
            zeroline: true,
          },
          margin: {
            l: 40,
            r: 40,
            b: 60,
            t: 20,
          },
          paper_bgcolor: '#2c2d3c',
          plot_bgcolor: '#2c2d3c',
          showlegend: false,
          legend: {
            xanchor: 'center',
            yanchor: 'top',
            y: -0.2,
            x: 0.5,
            orientation: 'h',
            font: {
              color: '#b2bac0',
            },
          },
        },
      },
      shouldContainerShow: false,
      chartCached: {
        trend: false,
        delay: false,
        loss: false,
      },
      endTimeStatus: ['finished', 'fail', 'warning'], // 需要展示结束时间的状态
    };
  },
  computed: {
    rtdList() {
      const options = Object.keys(this.activeChartOption).filter(key => key !== 'null');
      // eslint-disable-next-line vue/no-side-effects-in-computed-properties
      this.activeRtd = options[0] || '';
      return options.map(key => {
        return {
          id: key,
          name: key,
        };
      });
    },
  },
  watch: {
    'charts.types'(val, old) {
      if (val.toString() === old.toString()) return;
      this.sss = false;
      val.some(type => {
        if (type === 'hasTrend') {
          this.activeChartOption = this.charts.option.data_trend;
          this.$nextTick(() => {
            this.pointDebugDataCountCharts(this.activeChartOption);
          });
          return true;
        } else if (type === 'offline') {
          this.activeName = 'offline';
          this.activeChartOption = this.charts.option.rt_status;
          this.$nextTick(() => {
            if (this.charts.option.rt_status && this.charts.option.rt_status[this.activeRtd]) {
              this.charts.histroryLists = this.charts.option.rt_status[this.activeRtd].execute_history;
              this.charts.histroryLists
                && this.charts.histroryLists.forEach(item => {
                  item.isShow = false;
                });
            } else {
              this.charts.histroryLists = [];
            }
          });
          return true;
        }
      });
    },
  },
  created() {
    var e2 = document.createEvent('MouseEvents');
    e2.initEvent('click', true, true);
  },
  methods: {
    isWarning(item) {
      return item.status === 'fail' || item.status === 'warning';
    },
    handleVisibleToggle(isOpen) {
      this.isSelectorOpened = isOpen;
    },
    handleChartMourseDown() {
      // this.shouldContainerShow = true
      // setTimeout(() => {
      //     this.shouldContainerShow = false
      // }, 500)
    },
    expandErr(item) {
      item.isShow = !item.isShow;
      this.$forceUpdate();
    },
    test() {
      console.log('back');
    },
    // 图表切换
    handleClick(tab) {
      this.warningmsg = '';
      // 切换过程避免图表重复渲染，添加缓存标志位
      if (tab === 'dataDelay' && !this.chartCached.delay) {
        this.activeChartOption = this.charts.option.data_delay_max;
        this.$nextTick(() => {
          this.pointDebugDataDelayCharts(this.activeChartOption);
        });
      } else if (tab === 'dataLoss' && !this.chartCached.loss) {
        this.activeChartOption = this.charts.option.data_trend;
        this.$nextTick(() => {
          this.pointDebugDataLoseCharts(this.activeChartOption);
        });
      } else if (tab === 'dataTrend' && !this.chartCached.trend) {
        this.activeChartOption = this.charts.option.data_trend;
        this.$nextTick(() => {
          this.pointDebugDataCountCharts(this.activeChartOption);
        });
      } else if (tab === 'offline' && !this.chartCached.loss) {
        this.activeChartOption = this.charts.option.rt_status;
        this.$nextTick(() => {
          if (this.charts.option.rt_status) {
            this.charts.histroryLists = this.charts.option.rt_status[this.activeRtd].execute_history;
            this.charts.histroryLists
              && this.charts.histroryLists.forEach(item => {
                item.isShow = false;
              });
          } else {
            this.charts.histroryLists = [];
          }
        });
      }
    },
    // 进入图表的时候触发的事件
    enterCharts() {
      this.$emit('enterCharts');
    },
    // 重置图表类型,
    resetChartsOption() {
      this.charts.types = [];
      this.isloading = true;
    },
    // 重置可显示的图表类型
    resetChartsType() {
      this.charts.isInit = {
        hasTrend: false,
        hasDelay: false,
        hasOffline: false,
      };
    },
    // 初始化
    init(nodeId) {
      this.activeName = 'dataTrend';
      let charts = this.charts;
      charts.types = [];
      if (!nodeId) return;
      charts.data = this.chartsData.data;
      charts.nodeId = nodeId;

      charts.data.forEach(item => {
        for (let key in item) {
          if (parseInt(key) === parseInt(nodeId)) {
            charts.types = item[key].types;
            charts.option = item[key].options;
            charts.location = item[key].location;
          }
          break;
        }
      });
    },

    // 调整图表容器的位置
    resetChartsContainerPosition(location) {
      this.isloading = false;
    },
    // 绘制数据量趋势图
    pointDebugDataCountCharts(_options) {
      const options = _options[this.activeRtd] || {};
      let self = this;
      let legend = [];
      let series = [];
      let option = this.charts.option;
      for (let key in options) {
        if (key !== 'time' && options[key].length > 0) {
          this.charts.isInit.hasTrend = true;
        }
      }
      if (!this.charts.isInit.hasTrend) return;
      if ('input' in options) {
        legend = [this.$t('输入'), this.$t('输出'), this.$t('告警')];
        series = [
          {
            data: options.input,
            type: 'line',
            name: this.$t('输入'),
            linecolor: '#3a84ff',
          },
          {
            data: options.output,
            type: 'line',
            name: this.$t('输出'),
            linecolor: '#76be42',
          },
          {
            name: this.$t('告警'),
            type: 'line',
            linecolor: '#fe621d',
          },
        ];
      } else {
        legend = [this.$t('输出'), this.$t('告警')];
        series = [
          {
            data: options.output,
            type: 'line',
            name: this.$t('输出'),
            linecolor: '#76be42',
          },
          {
            name: this.$t('告警'),
            type: 'line',
            linecolor: '#fe621d',
          },
        ];
      }

      let chartData = [];
      series.forEach((item, index) => {
        chartData.push({
          type: 'lines',
          name: series[index].name,
          line: {
            color: series[index].linecolor,
          },
          x: (options.time || []).map(t => new Date(t * 1000)),
          y: series[index].data || [null],
        });
      });

      chartData = this.filterNoNullData(chartData);

      this.trendChartInfo.data = chartData.map(d => {
        d.text = d.x.map(t => moment(t).format('YYYY-MM-DD'));
        return d;
      });
      // this.chartCached.trend = true
    },
    // 绘制计算延迟图
    pointDebugDataDelayCharts(_options) {
      const options = _options[this.activeRtd] || {};
      for (let key in options) {
        if (key !== 'time' && options[key].length > 0) {
          this.charts.isInit.hasDelay = true;
        }
      }
      if (!this.charts.isInit.hasDelay) return;
      let chartData = [
        {
          x: (options.time || []).map(t => moment(t * 1000).format('HH:mm:ss')),
          y: options.delay_max,
          name: this.$t('计算延迟'),
          type: 'lines',
          line: {
            color: '#fe621d',
          },
        },
      ];
      this.delayChartInfo.data = this.filterNoNullData(chartData).map(d => {
        d.text = d.x.map(t => t);
        return d;
      });
      this.delayChartInfo.layout.xaxis.dtick = Math.ceil(this.delayChartInfo.data[0].x.length / 7);
      this.chartCached.delay = true;
    },
    // 绘制数据丢失图
    pointDebugDataLoseCharts(_options) {
      const options = _options[this.activeRtd] || {};
      this.resetChartsType();

      const chartData = [
        {
          type: 'lines',
          data: options,
          name: this.$t('输入'),
          line: {
            color: '#3a84ff',
          },
          x: options.time,
          y: [null],
        },
        {
          type: 'lines',
          data: [0, 0, 0, 0, 0, 0, 0],
          name: this.$t('输出'),
          line: {
            color: '#76be42',
          },
          x: options.time,
          y: [null],
        },
      ];

      this.lossChartInfo.data = chartData.map(d => {
        d.text = d.x.map(t => moment(t).format('YYYY-MM-DD'));
        return d;
      });
      this.chartCached.loss = true;
    },

    filterNoNullData(chartData) {
      chartData.forEach(data => {
        let indexList = [];
        data.y = data.y.filter((item, index) => {
          if (item !== null && item > 0) {
            indexList.push(index);
            return true;
          }

          return false;
        });

        data.x = data.x.filter((_, index) => indexList.includes(index));
      });
      if (chartData.some(d => d.y.length)) {
        chartData.forEach(d => {
          if (!d.y.length) {
            d.y = [null];
            d.x = [null];
          }
        });
      }
      return chartData;
    },
  },
};
</script>
<style lang="scss" scoped>
.slide-enter-active {
  transition: all 0.3s ease-in-out;
  overflow: hidden;
}
.slide-leave-active {
  transition: all 0.3s ease-in-out;
  height: 0;
  margin: 0;
  overflow: hidden;
}
.slide-leave-to {
  margin: 0 !important;
}
.slide-enter,
.slide-leave {
  height: 0;
  opacity: 0;
}
.charts-content {
  ::v-deep .bk-tab-section {
    padding: 0 36px 20px 36px;
  }
}
</style>
<style lang="scss">
$borderColor: #3d3d4b;
$headerBgcolor: #2c2d3c;
$titleColor: #b2bac0;
$bgColor: #23242e;
.charts {
  min-width: 450px;
  position: absolute;
  height: auto;
  top: 0;
  left: 0px;
  margin: -0.3rem -0.6rem;
  .arrow {
    content: '';
    position: absolute;
    border: 5px solid $bgColor;
    &.top {
      border-color: transparent $bgColor transparent transparent;
      left: -10px;
      top: 67px;
    }
    &.left {
      border-color: transparent transparent transparent $bgColor;
      left: 100%;
      top: 75px;
    }
    &.right {
      border-color: transparent transparent transparent $bgColor;
      left: 100%;
      top: 280px;
    }
    &.bottom {
      border-color: transparent $bgColor transparent transparent;
      left: -10px;
      top: 280px;
    }
  }
  &-none {
    width: 367px;
    height: 362px;
    border-radius: 2px;
  }
  .bk-loading {
    background-color: $headerBgcolor;
    &-title {
      color: $titleColor;
    }
  }
  &-header {
    width: 100%;
    height: 55px;
    line-height: 45px;
    padding: 0px 35px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: $headerBgcolor;
    border-top-left-radius: 2px;
    border-top-right-radius: 2px;
    font-weight: bold;
    border-bottom: 1px solid $borderColor;
    &::after {
      content: none !important;
    }
    .tips {
      //   color: #b2bac0;
      //   font-size: 14px;
      //   font-weight: bold;
      width: 200px;
      margin-right: 5px;
      .bk-selector {
        .bk-selector-wrapper {
          .bk-selector-input {
            height: 24px;
            line-height: 24px;
            font-size: 12px;
            font-weight: normal;
          }

          .bk-selector-icon {
            top: 50%;
            transform: translateY(-50%);
          }
        }
      }
      //   overflow: hidden;
      //   text-overflow: ellipsis;
      //   white-space: nowrap;
    }
    .time {
      color: #858b91;
      i.bk-icon {
        font-size: 10px;
      }
    }
  }
  &-content {
    background: $headerBgcolor;
    #charts-trend-content,
    #charts-delay-content,
    #charts-loss-content {
      height: 270px;
    }
    //   修改tab样式
    .bk-tab-header {
      height: 41px;
      .bk-tab-label-wrapper {
        padding-left: 36px;
      }
    }
    .charts-contents-nodata {
      text-align: center;
      padding-top: 30px;
      i {
        font-size: 28px;
      }
    }
    &-warning {
      z-index: 10001;
      position: absolute;
      top: 98px;
      width: 280px;
      padding: 0 20px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      font-size: 10px;
      color: #fe621d;
    }
    .header {
      border-bottom: 1px solid $borderColor;
      height: 45px;
      line-height: 45px;
      padding: 0 20px;
      background: $headerBgcolor;
      color: #b2bac0;
      font-weight: bold;
    }
    .recentlt-do {
      border-bottom: 1px solid $borderColor;
      height: 77px;
      background: $headerBgcolor;
      &-header {
        color: $titleColor;
        height: 40px;
        line-height: 40px;
        padding: 0px 20px;
      }
    }
    .history-do {
      &-header {
        color: $titleColor;
        height: 40px;
        line-height: 40px;
        padding: 0px 20px;
        .time {
          span {
            cursor: pointer;
          }
        }
      }
      background: $bgColor;
      &-record {
        max-height: 163px;
        min-height: 250px;
        &::-webkit-scrollbar {
          width: 5px;
          height: 5px;
        }
        &::-webkit-scrollbar-thumb {
          border-radius: 20px;
          background-color: #40404a;
        }
        overflow: auto;
        .history-info {
          padding: 8px 2px;
          cursor: pointer;
          color: $titleColor;
          &:last-of-type {
            padding-bottom: 15px;
          }
          .history-info-item {
            display: flex;
            justify-content: space-between;
            .status-label-success {
              margin-left: 20px;
            }
          }
          .history-container {
            background-color: rgb(43, 45, 60);
            margin-top: 8px;
            color: white;
            .history-time {
              display: flex;
              justify-content: space-between;
              padding: 4px 20px;
              font-size: 12px;
              .flex5 {
                flex: 0.5;
                text-align: left;
                padding-left: 6px;
              }
              .text-center {
                text-align: center;
              }
            }
          }
          .warning-color {
            color: #f6ae00;
          }
          .fail-color {
            color: #fe621d;
          }
        }
        .error-msg {
          width: 369px;
          padding: 4px 10px 4px 20px;
          border-radius: 2px;
          word-break: break-all;
          &:first-of-type {
            margin-top: 25px;
            padding: 5px;
          }
        }
      }
    }
    .status-label-success {
      background: #76be42;
      padding: 2px 5px;
      border-radius: 2px;
      color: #fff;
    }
    .status-label-failure {
      background: #fe621d;
      padding: 2px 5px;
      border-radius: 2px;
      color: #fff;
    }

    .status-label-warning {
      background: #f6ae00;
      padding: 2px 5px;
      border-radius: 2px;
      color: #fff;
    }
  }
  &-trend {
    width: 367px;
    background: $headerBgcolor;
    border-bottom: 1px solid $borderColor;
    &-title {
      height: 45px;
      line-height: 45px;
      padding: 0px 20px;
      color: $titleColor;
    }
    &-nodata {
      div {
        text-align: center;
        position: relative;
        top: 80px;
        width: 367px;
        .bk-icon {
          font-size: 28px;
        }
      }
    }
  }
  &-delay,
  &-loss {
    width: 367px;
    background: $headerBgcolor;
    border-bottom: 1px solid $borderColor;
    &-nodata {
      div {
        text-align: center;
        position: relative;
        top: 80px;
        width: 367px;
        .bk-icon {
          font-size: 28px;
        }
      }
    }
  }
  .el-tabs__header {
    margin-bottom: 0;
  }
  .el-tabs--card > .el-tabs__header .el-tabs__item {
    border: none;
    color: $titleColor;
  }
  .el-tabs__item {
    padding: 0 10px;
  }
  .el-tabs--card > .el-tabs__header .el-tabs__item.is-active {
    color: #fff;
    height: 20px;
    line-height: 20px;
    border-radius: 4px;
    background: #3a84ff;
  }
  .el-tabs--card > .el-tabs__header .el-tabs__nav {
    border: none;
  }
  .el-tabs--card > .el-tabs__header {
    border: none;
    padding: 0px 20px;
    height: 45px;
    line-height: 45px;
  }
}

.ms-chart-container {
  .js-plotly-plot .plotly .modebar-btn path {
    fill: #858b91;

    &:hover {
      fill: #3a84ff;
    }
  }
  .js-plotly-plot .plotly .modebar--hover {
    background: none;
  }
}
</style>

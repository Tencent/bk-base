

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
  <div v-bkloading="{ isLoading: isLoading }"
    class="chart-wrapper">
    <div v-show="isShowChart"
      class="chart-container">
      <div class="chart-title">
        {{ title }}
        <span v-if="titleTips.length"
          v-bk-tooltips="titleTips"
          class="icon-exclamation-circle" />
      </div>
      <PlotlyChart :chartData="chartData"
        :chartConfig="chartConfig"
        :chartLayout="layOut"
        :eventListen="['plotly_hover', 'plotly_unhover']"
        @plotly_hover="plotlyHover"
        @plotly_unhover="plotlyUnhover" />
    </div>
    <NoData v-show="!isShowChart"
      :width="'500px'"
      :height="noDataHeight"
      :no-data-text="noDataText" />
    <div ref="tippyContent"
      class="h-tooltip vue-tooltip vue-tooltip-visible"
      :style="{ top: tipsTop, left: tipsLeft, display: hidden }"
      x-placement="top"
      @mouseleave="hidden = 'none'">
      <div class="tooltip-arrow"
        x-arrow="top" />
      <div class="tooltip-content">
        <RecentDataTable :data="tableData"
          :tableHead="tableHead"
          :tableTime="tableTime"
          :isShowDark="true"
          :isSmallTable="true"
          :tableRateDes="tableRateDes" />
      </div>
    </div>
  </div>
</template>

<script>
import PlotlyChart from '@/components/plotly';
import { calcChartParams, getAxixParams, getNowTime } from '@/pages/datamart/common/utils.js';

const xFeild = {
  pie: 'labels',
  lines: 'x',
  histogram: 'x',
  bar: 'x',
  scatterpolar: 'r',
};
const yFeild = {
  pie: 'values',
  lines: 'y',
  histogram: 'y',
  bar: 'y',
  scatterpolar: 'y',
};

export default {
  components: {
    PlotlyChart,
    NoData: () => import('@/pages/datamart/DataDict/components/children/chartComponents/NoData.vue'),
    RecentDataTable: () => import('@/pages/datamart/common/components/RecentDataTable.vue'),
  },
  props: {
    isLoading: {
      type: Boolean,
      default: false,
    },
    chartData: {
      type: Array,
      default: () => [],
    },
    chartConfig: {
      type: Object,
      default: () => ({}),
    },
    chartLayout: {
      type: Object,
      default: () => ({}),
    },
    title: {
      type: String,
      default: '',
    },
    titleTips: {
      type: String,
      default: '',
    },
    isShowZero: {
      type: Boolean,
      default: false,
    },
    tableHead: {
      type: String,
      default: '',
    },
    noDataText: {
      type: String,
      default: $t('暂无数据'),
    },
    isModifyAxis: {
      type: Boolean,
      default: true,
    },
    noDataHeight: {
      type: String,
      default: '400px',
    },
    tableRateDes: {
      type: String,
      default: '',
    },
    isSetDick: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      tableData: [],
      tipsTop: 0,
      tipsLeft: 0,
      hidden: 'none',
      timer: null,
      tableTime: '',
      hoverData: null,
    };
  },
  computed: {
    orientation() {
      return (this.chartData[0] && this.chartData[0].orientation) || 'v';
    },
    adaptAxisField() {
      return this.orientation === 'v' ? this.yAxisField : this.xAxisField;
    },
    xAxisField() {
      return this.chartData[0] ? xFeild[this.chartData[0].type] : 'x';
    },
    yAxisField() {
      return this.chartData[0] ? yFeild[this.chartData[0].type] : 'y';
    },
    isShowChart() {
      return this.chartData.some(item => item[this.xAxisField] && item[this.xAxisField].length);
    },
    layOut() {
      return Object.assign({}, this.chartLayout, this.localLayout);
    },
    localLayout() {
      if (!this.isModifyAxis) return {};
      if (!this.chartData.length || !this.isShowZero) return {};

      const { dtick, range } = getAxixParams(this.getAxisMax(this.chartData));
      const axisName = this.orientation === 'v' ? 'yaxis' : 'xaxis';

      return Object.assign({}, this.chartLayout, {
        [axisName]: {
          autorange: false,
          // 图表缩放自适应，需要去掉dtick
          dtick: this.isSetDick ? dtick : null,
          range,
        },
      });
    },
  },
  mounted() {
    if (document.getElementsByClassName('layout-body')[0]) {
      document.getElementsByClassName('layout-body')[0].onscroll = this.hiddenTips();
    }
  },
  beforeDestroy() {
    if (document.getElementsByClassName('layout-body')[0]) {
      document.getElementsByClassName('layout-body')[0].onscroll = null;
    }
  },
  methods: {
    hiddenTips() {
      const that = this;
      let timer = null;
      return () => {
        clearTimeout(timer);
        timer = setTimeout(() => {
          clearTimeout(that.timer);
          that.hidden = 'none';
        }, 500);
      };
    },
    plotlyUnhover() {},
    getAxisMax(data) {
      let max = 0;
      let index = 0;

      data.forEach((item, idx) => {
        const maxValue = item[this.adaptAxisField].length ? Math.max(...item[this.adaptAxisField]) : 0;
        if (max < maxValue) {
          max = maxValue;
          index = idx;
        }
      });
      return data[index][this.adaptAxisField];
    },
    getNowTime(time) {
      this.tableTime = getNowTime(time);
    },
    getIndex(data) {
      const target = JSON.parse(JSON.stringify(data));
      const x = target.x.reverse();
      const y = target.y.reverse();
      let idx = 0;

      for (let index = 0; index < x.length; index++) {
        if (y[index]) {
          idx = x.length - 1 - index;
          return idx;
        }
      }
    },
    plotlyHover(data) {
      if (data.points[0].data.hoverinfo !== 'none') return;
      if (!data.points.length) return;

      this.hoverData = data.points;

      this.handleMoveEvent(data);

      this.targetMouseMove(data.event);
    },
    handleMoveEvent(data) {
      data.event.target.onmouseleave = this.targetMouseLeave;
    },
    targetMouseLeave(event) {
      // 当鼠标移到hover产生的表格时，禁止执行后面的逻辑
      if (!event.toElement || Array.from(event.toElement.classList).includes('tooltip-arrow')) return;
      clearTimeout(this.timer);
      this.hidden = 'none';
    },
    targetMouseMove(event) {
      this.timer && clearTimeout(this.timer);

      this.timer = setTimeout(() => {
        this.getNowTime(this.hoverData[0].x);

        this.tableData = [];
        const srotObj = {
          [this.$t('今日')]: 0,
          [this.$t('昨日')]: 1,
          [this.$t('上周')]: 2,
        };

        let maxIndex = 0;
        this.hoverData.forEach(item => {
          if (maxIndex < item.pointIndex) {
            maxIndex = item.pointIndex;
          }
        });
        this.hoverData.forEach(item => {
          let idx = this.getIndex(item.data);
          this.tableData.push({
            label: item.data.name,
            num: maxIndex > idx ? '—' : item[this.yAxisField],
            colspan: 2,
          });
        });

        if (this.tableData.length < 3) {
          for (const key of Object.keys(srotObj)) {
            if (!this.tableData.find(item => item.label === key)) {
              this.tableData.push({
                label: key,
                num: '—',
                colspan: 2,
              });
            }
          }
        }

        this.tableData.sort((a, b) => srotObj[a.label] - srotObj[b.label]);

        this.hidden = 'block';
        this.$nextTick(() => {
          const { width, height } = this.$refs.tippyContent.getBoundingClientRect();
          // 10为光标到tips的距离
          this.tipsTop = `${event.pageY - height / 2 - 10}px`;
          this.tipsLeft = `${event.pageX - width / 2}px`;
        });
      }, 100);
    },
  },
};
</script>

<style lang="scss" scoped>
.vue-tooltip {
  position: fixed;
  z-index: 10000;
  transform: translateY(-50%);
  max-width: 800px;
  .tooltip-arrow {
    position: absolute;
    left: 50%;
    top: 100%;
    transform: translateX(-50%);
  }
}
.tippy-content {
  opacity: 0;
  height: 0;
  overflow: hidden;
}
.chart-wrapper {
  display: flex;
  align-items: center;
  justify-content: center;
  .chart-container {
    position: relative;
    .chart-title {
      position: absolute;
      left: 0;
      right: 0;
      top: 20px;
      z-index: 1;
      text-align: center;
      color: #63656e;
      .icon-exclamation-circle {
        font-size: 14px;
        color: #3a84ff;
      }
    }
  }
}
</style>

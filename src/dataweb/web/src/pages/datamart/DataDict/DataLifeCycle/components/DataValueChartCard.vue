

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
  <div class="value-chart-card">
    <!-- <slot name="title-explain">
            <div></div>
        </slot>-->
    <div class="chart-explain">
      <div class="explain-header">
        <div class="explain-header-title">
          <slot name="title">
            {{ $t('7日') }} | {{ title }}
          </slot>
        </div>
        <p class="explain-header-time">
          <slot name="explain-header-time">
            {{ timeRange }} | {{ $t('最近7天') }}
          </slot>
        </p>
      </div>
      <div v-if="isShowScore"
        class="explain-score-rate">
        <div class="explain-score">
          <slot name="explain-score">
            {{ $t('今日') }}
            <span class="big-score">{{ calcTodayScore }}</span>
            {{ unit }}
            <i v-if="todayScoreTips"
              v-bk-tooltips="todayScoreTips"
              class="icon-question-circle" />
          </slot>
          <p v-if="percentRate && todayScore > 0"
            class="explain-score-text">
            {{ `${$t('大于')}${`${percentRate}%`}${$t('的数据')}` }}
          </p>
        </div>
        <div v-if="isShowRate"
          class="explain-rate">
          <div class="explain-chain-rate">
            <span :class="[getArrowClass(chainRate)]">{{ $t('环比') }}</span>
            {{ getPercentRate(chainRate) }}
          </div>
          <div class="explain-year-rate">
            <span :class="[getArrowClass(yearRate)]">{{ $t('同比') }}</span>
            {{ getPercentRate(yearRate) }}
          </div>
        </div>
      </div>
    </div>
    <div class="chart-container">
      <!-- 雷达图需要在图中间加上价值评分 -->
      <slot name="chart-text" />
      <slot name="chart-content">
        <Chart
          :isLoading="loading"
          :chartData="shiftChartData(chartData)"
          :chartConfig="config"
          :chartLayout="layout"
          :isShowZero="true"
          :isModifyAxis="isModifyAxis"
          :no-data-height="noDataHeight"
          :isSetDick="isSetDick" />
      </slot>
    </div>
  </div>
</template>

<script>
import Chart from '@/pages/datamart/common/components/Chart';
import moment from 'moment';

export default {
  components: {
    Chart,
  },
  props: {
    isShowScore: {
      type: Boolean,
      default: true,
    },
    isShowRate: {
      type: Boolean,
      default: true,
    },
    todayScoreTips: String,
    title: String,
    todayScore: [String, Number],
    percentRate: [String, Number],
    titleTips: {
      type: String,
      default: '',
    },
    loading: {
      type: Boolean,
      default: false,
    },
    chartData: {
      type: Array,
      default: () => [],
    },
    config: {
      type: Object,
      default: () => ({}),
    },
    layout: {
      type: Object,
      default: () => ({}),
    },
    isModifyAxis: {
      type: Boolean,
      default: true,
    },
    noDataHeight: {
      type: String,
      default: '300px',
    },
    isSetDick: {
      type: Boolean,
      default: false,
    },
    unit: {
      type: String,
      default: $t('分'),
    },
  },
  data() {
    return {
      timeRange: '',
    };
  },
  computed: {
    calcTodayScore() {
      if (!this.todayScore) return this.todayScore;
      return this.todayScore > 0 ? this.todayScore : '—';
    },
    chainRate() {
      if (!this.chartData.length) return '—';
      const target = this.chartData[0].y;
      const len = target.length;
      // 环比 和昨天比
      const yestoday = target[len - 2];
      if (!yestoday) return '—';
      const today = target[len - 1];
      if (today === null) return '—';
      return (((today - yestoday) / yestoday) * 100).toFixed(2);
    },
    yearRate() {
      if (!this.chartData.length) return '—';
      const target = this.chartData[0].y;
      const len = target.length;
      // 同比 和上周的今天比
      const lastDay = target[0];
      if (!lastDay) return '—';
      const today = target[len - 1];
      if (today === null) return '—';
      return (((today - lastDay) / lastDay) * 100).toFixed(2);
    },
  },
  created() {
    const nowDate = new Date();
    const sevenTime = 6 * 24 * 60 * 60 * 1000;
    const beforeSevenTime = nowDate - sevenTime;
    this.timeRange = `${moment(beforeSevenTime).format('YYYY-MM-DD')} ~ ${moment(nowDate).format('YYYY-MM-DD')}`;
  },
  methods: {
    getPercentRate(rate) {
      const target = Number(rate);
      return target ? `${Math.abs(target)}%` : '—';
    },
    // 指标的图表数据需要去掉第一位，仅保留7天的数据
    shiftChartData(chartData) {
      const target = JSON.parse(JSON.stringify(chartData));
      target.forEach(item => {
        item.y.shift();
        item.x.shift();
      });
      return target;
    },
    getArrowClass(rate) {
      // 比例为0时，隐藏箭头
      if (rate === '—') return 'mr10';
      if (Number(rate) === 0) return 'mr10';
      return `${rate > 0 ? 'up-icon' : 'down-icon'} rate-arrow`;
    },
  },
};
</script>

<style lang="scss" scoped>
.value-chart-card {
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  width: 33%;
  box-shadow: 0 0 10px 0 rgba(33, 34, 50, 0.05);
  padding: 10px;
  .header {
    text-indent: 2em;
    line-height: 1.5em;
    color: #63656e;
  }
  .chart-container {
    position: relative;
  }
  .chart-explain {
    padding: 10px 10px 0 10px;
    .explain-header {
      .explain-header-title {
        font-size: 14px;
        color: rgb(74, 74, 74);
      }
      .explain-header-time {
        font-size: 12px;
        margin: 5px 0;
      }
    }
    .explain-score-rate {
      display: flex;
      align-content: center;
      justify-content: space-between;
      font-size: 13px;
      .explain-score {
        color: #4a4a4a;
        .big-score {
          font-size: 15px;
          line-height: 30px;
          color: #000;
        }
        .explain-score-text {
          color: #979ba5;
        }
        .icon-question-circle {
          margin-left: 5px;
          color: #3a84ff;
        }
      }
      .explain-rate {
        display: flex;
        align-items: center;
        flex-direction: column;
        justify-content: center;
        border-left: 1px solid #c4c6cc;
        padding-left: 30px;
        .explain-chain-rate,
        .explain-year-rate {
          width: 100%;
          display: flex;
          align-items: center;
          justify-content: space-between;
        }
        .up-icon {
          &::after {
            border-bottom-color: #ea3636 !important;
            transform: translateY(-25%);
          }
        }
        .down-icon {
          &::after {
            border-top-color: #2dcb56 !important;
            transform: translateY(50%);
          }
        }
        .rate-arrow {
          text-align: right;
          &::after {
            content: '';
            display: inline-block;
            width: 0px;
            height: 0px;
            border: 6px solid white;
            border-left-color: white;
            border-right-color: white;
            margin: 0 10px;
          }
        }
      }
    }
  }
}
</style>

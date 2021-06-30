

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
    class="trends-chart">
    <bkdata-tab :active.sync="active"
      type="unborder-card">
      <bkdata-tab-panel v-bind="{ name: 'trend', label: '数据量趋势' }" />
      <bkdata-tab-panel v-bind="{ name: 'delay', label: '数据时间延迟' }" />
    </bkdata-tab>
    <div v-if="active === 'trend'"
      class="trend-container">
      <div class="bk-button-group">
        <bkdata-button
          :class="isShowSevenData ? 'is-selected' : ''"
          size="small"
          @click="changeChartData($t('7天前输出量'))">
          {{ $t('7天前') }}
        </bkdata-button>
        <bkdata-button
          :class="isShowOneData ? 'is-selected' : ''"
          size="small"
          @click="changeChartData($t('1天前输出量'))">
          {{ $t('1天前') }}
        </bkdata-button>
      </div>
      <ul v-if="isShowCount">
        <li>
          <label for="">{{ $t('总输入量') }}：</label>
          <span>{{ allInPut }}</span>
        </li>
        <li>
          <label for="">{{ $t('总输出量') }}：</label>
          <span>{{ allOutPut }}</span>
        </li>
      </ul>
    </div>
    <div v-if="judgeDta"
      class="no-data">
      <img alt
        src="@/common/images/no-data.png">
      <p>{{ $t('暂无数据') }}</p>
    </div>
    <div v-else
      id="detailChart"
      class="detail-chart">
      <template v-if="isShowChart">
        <PlotlyChart
          :chartConfig="cleanChartInfo.config"
          :chartData="cleanChartInfo.data"
          :chartLayout="cleanChartInfo.layout" />
      </template>
    </div>
  </div>
</template>

<script>
import PlotlyChart from '@/components/plotly';

export default {
  components: {
    PlotlyChart,
  },
  props: {
    cleanChartInfo: {
      type: Object,
      default: () => {
        return {
          data: [
            {
              x: [],
              y: [],
            },
          ],
          config: {},
          layout: {},
        };
      },
    },
    isLoading: {
      type: Boolean,
      default: false,
    },
    watch: {
      type: Number,
      default: 0,
    },
    allOutPut: {
      type: Number,
      default: 0,
    },
    allInPut: {
      type: Number,
      default: 0,
    },
    isShowCount: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      active: '',
      isShowChart: true,
      isShowSevenData: false,
      isShowOneData: false,
    };
  },
  computed: {
    judgeDta() {
      return this.cleanChartInfo.data.filter(item => item.x).length === 0;
    },
  },
  watch: {
    active(val) {
      this.isShowChart = false;
      this.$nextTick(() => {
        this.isShowChart = true;
      });
      this.isShowSevenData = false;
      this.isShowOneData = false;
      this.$emit('changeActive', val);
    },
    isShowSevenData(val) {
      if (val) {
        this.$emit('getChartData', 7);
      } else {
        this.$emit('filterChartData', [this.$t('7天前输出量'), this.$t('7天前输入量')]);
      }
    },
    isShowOneData(val) {
      if (val) {
        this.$emit('getChartData', 2);
      } else {
        this.$emit('filterChartData', [this.$t('1天前输出量'), this.$t('1天前输入量')]);
      }
    },
  },
  methods: {
    changeChartData(name) {
      if (name === this.$t('7天前输出量')) {
        this.isShowSevenData = !this.isShowSevenData;
      } else {
        this.isShowOneData = !this.isShowOneData;
      }
    },
  },
};
</script>

<style lang="scss" scoped>
.trend-container {
  display: flex;
  align-items: center;
  padding: 20px 0 0 20px;
  .bk-button-group {
    margin-right: 20px;
  }
  ul > li {
    float: left;
    margin-right: 10px;
  }
}
.trends-chart {
  width: 100%;
  position: relative;
  .no-data {
    height: 320px;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    padding-top: 20px;
    img {
      margin-bottom: 10px;
    }
  }
}
</style>

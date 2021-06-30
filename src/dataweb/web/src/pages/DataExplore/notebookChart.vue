

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
  <div class="search-result-table" />
</template>

<script>
import '@tencent/bkcharts-panel';
import '@tencent/bkcharts-panel/dist/main.min.css';

export default {
  props: {
    list: {
      type: Array,
      default: () => [],
    },
    // eslint-disable-next-line vue/prop-name-casing
    select_fields_order: {
      type: Array,
      default: () => [],
    },
    chartConfig: {
      type: Object,
      default: () => ({}),
    },
    isChart: {
      type: Boolean,
    },
  },
  data() {
    return {
      config: {
        tableHeight: 500,
        chart: {
          width: 460,
          heigth: 500,
          disabled: [],
        },
        watermark: {
          display: false,
        },
      },
      chartInstance: null,
    };
  },
  // 监听 chartConfig 变化，更新
  watch: {
    chartConfig: {
      deep: true,
      immediate: true,
      handler(val) {
        this.chartInstance && this.chartInstance.updateOptions(val);
      },
    },
  },
  mounted() {
    if (!this.isChart) {
      this.config.chart.disabled = ['line', 'bar', 'pie', 'scatter'];
    }
    const chartConfig = Object.assign(this.config, this.chartConfig);
    this.chartInstance = new window.BkChartsPanel(this.$el, this.list, this.select_fields_order, chartConfig);
  },
};
</script>

<style lang="scss" scoped>
.search-result-table {
  height: 100%;
  min-height: 400px;
}
/deep/ .bk-charts-panel-container .panel-content[data-v-a83bd3b0] {
  height: 515px;
}
</style>

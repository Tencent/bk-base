

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
  <div class="base-chart-container">
    <div v-if="isShowContent"
      v-bkloading="{ isLoading }"
      :class="[cssClasses]"
      :style="{ width, height }">
      <canvas ref="canvas" />
    </div>
    <NoData v-else
      :height="noDataHeight" />
  </div>
</template>

<script lang="ts">
import BKChart from '@blueking/bkcharts';
import { Component, Prop, Watch, Vue } from 'vue-property-decorator';
import NoData from '@/pages/datamart/DataDict/components/children/chartComponents/NoData.vue';

interface ChartData {
  labels: [];
  datasets: [];
}
@Component({
  components: {
    NoData,
  },
})
export default class BaseChart extends Vue {
  @Prop({ default: () => ({}), required: true }) chartData: Object;
  @Prop({ default: () => ({}) }) options: Object;
  @Prop({ required: true }) chartType: String;
  @Prop() width: string;
  @Prop() height: string;
  @Prop({ default: '100%' }) noDataHeight: string;
  @Prop() cssClasses: string;
  @Prop() isLoading: boolean;
  @Prop({ default: true }) isShowContent: boolean;
  @Prop({ default: () => [] }) plugins: Array<any>;

  _chart: any = null;
  _plugins: Array<any> = this.plugins;
  container: any = null;
  localWidth: string = '';

  @Watch('chartData', { immediate: true, deep: true })
  onChartDataChanged(val: ChartData, oldVal: ChartData) {
    if (!this.isShowContent) return;
    if (val.datasets && val.datasets.length) {
      this.$nextTick(() => {
        this.renderChart();
      });
    }
  }

  getCalcWidth() {
    this.localWidth = this.width || '100%';
    if (/^\d+%$/.test(this.width)) {
      const rect = this.$el.getBoundingClientRect();
      const percent = Number(this.width.replace(/%$/, '')) / 100;
      this.localWidth = rect.width * percent + 'px';
    }

    return this.localWidth || '100%';
  }

  /**
   * addPlugin
   */
  addPlugin(plugin: Array<any>) {
    this.$data._plugins.push(plugin);
  }

  /**
   * generateLegend
   */
  generateLegend() {
    if (this.$data._chart) {
      return this.$data._chart.generateLegend();
    }
  }

  /**
   * renderChart
   */
  renderChart() {
    if (this.$data._chart) this.$data._chart.destroy();

    this.$data._chart = new BKChart(this.$refs.canvas, {
      type: this.chartType,
      data: this.chartData,
      options: this.options,
      plugins: this.$data._plugins,
    });
  }

  beforeDestroy() {
    if (this.$data._chart) {
      this.$data._chart.clear();
      this.$data._chart.destroy();
    }
  }
}
</script>

<style lang="scss" scoped>
.base-chart-container {
  width: 100%;
}
</style>

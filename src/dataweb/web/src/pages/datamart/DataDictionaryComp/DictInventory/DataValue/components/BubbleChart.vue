

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
  <div v-bkloading="{ isLoading }"
    :class="['bubble-container', isShowContent ? '' : 'flex-center']">
    <canvas v-if="isShowContent"
      :id="id"
      height="380" />
    <NoData v-else />
  </div>
</template>

<script>
import BKChart from '@blueking/bkcharts';
import NoData from '@/pages/datamart/DataDict/components/children/chartComponents/NoData.vue';
import { QueryableWorker } from '@/common/js/QueryableWorker';
export default {
  components: {
    NoData,
  },
  props: {
    isLoading: Boolean,
    id: String,
  },
  data() {
    return {
      isShowContent: true,
      worker: null,
    };
  },
  beforeDestroy() {
    this.clearChart();
  },
  created() {
    // this.initWebwork();
  },
  methods: {
    initChart(chartData, options) {
      Object.assign(options, {
        animation: false,
        spanGaps: true,
        showLine: false,
        datasets: {
          line: {
            pointRadius: 0, // disable for all `'line'` datasets
          },
        },
        elements: {
          line: {
            tension: 0, // disables bezier curves
            fill: false,
            stepped: false,
            borderDash: [],
          },
          point: {
            radius: 0, // default to disabled in all datasets
          },
        },
      });

      this.clearChart();
      if (!chartData.labels.length) {
        this.isShowContent = false;
        return;
      }
      window[this.id] = new BKChart(this.id, {
        type: 'bubble',
        data: chartData,
        options: options,
      });

      // const config = JSON.parse(
      //   JSON.stringify(
      //     Object.freeze({
      //       type: 'bubble',
      //       data: chartData,
      //       options: options,
      //     })
      //   )
      // );

      // const canvas = document.createElement('canvas');
      // const offscreenCanvas = canvas.transferControlToOffscreen();
      // this.worker.sendQuery('init', [offscreenCanvas, config], [offscreenCanvas]);
    },
    clearChart() {
      if (window[this.id] && window[this.id].config) {
        window[this.id].clear();
        window[this.id].destroy();
        delete window[this.id];
      }
    },
    initWebwork() {
      if (!this.worker) {
        this.worker = new QueryableWorker('./static/dist/web_worker/bundle.worker.bkchartjs.js');

        this.worker.addListeners('initComplete', instance => {
          this.instance = event.data.target;
          this.$emit('init', this.instance);
        });

        this.worker.addListeners('updateComplete', instance => {
          this.instance = event.data.target;
          this.$emit('init', this.instance);
        });
      }
    },
  },
};
</script>

<style lang="scss" scoped>
.flex-center {
  display: flex;
  align-items: center;
  justify-content: center;
}
.bubble-container {
  width: 100%;
  height: 100%;
  canvas {
    width: 100% !important;
  }
}
</style>

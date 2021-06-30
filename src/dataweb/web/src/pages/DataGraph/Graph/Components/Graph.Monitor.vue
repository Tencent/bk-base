

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
  <!-- 监控图表盘 -->
  <charts-temp
    v-if="charts.isShowCharts"
    ref="chartsTemp"
    :zoom="zoomValue"
    :chartsData="charts"
    @disposeCharts="disposeCharts"
    @enterCharts="enterCharts"
    @showCharts="showCharts"
    @mouseenter="handleChartMouseEnter"
    @mouseleave="handleChartMouseleave" />
</template>

<script>
import { mapGetters, mapState } from 'vuex';
import { copyObj } from '@/common/js/util';
import chartsTemp from '@/components/dataflow/echartsTemp';
import tippy from 'tippy.js';
export default {
  name: 'Graph-monitor',
  components: {
    chartsTemp,
  },
  props: {
    zoomValue: {
      type: Number,
      default: 0,
    },
    flowId: {
      type: [String, Number],
      default: '',
    },
    nodeConfig: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      isPointDebug: true,
      popInstances: [],
      isChartMouseEnter: false,
      fireTimer: 0,
      bkPopoverTimerId: 0,
      activeNodeId: '',
      charts: {
        types: [],
        options: [],
        isShowCharts: false,
        data: [],
        timer: '',
        inside: false,
      },
    };
  },
  methods: {
    handleChartMouseEnter() {
      this.isChartMouseEnter = true;
      this.fireTimer && clearTimeout(this.fireTimer);
    },
    handleChartMouseleave() {
      this.isChartMouseEnter = false;
      this.handleMouseleave();
    },
    /**
     * mouseenter的时候显示打点调试信息
     */
    handleMouseenter(e) {
      clearTimeout(this.charts.timer);
      this.fireTimer && clearTimeout(this.fireTimer);
      this.charts.timer = setTimeout(() => {
        if (this.nodeConfig.node_id) {
          this.charts.isShowCharts = true;
          this.getChartOptions(this.nodeConfig.id);
          this.$nextTick(() => {
            if (this.activeNodeId !== this.nodeConfig.id) {
              this.activeNodeId = this.nodeConfig.id;
              this.bkPopoverTimerId && clearTimeout(this.bkPopoverTimerId);
              this.bkPopoverTimerId = setTimeout(() => {
                this.fireInstance();
                const popInstance = tippy(e.target, {
                  content: this.$refs.chartsTemp.$el,
                  trigger: 'manual',
                  arrow: true,
                  placement: 'left',
                  boundary: 'document.body',
                  maxWidth: 500,
                  interactive: true,
                  zIndex: 2000,
                  hideOnClick: false,
                });

                popInstance && popInstance.show();
                this.popInstances.push(popInstance);
              }, 400);
            }
          });
        }
      }, 100);
    },
    /**
     * mouseleave的时候隐藏打点调试信息
     */
    handleMouseleave(e) {
      this.fireTimer && clearTimeout(this.fireTimer);
      if (!this.isChartMouseEnter) {
        this.fireTimer = setTimeout(() => {
          // this.$refs.chartsTemp.resetChartsType()
          this.charts.isShowCharts = false;
          this.bkPopoverTimerId && clearTimeout(this.bkPopoverTimerId);
          this.fireInstance();
          this.activeNodeId = null;
        }, 300);
      }
    },
    /**
     * 销毁图表
     */
    disposeCharts() {
      this.charts.isShowCharts = false;
      this.charts.inside = false;
    },
    enterCharts() {
      this.charts.inside = true;
    },
    showCharts() {
      this.charts.isShowCharts = true;
    },

    fireInstance() {
      while (this.popInstances.length) {
        const instance = this.popInstances[0];
        instance && instance.destroy();
        this.popInstances.shift();
      }
    },

    /** @function
     * 获取图表数据
     */
    getChartOptions(id) {
      let location = this.nodeConfig;
      let nodeId = location.node_id;
      let charts = this.charts;
      // this.$refs.chartsTemp.resetChartsContainerPosition(location)
      let chartsType = {};
      let time = new Date();
      if (!location.monitorStartTime) {
        chartsType.monitorStartTime = time;
      } else {
        let interval = time - location.monitorStartTime;
        this.$refs.chartsTemp.init(nodeId);
        // 距离上次请求低于五分钟就不发请求
        if (interval / 1000 < 300) return;
      }

      this.$store
        .dispatch('api/flows/listMonitorData', { nodeId: nodeId, flowId: this.flowId })
        .then(resp => {
          if (resp.result) {
            let types = [];
            if ('data_delay_max' in resp.data) {
              types.push('hasDelay');
            }
            if ('data_trend' in resp.data) {
              types.push('hasTrend');
            }
            if ('rt_status' in resp.data) {
              types.push('offline');
            }

            if (!types.length) {
              types.push('null');
            }
            charts.data.push({
              [nodeId]: {
                types: types,
                options: resp.data,
                location: copyObj(location),
              },
            });
          } else {
            this.getMethodWarning(resp.message, resp.code);
          }
        })
        .then(() => {
          this.$refs.chartsTemp && this.$refs.chartsTemp.init(nodeId);
        });
    },
  },
};
</script>

<style></style>

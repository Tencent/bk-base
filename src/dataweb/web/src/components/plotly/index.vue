

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
  <div />
</template>
<script>
import Plotly from 'plotly.js/lib/core';
import { deepAssign } from '@/common/js/util.js';

// 图表操作按钮组配置
//  - sendDataToCloud
//  - (2D): zoom2d, pan2d, select2d, lasso2d, zoomIn2d, zoomOut2d, autoScale2d, resetScale2d
//  - (Cartesian): hoverClosestCartesian, hoverCompareCartesian
//  - (3D): zoom3d,pan3d, orbitRotation, tableRotation, handleDrag3d, resetCameraDefault3d, resetCameraLastSave3d,
//          hoverClosest3d
//  - (Geo): zoomInGeo, zoomOutGeo, resetGeo, hoverClosestGeo
//  - hoverClosestGl2d, hoverClosestPie, toggleHover, resetViews
const CHART_CONFIG = [
  'toImage', // 导出图片
  'sendDataToCloud', // 在线预览及调试数据
  'zoomIn2d', // 放大
  'zoomOut2d', // 缩小
  'autoScale2d', // 还原
  'toggleSpikelines', // 展示当前触发元素的基准线
  'hoverClosestCartesian', // show closest data on hover, hover时不会展示当前坐标信息
  'hoverCompareCartesian', // compare data on hover, hover时展示当前坐标信息
  'zoom2d', // Zoom, 拖拽选择区域时放大
  'pan2d', // Pan, 拖拽选择区域时移动图表
  'lasso2d', // 套索选区
  'select2d', // 矩形选区
  'resetScale2d', // home
];

/**
 * PLOTLY_EVENTS
 * @example https://plot.ly/javascript/plotlyjs-events/
 * @description plotly图像实例支持绑定的事件
 * @event onmousedown
 * @event plotly_relayout 更新layout时触发
 * @event plotly_click
 * @event plotly_legendclick
 * @event plotly_legenddoubleclick
 * @event plotly_hover
 * @event plotly_unhover
 * @event plotly_selecting
 * @event plotly_selected
 * @event plotly_afterplot
 * @event plotly_doubleclick
 */
const PLOTLY_EVENTS = [
  'onmousedown',
  'plotly_relayout',
  'plotly_click',
  'plotly_legendclick',
  'plotly_legenddoubleclick',
  'plotly_hover',
  'plotly_unhover',
  'plotly_selecting',
  'plotly_selected',
  'plotly_afterplot',
  'plotly_doubleclick',
];

/**
 * @param chartData 图表数据
 * @param chartLayout 图表样式设置
 * @param chartConfig 图表操作项配置
 * @param eventListen 图表事件监听绑定
 * 注意：目前图表数据变动会导致图表重绘，会导致初始化时相关绑定会失效
 */

export default {
  props: {
    chartData: {
      type: Array,
      default: () => [],
    },
    chartLayout: {
      type: Object,
      default: () => ({}),
    },
    chartConfig: {
      type: Object,
      default: () => ({}),
    },
    eventListen: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      chartType: {},
      defaultConfig: {
        displaylogo: false,
        modeBarButtonsToRemove: [],
      },
    };
  },
  computed: {
    targetConfig: {
      get() {
        const defaultConfig = {
          displaylogo: false,
          modeBarButtonsToRemove: [],
        };
        return deepAssign({}, [this.chartConfig, defaultConfig]);
      },
      set(val) {
        const defaultConfig = {
          displaylogo: false,
          modeBarButtonsToRemove: [],
        };

        // 将需保留的按钮从队列中剔除
        if (this.chartConfig && this.chartConfig.operationButtons) {
          // eslint-disable-next-line vue/no-mutating-props
          this.chartConfig.modeBarButtonsToRemove = CHART_CONFIG.filter(
            opt => !this.chartConfig.operationButtons.includes(opt)
          );
        }

        return deepAssign({}, [this.chartConfig, defaultConfig]);
      },
    },
  },
  watch: {
    chartData(val, oldValue) {
      this.registerChartType(this.filterDataChartType(val));
      Plotly.react(this.$el, val, this.chartLayout, this.targetConfig);
    },
    chartLayout(val, oldValue) {
      Plotly.react(this.$el, this.chartData, val, this.targetConfig);
    },
    targetConfig(val, oldValue) {
      Plotly.react(this.$el, this.chartData, this.chartLayout, val);
    },
  },
  mounted() {
    Plotly.newPlot(this.$el, this.chartData, this.chartLayout, this.targetConfig);
    this.initEventBinding();
  },
  created() {
    this.registerChartType(this.filterDataChartType(this.chartData));
  },
  beforeDestroy() {},
  methods: {
    registerChartType(types) {
      types
        && types.length
        && Plotly.register(
          Array.prototype.map
            .call(types, type => {
              try {
                return require(`plotly.js/lib/${type}`);
              } catch (e) {
                return null;
              }
            })
            .filter(t => t != null)
        );
    },
    filterDataChartType(data) {
      return Object.keys(
        Array.prototype.reduce.call(
          data || [],
          (pre, curr) => {
            if (curr.type && !this.chartType[curr.type]) {
              pre[curr.type] = true;
              this.chartType[curr.type] = true;
            }
            return pre;
          },
          {}
        )
      );
    },
    initEventBinding() {
      if (this.eventListen.length) {
        setTimeout(() => {
          // 这里setTimeout不安全，后续找到稳定方法建议替换
          for (let i = 0, l = this.eventListen.length; i < l; i++) {
            const eventName = this.eventListen[i];

            // 拒绝非法事件绑定
            if (!PLOTLY_EVENTS.includes(eventName)) continue;

            // onmousedown绑定和其他事件存在差异，需单独初始化
            if (eventName === 'onmousedown') {
              this.$el.onmousedown = data => {
                this.$emit('onmousedown', data);
              };
              continue;
            }

            // 常规绑定并emit触发父组件事件
            this.$el.on(eventName, data => {
              this.$emit(eventName, data);
            });
          }
        }, 1500);
      }
    },
  },
};
</script>

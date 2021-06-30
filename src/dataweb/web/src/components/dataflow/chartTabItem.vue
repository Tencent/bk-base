

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
  <div id="charts-trend-content">
    <div v-if="!chartData.length"
      class="charts-trend-nodata">
      <div>
        <i class="bk-icon icon-no-data" />
        <p>{{ $t('暂无数据') }}</p>
      </div>
    </div>
    <PlotlyChart v-else
      :chartData="chartData"
      :chartConfig="config"
      :chartLayout="layout" />
  </div>
</template>
<script>
import PlotlyChart from '@/components/plotly';
export default {
  components: {
    PlotlyChart,
  },
  props: {
    chartData: {
      type: Array,
      default: () => {
        return [];
      },
    },
  },
  data() {
    return {
      config: {
        operationButtons: ['resetScale2d', 'zoomIn2d', 'select2d'],
      },
      layout: {
        hovermode: 'closest',
        width: 450,
        height: 270,
        xaxis: {
          autorange: false,
          range: [0, 1440],
          dtick: 360,
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
          l: 50,
          r: 50,
          b: 60,
          t: 20,
        },
        paper_bgcolor: '#2c2d3c',
        plot_bgcolor: '#2c2d3c',
      },
    };
  },
};
</script>



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
  <div class="ace_editor">
    <select v-model="chartType">
      <option v-for="(option, index) in chartTypes" :key="index" :value="option">
        {{ option }}
      </option>
    </select>
    <chartEditor :chartData="chartConfig.data" :chartLayout="chartConfig.layout" />
  </div>
</template>

<script type="text/javascript">
import Vue from 'vue';
import chartEditor from '@/components/plotly';

export default {
  components: {
    chartEditor,
  },
  data() {
    return {
      sqlData: '',
      chartType: 'scatterpolar',
      defineFields: [],
      chartDatas: {
        bar: {
          data: [
            {
              x: ['giraffes', 'orangutans', 'monkeys'],
              y: [20, 14, 23],
              name: 'SF Zoo',
              type: 'bar',
            },
            {
              x: ['giraffes', 'orangutans', 'monkeys'],
              y: [12, 18, 29],
              name: 'LA Zoo',
              type: 'bar',
            },
          ],
        },
        lines: {
          data: [
            {
              type: 'scatter',
              x: [1, 2, 3, 4],
              y: [10, 15, 13, 17],
              mode: 'lines',
              name: 'Red',
              line: {
                color: 'rgb(219, 64, 82)',
                width: 3,
              },
            },
            {
              type: 'scatter',
              x: [1, 2, 3, 4],
              y: [12, 9, 15, 12],
              mode: 'lines',
              name: 'Blue',
              line: {
                color: 'rgb(55, 128, 191)',
                width: 1,
              },
            },
          ],
        },
        scatter: {
          data: [
            {
              x: [1, 2, 3, 4],
              y: [10, 15, 13, 17],
              mode: 'markers',
              type: 'scatter',
            },
            {
              x: [2, 3, 4, 5],
              y: [16, 5, 11, 9],
              mode: 'lines',
              type: 'scatter',
            },
            {
              x: [1, 2, 3, 4],
              y: [12, 9, 15, 12],
              mode: 'lines+markers',
              type: 'scatter',
            },
          ],
        },
        pie: {
          data: [
            {
              values: [19, 26, 55],
              labels: ['Residential', 'Non-Residential', 'Utility'],
              type: 'pie',
            },
          ],
        },
        scatter2: {
          data: [
            {
              x: [1, 2, 3, 4],
              y: [10, 11, 12, 13],
              mode: 'markers',
              marker: {
                color: ['hsl(0,100,40)', 'hsl(33,100,40)', 'hsl(66,100,40)', 'hsl(99,100,40)'],
                size: [12, 22, 32, 42],
                opacity: [0.6, 0.7, 0.8, 0.9],
              },
              type: 'scatter',
            },
            {
              x: [1, 2, 3, 4],
              y: [11, 12, 13, 14],
              mode: 'markers',
              marker: {
                color: 'rgb(31, 119, 180)',
                size: 18,
                symbol: ['circle', 'square', 'diamond', 'cross'],
              },
              type: 'scatter',
            },
            {
              x: [1, 2, 3, 4],
              y: [12, 13, 14, 15],
              mode: 'markers',
              marker: {
                size: 18,
                line: {
                  color: ['rgb(120,120,120)', 'rgb(120,120,120)', 'red', 'rgb(120,120,120)'],
                  width: [2, 2, 6, 2],
                },
              },
              type: 'scatter',
            },
          ],
        },
        scatterpolar: {
          data: [
            {
              type: 'scatterpolar',
              r: [39, 28, 8, 7, 28, 39],
              theta: ['A', 'B', 'C', 'D', 'E', 'A'],
              fill: 'toself',
              name: 'Group A',
            },
            {
              type: 'scatterpolar',
              r: [1.5, 10, 39, 31, 15, 1.5],
              theta: ['A', 'B', 'C', 'D', 'E', 'A'],
              fill: 'toself',
              name: 'Group B',
            },
          ],
          layout: {
            polar: {
              radialaxis: {
                visible: true,
                range: [0, 50],
              },
            },
            height: 400,
            width: 500,
          },
        },
        BarLinePlot: () => {
          var xSavings = [
            1.3586,
            2.2623000000000002,
            4.9821999999999997,
            6.5096999999999996,
            7.4812000000000003,
            7.5133000000000001,
            15.2148,
            17.520499999999998,
          ];

          var xNetworth = [
            93453.919999999998,
            81666.570000000007,
            69889.619999999995,
            78381.529999999999,
            141395.29999999999,
            92969.020000000004,
            66090.179999999993,
            122379.3,
          ];

          var ySavings = [
            'Japan',
            'United Kingdom',
            'Canada',
            'Netherlands',
            'United States',
            'Belgium',
            'Sweden',
            'Switzerland',
          ];

          var yNetworth = [
            'Japan',
            'United Kingdom',
            'Canada',
            'Netherlands',
            'United States',
            'Belgium',
            'Sweden',
            'Switzerland',
          ];

          var trace1 = {
            x: xSavings,
            y: ySavings,
            xaxis: 'x1',
            yaxis: 'y1',
            type: 'bar',
            marker: {
              color: 'rgba(50,171,96,0.6)',
              line: {
                color: 'rgba(50,171,96,1.0)',
                width: 1,
              },
            },
            name: 'Household savings, percentage of household disposable income',
            orientation: 'h',
          };

          var trace2 = {
            x: xNetworth,
            y: yNetworth,
            xaxis: 'x2',
            yaxis: 'y1',
            mode: 'lines+markers',
            line: {
              color: 'rgb(128,0,128)',
            },
            name: 'Household net worth, Million USD/capita',
          };

          var data = [trace1, trace2];

          var layout = {
            title: 'Household Savings & Net Worth for Eight OECD Countries',
            xaxis1: {
              range: [0, 20],
              domain: [0, 0.5],
              zeroline: false,
              showline: false,
              showticklabels: true,
              showgrid: true,
            },
            xaxis2: {
              range: [25000, 150000],
              domain: [0.5, 1],
              zeroline: false,
              showline: false,
              showticklabels: true,
              showgrid: true,
              side: 'top',
              dtick: 25000,
            },
            legend: {
              x: 0.029,
              y: 1.238,
              font: {
                size: 10,
              },
            },
            margin: {
              l: 100,
              r: 20,
              t: 200,
              b: 70,
            },
            width: 600,
            height: 600,
            paper_bgcolor: 'rgb(248,248,255)',
            plot_bgcolor: 'rgb(248,248,255)',
            annotations: [
              {
                xref: 'paper',
                yref: 'paper',
                x: -0.2,
                y: -0.109,
                text:
                  'OECD ' +
                  '(2015), Household savings (indicator), ' +
                  'Household net worth (indicator). doi: ' +
                  '10.1787/cfc6f499-en (Accessed on 05 June 2015)',
                showarrow: false,
                font: {
                  family: 'Arial',
                  size: 10,
                  color: 'rgb(150,150,150)',
                },
              },
            ],
          };

          for (var i = 0; i < xSavings.length; i++) {
            var result = {
              xref: 'x1',
              yref: 'y1',
              x: xSavings[i] + 2.3,
              y: ySavings[i],
              text: xSavings[i] + '%',
              font: {
                family: 'Arial',
                size: 12,
                color: 'rgb(50, 171, 96)',
              },
              showarrow: false,
            };
            var result2 = {
              xref: 'x2',
              yref: 'y1',
              x: xNetworth[i] - 20000,
              y: yNetworth[i],
              text: xNetworth[i] + ' M',
              font: {
                family: 'Arial',
                size: 12,
                color: 'rgb(128, 0, 128)',
              },
              showarrow: false,
            };
            layout.annotations.push(result, result2);
          }

          return { data: data, layout: layout };
        },
        contour: {
          data: [
            {
              z: [
                [2, 4, 7, 12, 13, 14, 15, 16],
                [3, 1, 6, 11, 12, 13, 16, 17],
                [4, 2, 7, 7, 11, 14, 17, 18],
                [5, 3, 8, 8, 13, 15, 18, 19],
                [7, 4, 10, 9, 16, 18, 20, 19],
                [9, 10, 5, 27, 23, 21, 21, 21],
                [11, 14, 17, 26, 25, 24, 23, 22],
              ],
              type: 'contour',
              line: {
                smoothing: 0,
              },
              xaxis: 'x1',
              yaxis: 'y1',
            },
            {
              z: [
                [2, 4, 7, 12, 13, 14, 15, 16],
                [3, 1, 6, 11, 12, 13, 16, 17],
                [4, 2, 7, 7, 11, 14, 17, 18],
                [5, 3, 8, 8, 13, 15, 18, 19],
                [7, 4, 10, 9, 16, 18, 20, 19],
                [9, 10, 5, 27, 23, 21, 21, 21],
                [11, 14, 17, 26, 25, 24, 23, 22],
              ],
              type: 'contour',
              line: {
                smoothing: 0.85,
              },
              xaxis: 'x2',
              yaxis: 'y2',
            },
          ],
          layout: {
            title: 'Smoothing Contour Lines',
            xaxis: { domain: [0, 0.45], anchor: 'y1' },
            yaxis: { domain: [0, 1], anchor: 'x1' },
            xaxis2: { domain: [0.55, 1], anchor: 'y2' },
            yaxis2: { domain: [0, 1], anchor: 'x2' },
            height: 400,
            width: 500,
          },
        },
        sankey: () => {
          let fig = require('./sankey.json');
          var data = {
            type: 'sankey',
            domain: {
              x: [0, 1],
              y: [0, 1],
            },
            orientation: 'h',
            valueformat: '.0f',
            valuesuffix: 'TWh',
            node: {
              pad: 15,
              thickness: 15,
              line: {
                color: 'black',
                width: 0.5,
              },
              label: fig.data[0].node.label,
              color: fig.data[0].node.color,
            },

            link: {
              source: fig.data[0].link.source,
              target: fig.data[0].link.target,
              value: fig.data[0].link.value,
              label: fig.data[0].link.label,
            },
          };

          return {
            data: [data],
            layout: {
              title:
                'Energy forecast for 2050 Source: Department of Energy & Climate Change, Tom Counsell via Mike Bostock',
              width: 1118,
              height: 772,
              font: {
                size: 10,
              },
            },
          };
        },
        area: {
          data: [
            {
              r: [77.5, 72.5, 70.0, 45.0, 22.5, 42.5, 40.0, 62.5],
              t: ['North', 'N-E', 'East', 'S-E', 'South', 'S-W', 'West', 'N-W'],
              marker: { color: 'rgb(106,81,163)' },
              name: '11-14 m/s',
              type: 'area',
            },
            {
              r: [57.5, 50.0, 45.0, 35.0, 20.0, 22.5, 37.5, 55.0],
              t: ['North', 'N-E', 'East', 'S-E', 'South', 'S-W', 'West', 'N-W'],
              marker: { color: 'rgb(158,154,200)' },
              name: '8-11 m/s',
              type: 'area',
            },
            {
              r: [40.0, 30.0, 30.0, 35.0, 7.5, 7.5, 32.5, 40.0],
              t: ['North', 'N-E', 'East', 'S-E', 'South', 'S-W', 'West', 'N-W'],
              marker: { color: 'rgb(203,201,226)' },
              name: '5-8 m/s',
              type: 'area',
            },
            {
              r: [20.0, 7.5, 15.0, 22.5, 2.5, 2.5, 12.5, 22.5],
              t: ['North', 'N-E', 'East', 'S-E', 'South', 'S-W', 'West', 'N-W'],
              marker: { color: 'rgb(242,240,247)' },
              name: '< 5 m/s',
              type: 'area',
            },
          ],
          layout: {
            autosize: false,
            font: { size: 16 },
            height: 500,
            legend: { font: { size: 16 } },
            margin: {
              r: 50,
              t: 50,
              b: 50,
              l: 50,
            },
            orientation: 270,
            radialaxis: { ticksuffix: '%' },
            title: 'Wind Speed Distribution in Laurel, NE',
            width: 500,
          },
        },
      },
      chartLayout: {
        height: 400,
        width: 500,
      },
    };
  },
  computed: {
    chartTypes() {
      return ['BarLinePlot', 'bar', 'lines', 'scatter', 'scatter2', 'pie', 'scatterpolar', 'contour', 'sankey', 'area'];
    },
    chartConfig() {
      let chart = this.chartDatas[this.chartType];
      let charConfig = (typeof chart === 'function' && chart()) || chart;
      return { data: charConfig.data, layout: charConfig.layout || this.chartLayout };
    },
  },
};
</script>

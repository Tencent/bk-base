

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
// import mapImg from './demo/map-imgs/268697616.png'
export default {
  mounted() {
    let d3 = Plotly.d3;
    var data = Array.apply(0, Array(31)).map(function (item, i) {
      i++;
      return { date: parseInt(Math.random() * 1024), pv: parseInt(Math.random() * 1024) };
    });

    d3.csv('./data/map-imgs/268697616_data.csv', function (data) {
      console.log(data);
    });

    var line = d3.svg
      .line()
      .x(function (d) {
        return x(d.date);
      })
      .y(function (d) {
        return y(d.pv);
      })
      .interpolate('monotone');
    var svg = Plotly.d3
      .select(this.$el)
      .append('svg')
      .attr({
        width: 1024,
        height: 1024,
        border: '1px solid #ccc',
      });

    let r0 = 5;
    let r1 = 8;

    var x = d3.scale
      .linear()
      .domain([0, 1024])
      .range([0, 1024]);

    var y = d3.scale
      .linear()
      .domain([0, 1024])
      .range([0, 1024]);

    svg.append('svg:image').attr({
      'xlink:href': '', // mapImg, // 268697616.png  // can also add svg file here
      x: 0,
      y: 0,
      width: 1024,
      height: 1024,
    });

    var g = svg
      .selectAll('circle')
      .data(data)
      .enter()
      .append('g')
      .append('circle')
      .attr('class', 'linecircle')
      .attr('cx', line.x())
      .attr('cy', line.y())
      .attr('r', r0)
      .on('mouseover', function () {
        d3.select(this)
          .transition()
          .duration(500)
          .attr('r', 5);
      })
      .on('mouseout', function () {
        d3.select(this)
          .transition()
          .duration(500)
          .attr('r', 3.5);
      });
  },
};
</script>

/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
 */

/* eslint-disable no-param-reassign */

const BKChart = require('@tencent/bkchart.js/dist/bkcharts');
const chartManager = {
  instance: null,
  init(canvas, config) {
    const chart = new BKChart(canvas, config);

    // Resizing the chart must be done manually, since OffscreenCanvas does not include event listeners.
    canvas.width = 100;
    canvas.height = 100;
    chart.resize();

    postMessage({
      method: 'initComplete',
      target: chart,
    });
  },

  update(instance, config) {
    const { datasets, labels, data, options } = config;
    instance.data.datasets = datasets || [];
    instance.data.labels = labels || [];
    if (data.xLabels) {
      instance.data.xLabels = data.xLabels;
    }

    if (data.yLabels) {
      instance.data.yLabels = data.yLabels;
    }

    instance.options = options;
    instance.update();

    postMessage({
      method: 'updateComplete',
      target: instance,
    });
  },
};

onmessage = function (event) {
  const { queryMethod, queryArguments } = event.data;
  console.log('on webwork message', event);
  if (Object.prototype.hasOwnProperty.call(chartManager, queryMethod)) {
    chartManager[queryMethod].apply(self, queryArguments);
  }
};

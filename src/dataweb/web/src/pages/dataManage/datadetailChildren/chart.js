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

export function cleanChart(data) {
  const option = {
    backgroundColor: [`#fff url(${window.userPic}) repeat`],
    tooltip: {
      trigger: 'axis',
    },
    grid: {
      top: '-1px',
      left: '-1px',
      right: '-1px',
      bottom: '0%',
    },
    xAxis: [
      {
        type: 'category',
        boundaryGap: false, // 占满横坐标
        splitLine: {
          // X网格线
          show: true,
        },
        data: data.time,
      },
    ],
    yAxis: [
      {
        type: 'value',
      },
    ],
    series: [
      {
        name: '输入',
        type: 'line',
        smooth: true,
        symbol: 'none', // 圆点
        areaStyle: {
          // 区域填充
          normal: {
            color: 'rgba(194, 208, 227, 1)',
          },
        },
        itemStyle: {
          // 区域颜色
          normal: {
            color: 'rgba(194, 208, 227, 1)',
          },
        },
        data: data.input,
      },
      {
        name: '输出',
        type: 'line',
        smooth: true,
        symbol: 'none',
        areaStyle: {
          normal: { color: 'rgba(133, 142, 200, 1)' },
        },
        itemStyle: {
          normal: {
            color: 'rgba(133, 142, 200, 1)',
          },
        },
        data: data.output,
      },
    ],
  };
  return option;
}
export function detailChart(data) {
  const option = {
    backgroundColor: [`#fff url(${window.userPic}) repeat`],
    tooltip: {
      trigger: 'axis',
    },
    toolbox: {
      show: true,
      feature: {
        dataZoom: {
          show: true,
          title: {
            zoom: window.gVue.$t('缩放'),
            back: window.gVue.$t('缩放后退'),
          },
        },
        magicType: {
          show: true,
          type: ['bar'],
          title: {
            bar: window.gVue.$t('柱状图'),
          },
        },
        restore: {
          show: true,
          title: window.gVue.$t('还原'),
        },
      },
      right: 15,
    },
    grid: {
      top: '30px',
      left: '5px',
      right: '20px',
      bottom: '5px',
      containLabel: true,
    },
    xAxis: [
      {
        type: 'category',
        boundaryGap: false, // 占满横坐标
        splitLine: {
          // X网格线
          show: true,
        },
        data: data.time,
      },
    ],
    yAxis: [
      {
        type: 'value',
        boundaryGap: [0, '3%'],
      },
    ],
    series: [
      {
        name: '输入',
        type: 'line',
        smooth: true,
        areaStyle: {
          // 区域填充
          normal: {
            color: 'rgba(194, 208, 227, 1)',
          },
        },
        itemStyle: {
          // 区域颜色
          normal: {
            color: 'rgba(194, 208, 227, 1)',
          },
        },
        data: data.input,
      },
      {
        name: '输出',
        type: 'line',
        smooth: true,
        areaStyle: {
          normal: { color: 'rgba(133, 142, 200, 1)' },
        },
        itemStyle: {
          normal: {
            color: 'rgba(133, 142, 200, 1)',
          },
        },
        data: data.output,
      },
    ],
  };
  return option;
}

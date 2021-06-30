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

/* eslint-disable no-undef */
/* eslint-disable no-underscore-dangle */
/* eslint-disable no-unused-vars */
/* eslint-disable no-param-reassign */
/* eslint-disable no-multi-assign */
'use strict';
import { throttled } from './utils';
export default function registerScriptPan(Chart) {
  const { helpers } = Chart;

  // Take the zoom namespace of Chart
  const zoomNS = (Chart.ScriptPan = Chart.ScriptPan || {});

  Chart.ScriptPan.defaults = Chart.defaults.plugins.ScriptPan = {
    enabled: false,
    mode: 'xy',
    speed: 20,
    threshold: 10,
    /**
     * 默认是否绘制静态面板
     * 用于标注某段数据
     */
    drawStaticPan: false,

    /**
     * 需要绘制的静态数据
     * 数据格式:
     * {
     * 		position: top|bottom|y|x,
     * 		offset: { x: 0, y: 0 },
     * 		height: 10
     *      tickType: linear | timestamp | static,
     * 		tickSize: 'auto'
     * 		data: [{ start: '', end: '', backgroundColor: '', borderColor: ''  }]
     * }
     */
    staticDatas: [],

    /** 绘制静态标注配置项 */
    static: {
      /** linear | timestamp | static */
      tickType: 'timestamp',

      tickSize: 'auto',
    },
    drag: {
      borderColor: 'rgba(225,225,225,0.2)',
      borderWidth: 1,
      backgroundColor: 'rgba(225,225,225,0.4)',
      animationDuration: 0,
    },
    // 鼠标移上去显示线
    line: {
      enable: false,
      color: '#3A84FF',
      width: 1,
      dashPattern: [],
      fillRect: false,
      background: 'rgba(225, 0, 0, 0.1)',
      position: 'right', // 位置，left/right
    },

    /** 选中区域鼠标放开事件 */
    onPaned(chartInstance, selectedData) {},

    /** 选中区域鼠标放开事件 */
    onPaneding(chartInstance, area) {},
  };

  function resolveOptions(chart, options) {
    const deprecatedOptions = {};
    if (typeof chart.options.ScriptPan !== 'undefined') {
      deprecatedOptions.ScriptPan = chart.options.ScriptPan;
    }

    const props = chart.$span;
    options = props._options = helpers.merge({}, [options, deprecatedOptions]);
  }

  /**
   * @param {string} mode can be 'x', 'y' or 'xy'
   * @param {string} dir can be 'x' or 'y'
   * @param {Chart} chart instance of the chart in question
   */
  function directionEnabled(mode, dir, chart) {
    if (mode === undefined) {
      return true;
    }
    if (typeof mode === 'string') {
      return mode.indexOf(dir) !== -1;
    }
    if (typeof mode === 'function') {
      return mode({ chart }).indexOf(dir) !== -1;
    }

    return false;
  }

  function getXAxis(chartInstance) {
    const { scales } = chartInstance;
    const scaleIds = Object.keys(scales);
    for (let i = 0; i < scaleIds.length; i++) {
      const scale = scales[scaleIds[i]];

      if (scale.isHorizontal()) {
        return scale;
      }
    }
  }

  function getYAxis(chartInstance) {
    const { scales } = chartInstance;
    const scaleIds = Object.keys(scales);
    for (let i = 0; i < scaleIds.length; i++) {
      const scale = scales[scaleIds[i]];

      if (!scale.isHorizontal()) {
        return scale;
      }
    }
  }

  /**
   * 判断鼠标是单击事件还是点击选中区域事件
   * @param {*} sourceEvent mouse down event
   * @param {*} targetEvent mouse up event
   */
  function isMoved(sourceEvent, targetEvent) {
    return sourceEvent && targetEvent && (sourceEvent.x !== targetEvent.x || sourceEvent.y !== targetEvent.y);
  }

  function isPanMoved(chartInstance) {
    return isMoved(chartInstance.$span._dragZoomStart, chartInstance.$span._dragZoomEnd);
  }

  function getRect(chartInstance) {
    const xAxis = getXAxis(chartInstance);
    const yAxis = getYAxis(chartInstance);
    const beginPoint = chartInstance.$span._dragZoomStart;
    const endPoint = chartInstance.$span._dragZoomEnd;

    const origin = {
      startX: xAxis.left,
      endX: xAxis.right,
      startY: yAxis.top,
      endY: yAxis.bottom,
    };

    let { startX, endX, startY, endY } = origin;

    if (directionEnabled(chartInstance.$span._options.mode, 'x', chartInstance)) {
      startX = Math.min(beginPoint.offsetX, endPoint.offsetX);
      endX = Math.max(beginPoint.offsetX, endPoint.offsetX);
    }

    if (directionEnabled(chartInstance.$span._options.mode, 'y', chartInstance)) {
      startY = Math.min(beginPoint.offsetY, endPoint.offsetY);
      endY = Math.max(beginPoint.offsetY, endPoint.offsetY);
    }
    startX = startX < origin.startX ? origin.startX : startX;
    endX = endX > origin.endX ? origin.endX : endX;

    startY = startY < origin.startY ? origin.startY : startY;
    endY = endY > origin.endY ? origin.endY : endY;

    const rectWidth = endX - startX;
    const rectHeight = endY - startY;

    return { rectWidth, rectHeight, startX, startY };
  }

  /** 绘制鼠标选中区域 */
  function drawPan(chartInstance) {
    if (isPanMoved(chartInstance)) {
      const ctx = chartInstance.assistCtx;
      const { rectWidth, rectHeight, startX, startY } = getRect(chartInstance);
      const dragOptions = chartInstance.$span._options.drag;

      if (rectWidth < 1 || rectHeight < 1) {
        return;
      }

      ctx.save();
      ctx.clearRect(startX, startY, rectWidth, rectHeight);
      ctx.beginPath();
      ctx.fillStyle = dragOptions.backgroundColor || 'rgba(225,225,225,0.4)';
      ctx.fillRect(startX, startY, rectWidth, rectHeight);

      if (dragOptions.borderWidth > 0) {
        ctx.lineWidth = dragOptions.borderWidth;
        ctx.strokeStyle = dragOptions.borderColor || 'rgba(225,225,225, 0.4)';
        ctx.strokeRect(startX, startY, rectWidth, rectHeight);
      }
      ctx.restore();
    }
  }

  function getPanedDataByValue(chartInstance, startValue, endValue) {
    const xAxis = getXAxis(chartInstance);
    const startX = getPixcelByClosetValue(startValue, xAxis, 'left');
    const endX = getPixcelByClosetValue(endValue, xAxis, 'left');
    return { startValue, endValue, startX, endX };
  }

  function getPixcelByClosetValue(matchValue, scale, match) {
    const closetTicks = getClosetTick(scale.ticks, 'value', matchValue);
    let result = 0;
    if (closetTicks.length === 2) {
      const tick = match === 'left' ? closetTicks[0] : closetTicks[1];
      const index = scale.ticks.findIndex(item => item.value === tick.value);
      result = scale.getPixelForTick(index);

      const { _ticksLength, _length, ticks } = scale;
      const stepLen = _length / (_ticksLength || ticks.length);
      result += _getOffset(closetTicks, matchValue, match, stepLen);
    }

    if (closetTicks.length === 1) {
      const index = scale.ticks.findIndex(item => item.value === closetTicks[0].value);
      result = scale.getPixelForTick(index);
    }

    return result;
  }

  function _getOffset(matchedTicks, value, matchType, stepLength) {
    let offset = 0;
    if (matchedTicks.length === 2) {
      const match = (matchedTicks[1].value - matchedTicks[0].value);
      if (matchType === 'left') {
        offset = ((value - matchedTicks[0].value) / match) * stepLength;
      }

      if (matchType === 'right') {
        offset = (-(matchedTicks[1].value - value) / match) * stepLength;
      }
    }

    return offset;
  }

  function getClosetTick(ticks, matchKey, matchValue) {
    const { length } = ticks;
    if (length <= 2) {
      return ticks;
    }
    const middleIndex = Math.floor(length / 2);
    if (ticks[middleIndex][matchKey] > matchValue) {
      if (ticks[middleIndex - 1][matchKey] < matchValue) {
        return [ticks[middleIndex - 1], ticks[middleIndex]];
      }
      return getClosetTick(ticks.slice(0, middleIndex + 1), matchKey, matchValue);
    }
    if (ticks[middleIndex + 1][matchKey] > matchValue) {
      return [ticks[middleIndex], ticks[middleIndex + 1]];
    }
    return getClosetTick(ticks.slice(middleIndex + 1, length), matchKey, matchValue);
  }

  function getPanedDataByLabel(chartInstance, startLabel, endLabel) {
    const xAxis = getXAxis(chartInstance);
    const { startX, endX, rectWidth } = getTimestampTickX(xAxis, startLabel, endLabel);

    const startValue = xAxis.getValueForPixel(startX);
    const endValue = xAxis.getValueForPixel(endX);

    return { startValue, endValue, startLabel, endLabel, startX, endX, rectWidth };
  }

  function isMouseInChartArea(chart, event) {
    const { left, right, top, bottom } = chart.chartArea;
    const { offsetX, offsetY } = event.native;
    return offsetX >= left && offsetX <= right && offsetY >= top && offsetY <= bottom;
  }

  function isExpectedTarget(event, chart) {
    return (
      event.native.target === chart.canvas
      || event.native.target === chart.assistCanvas
      || event.native.target === chart.container
    );
  }

  zoomNS.panCumulativeDelta = 0;
  zoomNS.zoomCumulativeDelta = 0;

  class ScriptPan {
    constructor(options, chart) {
      this.options = options;
      this._isMousedown = undefined;
      this._isMousedown = undefined;
      this._dragZoomStart = undefined;
      this._dragZoomEnd = undefined;
      this._dragZoomStart = undefined;
      this._chart = chart;
    }

    _getXAxis() {
      return getXAxis(this._chart);
    }

    _getPanedDataByValues(values) {
      const startIndex = values[0];
      const endIndex = values.slice(-1)[0];

      return getPanedDataByValue(this._chart, startIndex, endIndex);
    }

    _getStaicPanOption(options, rectHeight = null) {
      return getStaicPanOption(this._chart, options, rectHeight);
    }

    _getPanedDataByValue(startValue, endValue) {
      return getPanedDataByValue(this._chart, startValue, endValue);
    }

    _getPanedDataByLabel(startLabel, endLabel) {
      return getPanedDataByLabel(this._chart, startLabel, endLabel);
    }

    handleEventChanged(chart, event) {
      if (event.type === 'mousedown') {
        if (isExpectedTarget(event, chart) && !isMouseInChartArea(chart, event)) {
          return;
        }
        this._isMousedown = true;
        this._dragZoomStart = null;
        this._dragZoomEnd = null;
        this._dragZoomStart = event.native;
      }
      if (event.type === 'mouseup') {
        this._isMousedown = false;
        if (!isExpectedTarget(event, chart)) {
          return;
        }

        const isMoved = isPanMoved(chart);

        if (!this._dragZoomStart) {
          return;
        }

        if (isMoved && typeof this.options.onPaned === 'function') {
          const xAxis = getXAxis(chart);

          /** 左右拖拽导致startX和endX顺序不对 */
          const startX =            this._dragZoomStart.offsetX < this._dragZoomEnd.offsetX
            ? this._dragZoomStart.offsetX
            : this._dragZoomEnd.offsetX;
          const endX =            this._dragZoomStart.offsetX > this._dragZoomEnd.offsetX
            ? this._dragZoomStart.offsetX
            : this._dragZoomEnd.offsetX;

          const startValue = xAxis.getValueForPixel(startX);
          const endValue = xAxis.getValueForPixel(endX);
          const { ticks } = xAxis;
          this.options.onPaned(chart, event, { startValue, endValue, ticks, startX, endX });
        }
      }
      if (event.type === 'mousemove') {
        if (!isExpectedTarget(event, chart)) {
          return;
        }

        if (!isMouseInChartArea(chart, event)) {
          return;
        }

        if (!this._isMousedown) {
          return;
        }

        if (this._dragZoomStart) {
          this._dragZoomEnd = event.native;
          throttled(this.options.onPaneding)(chart, this._dragZoomStart, this._dragZoomEnd);
        }
      }
    }
  }

  // Chartjs Zoom Plugin
  const ScriptPanPlugin = {
    id: 'ScriptPan',
    _element: ScriptPan,

    beforeInit(chartInstance, pluginOptions) {
      if (pluginOptions.enabled) {
        chartInstance.$span = new ScriptPan(pluginOptions, chartInstance);
        resolveOptions(chartInstance, pluginOptions);
      }
    },

    afterEvent(chart, event) {
      if (chart.$span) {
        chart.$span.handleEventChanged(chart, event);
      }
    },

    beforeDraw(chartInstance) {
      if (!chartInstance.$span) {
        return;
      }

      if (chartInstance.$span._dragZoomStart) {
        drawPan(chartInstance);
      }
    },

    destroy(chartInstance) {
      if (!chartInstance.$span) {
        return;
      }

      delete chartInstance.$span;
    },
  };

  Chart.register(ScriptPanPlugin);
}

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

/* eslint-disable */

import { select } from 'd3-selection';
import { scaleTime } from 'd3-scale';
import { axisTop } from 'd3-axis';
import { timeHour, timeDay, timeMonday, timeMonth, timeWeek, timeYear } from 'd3-time';
import { timeFormat } from 'd3-time-format';
import { zoom } from 'd3-zoom';
import * as utils from './utils';


export default class timeLineChart {
  constructor(config) {
    const { layout, domain, schedulingPeriod, data, rectHeight } = config;
    this.layout = Object.assign(
      {
        height: 240,
        width: 1300,
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        firstAxisTop: 22,
        secondAxisTop: 40,
      },
      layout
    );
    this.width = this.layout.width - this.layout.left - this.layout.right;
    this.height = this.layout.height - this.layout.top - this.layout.bottom;
    this.schedulingPeriod = JSON.parse(JSON.stringify(schedulingPeriod));
    this.timeRange = domain;
    this.timeSteps = {
      dayStep: 15,
    };
    this.data = JSON.parse(JSON.stringify(data));
    this.startTimeData = [];
    this.rectHeight = rectHeight || 6;
    this.isInit = false;

    this.canvas = null;
    this.xScale = scaleTime().range([0, this.width]).domain(this.timeRange);
    this.hourAxis = null;
    this.dayAxis = null;
    this.weekAxis = null;
    this.visiableCount = [0, 1, 2, 3, 4]; // 可见调度周期，累加窗口相关配置会影响是否调度，不调度的区域不做展示

    this.zoomScale = null;
    this.zoomState = {
      small: {},
      large: {},
    };

    /** 缩放zoom相关 */
    const zoomExtent = [
      [this.layout.left, this.layout.top],
      [this.width - this.layout.right, this.height - this.layout.top],
    ];
    this.zoom = zoom().extent(zoomExtent).on('zoom', this.zoomFunc.bind(this));
  }

  /** 根据当前比例尺，设置一定的缩放比例，不循序无线缩小 */
  getZoomScaleExtend() {
    const domain = this.xScale.domain();
    const leftBoundary = timeYear.offset(domain[0], -1);
    const rightBoundary = timeYear.offset(domain[1], 1);
    const minScale = (domain[1].getTime() - domain[0]) / (rightBoundary.getTime() - leftBoundary.getTime());
    const maxScale = (domain[1].getTime() - domain[0].getTime()) / (1000 * 60 * 60);
    return [minScale, maxScale];
  }

  initXScale() {
    this.timeRange = [new Date(this.schedulingPeriod.start_time), new Date(this.schedulingPeriod.start_time)];
    this.xScale.domain(this.timeRange);
    this.data.forEach(item => {
      if (item.window_type === 'scroll') {
        item.window_size_unit = this.schedulingPeriod.schedule_period;
        item.window_size = this.schedulingPeriod.count_freq;
      }
      const chartData = utils.getChartDataExtend(item, this.schedulingPeriod);

      if (chartData.length) {
        const len = chartData.length - 1;
        this.timeRange[0] = new Date(Math.min(this.timeRange[0].getTime(), chartData[0].start.getTime()));
        this.timeRange[1] = new Date(Math.max(this.timeRange[1].getTime(), chartData[len].end.getTime()));
      }
    });

    this.xScale.domain(this.timeRange);
    const scaleExtend = this.getZoomScaleExtend();
    this.zoom.scaleExtent(scaleExtend);
  }

  updateChart(data, cycleConfig) {
    if (cycleConfig) {
      if (!cycleConfig.count_freq) return;
      this.schedulingPeriod = JSON.parse(JSON.stringify(cycleConfig));
      this.renderStartTime();
    }
    const scale = this.zoomScale || this.xScale;
    this.data = JSON.parse(JSON.stringify(data));
    this.visiableCount = [0, 1, 2, 3, 4];
    // this.initXScale(); // 更新数据时，重新初始化xScale，但是如果手动变动过比例尺(zooomScale),则不会变化
    this.renderMainChart(scale);
    this.renderAxis(scale);
  }

  render(el, data = this.data, schedulingPeriod = this.schedulingPeriod) {
    this.data = JSON.parse(JSON.stringify(data));
    this.schedulingPeriod = JSON.parse(JSON.stringify(schedulingPeriod));
    this.initXScale(); // 根据数据，自动计算出做合适的视窗
    const svg = select(el)
      .append('svg')
      .attr('height', this.height + this.layout.top + this.layout.bottom)
      .attr('width', this.width + this.layout.left + this.layout.right)
      //.attr('viewBox', `0 0 ${this.width + this.layout.left + this.layout.right} ${this.height + this.layout.top + this.layout.bottom}`)
      .attr('class', 'offline-node-chart')
      .style('background', '#252529')
      .call(this.zoom);

    this.canvas = svg.append('g').attr('transform', `translate(${this.layout.left}, ${this.layout.top})`);

    this.canvas.append('g').attr('transform', `translate(0, ${this.layout.secondAxisTop})`).attr('class', 'date-label');

    this.hourAxis = utils.addAxis(this.canvas, this.layout.secondAxisTop);
    this.dayAxis = utils.addAxis(this.canvas, 0);

    this.canvas.append('g').attr('transform', `translate(0, ${this.layout.secondAxisTop})`).attr('class', 'start-time-group');

    this.canvas.append('g').attr('transform', `translate(0, ${this.layout.secondAxisTop})`).attr('class', 'main-chart');

    this.renderAxis();
    this.renderStartTime();
    this.renderMainChart();

    this.isInit = true;
  }

  /** 渲染主图 */
  renderMainChart(scale = this.xScale) {
    const self = this;
    const chartHeight = 35;
    const chartPadding = 16;

    /** chartGroup
     *  根据传入的Data，每个表代表一个Group，每个Group绘制rectChount个rect
     *  selection.each()
     *    对每一个group，根据type做计算和渲染
     */
    const chartGroup = this.canvas
      .select('.main-chart')
      .selectAll('.chart')
      .data(this.data, d => d.name);

    chartGroup
      .enter()
      .append('g')
      .attr('class', 'chart')
      .attr('height', chartHeight)
      .attr('transform', (d, index) => {
        return `translate(0, ${index * chartHeight + chartPadding * (index + 1)})`;
      })
      .merge(chartGroup)
      .each(function (p, j) {
        switch (p.window_type) {
          case 'slide':
            self.renderSlideChart(p, this); // 此处的this，是存在的<g> 实例
            break;
          case 'scroll':
            p.window_size_unit = self.schedulingPeriod.schedule_period;
            p.window_size = self.schedulingPeriod.count_freq;
            self.renderSlideChart(p, this);
            break;
          case 'accumulate':
            self.renderAccChart(p, this);
            break;
          case 'whole':
            self.renderWholeChart(this);
          default:
            break;
        }
      });

    chartGroup.exit().remove();
  }

  /** 渲染全量窗口 */
  renderWholeChart(context) {
    const scale = this.zoomScale || this.xScale;
    const self = this;

    const chartData = [
      {
        start: scale.range()[0],
        end: scale.range()[1],
      },
    ];

    const rect = select(context).selectAll('foreignObject').data(chartData);

    rect
      .enter()
      .append('foreignObject')
      .attr('class', 'whole-task')
      .merge(rect)
      .attr('x', d => d.start)
      .attr('y', (self.rectHeight + 1) * 2)
      .attr('height', self.rectHeight)
      .attr('width', d => d.end - d.start)
      .append('xhtml:div')
      .attr('class', 'innter-div')
      .html('.')
      .style('width', '100%')
      .style('height', '100%')
      .style('border-radius', '2px')
      .style(
        'background',
        `repeating-linear-gradient(
              45deg,
              #935280,
              #935280 6px,
              #67405d 6px,
              #67405d 12px`
      );

    rect.exit().remove();
  }

  renderSlideChart(data, context) {
    const scale = this.zoomScale || this.xScale;
    const rectCountEachGroup = 5;
    const self = this;
    const chartData = utils.getAllSliderChartData(data, this.schedulingPeriod);
    const rect = select(context).selectAll('rect').data(chartData);

    rect
      .enter()
      .append('rect')
      .attr('class', 'scroll-task')
      .merge(rect)
      .attr('x', d => {
        return scale(d.start);
      })
      .attr('y', (d, i) => {
        return (i % rectCountEachGroup) * (self.rectHeight + 1);
        // return Math.abs((i % rectCountEachGroup) - 4) * (self.rectHeight + 1);
      })
      .attr('height', self.rectHeight)
      .attr('width', d => {
        return scale(d.end) - scale(d.start);
      })
      .attr('rx', 2)
      .attr('ry', 2)
      .attr('fill', data.color || 'red')
      .style('display', (d, i) => (self.visiableCount.includes(i) ? 'initial' : 'none'));

    rect.exit().remove();
  }

  updateVisiableCount(chartData) {
    const chartCount = chartData.map(item => item.count);
    const newCount = [];

    this.visiableCount.forEach(count => {
      if (chartCount.includes(count)) {
        newCount.push(count);
      }
    });

    /** 如果数据有变化，重新渲染主图 */
    if (newCount.length !== this.visiableCount.length) {
      this.visiableCount = newCount;
      setTimeout(() => {
        this.renderMainChart();
      }, 0);
    }
  }

  renderAccChart(data, context) {
    const curScale = this.zoomScale || this.xScale;
    const self = this;
    if (!data.window_size) return;
    const chartData = utils.getAccChartData(data, this.schedulingPeriod);
    this.updateVisiableCount(chartData); // 根据累加窗口更新可见调度周期

    const rect = select(context).selectAll('rect').data(chartData);

    rect
      .enter()
      .append('rect')
      .attr('class', 'accumulate-task')
      .merge(rect)
      .attr('x', d => curScale(d.start))
      .attr('y', (d, i) => (i % 5) * (self.rectHeight + 1))
      .attr('height', self.rectHeight)
      .attr('width', d => curScale(d.end) - curScale(d.start))
      .attr('rx', 2)
      .attr('ry', 2)
      .attr('fill', data.color || 'red');

    rect.exit().remove();
  }

  /** 启动时间计算 */
  renderStartTime(zoomScale) {
    if (this.canvas === null) return;
    const scale = zoomScale || this.zoomScale || this.xScale;
    this.startTimeData = utils.getAllStartTime(this.schedulingPeriod.start_time, this.schedulingPeriod); // 根据当前比例尺获取启动周期
    const timelines = this.canvas
      .select('.start-time-group')
      .selectAll('.start-time')
      .data(this.startTimeData, d => d.getTime());

    timelines
      .enter()
      .append('line')
      .attr('y2', this.height - this.layout.secondAxisTop)
      .attr('stroke', '#f3bf4f')
      .attr('class', 'start-time')
      .style('stroke-width', 2)
      .merge(timelines)
      .attr('transform', d => {
        return `translate(${scale(d)}, 0)`;
      });

    timelines.exit().remove();
  }

  renderAxis(scale = this.xScale) {
    this.setZoomState(scale);
    this.renderSmallUnitAxis(scale);
    this.renderLargeUnitAxis(scale);
  }

  /** 小单位的tick生成器 */
  hourTicksGenerator(scale) {
    return axisTop(scale)
      .ticks(this.zoomState.smallTick)
      .tickSize(-(this.height - this.layout.secondAxisTop))
      .tickFormat(this.zoomState.smallTimeFormat);
  }

  /** 大单位的tick生成器 */
  dayTicksGenerator(scale) {
    const self = this;
    return axisTop(scale).ticks(self.zoomState.largeTick).tickSize(-this.height).tickFormat(this.zoomState.largeTimeFormat);
  }

  /** 将label移至Tick的中间
   * @unit 单位，默认为'day'
   * @offsetTop 渲染label的竖向偏移
   */
  translateLableToCenter(selection, unit = 'day', offsetTop = this.layout.firstAxisTop) {
    selection
      .selectAll('.tick text')
      .attr('transform', `translate(${this.timeToPixels(unit, 1) / 2} , ${offsetTop})`)
      .attr('font-size', '12px');
  }

  /** 重置label的位置，因为小单位有时在tick上，有时在中间，因此需要根据实际scale重置位置 */
  resetLabelPosition(selection) {
    selection.selectAll('.tick text').attr('transform', `translate(0, 0)`);
  }

  /** 渲染大单位坐标轴 */
  renderLargeUnitAxis(scale) {
    const renderScale = scale || this.xScale;

    this.dayAxis
      .call(this.dayTicksGenerator(renderScale))
      .call(utils.removeAxisPath)
      .call(selection => this.translateLableToCenter(selection, this.zoomState.largeUnit))
      .call(selection => {
        utils.setLineColor(selection, '#4b4b52');
      });
  }

  /** 渲染小单位坐标轴 */
  renderSmallUnitAxis(scale) {
    const renderScale = scale || this.xScale;

    this.hourAxis
      .call(this.hourTicksGenerator(renderScale))
      .call(utils.removeAxisPath)
      .call(selection => {
        utils.setLineColor(selection, '#38383d');
      })
      .call(selection => selection.selectAll('.tick text').attr('font-size', '12px'))
      .call(selection => {
        if (['day', 'month'].includes(this.zoomState.smallUnit)) {
          this.translateLableToCenter(selection, this.zoomState.smallUnit, 0);
          this.zoomState.smallUnit === 'day' && this.renderDayDate('day', renderScale.domain());
          return;
        }
        this.resetLabelPosition(selection);
        this.renderDayDate('clear');
      });
  }

  /** 在smallAxis为日时，需要添加Date日期的显示 */
  renderDayDate(unit, domain) {
    const scale = this.zoomScale || this.xScale;
    const data = unit === 'clear' ? [] : utils.getVisableTick(unit, domain);

    const dates = this.canvas
      .select('.date-label')
      .selectAll('.date')
      .data(data, d => d.getTime());

    dates
      .enter()
      .append('text')
      .attr('class', 'date')
      .attr('fill', '#63656e')
      .attr('font-size', '8px')
      .merge(dates)
      .text(d => d.getDate())
      .attr('x', d => {
        return scale(d) + this.timeToPixels('day', 1) / 2 + 8;
      })
      .attr('y', -10);

    dates.exit().remove();
  }

  /** zoom函数，需要重新绘制坐标和图像 */
  zoomFunc(event) {
    const { transform } = event;
    // const zoomFactor = transform.k;
    // this.zoom.translateExtent([[this.layout.left, this.layout.top], [this.width - this.layout.right / zoomFactor, this.height - this.layout.top]])  根据放大倍数，设置拖拽限制
    this.zoomScale = transform.rescaleX(this.xScale);

    this.renderAxis(this.zoomScale);
    this.renderStartTime(this.zoomScale);
    this.renderMainChart(this.zoomScale);
  }

  /** 不同缩放条件下，tick的周期和单位都不同，因此需要根据scale设置zoom状态 */
  setZoomState(scale) {
    if (this.timeToPixels('hour', 1, scale) > this.timeSteps.dayStep) {
      this.zoomState = {
        smallTick: timeHour.every(1),
        smallTimeFormat: (domain, number) => {
          const hourFormat = timeFormat('%H:%S');
          return this.timeToPixels('hour', 1, scale) > 70 ? hourFormat(domain) : domain.getHours() % 2 === 0 ? hourFormat(domain) : '';
        },
        smallUnit: 'hour',
        largeTick: timeDay.every(1),
        largeTimeFormat: timeFormat('%Y-%m-%d'),
        largeUnit: 'day',
      };
    } else if (this.timeToPixels('hour', 1, scale) < this.timeSteps.dayStep && this.timeToPixels('day', 1, scale) > this.timeSteps.dayStep) {
      this.zoomState = {
        smallTick: timeDay.every(1),
        smallTimeFormat: domain => {
          const format = timeFormat('%u');
          return utils.toChNum(format(domain));
        },
        smallUnit: 'day',
        largeTick: timeMonday.every(1),
        largeTimeFormat: domain => {
          const headFormat = timeFormat('%Y-%m-%d');
          const endFormat = timeFormat('%m-%d');
          const end = timeDay.offset(domain, 6);
          return `${headFormat(domain)} ~ ${endFormat(end)}`;
        },
        largeUnit: 'week',
      };
    } else if (this.timeToPixels('day', 1, scale) < this.timeSteps.dayStep && this.timeToPixels('week', 1, scale) > this.timeSteps.dayStep) {
      this.zoomState = {
        smallTick: timeMonday.every(1),
        smallTimeFormat: timeFormat('%d'),
        smallUnit: 'week',
        largeTick: timeMonth.every(1),
        largeTimeFormat: domain => {
          return `${domain.getFullYear()}年${utils.toChNum(domain.getMonth() + 1)}月`;
        },
        largeUnit: 'month',
      };
    } else {
      this.zoomState = {
        smallTick: timeMonth.every(1),
        smallTimeFormat: domain => {
          return `${domain.getMonth() + 1}月`;
        },
        smallUnit: 'month',
        largeTick: timeYear.every(1),
        largeTimeFormat: domain => {
          return `${domain.getFullYear()}年`;
        },
        largeUnit: 'year',
      };
    }
  }

  /** 将时间纬度转换为长度，用于可视化渲染和位置计算 */
  timeToPixels(unit, time, scale) {
    let timeUnit;
    switch (unit) {
      case 'hour':
        timeUnit = timeHour;
        break;
      case 'day':
        timeUnit = timeDay;
        break;
      case 'week':
        timeUnit = timeWeek;
        break;
      case 'month':
        timeUnit = timeMonth;
        break;
      case 'year':
        timeUnit = timeYear;
        break;
    }
    const d = new Date();
    const timeScale = scale || this.zoomScale || this.xScale;
    return timeScale(timeUnit.offset(d, time)) - timeScale(d);
  }
}

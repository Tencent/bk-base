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

import Calender from './ChartCalender';
import { IPreviewConfig, SchedulingPeriod, Data } from './interface';
import { select } from 'd3-selection';
import { ScaleTime } from 'd3-scale';
import { D3ZoomEvent } from 'd3-zoom';
import * as utils from './utils';

export default class PreviewChart extends Calender {
  private data: Data[];
  private schedulingPeriod: SchedulingPeriod;
  private visiableCount: number[] = [0, 1, 2, 3, 4];
  private rectHeight: number;
  private startTimeData: Date[] = [];

  constructor(config: IPreviewConfig) {
    super(config);
    this.data = JSON.parse(JSON.stringify(config.data));
    this.schedulingPeriod = JSON.parse(JSON.stringify(config.schedulingPeriod));
    this.rectHeight = config.rectHeight || 6;

  }

  // 根据传入数据，自动计算最合适的比例尺
  initScaleByData() {
    this.timeRange = [new Date(this.schedulingPeriod.start_time), new Date(this.schedulingPeriod.start_time)];
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

  renderChart(data = this.data, schedulingPeriod = this.schedulingPeriod) {
    this.data = JSON.parse(JSON.stringify(data));
    this.schedulingPeriod = JSON.parse(JSON.stringify(schedulingPeriod));
    this.initScaleByData();

    this.renderAxis();
    this.renderStartTime();
    this.renderMainChart();
  }

  updateChart(data: Data, cycleConfig: SchedulingPeriod) {
    if (cycleConfig) {
      if (!cycleConfig.count_freq) return;
      this.schedulingPeriod = JSON.parse(JSON.stringify(cycleConfig));
      this.renderStartTime();
    }
    const scale = this.zoomScale || this.xScale;
    this.data = JSON.parse(JSON.stringify(data));
    this.visiableCount = [0, 1, 2, 3, 4];
    // this.initXScale(); // 更新数据时，重新初始化xScale，但是如果手动变动过比例尺(zooomScale),则不会变化
    this.renderMainChart();
    this.renderAxis(scale);
  }

  zoomFunc(event: D3ZoomEvent<SVGAElement, number>) {
    const { transform } = event;
    this.zoomScale = transform.rescaleX(this.xScale);

    this.renderAxis(this.zoomScale);
    this.renderStartTime(this.zoomScale);
    this.renderMainChart();
  }

  /** 渲染主图 */
  renderMainChart() {
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
      .data(this.data, (d: any) => d.name);

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
  renderWholeChart(context: SVGElement) {
    const scale = this.zoomScale || this.xScale;
    const self = this;

    const chartData = [
      {
        start: scale.range()[0],
        end: scale.range()[1],
      },
    ];

    const rect = select(context)
      .selectAll('foreignObject')
      .data(chartData);

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

  renderSlideChart(data: Data, context: SVGElement) {
    const scale = this.zoomScale || this.xScale;
    const rectCountEachGroup = 5;
    const self = this;
    const chartData = utils.getAllSliderChartData(data, this.schedulingPeriod);
    const rect = select(context)
      .selectAll('rect')
      .data(chartData);


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

  updateVisiableCount(chartData: Data[]) {
    const chartCount = chartData.map((item: any) => item.count);
    const newCount: number[] = [];

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

  renderAccChart(data: Data, context: SVGElement) {
    const curScale = this.zoomScale || this.xScale;
    const self = this;
    if (!data.window_size) return;
    const chartData = utils.getAccChartData(data, this.schedulingPeriod);
    this.updateVisiableCount(chartData); // 根据累加窗口更新可见调度周期

    const rect = select(context)
      .selectAll('rect')
      .data(chartData);

    rect
      .enter()
      .append('rect')
      .attr('class', 'accumulate-task')
      .merge(rect)
      .attr('x', (d: any) => curScale(d.start))
      .attr('y', (d, i) => (i % 5) * (self.rectHeight + 1))
      .attr('height', self.rectHeight)
      .attr('width', (d: any) => curScale(d.end) - curScale(d.start))
      .attr('rx', 2)
      .attr('ry', 2)
      .attr('fill', data.color || 'red');

    rect.exit().remove();
  }

  /** 启动时间计算 */
  renderStartTime(zoomScale?: ScaleTime<number, number>) {
    if (this.canvas === null) return;
    const scale = zoomScale || this.zoomScale || this.xScale;
    // 根据当前比例尺获取启动周期
    this.startTimeData = utils.getAllStartTime(this.schedulingPeriod.start_time, this.schedulingPeriod);
    const timelines = this.canvas
      .select('.start-time-group')
      .selectAll('.start-time')
      .data(this.startTimeData, (d: any) => d.getTime());

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
}

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

import { axisTop } from 'd3-axis';
import { scaleTime, ScaleTime } from 'd3-scale';
import { select, Selection } from 'd3-selection';
import { timeDay, timeHour, timeMonday, timeMonth, timeWeek, timeYear, TimeInterval } from 'd3-time';
import { timeFormat } from 'd3-time-format';
import { zoom, D3ZoomEvent, ZoomBehavior } from 'd3-zoom';
import { IConfig, Layout, SchedulingPeriod } from './interface';
import * as utils from './utils';

export default class Calender {
    public layout: Layout;
    public width: number;
    public height: number;
    public timeRange: Date[];
    public timeSteps: number = 15;
    public isInit: boolean = false;
    protected canvas: Selection<SVGGElement, any, any, any>;
    protected xScale: ScaleTime<number, number>;
    protected hourAxis: Selection<SVGGElement, any, any, any>;
    protected dayAxis: Selection<SVGGElement, any, any, any>;
    protected zoomScale: ScaleTime<number, number>;
    protected zoom: ZoomBehavior<Element, any>;
    protected zoomState: any;
    protected container: string;
    constructor(config: IConfig) {
        const { layout, domain, container } = config;
        this.container = container;
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
        this.timeRange = (domain && domain.length && domain) || this.getDefaultTimeRange();
        this.xScale = scaleTime().range([0, this.width]).domain(this.timeRange);
        this.zoomScale = this.xScale;

        this.zoomState = {
            small: {},
            large: {},
        };

        /** 缩放zoom相关 */
        const zoomExtent: [[number, number], [number, number]] = [
            [this.layout.left, this.layout.top],
            [this.width - this.layout.right, this.height - this.layout.top],
        ];
        this.zoom = zoom().extent(zoomExtent).on('zoom', this.zoomFunc.bind(this));

        this.render();
    }

    getDefaultTimeRange(): Date[] {
        const yesterday = new Date();
        yesterday.setDate(yesterday.getDate() - 1);

        return [yesterday, new Date()];
    }

    public getZoomScaleExtend(): [number, number] {
        const domain = this.xScale.domain();
        const leftBoundary = timeYear.offset(domain[0], -1);
        const rightBoundary = timeYear.offset(domain[1], 1);
        const minScale =
            (domain[1].getTime() - domain[0].getTime()) / (rightBoundary.getTime() - leftBoundary.getTime());
        const maxScale = (domain[1].getTime() - domain[0].getTime()) / (1000 * 60 * 60);
        return [minScale, maxScale];
    }

    public initXScale() {
        this.xScale.domain(this.timeRange);
        const scaleExtend = this.getZoomScaleExtend();
        this.zoom.scaleExtent(scaleExtend);
    }

    public render(el: string = this.container) {
        this.initXScale();
        const svg = select(el)
            .append('svg')
            .attr('height', this.height + this.layout.top + this.layout.bottom)
            .attr('width', this.width + this.layout.left + this.layout.right)
            .attr('class', 'offline-node-chart')
            .style('background', '#252529')
            .call(this.zoom);

        this.canvas = svg.append('g').attr('transform', `translate(${this.layout.left}, ${this.layout.top})`);

        this.canvas
            .append('g')
            .attr('transform', `translate(0, ${this.layout.secondAxisTop})`)
            .attr('class', 'date-label');

        this.hourAxis = utils.addAxis(this.canvas, this.layout.secondAxisTop);
        this.dayAxis = utils.addAxis(this.canvas, 0);

        this.canvas
            .append('g')
            .attr('transform', `translate(0, ${this.layout.secondAxisTop})`)
            .attr('class', 'start-time-group');

        this.canvas
            .append('g')
            .attr('transform', `translate(0, ${this.layout.secondAxisTop})`)
            .attr('class', 'main-chart');

        this.renderAxis();
        this.isInit = true;
    }

    public renderAxis(scale: ScaleTime<number, number> = this.xScale) {
        this.setZoomState(scale);
        this.renderSmallUnitAxis(scale);
        this.renderLargeUnitAxis(scale);
    }

    /** 小单位的tick生成器 */
    public hourTicksGenerator(scale: ScaleTime<number, number>) {
        return axisTop(scale)
            .ticks(this.zoomState.smallTick)
            .tickSize(-(this.height - this.layout.secondAxisTop))
            .tickFormat(this.zoomState.smallTimeFormat);
    }

    /** 大单位的tick生成器 */
    public dayTicksGenerator(scale: ScaleTime<number, number>) {
        const self = this;
        return axisTop(scale)
            .ticks(self.zoomState.largeTick)
            .tickSize(-this.height)
            .tickFormat(this.zoomState.largeTimeFormat);
    }

    /** 将label移至Tick的中间
     * @unit 单位，默认为'day'
     * @offsetTop 渲染label的竖向偏移
     */
    public translateLableToCenter(selection: Selection<SVGGElement, any, any, any>, unit: string = 'day', offsetTop: number = this.layout.firstAxisTop) {
        selection
            .selectAll('.tick text')
            .attr('transform', `translate(${this.timeToPixels(unit, 1) / 2} , ${offsetTop})`)
            .attr('font-size', '12px');
    }

    /** 重置label的位置，因为小单位有时在tick上，有时在中间，因此需要根据实际scale重置位置 */
    public resetLabelPosition(selection: Selection<SVGGElement, any, any, any>) {
        selection.selectAll('.tick text').attr('transform', `translate(0, 0)`);
    }

    /** 渲染大单位坐标轴 */
    public renderLargeUnitAxis(scale: ScaleTime<number, number>) {
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
    public renderSmallUnitAxis(scale: ScaleTime<number, number>) {
        const renderScale = scale || this.xScale;

        this.hourAxis
            .call(this.hourTicksGenerator(renderScale))
            .call(utils.removeAxisPath)
            .call((selection: Selection<SVGGElement, any, any, any>) => {
                utils.setLineColor(selection, '#38383d');
            })
            .call((selection: Selection<SVGGElement, any, any, any>) =>
                selection.selectAll('.tick text').attr('font-size', '12px')
            )
            .call((selection: Selection<SVGGElement, any, any, any>) => {
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
    public renderDayDate(unit: string, domain?: Date[]) {
        const scale = this.zoomScale || this.xScale;
        const data = unit === 'clear' ? [] : utils.getVisableTick(unit, domain);

        const dates = this.canvas
            .select('.date-label')
            .selectAll('.date')
            .data(data, (d: Date) => d.getTime());

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
    public zoomFunc(event: D3ZoomEvent<SVGAElement, number>) {
        const { transform } = event;
        // const zoomFactor = transform.k;
        // this.zoom.translateExtent([[this.layout.left, this.layout.top], [this.width - this.layout.right / zoomFactor, this.height - this.layout.top]])  根据放大倍数，设置拖拽限制
        this.zoomScale = transform.rescaleX(this.xScale);

        this.renderAxis(this.zoomScale);
    }

    /** 不同缩放条件下，tick的周期和单位都不同，因此需要根据scale设置zoom状态 */
    public setZoomState(scale: ScaleTime<number, number>) {
        if (this.timeToPixels('hour', 1, scale) > this.timeSteps) {
            this.zoomState = {
                smallTick: timeHour.every(1),
                smallTimeFormat: (domain: Date) => {
                    const hourFormat = timeFormat('%H:%S');
                    return this.timeToPixels('hour', 1, scale) > 70
                        ? hourFormat(domain)
                        : domain.getHours() % 2 === 0
                            ? hourFormat(domain)
                            : '';
                },
                smallUnit: 'hour',
                largeTick: timeDay.every(1),
                largeTimeFormat: timeFormat('%Y-%m-%d'),
                largeUnit: 'day',
            };
        } else if (
            this.timeToPixels('hour', 1, scale) < this.timeSteps &&
            this.timeToPixels('day', 1, scale) > this.timeSteps
        ) {
            this.zoomState = {
                smallTick: timeDay.every(1),
                smallTimeFormat: (domain: Date) => {
                    const format = timeFormat('%u');
                    return utils.toChNum(format(domain));
                },
                smallUnit: 'day',
                largeTick: timeMonday.every(1),
                largeTimeFormat: (domain: Date) => {
                    const headFormat = timeFormat('%Y-%m-%d');
                    const endFormat = timeFormat('%m-%d');
                    const end = timeDay.offset(domain, 6);
                    return `${headFormat(domain)} ~ ${endFormat(end)}`;
                },
                largeUnit: 'week',
            };
        } else if (
            this.timeToPixels('day', 1, scale) < this.timeSteps &&
            this.timeToPixels('week', 1, scale) > this.timeSteps
        ) {
            this.zoomState = {
                smallTick: timeMonday.every(1),
                smallTimeFormat: timeFormat('%d'),
                smallUnit: 'week',
                largeTick: timeMonth.every(1),
                largeTimeFormat: (domain: Date) => {
                    return `${domain.getFullYear()}年${utils.toChNum(domain.getMonth() + 1)}月`;
                },
                largeUnit: 'month',
            };
        } else {
            this.zoomState = {
                smallTick: timeMonth.every(1),
                smallTimeFormat: (domain: Date) => {
                    return `${domain.getMonth() + 1}月`;
                },
                smallUnit: 'month',
                largeTick: timeYear.every(1),
                largeTimeFormat: (domain: Date) => {
                    return `${domain.getFullYear()}年`;
                },
                largeUnit: 'year',
            };
        }
    }

    /** 将时间纬度转换为长度，用于可视化渲染和位置计算 */
    public timeToPixels(unit: string, time: number, scale?: ScaleTime<number, number>) {
        let timeUnit: TimeInterval = timeHour;
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

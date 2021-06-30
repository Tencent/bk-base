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

import { QueryableWorker } from '@/common/js/QueryableWorker.js';
import { Component, Prop, Vue, Watch } from 'vue-property-decorator';
import BKChart from './BKCharts';
@Component({})
export default class BaseSettings extends Vue {
  /** 宽度 */
  @Prop({ default: null }) public width?: string;

  /** 高度 */
  @Prop({ default: null }) public height?: string;

  /** 轴上数据显示标题 */
  @Prop() public labels?: any[];

  /** 具体数据 */
  @Prop() public series?: any[];

  /** 标题 */
  @Prop() public title: any;

  /** 图例位置 */
  @Prop({ default: () => ({ position: 'top' }) }) public legend: any;

  /** 是否启用动画 */
  @Prop() public animation: any;

  /** 是否自适应尺寸 */
  @Prop({ default: true }) public responsive?: boolean;

  /** 是否保持比例缩放 */
  @Prop({ default: false }) public maintainAspectRatio?: boolean;

  /** 鼠标滑过弹出内容设置 */
  @Prop({ default: () => ({}) }) public tooltips: any;

  /** 配置项 */
  @Prop({ default: () => ({}) }) public options: any;

  /** 图表类型 */
  @Prop({ default: '' }) public chartType?: string;

  /** 渲染数据 */
  @Prop({ default: () => ({}) }) public data: any;

  /** 是否启用work，性能优化 */
  @Prop({ default: false }) public webwork?: boolean;

  /** 当配置项|Datasets改变时自动更新图表 */
  @Prop({ default: true }) public autoUpdate?: boolean;

  /** 图表配色 */
  public chartColors: any = {
    red: 'rgb(255, 99, 132)',
    orange: 'rgb(255, 159, 64)',
    yellow: 'rgb(255, 205, 86)',
    green: 'rgb(75, 192, 192)',
    blue: 'rgb(54, 162, 235)',
    purple: 'rgb(153, 102, 255)',
    grey: 'rgb(201, 203, 207)',
  };

  /** 容器组件节点 */
  public containerNode?: HTMLElement = undefined;

  /** 容器组件节点 Canvas */
  public container?: HTMLElement | null = undefined;

  /** canvas 内容 */
  public context: any = null;

  /** Chartjs实例 */
  public instance: any = null;

  /** 图表类型 */
  public type = '';

  /** 扩展组件 */
  public plugins: any = {};

  public localWidth: any = '';

  public worker?: QueryableWorker = undefined;

  public offscreenCanvas: any = null;

  public resizeAfterWidthChanged() {
    this.instance.resize();
  }

  public update(datasets: any, options: any, force: boolean) {
    /** 禁止自动更新 */
    if (!this.autoUpdate && !force) {
      return;
    }

    if (!this.webwork) {
      this.instance.data.datasets = datasets || [];
      this.instance.data.labels = this.labels || [];
      if (this.data.xLabels) {
        this.instance.data.xLabels = this.data.xLabels;
      }

      if (this.data.yLabels) {
        this.instance.data.yLabels = this.data.yLabels;
      }

      this.instance.options = Object.assign({}, options || {}, this.options);
      this.instance.update();
      this.$emit('update', this.instance);
    } else {
      const instance = this.instance;
      const origin = {
        datasets: datasets || [],
        labels: this.labels || [],
        data: this.data,
        options: Object.assign({}, options || {}, this.options),
      };
      const config = JSON.parse(JSON.stringify(origin));
      this.worker?.sendQuery('update', [instance, config], [this.offscreenCanvas]);
    }
  }

  public init(node?: HTMLElement) {
    if (node) {
      this.containerNode = node;
    }

    this.container = this.containerNode?.querySelector('canvas');
    if (!this.container) {
      this.container = document.createElement('canvas');
    }

    if (this.webwork) {
      if (!this.worker) {
        this.worker = new QueryableWorker('./static/dist/web_worker/bundle.worker.bkchartjs.js');
      }

      this.worker.addListeners('initComplete', (event: any) => {
        this.instance = event?.data.target;
        this.$emit('init', this.instance);
      });

      this.worker.addListeners('updateComplete', (event: any) => {
        this.instance = event?.data.target;
        this.$emit('init', this.instance);
      });
    } else {
      this.getCalcWidth();

      if (this.width) {
        this.container.style.width = this.localWidth;
      }

      if (this.height) {
        this.container.style.height = this.height + 'px';
      }

      this.container.setAttribute('width', this.localWidth.replace(/(px|%|vm|vh|rem|em)/, ''));
      this.container.setAttribute('height', `${this.height}`);
      this.containerNode?.append(this.container);
      this.context = (this.container as HTMLCanvasElement).getContext('2d');
    }
  }

  public renderByWebworker(config: any) {
    const canvas = document.createElement('canvas');
    const offscreenCanvas = canvas.transferControlToOffscreen();
    this.offscreenCanvas = offscreenCanvas;
    const mConfig = JSON.parse(JSON.stringify(config));
    this.worker?.sendQuery('init', [offscreenCanvas, mConfig], [offscreenCanvas]);
  }

  public getCalcWidth() {
    this.localWidth = this.width || '100%';
    if (/^\d+%$/.test(`${this.width}`)) {
      const rect = this.$el.getBoundingClientRect();
      const percent = Number(`${this.width}`.replace(/%$/, '')) / 100;
      this.localWidth = rect.width * percent + 'px';
    }

    return this.localWidth || '100%';
  }

  get BKChart() {
    return BKChart;
  }

  get color() {
    return this.BKChart.helpers.color;
  }

  public destroyInstance() {
    if (this.instance) {
      this.instance.clear();
      this.instance.destroy();
      this.instance = undefined;
      this.container = undefined;
      this.context = undefined;
      this.containerNode = undefined;
    }

    if (this.worker) {
      this.worker.terminate();
      this.worker = undefined;
    }
  }

  public beforeDestroy() {
    this.destroyInstance();
  }
}

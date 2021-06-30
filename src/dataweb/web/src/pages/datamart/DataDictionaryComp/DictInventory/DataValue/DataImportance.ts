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

import Chart from '@/components/bkChartVue/Components/BaseChart.vue';
import PlotlyChart from '@/components/plotly/index.vue';
import {
  getBizImportanceDistribution,
  getImportanceDistribution,
  getScoreDistribution,
} from '@/pages/datamart/Api/DataInventory';
import HeaderType from '@/pages/datamart/common/components/HeaderType';
import { chartColors, sensitiveObj } from '@/pages/datamart/common/config';
import { tranNumber } from '@/pages/datamart/common/utils.js';
import NoData from '@/pages/datamart/DataDict/components/children/chartComponents/NoData.vue';
// 数据接口
import { IImportanceDistribution } from '@/pages/datamart/InterFace/DataInventory';
import { Component, Inject } from 'vue-property-decorator';
import ChartPart from './components/ChartPart';
import PieWidthTable from './components/PieWithTable';
import bubbleChartOption from './mixin/mixin';

/**
 * 数据重要度图表相关配置
 * @description
 *
 * 此图是数据重要度相关图表的配置和文件，包括：
 *  数据重要度评分
 *  数据重要度等级分布
 *  影响数据重要度的各项指标分布
 *  影响数据重要度的各项指标分布
 */

interface IndexData {
  label: string;
  value: string;
}

@Component({
  components: {
    HeaderType,
    Chart,
    ChartPart,
    PieWidthTable,
    PlotlyChart,
    NoData,
  },
})
export default class DataHeat extends bubbleChartOption {
  @Inject()
  public getParams!: Function;

  public isRequested = false;

  public loading = {
    importanceLoading: true, //数据重要度评分分布图
    importanceLevelLoading: true,
    activeLoading: true,
    sensitivityLoading: true,
    isBipLoading: true,
    heatMapLoading: true,
  };

  // 图表布局配置
  public heatMapLayout = {
    title: {
      font: {
        size: 14,
      },
    },
    annotations: [],
    yaxis: {
      // ticks: '',
      ticksuffix: ' ',
      width: 700,
      height: 700,
      autosize: false,
    },
    margin: {
      l: 100,
      t: 20,
      r: 5,
    },
    height: 360,
    // width: 510
  };

  // 热力图默认数据
  public heatMapData = [
    {
      x: [],
      y: [],
      z: [],
      type: 'heatmap',
      colorscale: '',
      showscale: false,
    },
  ];

  public importanceChartData = {
    labels: [],
    datasets: [
      {
        data: [],
      },
    ],
  };

  public generateTypeData: IndexData[] = [];
  public sensitivityData = {
    isLoading: false,
    labels: [],
    datasets: [
      {
        data: [],
      },
    ],
  };
  public activeData = {
    isLoading: false,
    labels: [],
    datasets: [
      {
        data: [],
      },
    ],
  };
  public isBipData: IndexData[] = [];
  public appImportantLevelNameData = {
    isLoading: false,
    labels: [],
    datasets: [
      {
        data: [],
      },
    ],
  };

  public importanceChartOptions = {};

  public pieOption = {
    plugins: {
      legend: {
        display: true,
        position: 'right',
        align: 'end',
      },
    },
  };

  public pieWidth = '250px';

  public pieHeight = '135px';

  public initData() {
    this.isRequested = false;
  }

  public getData() {
    if (this.isRequested) { // getData方法只需执行一次
      return;
    }
    this.getScoreDistribution();
    this.getImportanceDistribution(
      'app_important_level_name',
      'appImportantLevelNameData',
      this.getAppImportantLevelData,
      'importanceLevelLoading'
    );
    this.getImportanceDistribution('active', 'activeData', this.getIsBipData, 'activeLoading');
    this.getImportanceDistribution('sensitivity', 'sensitivityData', this.getSensitivityData, 'sensitivityLoading');
    this.getBizImportanceDistribution();
    this.$nextTick(() => {
      this.$refs.importanceDistributed.initData();
    });
    this.isRequested = true;
  }

  // 获取饼图配置
  public getPieTitleOption(text: string, pieData: any, type: string) {
    const tipsCbMap = {
      proj: {
        beforeLabel(tooltipItem: any) {
          return `${pieData.labels[tooltipItem.dataIndex]}`;
        },
        label(tooltipItem: any) {
          return `数据个数：${pieData.dataset_count[tooltipItem.dataIndex]}`;
        },
        afterLabel(tooltipItem: any) {
          return `项目个数：${pieData.project_count[tooltipItem.dataIndex]}`;
        },
      },
      biz: {
        beforeLabel(tooltipItem: any) {
          return `${pieData.labels[tooltipItem.dataIndex]}`;
        },
        label(tooltipItem: any) {
          return `数据个数：${pieData.dataset_count[tooltipItem.dataIndex]}`;
        },
        afterLabel(tooltipItem: any) {
          return `业务个数：${pieData.biz_count[tooltipItem.dataIndex]}`;
        },
      },
      sensitive: {
        label(tooltipItem: any) {
          const { dataIndex } = tooltipItem;
          return `${pieData.labels[dataIndex]}：${pieData.datasets[0].data[dataIndex]}`;
        },
      },
    };
    const paddingMap = {
      proj: 20,
      biz: 40,
      sensitive: 20,
    };
    return Object.assign({}, this.pieOption, {
      responsive: true,
      animation: false,
      maintainAspectRatio: false,
      plugins: {
        title: {
          display: false,
          text,
        },
        legend: {
          display: true,
          position: 'right',
        },
        tooltip: {
          callbacks: tipsCbMap[type],
          // backgroundColor: '#FFF',
          titleFontSize: 16,
          titleFontColor: '#0066ff',
          bodyFontColor: 'white',
          bodyFontSize: 12,
          displayColors: false,
        },
      },
      labels: {
        fontSize: 12,
      },
      layout: {
        padding: {
          top: 20,
          left: -10,
          // right: paddingMap[type],
        },
      },
    });
  }

  public getBizImportanceDistribution() {
    this.loading.heatMapLoading = true;
    this.heatMapData = [
      {
        x: [],
        y: [],
        z: [],
        type: 'heatmap',
        colorscale: '',
        showscale: false,
      },
    ];
    getBizImportanceDistribution(this.getParams()).then(res => {
      if (res.result && res.data) {
        const xValues = res.data.oper_state_name;
        const yValues = res.data.bip_grade_name;
        const zValues = res.data.dataset_count.map(item => tranNumber(item, 1));
        this.heatMapData = [
          {
            x: xValues,
            y: yValues,
            z: res.data.dataset_count,
            text: res.data.biz_count.map((item, index) => {
              const hoverText = `${$t('运营状态')}：${xValues[index]}`
                                + `<br>${$t('业务星级')}：${yValues[index]}</br>`
                                + `${$t('数据个数')}：${res.data.dataset_count[index]}</br>`
                                + `${$t('业务个数')}: ${item}`;
              return hoverText;
            }),
            hoverinfo: 'text',
            type: 'heatmap',
            // colorscale: 'Portland',
            colorscale: [
              [0, 'rgba(12,54,133, 1)'],
              [0.4, 'rgba(10,120,175, 1)'],
              [0.8, 'rgba(242,186,56, 1)'],
              [1, 'rgba(219,37,32, 1)'],
            ],
            showscale: true,
            colorbar: {
              outlinewidth: 0,
              showticklabels: false,
            },
          },
        ];
        this.heatMapLayout.annotations = [];
        for (let j = 0; j < xValues.length; j++) {
          this.heatMapLayout.annotations.push({
            xref: 'x1',
            yref: 'y1',
            x: xValues[j],
            y: yValues[j],
            text: zValues[j],
            font: {
              size: 12,
              color: 'white',
            },
            showarrow: false,
          });
        }
      }
      this.loading.heatMapLoading = false;
    });
  }

  public getScoreDistribution() {
    this.loading.importanceLoading = true;
    this.importanceChartData = {
      labels: [],
      datasets: [
        {
          data: [],
        },
      ],
    };
    getScoreDistribution('importance', this.getParams()).then(res => {
      if (res.result && res.data) {
        this.$refs.importantRate.initChart(
          this.getChartData(res),
          this.getBubbleOption(
            res,
            this.$t('重要度评分'),
            this.$t('数据个数'),
            this.$t('重要度评分（100分制）'),
            this.$t('数据累计占比（%）')
          )
        );
      }
      this.loading.importanceLoading = false;
    });
  }

  public getAppImportantLevelData(res: IImportanceDistribution) {
    return {
      labels: res.data.metric,
      dataset_count: res.data.dataset_count,
      biz_count: res.data.biz_count,
      datasets: [
        {
          data: res.data.dataset_count,
          backgroundColor: chartColors.slice(0, res.data.metric.length),
        },
      ],
    };
  }

  public getIsBipData(res: IImportanceDistribution) {
    return {
      labels: res.data.metric.map(item => {
        return item ? this.$t('正常运营') : this.$t('已下线');
      }),
      dataset_count: res.data.dataset_count,
      project_count: res.data.project_count,
      datasets: [
        {
          data: res.data.dataset_count,
          backgroundColor: chartColors.slice(0, res.data.metric.length),
        },
      ],
    };
  }

  public getSensitivityData(res: IImportanceDistribution) {
    return {
      labels: res.data.metric.map(item => sensitiveObj[item]),
      datasets: [
        {
          data: res.data.dataset_count,
          backgroundColor: chartColors.slice(0, res.data.metric.length),
        },
      ],
    };
  }

  public getImportanceDistribution(metric_type: string, dataName: string, handleFunc: Function, loadingName: string) {
    this[dataName] = {
      isLoading: true,
      labels: [],
      datasets: [
        {
          data: [],
        },
      ],
    };
    getImportanceDistribution(
      Object.assign({}, this.getParams(), {
        metric_type,
      })
    ).then(res => {
      if (res.result && res.data) {
        this[dataName] = handleFunc(res);
        this.loading[loadingName] = false;
      }
      this[dataName].isLoading = false;
    });
  }
}

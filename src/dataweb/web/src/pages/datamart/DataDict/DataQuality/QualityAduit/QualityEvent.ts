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

import { Component, Vue } from 'vue-property-decorator';
import Mixin from './Mixin';
import HeaderType from '@/pages/datamart/common/components/HeaderType.vue';
import DataTable from '@/pages/datamart/common/components/DataTable.vue';
import chart from '@/components/bkChartVue/Components/BaseChart.vue';
import QualityIndexChart from './components/QualityIndexChart';
// api请求函数
import { queryQualityEventInfo, queryQualityEventTableData, getNotifyWayList } from '@/pages/datamart/Api/DataQuality';
// 数据接口
import {
  IQualityEventInfo,
  IQualityEventTableData,
  INotifyWayList,
  INotifyWayData,
  IEventData,
  IData,
} from '@/pages/datamart/InterFace/DataQuality';
import { BKHttpResponse } from '@/common/js/BKHttpResponse';
import { chartColors } from '@/pages/datamart/common/config';
import NoData from '@/pages/datamart/DataDict/components/children/chartComponents/NoData.vue';
import moment from 'moment';
import { showMsg } from '@/common/js/util.js';
import scrollBottom from '@/common/directive/scroll-bottom.js';
import operationGroup from '@/bkdata-ui/components/operationGroup/operationGroup.vue';

interface ChartData {
  labels: Array<string>;
  datasets: Array<any>;
}
@Component({
  directives: {
    scrollBottom,
  },
  mixins: [Mixin],
  components: {
    HeaderType,
    chart,
    DataTable,
    QualityIndexChart,
    NoData,
    operationGroup,
  },
})
export default class QualityEvent extends Vue {
  isShowIndex = false;
  isChartLoading = true;
  isPieLoading = true;

  isShowExpandTable = false;

  isTableLoading = true;

  ruleId = '';

  eventInstances: IData[] = [];

  eventPolarityMap = {
    negative: $t('负面'),
    positive: $t('正面'),
    neutral: $t('中性'),
  };

  eventSensitivityMap = {
    public: this.$t('公开'),
    private: this.$t('业务私有'),
    confidential: this.$t('业务机密'),
    topsecret: this.$t('业务绝密'),
  };

  notifyWays: Array<INotifyWayData> = [
    {
      description: '',
      notifyWayAlias: '',
      active: false,
      notifyWay: '',
      notifyWayName: '',
      icon: '',
    },
  ];

  totalData: IData[] = [];

  chartTotalData: IEventData;

  chartData = {
    labels: ['无效数据', '自定义事件1', '数据中断', '数据丢失'],
    datasets: [
      {
        label: 'Data One',
        backgroundColor: ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728'],
        data: [210, 120, 120, 210],
      },
    ],
  };
  searchContent = '';

  pieData: ChartData = {
    labels: [],
    datasets: [],
  };

  barData: ChartData = {
    labels: [],
    datasets: [],
  };

  barChartOptions = {
    title: {
      display: true,
      text: $t('事件发生的个数'),
      fontSize: 14,
      padding: 30,
    },
    scales: {
      yAxes: {
        stacked: true,
        gridLines: {
          display: true,
          color: 'rgba(255,99,132,0.2)',
        },
      },
      xAxes: {
        stacked: true,
        gridLines: {
          display: false,
        },
      },
    },
  };

  config: object = {};

  layout: object = {};

  timeRange: number[] = [];

  calcPageSize = 10;

  expendPageSize = 10;

  get timestampRange() {
    if (!this.timeRange.every(item => item)) {
      return [];
    } else {
      return this.timeRange.map(item => {
        return Math.floor(new Date(item).getTime() / 1000);
      });
    }
  }

  get tableData() {
    if (!this.totalData.length) return [];
    if (!this.searchContent) return this.totalData;
    // 事件名称、事件描述、规则名称、规则内容
    const params: string[] = ['eventAlias', 'description', 'rule.ruleName', 'rule.ruleConfigAlias'];
    return this.totalData.filter(child => {
      return params.some(item => {
        let target: any;
        if (item.includes('.')) {
          target = JSON.parse(JSON.stringify(child));
          item.split('.').forEach(i => {
            target = target[i];
          });
        } else {
          target = child[item];
        }
        return target.includes(this.searchContent);
      });
    });
  }

  mounted() {
    // 设置timeRange为最近的24小时区间
    const endTime = Date.parse(String(new Date())) / 1000;
    const startTime = endTime - 24 * 60 * 60; // 24小时
    this.timeRange = [startTime * 1000, endTime * 1000];

    this.queryQualityEventInfo(startTime, endTime);
    this.queryQualityEventTableData();
    this.getNotifyWayList();
  }

  editRuleConfig(data: IData) {
    this.$emit('editRuleConfig', data.eventId);
  }

  openDialog(data: IData) {
    this.isShowExpandTable = true;
    this.eventInstances = data.eventInstances;
  }

  hiddenDialog() {
    this.isShowExpandTable = false;
    this.eventInstances = [];
  }

  lookForIndex(data: IData) {
    this.isShowIndex = true;
    this.ruleId = data.rule.ruleId;
    this.$nextTick(() => {
      this.$refs.qualityIndexChart.dialogSetting.isShow = true;
    });
  }

  handleTimePick() {
    if (this.timeRange.some(item => !item)) {
      showMsg('请选择时间范围', 'warning', { delay: 2500 });
    } else {
      this.queryQualityEventInfo(...this.timestampRange);
    }
  }

  getPieData() {
    if (!Object.keys(this.chartTotalData.summary).length) return;

    const labels: Array<string> = [];
    const data: Array<number> = [];
    Object.values(this.chartTotalData.summary).forEach(item => {
      labels.push(item.alias);
      data.push(item.count);
    });
    this.pieData = {
      labels,
      datasets: [
        {
          backgroundColor: chartColors.slice(0, 4),
          data,
        },
      ],
    };
  }

  getBarData() {
    if (!Object.keys(this.chartTotalData.trend).length) return;

    this.barData.datasets = [];
    Object.values(this.chartTotalData.trend).forEach((item, index) => {
      if (!this.barData.labels.length) {
        this.barData.labels = item.times.map(child => moment(child).format('HH:mm'));
      }
      this.barData.datasets.push({
        type: 'bar',
        label: item.alias,
        backgroundColor: chartColors[index],
        data: item.count,
      });
    });
  }

  getNotifyIcon(data: Array<string>) {
    const resultArr: Array<object> = [];
    data.forEach(item => {
      const target = this.notifyWays.find(child => child.notifyWay === item);
      if (target) {
        resultArr.push({
          src: `data:image/png;base64,${target.icon}`,
          title: target.notifyWay,
        });
      }
    });
    return resultArr;
  }

  // 获取质量事件相关图表数据
  queryQualityEventInfo(start_time?: number, end_time?: number) {
    queryQualityEventInfo(this.DataId, start_time, end_time).then(res => {
      const instance = new BKHttpResponse<IQualityEventInfo>(res);
      instance.setData(this, 'chartTotalData');

      this.isChartLoading = false;
      if (!this.chartTotalData) return;
      this.getPieData();
      this.getBarData();
    });
  }

  // 获取质量事件表格数据
  queryQualityEventTableData() {
    queryQualityEventTableData(this.DataId).then(res => {
      const instance = new BKHttpResponse<IQualityEventTableData>(res);
      instance.setData(this, 'totalData');
      this.totalData.forEach(item => {
        this.$set(item, 'expandTable', item.eventInstances.slice(0, 11));
        this.$set(item, 'isExpand', false);
      });
      this.isTableLoading = false;
    });
  }

  // 获取数据平台支持的所有告警通知方式
  getNotifyWayList() {
    getNotifyWayList().then(res => {
      const instance = new BKHttpResponse<INotifyWayList>(res);
      instance.setData(this, 'notifyWays');
      // this.isTableLoading = false
    });
  }
}

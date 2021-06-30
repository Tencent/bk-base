

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
  <div>
    <HeaderType :title="title">
      <section class="container">
        <div class="data-delay flex-grow">
          <div>
            <Chart
              :isLoading="leftChartConfig.isLoading"
              :chartData="leftChartConfig.chartData"
              :chartConfig="config"
              :chartLayout="chartLayout"
              :isShowZero="true"
              :title="leftChartTitle"
              :titleTips="leftChartTips"
              :tableHead="leftTipsTableHead"
              :no-data-text="leftChartConfig.noDataText"
              :no-data-height="noDataHeight"
              :tableRateDes="leftTipsTableRateDes" />
          </div>
          <RecentDataTable v-bkloading="{ isLoading: leftTableConfig.isLoading }"
            :data="leftTableConfig.tableData"
            :tableHead="leftTableHead"
            :tableTime="leftChartConfig.tableTime"
            :tableRateDes="leftTableRateDes"
            :isFixed="true">
            <template slot="primary-index">
              <th>
                {{ leftTableHead }}
                <span v-bk-tooltips="`当日${leftChartConfig.tableTime}的${leftTableHead}`"
                  class="icon-exclamation-circle" />
              </th>
            </template>
          </RecentDataTable>
        </div>
        <div class="process-delay flex-grow">
          <div>
            <Chart
              :isLoading="rightChartConfig.isLoading"
              :chartData="rightChartConfig.chartData"
              :chartConfig="config"
              :chartLayout="chartLayout"
              :title="rightChartTitle"
              :titleTips="rightChartTips"
              :isShowZero="true"
              :tableHead="rightTipsTableHead"
              :no-data-text="rightChartConfig.noDataText"
              :no-data-height="noDataHeight"
              :tableRateDes="rightTipsTableRateDes" />
          </div>
          <RecentDataTable v-bkloading="{ isLoading: rightTableConfig.isLoading }"
            :data="rightTableConfig.tableData"
            :tableHead="rightTableHead"
            :tableTime="rightChartConfig.tableTime"
            :tableRateDes="rightTableRateDes"
            :isFixed="true">
            <template slot="primary-index">
              <th>
                {{ rightTableHead }}
                <span v-bk-tooltips="`当日${rightChartConfig.tableTime}的${rightTableHead}`"
                  class="icon-exclamation-circle" />
              </th>
            </template>
          </RecentDataTable>
        </div>
      </section>
    </HeaderType>
  </div>
</template>

<script>
import dataQualityMixins from '../../common/mixins.js';
import HeaderType from '@/pages/datamart/common/components/HeaderType';
import Chart from '@/pages/datamart/common/components/Chart';
import RecentDataTable from '@/pages/datamart/common/components/RecentDataTable.vue';

const qualityFieldObj = {
  data_time_delay: {
    field: 'dataDelay',
    noDataText: $t('数据延迟'),
  },
  process_time_delay: {
    field: 'dataProcessDelay',
    noDataText: $t('处理延迟'),
  },
  loss_count: {
    field: 'dataLoss',
    noDataText: $t('数据丢失'),
  },
  drop_count: {
    field: 'dataDrop',
    noDataText: $t('无效数据'),
  },
};

export default {
  components: {
    HeaderType,
    Chart,
    RecentDataTable,
  },
  mixins: [dataQualityMixins],
  props: {
    title: String,
    leftApiParams: String,
    rightApiParams: String,
    leftChartTitle: String,
    rightChartTitle: String,
    leftChartTips: String,
    rightChartTips: String,
    leftTableHead: String,
    rightTableHead: String,
    rightTableRateDes: String,
    leftTableRateDes: String,
    leftTipsTableRateDes: String,
    rightTipsTableRateDes: String,
    rightTipsTableHead: String,
    leftTipsTableHead: String,
  },
  data() {
    return {
      config: {},
      timer: [],
      leftChartConfig: {
        isLoading: false,
        chartData: [],
        noDataText: '',
        tableTime: '',
      },
      rightChartConfig: {
        isLoading: false,
        chartData: [],
        noDataText: '',
        tableTime: '',
      },
      leftTableConfig: {
        isLoading: false,
        tableData: [],
      },
      rightTableConfig: {
        isLoading: false,
        tableData: [],
      },
    };
  },
  computed: {
    chartLayout() {
      return Object.assign({}, this.layout, {
        xaxis: {
          dtick: 80,
        },
      });
    },
    noDataHeight() {
      // 两个图表都没有数据时，需要把nodata组件高度设置得小一些
      return !this.leftChartConfig.chartData.length && !this.rightChartConfig.chartData.length ? '200px' : '400px';
    },
  },
  mounted() {
    this.init();
  },
  beforeDestroy() {
    this.cancelRefresh();
  },
  methods: {
    init() {
      this.getChartData('leftChartData');
      this.getChartData('rightChartData');
      this.getTableData('leftTableData');
      this.getTableData('rightTableData');
    },
    cancelRefresh() {
      this.timer.forEach(item => {
        clearTimeout(item);
      });
    },
    getApiParams(type) {
      const params = {
        leftTableData: this.leftApiParams,
        rightTableData: this.rightApiParams,
        leftChartData: this.leftApiParams,
        rightChartData: this.rightApiParams,
      };
      return params[type];
    },
    pollingInit(timeout) {
      this.getChartData('leftChartData', true, timeout);
      this.getChartData('rightChartData', true, timeout);
      this.getTableData('leftTableData', true, timeout);
      this.getTableData('rightTableData', true, timeout);
    },
    pollingData(type, timeout) {
      const pullAjax = {
        leftTableData: this.getTableData,
        rightTableData: this.getTableData,
        leftChartData: this.getChartData,
        rightChartData: this.getChartData,
      };
      this.timer.push(
        setTimeout(() => {
          pullAjax[type](type, true, timeout);
        }, timeout)
      );
    },
    getNodataText(field) {
      return `${this.formateTime(
        new Date(new Date().toLocaleDateString())
          .getTime() / 1000, 'YYYY-MM-DD HH:mm:ss')} ~ 当前，
                ${qualityFieldObj[field].noDataText}${this.$t('为')}0`;
    },
    getChartData(type, isPollData, timeout) {
      const dataObj = {
        leftChartData: this.leftChartConfig,
        rightChartData: this.rightChartConfig,
      };
      dataObj[type].chartData = [];
      dataObj[type].isLoading = true;

      this.getDmonitorMetrics(this.getApiParams(type), this.detailId)
        .then(res => {
          if (res.every(item => !item.data.length)) {
            // 获取当天零点时间
            dataObj[type].noDataText = this.getNodataText(this.getApiParams(type));
            return;
          }
          let timeSeries = [];
          for (const data of res) {
            const dataSeries = [];
            const timeSeries = [];
            for (const item of data.data) {
              timeSeries.push(this.formateTime(item.time));
              dataSeries.push(item[this.getApiParams(type)]);
            }

            dataObj[type].chartData.push({
              x: timeSeries,
              y: dataSeries,
              name: data.name,
              hoverinfo: 'none',
              type: 'lines',
              line: {
                width: 2,
              },
            });
          }
          dataObj[type].chartData.reverse();
        })
        ['finally'](() => {
          dataObj[type].tableTime = this.getNowTime(dataObj[type].chartData);
          dataObj[type].isLoading = false;
          if (isPollData) {
            this.pollingData(type, timeout);
          }
        });
    },
    getTableData(type, isPollData, timeout) {
      const dataObj = {
        leftTableData: this.leftTableConfig,
        rightTableData: this.rightTableConfig,
      };
      dataObj[type].isLoading = true;
      this.getDataQualitySummary(this.getApiParams(type)).then(res => {
        dataObj[type].tableData = Object.keys(res.data).length
          ? this.handleTableData(res, this.getApiParams(type))
          : [];
        dataObj[type].isLoading = false;
        this.$emit('updateDataInfo',
          qualityFieldObj[this.getApiParams(type)].field,
          Object.keys(res.data).length ? this.handleTipsData(res, this.getApiParams(type)) : []);

        if (isPollData) {
          this.pollingData(type, timeout);
        }
      });
    },
  },
};
</script>

<style lang="scss" scoped>
.container {
  display: flex;
  justify-content: space-between;
  flex-wrap: nowrap;
  .flex-grow {
    flex: 0.48;
  }
}
</style>

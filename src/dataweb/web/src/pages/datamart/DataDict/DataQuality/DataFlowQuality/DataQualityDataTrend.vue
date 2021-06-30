

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
    <HeaderType :title="$t('数据量信息')">
      <div class="content">
        <Chart
          class="min-width537"
          :isLoading="isChartLoading"
          :chartData="chartData"
          :chartConfig="config"
          :chartLayout="chartLayout"
          :title="$t('数据量趋势 (条)')"
          :isShowZero="true"
          :tableHead="$t('数据量 (条)')"
          :no-data-text="noDataText"
          :tableRateDes="$t('变化量/率（当前时刻数据量同比昨日、上周变化）')" />
        <RecentDataTable
          v-bkloading="{ isLoading: isRecentTableLoading }"
          class="flex-grow"
          :tableTime="tableTime"
          :tableRateDes="`变化量/率（今日累计数据量同比昨日、上周变化）`"
          :data="recentData">
          <template slot="primary-index">
            <th>
              {{ $t('累计数据量 (条)') }}
              <span v-bk-tooltips="`当日${tableTime}的累计数据量（条）`"
                class="icon-exclamation-circle" />
            </th>
          </template>
        </RecentDataTable>
      </div>
    </HeaderType>
  </div>
</template>

<script>
import dataQualityMixins from '../common/mixins.js';

export default {
  components: {
    HeaderType: () => import('@/pages/datamart/common/components/HeaderType'),
    Chart: () => import('@/pages/datamart/common/components/Chart'),
    RecentDataTable: () => import('@/pages/datamart/common/components/RecentDataTable.vue'),
  },
  mixins: [dataQualityMixins],
  data() {
    return {
      isShowChart: true,
      isChartLoading: false,
      chartData: [],
      recentData: [],
      isRecentTableLoading: false,
      noDataText: this.$t('暂无数据'),
      timer: [],
      tableTime: '',
    };
  },
  computed: {
    chartLayout() {
      return Object.assign({}, this.layout, {
        xaxis: {
          dtick: 80,
          rangemode: 'normal',
          autorange: true,
        },
        yaxis: {
          rangemode: 'normal',
          autorange: true,
        },
      });
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
      this.getTableData();
      this.getChartData();
    },
    cancelRefresh() {
      this.timer.forEach(item => {
        clearTimeout(item);
      });
    },
    pollingInit(timeout) {
      this.getTableData(true, timeout);
      this.getChartData(true, timeout);
    },
    pollingData(type, timeout) {
      const pullAjax = {
        tableData: this.getTableData,
        chartData: this.getChartData,
      };
      this.timer.push(
        setTimeout(() => {
          pullAjax[type](true, timeout);
        }, timeout)
      );
    },
    getTableData(isPollData, timeout) {
      this.isRecentTableLoading = true;
      this.getDataQualitySummary('output_count')
        .then(res => {
          this.recentData = Object.keys(res).length ? this.handleTableData(res, 'output_count') : [];
          this.$emit(
            'updateDataInfo',
            'dataTrend',
            Object.keys(res).length ? this.handleTipsData(res, 'output_count') : []
          );

          if (isPollData) {
            this.pollingData('tableData', timeout);
          }
        })
        ['finally'](() => {
          this.isRecentTableLoading = false;
        });
    },
    getChartData(isPollData, timeout) {
      this.chartData = [];
      this.isChartLoading = true;
      this.getDmonitorMetrics('output_count', this.detailId).then(res => {
        if (res.every(item => !item.data.length)) {
          this.isShowChart = false;
          this.isChartLoading = false;
          this.noDataText = `${this.formateTime(
            new Date(new Date().toLocaleDateString()).getTime() / 1000,
            'YYYY-MM-DD HH:mm:ss'
          )} ~ 当前，${this.$t('数据量')}${this.$t('为')}0`;
          return;
        }
        let timeSeries = [];
        for (const data of res) {
          const dataSeries = [];
          const timeSeries = [];
          for (const item of data.data) {
            timeSeries.push(this.formateTime(item.time));
            dataSeries.push(item.output_count);
          }
          this.chartData.push({
            x: timeSeries,
            y: dataSeries,
            name: data.name,
            type: 'lines',
            line: {
              width: 2,
            },
            hoverinfo: 'none',
          });
        }
        this.chartData.reverse();
        this.isChartLoading = false;

        this.tableTime = this.getNowTime(this.chartData);

        if (isPollData) {
          this.pollingData('chartData', timeout);
        }
      });
    },
  },
};
</script>

<style lang="scss" scoped>
.content {
  display: flex;
  align-items: center;
  .flex-grow {
    flex: 1;
  }
  .min-width537 {
    min-width: 537px;
  }
}
</style>

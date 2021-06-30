

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
    <HeaderType :title="$t('数据类型')"
      :tipsContent="$t('不同数据类型分布')">
      <div class="chart-container">
        <div class="chart">
          <div class="chart-title">
            {{ $t('数据类型分布') }}
          </div>
          <PlotlyChart v-if="isShowtypeChart"
            v-bkloading="{ isLoading: isBarLoading }"
            :eventListen="['plotly_relayout', 'onmousedown']"
            :chartData="typeData"
            :chartConfig="{ operationButtons: ['autoScale2d', 'resetScale2d'], responsive: true }"
            :chartLayout="typeLayout" />
          <NoData v-else />
        </div>
        <div class="chart">
          <div class="chart-title">
            {{ $t('数据是否标准化分布') }}
          </div>
          <PlotlyChart v-if="isShowPieChart"
            v-bkloading="{ isLoading: isPieLoading }"
            :eventListen="['plotly_relayout', 'onmousedown']"
            :chartData="pieData"
            :chartConfig="{ staticPlot: true, responsive: true }"
            :chartLayout="pieLayout" />
          <NoData v-else />
        </div>
      </div>
    </HeaderType>
  </div>
</template>

<script>
import HeaderType from '@/pages/datamart/common/components/HeaderType';
import PlotlyChart from '@/components/plotly';
import { calcChartParams } from '@/pages/datamart/common/utils.js';

export default {
  name: 'DataType',
  components: {
    HeaderType,
    PlotlyChart,
    NoData: () => import('@/components/global/NoData.vue'),
  },
  inject: ['selectedParams', 'tagParam', 'getTags', 'handerSafeVal', 'getParams'],
  data() {
    return {
      typeData: [],
      typeLayout: {},
      pieData: [],
      pieLayout: {},
      layout: {
        grid: { rows: 1, columns: 1 },
      },
      isPieLoading: false,
      isBarLoading: false,
      isShowPieChart: true,
      isShowtypeChart: true,
    };
  },
  mounted() {
    this.initData();
  },
  methods: {
    initData() {
      this.getStandarDistribution();
      this.getDataTypeDistributed();
    },
    getStandarDistribution() {
      this.isPieLoading = true;
      this.bkRequest
        .httpRequest('dataInventory/getStandarDistribution', {
          params: this.getParams(),
        })
        .then(res => {
          if (res.result) {
            if ([res.data.dataset_count - res.data.standard_dataset_count, res.data.standard_dataset_count]
              .every(item => !item)) {
              this.isShowPieChart = false;
              return;
            }
            this.isShowPieChart = true;
            this.pieData = [
              {
                values: [res.data.dataset_count - res.data.standard_dataset_count, res.data.standard_dataset_count],
                labels: [this.$t('未标准化'), this.$t('已标准化')],
                domain: { column: 0 },
                hoverinfo: 'label+percent',
                type: 'pie',
                pull: 0.1,
              },
            ];
            this.pieLayout = Object.assign({}, this.layout, {
              // title: {
              //     font: {
              //         size: 14
              //     },
              //     text: this.$t('数据是否标准化分布'),
              //     x: 0.1
              // },
              legend: {
                x: 1,
                y: 0,
              },
            });
          } else {
            this.isShowPieChart = false;
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isPieLoading = false;
        });
    },
    getDataTypeDistributed() {
      this.isBarLoading = true;
      this.bkRequest
        .httpRequest('dataInventory/getDataTypeDistributed', {
          params: Object.assign({}, this.getParams(), {
            top: 5,
            parent_tag_code: 'components',
          }),
        })
        .then(res => {
          if (res.result) {
            if (res.data.tag_count_list.every(item => !item)) {
              this.isShowtypeChart = false;
              return;
            }
            this.isShowtypeChart = true;
            this.typeData = [
              {
                x: this.$i18n.locale === 'en' ? res.data.tag_code_list : res.data.tag_alias_list,
                y: res.data.tag_count_list,
                type: 'bar',
                marker: {
                  color: 'rgb(43,216,196)',
                },
                mode: 'lines+makers',
              },
            ];
            const maxXaxis = Math.max.apply(Math, res.data.tag_count_list);
            const minXaxis = Math.min.apply(Math, res.data.tag_count_list);
            let dtick = maxXaxis > 1 ? calcChartParams(res.data.tag_count_list).applyNodeDtick : 1;
            if (calcChartParams(res.data.tag_count_list).maxApplyNode > 500) {
              dtick = null;
            }
            this.typeLayout = Object.assign({}, this.layout, {
              // title: {
              //     font: {
              //         size: 14
              //     },
              //     text: this.$t('数据类型分布'),
              //     x: 0.1
              // },
              yaxis: {
                showgrid: true,
                dtick,
                range: [minXaxis, calcChartParams(res.data.tag_count_list).maxApplyNode],
              },
            });
          } else {
            this.isShowtypeChart = false;
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isBarLoading = false;
        });
    },
  },
};
</script>

<style lang="scss" scoped>
.chart-container {
  .chart {
    position: relative;
    width: 600px;
    min-height: 430px;
    overflow: hidden;
  }
}
::v-deep .no-data {
  max-width: 600px;
  height: 400px !important;
}
</style>

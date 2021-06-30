

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
    <HeaderType :title="$t('数据价值')">
      <bkdata-collapse v-model="activeName"
        extCls="collapse-background-style">
        <bkdata-collapse-item name="0">
          <div class="value-title">
            {{ $t('综合评估指标') }}
            <span class="ml10 value-title-explain">
              {{
                $t('当前数据价值评估有2个综合指标：数据价值评分，数据价值成本比，评估越高该数据价值越高')
              }}
            </span>
          </div>
          <div slot="content"
            class="f13">
            <div class="data-value-container">
              <div class="data-value-child">
                <DataValueChartCard class="ml10 mr10"
                  :todayScore="indexExplain.asset_value_score"
                  :isShowRate="false">
                  <template slot="title">
                    {{ $t('价值评分') }}
                  </template>
                  <template slot="explain-header-time">
                    {{ $t('数据热度、广度、重要度3个指标综合计算') }}
                  </template>
                  <chart
                    slot="chart-content"
                    :isLoading="chartData.valueRate.loading"
                    :chartData="chartData.valueRate.data"
                    :chartType="chartData.valueRate.type"
                    :options="{
                      responsive: true,
                      maintainAspectRatio: false,
                      plugins: {
                        legend: {
                          display: false,
                        },
                      },
                      scale: {
                        min: 0,
                        max: 100,
                        ticks: {
                          beginAtZero: true,
                          stepSize: 20,
                          fontColor: '#979ba5',
                        },
                        pointLabels: {
                          fontSize: 12,
                        },
                      },
                      layout: {
                        padding: {
                          top: 50,
                        },
                      },
                    }"
                    width="350px"
                    height="260px" />
                </DataValueChartCard>
                <!-- 数据价值评分变化趋势 -->
                <DataValueChartCard
                  class="ml10 mr10"
                  :todayScore="indexExplain.asset_value_score"
                  :percentRate="handlerPercentText(indexExplain.asset_value_score_ranking)"
                  :config="config"
                  :loading="chartData.valueRateTrend.loading"
                  :title="chartData.valueRateTrend.name"
                  :chartData="chartData.valueRateTrend.data"
                  :layout="layout" />
                <!-- 数据价值成本比变化趋势 -->
                <DataValueChartCard
                  :config="config"
                  :titleTips="$t('数据价值与占用成本比值，价值越高占用成本越少，则比值越大，否则越小')"
                  :loading="chartData.valueCost.loading"
                  :todayScore="indexExplain.assetvalue_to_cost"
                  :percentRate="handlerPercentText(indexExplain.assetvalue_to_cost_ranking)"
                  :title="chartData.valueCost.name"
                  :chartData="chartData.valueCost.data"
                  :isModifyAxis="false"
                  unit=""
                  :layout="valueCostLayout" />
              </div>
            </div>
          </div>
        </bkdata-collapse-item>
        <bkdata-collapse-item name="1">
          <div class="value-title">
            {{ $t('评估指标详情') }}
            <span class="ml10 value-title-explain">
              {{
                $t('当前数据价值评估主要从数据应用热度、广度、重要程度3个方面综合计算，展开可查看指标详情')
              }}
            </span>
          </div>
          <div slot="content"
            class="f13">
            <div class="data-value-container">
              <div class="data-value-child">
                <!-- 热度评分变化趋势 -->
                <DataValueChartCard
                  :config="config"
                  :loading="chartData.hotRateTrend.loading"
                  :title="chartData.hotRateTrend.name"
                  :todayScore="indexExplain.heat_score"
                  :percentRate="handlerPercentText(indexExplain.heat_score_ranking)"
                  :chartData="chartData.hotRateTrend.hotRateData"
                  :layout="layout" />
                <!-- 数据查询次数变化趋势 -->
                <DataValueChartCard
                  :config="config"
                  :loading="chartData.hotRateTrend.loading"
                  :title="chartData.hotRateTrend.queryTimeName"
                  :chartData="chartData.hotRateTrend.queryTimeData"
                  :unit="$t('次')"
                  :todayScore="chartData.hotRateTrend.todayScore"
                  :todayScoreTips="chartData.hotRateTrend.todayScoreTips"
                  :layout="layout"
                  :isSetDick="true" />
                <DataValueChartCard class="hidden" />
              </div>
              <div class="data-value-child">
                <!-- 广度评分变化趋势 -->
                <DataValueChartCard
                  :config="config"
                  :loading="chartData.rangeRateTrend.loading"
                  :title="chartData.rangeRateTrend.name"
                  :chartData="chartData.rangeRateTrend.rangeData"
                  :todayScore="indexExplain.normalized_range_score"
                  :percentRate="handlerPercentText(indexExplain.range_score_ranking)"
                  :layout="rangeChartLayout" />
                <!-- 数据应用 -->
                <DataValueChartCard
                  :config="config"
                  :loading="chartData.rangeRateTrend.loading"
                  :isShowScore="false"
                  :title="chartData.rangeRateTrend.dataApplicationName"
                  :chartData="chartData.rangeRateTrend.dataApplicationData"
                  :layout="dataApplicationLayout"
                  :isModifyAxis="true" />
                <!-- 后继依赖节点个数变化趋势 -->
                <DataValueChartCard
                  :config="config"
                  :loading="chartData.rangeRateTrend.loading"
                  :unit="$t('个')"
                  :todayScore="chartData.rangeRateTrend.todayQueryTimes"
                  :todayScoreTips="chartData.rangeRateTrend.todayQueryTips"
                  :title="chartData.rangeRateTrend.nodeCountName"
                  :chartData="chartData.rangeRateTrend.nodeCountData"
                  :layout="layout"
                  :isSetDick="true" />
              </div>
              <div class="data-value-child">
                <!-- 重要度评分变化趋势 -->
                <DataValueChartCard
                  :config="config"
                  :loading="chartData.importanceTrend.loading"
                  :title="chartData.importanceTrend.name"
                  :chartData="chartData.importanceTrend.data"
                  :todayScore="indexExplain.importance_score"
                  :percentRate="handlerPercentText(indexExplain.importance_score_ranking)"
                  :layout="layout" />
                <DataImportanceIndex :tagList="tagList"
                  :sensitivity="sensitivity" />
              </div>
            </div>
          </div>
        </bkdata-collapse-item>
      </bkdata-collapse>
    </HeaderType>
    <div class="tips-wrap">
      <div id="range-tips">
        <div v-for="(item, index) in rangeIndex"
          :key="index">
          {{ item.label }}：<span>{{ getRangeRateTips(item.altIndex, item.field) }} {{ $t('个') }}</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { dataTypeIds, sensitiveObj } from '@/pages/datamart/common/config';
import { handlerPercentText } from '@/pages/datamart/common/utils.js';
import { showMsg } from '@/common/js/util.js';
import HeaderType from '@/pages/datamart/common/components/HeaderType';
import DataValueChartCard from './components/DataValueChartCard';
import DataImportanceIndex from './components/DataImportanceIndex';
import chart from '@/components/bkChartVue/Components/BaseChart.vue';

const raderChartField = [
  {
    id: 'heat_score',
    name: $t('热度'),
  },
  {
    id: 'normalized_range_score',
    name: $t('广度'),
  },
  {
    id: 'importance_score',
    name: $t('重要度'),
  },
];

export default {
  components: {
    HeaderType,
    DataValueChartCard,
    DataImportanceIndex,
    chart,
  },
  data() {
    return {
      rangeIndex: [
        {
          label: this.$t('应用数据'),
          field: 'node_count',
          // 备选数字
          altIndex: 0,
        },
        {
          label: this.$t('应用业务'),
          field: 'biz_count',
          altIndex: 1,
        },
        {
          label: this.$t('应用项目'),
          field: 'proj_count',
          altIndex: 1,
        },
        {
          label: this.$t('应用APP'),
          field: 'app_code_count',
          altIndex: 0,
        },
      ],
      htmlConfig: {
        maxWidth: 800,
        allowHtml: true,
        content: '#rate-show-content',
      },
      rateToolText: {
        [this.$t('热度评分')]: this.$t('热度评分tips'),
        [this.$t('广度评分')]: this.$t('广度评分tips'),
      },
      activeName: '0',
      config: {
        operationButtons: ['autoScale2d', 'resetScale2d'],
      },
      layout: {
        width: 350,
        height: 260,
        xaxis: {
          tickangle: 30,
          autorange: true,
          type: 'category',
        },
        legend: {
          orientation: 'h',
          x: 0.15,
          y: 1.15,
          font: {
            size: 10,
          },
        },
        margin: {
          l: 30,
          r: 30,
          t: 20,
          b: 40,
        },
      },
      chartData: {
        // 热度、广度、重要度评分指标雷达图
        valueRate: {
          loading: true,
          data: {
            labels: [],
            datasets: [],
          },
          url: 'getValueRate',
          type: 'radar',
        },
        // 数据价值评分趋势
        valueRateTrend: {
          loading: true,
          data: [],
          url: 'getValueRateTrend',
          xLabel: 'time',
          yLabel: 'score',
          name: this.$t('价值评分'),
          type: 'lines',
        },
        // 数据价值成本比变化趋势
        valueCost: {
          loading: true,
          data: [],
          url: 'getDataValueCost',
          xLabel: 'time',
          yLabel: 'score',
          name: this.$t('收益比（价值/成本）'),
          type: 'lines',
        },
        // 热度评分变化趋势
        hotRateTrend: {
          loading: true,
          hotRateData: [],
          // 数据查询次数变化趋势
          queryTimeData: [],
          url: 'getHeatTrendData',
          name: this.$t('热度评分'),
          queryTimeName: this.$t('查询次数（次）'),
          todayScoreTips: '',
          todayScore: 0,
        },
        // 广度评分变化趋势
        rangeRateTrend: {
          loading: true,
          rangeData: [],
          // 数据应用分布
          dataApplicationData: [],
          // 后继依赖节点变化趋势
          nodeCountData: [],
          url: 'getRangeTrendData',
          dataApplicationName: '应用（业务、项目、APP）分布',
          nodeCountName: '后继依赖节点个数',
          name: this.$t('广度评分'),
        },
        // 重要度评分变化趋势
        importanceTrend: {
          loading: true,
          data: [],
          url: 'getImportanceRateTrend',
          xLabel: 'time',
          yLabel: 'score',
          name: this.$t('重要度评分'),
          type: 'lines',
        },
      },
      indexExplain: {
        asset_value_score: 0,
        asset_value_score_ranking: 0,
        assetvalue_to_cost: 0,
        assetvalue_to_cost_ranking: 0,
        heat_score: 0,
        heat_score_ranking: 0,
        normalized_range_score: 0,
        range_score_ranking: 0,
        importance_score: 0,
        importance_score_ranking: 0,
      },
    };
  },
  computed: {
    queryTimes() {
      if (!this.$attrs.resultTableInfo.heat_metric) return 0;
      return this.$attrs.resultTableInfo.heat_metric.count || this.$attrs.resultTableInfo.heat_metric.query_count;
    },
    radarChartLayout() {
      return Object.assign({}, this.layout, {
        polar: {
          radialaxis: {
            // angle: -45,
            visible: true,
            range: [0, 100],
            linewidth: 0.5,
            showline: true,
            linecolor: 'rgba(190, 190, 190, 80)',
            gridcolor: 'rgba(200, 200, 200, 80)',
            gridwidth: 0.5,
            width: 20,
            tickfont: {
              size: 8,
            },
            visible: true,
            title: {
              // text: '1',
              font: {
                color: 'blue',
                width: 20,
              },
            },
            // color: 'red',
            // linecolor: 'red'
          },
        },
        width: 380,
        height: 350,
        margin: {
          l: 25,
          r: 105,
          t: 20,
          b: 40,
        },
      });
    },
    rangeChartLayout() {
      return Object.assign({}, this.layout, {
        margin: {
          r: 30,
          l: 30,
          t: 20,
          b: 40,
        },
      });
    },
    sensitivity() {
      return sensitiveObj[this.$attrs.resultTableInfo.sensitivity];
    },
    tagList() {
      return this.$attrs.resultTableInfo.tag_list || [];
    },
    dataApplicationLayout() {
      return Object.assign({}, this.layout, {
        margin: {
          l: 18,
          r: 15,
          b: 40,
          t: 20,
        },
      });
    },
    valueCostLayout() {
      return Object.assign({}, this.layout, {
        yaxis: {
          range: [0, 1],
        },
      });
    },
  },
  mounted() {
    this.getRadarData();
    this.batchGetData();
    this.getHeatTrendData();
    this.getRangeTrendData();
  },
  methods: {
    getRangeRateTips(num, field) {
      if (!this.$attrs.resultTableInfo.range_metric) return num;
      return this.$attrs.resultTableInfo.range_metric[field];
    },
    getAssetValueToCost(num) {
      return num < 0
        ? this.$t('暂无统计')
        : `${num}，大于平台 ${this.handlerPercentText(this.indexExplain.assetvalue_to_cost_ranking)}%的数据。`;
    },
    getChartConfig() {
      return {
        staticPlot: true,
      };
    },
    handlerPercentText(num, digit) {
      return handlerPercentText(num, digit);
    },
    getChartData(url, params) {
      return this.bkRequest
        .httpRequest(`dataDict/${url}`, {
          [params]: {
            dataset_id: this.$route.query[dataTypeIds[this.$route.query.dataType]],
            dataset_type: this.$route.query.dataType,
          },
        })
        .then(res => {
          if (res.result && res.data) {
            return Promise.resolve(res);
          } else {
            this.getMethodWarning(res.message, res.code);
            return Promise.reject(res);
          }
        })
        ['catch'](err => {
          showMsg(err.message, 'error');
        });
    },
    batchGetData() {
      const fields = ['valueRateTrend', 'valueCost', 'importanceTrend'];
      fields.forEach(field => {
        this.getBatchChartData(field, 'query');
      });
    },
    getBatchChartData(field, params) {
      this.getChartData(this.chartData[field].url, params)
        .then(res => {
          if (!res.data) return;
          this.chartData[field].data.push({
            x: res.data[this.chartData[field].xLabel],
            y: res.data[this.chartData[field].yLabel],
            name: this.chartData[field].name,
            type: this.chartData[field].type,
            marker: {
              color: '#3a84ff',
            },
          });
        })
        ['finally'](() => {
          this.chartData[field].loading = false;
        });
    },
    getRadarData() {
      this.getChartData('getValueRate', 'query')
        .then(res => {
          if (res.data) {
            this.indexExplain = res.data;
            const theta = raderChartField.map(item => `${item.name}：${res.data[item.id]}`);
            const r = raderChartField.map(item => res.data[item.id]);

            this.chartData.valueRate.data.labels = theta;
            this.chartData.valueRate.data.datasets = [
              {
                data: r,
                backgroundColor: 'rgba(51,157,255,0.3)',
                borderColor: '#3a84ff',
                fill: true,
              },
            ];
          }
        })
        ['finally'](() => {
          this.chartData.valueRate.loading = false;
        });
    },
    getHeatTrendData() {
      this.getChartData('getHeatTrendData', 'query').then(res => {
        if (!res.data) return;
        this.chartData.hotRateTrend.hotRateData.push({
          x: res.data.time,
          y: res.data.score,
          name: '热度评分变化趋势',
          type: 'lines',
          marker: {
            color: '#3a84ff',
          },
        });
        this.chartData.hotRateTrend.queryTimeData.push({
          x: res.data.time,
          y: res.data.day_query_count,
          name: this.$t('数据查询次数（次）变化趋势'),
          type: 'lines',
          marker: {
            color: '#3a84ff',
          },
        });
        this.chartData.hotRateTrend.todayScoreTips = res.data.day_query_count_tooltip;
        this.chartData.hotRateTrend.todayScore = res.data.day_query_count.slice(-1)[0];
        this.chartData.hotRateTrend.loading = false;
      });
    },
    getRangeTrendData() {
      this.getChartData('getRangeTrendData', 'query').then(res => {
        if (!res.data) return;
        this.chartData.rangeRateTrend.rangeData.push({
          x: res.data.time,
          y: res.data.score,
          name: '广度评分变化趋势',
          type: 'lines',
          marker: {
            color: '#3a84ff',
          },
        });
        this.chartData.rangeRateTrend.dataApplicationData.push({
          x: res.data.time,
          y: res.data.proj_count,
          name: this.$t('项目'),
          type: 'bar',
          marker: {
            color: '#3a84ff',
          },
        });
        this.chartData.rangeRateTrend.dataApplicationData.push({
          x: res.data.time,
          y: res.data.biz_count,
          name: this.$t('业务'),
          type: 'bar',
        });
        this.chartData.rangeRateTrend.dataApplicationData.push({
          x: res.data.time,
          y: res.data.app_code_count,
          name: this.$t('应用APP'),
          type: 'bar',
        });
        this.chartData.rangeRateTrend.nodeCountData.push({
          x: res.data.time,
          y: res.data.node_count,
          name: this.$t('后继依赖节点个数变化趋势'),
          type: 'bar',
          marker: {
            color: '#3a84ff',
          },
        });
        this.chartData.rangeRateTrend.todayQueryTips = res.data.node_count_tooltip;
        this.chartData.rangeRateTrend.todayQueryTimes = res.data.node_count.slice(-1)[0];
        this.chartData.rangeRateTrend.loading = false;
      });
    },
  },
};
</script>

<style lang="scss" scoped>
.hidden {
  opacity: 0;
}
.value-title {
  font-size: 14px;
  font-weight: 550;
  .value-title-explain {
    font-size: 12px;
    font-weight: normal;
    color: #737987;
  }
}
.data-value-container {
  padding: 10px 0;
  .data-value-child {
    display: flex;
    flex-wrap: nowrap;
    justify-content: space-between;
  }
}
.tips-wrap {
  height: 0;
  overflow: hidden;
}
</style>

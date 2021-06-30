

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
    <HeaderType :title="$t('数据来源')"
      :tipsContent="$t('描述数据的来源方式')">
      <div class="chart-container">
        <div v-bkloading="{ isLoading: isPieLoading }"
          class="chart"
          @click="chartMouseDown">
          <div class="chart-title">
            {{ $t('数据来源分布') }}
          </div>
          <PlotlyChart v-if="isShowPieChart"
            :eventListen="['plotly_relayout', 'onmousedown', 'plotly_click']"
            :chartData="pieData"
            :chartConfig="{ staticPlot: true, responsive: true }"
            :chartLayout="pieLayout"
            @plotly_click="pieClick"
            @onmousedown="chartMouseDown" />
          <NoData v-else />
        </div>
        <div v-bkloading="{ isLoading: isBarLoading }"
          class="device-chart">
          <div class="chart-title">
            {{ $t('数据来源') }}
            <span v-if="keyWord">
              （<span class="head-word">{{ keyWord }}）</span>
            </span>{{ $t('详情') }} {{ $t('分布') }}
          </div>
          <PlotlyChart
            v-if="isShowDeviceChart"
            ref="chartInstance"
            :eventListen="['plotly_relayout', 'onmousedown', 'plotly_click']"
            :chartData="deviceData"
            :chartConfig="{ operationButtons: ['autoScale2d', 'resetScale2d'], responsive: true }"
            :chartLayout="deviceLayout"
            @plotly_click="deviceClick" />
          <NoData v-else />
        </div>
      </div>
    </HeaderType>
  </div>
</template>

<script>
import HeaderType from '@/pages/datamart/common/components/HeaderType';
import PlotlyChart from '@/components/plotly';
import Bus from '@/common/js/bus.js';
import { calcChartParams } from '@/pages/datamart/common/utils.js';

const sourceChartcolors = ['rgb(36, 73, 147)',
  'rgb(79, 129, 102)',
  'rgb(151, 179, 100)',
  'rgb(33, 75, 99)'];
const opacityColors = ['rgba(36, 73, 147, 0.5)',
  'rgba(79, 129, 102, 0.5)',
  'rgba(151, 179, 100, 0.5)',
  'rgba(33, 75, 99, 0.5)'];
export default {
  name: 'DataSource',
  components: {
    HeaderType,
    PlotlyChart,
    NoData: () => import('@/components/global/NoData.vue'),
  },
  inject: ['selectedParams', 'tagParam', 'getTags', 'handerSafeVal', 'getParams'],
  data() {
    return {
      deviceData: [],
      deviceLayout: {},
      pieData: [
        {
          values: [],
          labels: [],
          domain: { column: 0 },
          hoverinfo: 'label+percent',
          hole: 0.4,
          type: 'pie',
          sort: false,
          marker: {
            colors: [],
          },
        },
      ],
      pieLayout: {},
      layout: {
        grid: { rows: 1, columns: 1 },
      },
      isPieLoading: false,
      isBarLoading: false,
      tagCodeList: [],
      keyWord: '',
      tagCondition: [],
      isShowPieChart: true,
      isShowDeviceChart: true,
      barColor: '',
    };
  },
  watch: {
    'tagParam.selectedTags'() {
      this.getDataDevice();
    },
  },
  mounted() {
    this.initData();
  },
  methods: {
    resetChartColors() {
      this.pieData[0].marker.colors = sourceChartcolors;
    },
    resetChartStatus() {
      this.resetChartColors();
      this.keyWord = '';
      this.barColor = '';
      this.getDataDevice('all');
    },
    chartMouseDown(data) {
      if (data.toElement.tagName === 'path') return;
      if (this.pieData[0].marker.colors.some(item => sourceChartcolors.includes(item))) {
        this.resetChartStatus();
      }
    },
    initData() {
      this.getDataSource();
      this.getDataDevice();
    },
    pieClick(data) {
      const label = data.points[0].label;
      if (label === this.$t('其他')) return;
      const index = data.points[0].data.labels.findIndex(item => item === label);
      if (this.pieData[0].marker.colors[index] === sourceChartcolors[index]
        && this.pieData[0].marker.colors.filter(item => /^rgba/.test(item)).length === 3) {
        this.resetChartStatus();
      } else {
        this.resetChartColors();
        this.keyWord = label;
        const colors = [...opacityColors];
        this.barColor = colors[index] = sourceChartcolors[index];
        this.pieData[0].marker.colors = colors;
        this.getDataDevice(this.tagCodeList[index]);
      }
      // Bus.$emit('changeTags', data.points[0].label)
    },
    deviceClick(data) {
      const index = data.points[0].data.y.findIndex(item => item == data.points[0].y);
      const tags = this.tagParam.selectedTags.map(item => item.tag_code);
      tags.push(this.tagCondition[index]);
      this.$router.push({
        name: 'DataSearchResult',
        params: {
          nodeType: 'all',
          dataMapParams: {
            biz_id: this.selectedParams.biz_id,
            project_id: this.selectedParams.project_id,
            search_text: this.tagParam.searchTest,
            chosen_tag_list: tags,
          },
        },
      });
    },
    getDataSource() {
      this.isPieLoading = true;
      this.bkRequest
        .httpRequest('dataInventory/getDataSource', {
          params: this.getParams(),
        })
        .then(res => {
          if (res.result) {
            if (res.data.tag_count_list.every(item => item === 0)) {
              this.isShowPieChart = false;
              return;
            }
            this.isShowPieChart = true;
            this.tagCodeList = res.data.tag_code_list;
            this.pieData = [
              {
                values: res.data.tag_count_list,
                labels: this.$i18n.locale === 'en' ? res.data.tag_code_list : res.data.tag_alias_list,
                domain: { column: 0 },
                hoverinfo: 'label+percent',
                hole: 0.4,
                type: 'pie',
                sort: false,
                marker: {
                  colors: sourceChartcolors,
                },
              },
            ];
            this.pieLayout = Object.assign({}, this.layout, {
              legend: {
                x: 0,
                y: 0,
              },
              annotations: [
                {
                  font: {
                    size: 12,
                  },
                  align: 'center',
                  showarrow: false,
                  text: `${res.data.tag_count_list.reduce((x, y) => x + y)} <br>Total`,
                },
              ],
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
    getDataDevice(label) {
      this.isBarLoading = true;
      const parentTagCode = label ? label : 'all';
      this.bkRequest
        .httpRequest('dataInventory/getDataDevice', {
          params: Object.assign({}, this.getParams(), {
            parent_tag_code: parentTagCode,
          }),
        })
        .then(res => {
          if (res.result) {
            if (res.data.tag_count_list.every(item => item === 0)) {
              this.isShowDeviceChart = false;
              return;
            }
            this.isShowDeviceChart = true;
            this.tagCondition = res.data.tag_code_list.reverse();
            const maxXaxis = Math.max.apply(Math, res.data.tag_count_list);
            const minXaxis = Math.min.apply(Math, res.data.tag_count_list);
            this.deviceData = [
              {
                y: this.$i18n.locale === 'en' ? this.tagCondition : res.data.tag_alias_list.reverse(),
                x: res.data.tag_count_list.reverse(),
                domain: { column: 0 },
                orientation: 'h',
                type: 'bar',
                marker: {
                  color: this.barColor,
                },
              },
            ];
            let dtick = maxXaxis > 1 ? calcChartParams(res.data.tag_count_list).applyNodeDtick : 1;
            if (calcChartParams(res.data.tag_count_list).maxApplyNode > 100) {
              dtick = null;
            }
            this.deviceLayout = Object.assign({}, this.layout, {
              xaxis: {
                automargin: true,
                dtick,
                rangemode: 'nonnegative',
                range: [minXaxis, calcChartParams(res.data.tag_count_list).maxApplyNode],
              },
              yaxis: {
                automargin: true,
              },
            });
          } else {
            this.isShowDeviceChart = false;
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
  .device-chart {
    max-width: 600px;
    position: relative;
    color: #373d41;
    width: 600px;
    min-height: 400px;
    overflow: hidden;
    .head {
      position: absolute;
      top: 20px;
      left: 20px;
      z-index: 1;
      .head-word {
        color: #3a84ff;
      }
    }
  }
  .chart {
    position: relative;
    width: 600px;
    min-height: 400px;
    overflow: hidden;
  }
}
::v-deep .no-data {
  max-width: 600px;
  height: 400px !important;
}
</style>

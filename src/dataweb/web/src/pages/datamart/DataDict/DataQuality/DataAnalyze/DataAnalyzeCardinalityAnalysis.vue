

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
  <div class="data-analyze-container">
    <HeaderType
      :title="$t('基数分析')"
      :tipsContent="$t('当前选中字段中唯一值的个数分布（仅适用于唯一值个数<30的字段）')"
      :isShowLeft="false"
      :isPaddding="false">
      <template slot="title-right">
        <div class="bk-form">
          <div class="bk-form-item">
            <label class="bk-label pr5">{{ $t('字段名称') }}：</label>
            <div class="bk-form-content">
              <bkdata-selector
                :selected.sync="selectedFeild"
                :searchable="true"
                :list="fieldList"
                :settingKey="'name'"
                :displayKey="'name'"
                :searchKey="'name'"
                @change="fieldChange" />
            </div>
          </div>
        </div>
      </template>
      <div v-bkloading="{ isLoading }"
        class="content">
        <div class="chart-container">
          <Chart
            :isLoading="isChartLoading"
            :chartData="chartData"
            :chartConfig="config"
            :chartLayout="layout"
            :isShowZero="true" />
        </div>
        <div v-if="tableData.length"
          class="table-container">
          <table class="bk-table bk-table-outer-border"
            border="1"
            cellspacing="0"
            cellpadding="0">
            <tr>
              <th class="pl30"
                :title="field">
                {{ field }}
              </th>
              <th class="pl30">
                {{ $t('个数') }}
              </th>
              <th class="pl30">
                {{ $t('百分比') }}
              </th>
            </tr>
            <tbody>
              <tr v-for="(item, index) in tableData"
                :key="index">
                <td
                  class="text-overflow pl30"
                  :class="{ 'other-value-color': item.label === $t('其他') }"
                  :title="item.label === $t('其他') ? $t(`其余${item.value}个属性值`) : item.label">
                  {{ item.label }}
                </td>
                <td class="text-overflow pl30"
                  :title="item.value">
                  {{ item.value }}
                </td>
                <td class="text-right">
                  {{ item.percent }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </HeaderType>
  </div>
</template>

<script>
import Chart from '@/pages/datamart/common/components/Chart';
import dataQualityMixins from '../common/mixins.js';
import HeaderType from '@/pages/datamart/common/components/HeaderType';
import { getAxixParams } from '@/pages/datamart/common/utils.js';
import dataAnalyzeMixins from './common/mixins.js';

export default {
  components: {
    Chart,
    HeaderType,
  },
  mixins: [dataQualityMixins, dataAnalyzeMixins],
  props: {
    field: String,
    isLoading: Boolean,
    totalCount: Number,
    distributionData: {
      type: Object,
      default: () => ({}),
    },
    uniqueCount: {
      type: Number,
      default: 0,
    },
  },
  data() {
    return {
      tableData: [],
      chartData: [],
      config: {},
      layout: {
        width: 800,
        height: 500,
        legend: {
          orientation: 'v',
          x: -0.1,
        },
        yaxis: {
          tickfont: {
            size: 12,
          },
        },
        xaxis: {
          autorange: true,
        },
      },
      isChartLoading: false,
      pieData: [],
      barData: [],
      barLayout: {},
    };
  },
  watch: {
    distributionData(val) {
      const distribution = val.distribution;
      if (distribution) {
        const keyArr = Object.keys(distribution);
        if (keyArr.length) {
          this.tableData = keyArr
            .map(child => {
              return {
                label: child,
                value: distribution[child],
                percent: `${((distribution[child] / this.totalCount) * 100).toFixed(2)}%`,
              };
            })
            .sort((a, b) => a.value - b.value);
          this.getChartData(
            keyArr,
            keyArr.map(item => distribution[item])
          );
        }
      }
    },
  },
  methods: {
    getChartData(x, y) {
      // 唯一值个数小于15展示饼图
      if (this.uniqueCount < 15) {
        this.chartData = [
          {
            labels: x,
            values: y,
            type: 'pie',
            hole: 0.5,
            rotation: 120,
          },
        ];
        this.config = {
          staticPlot: true,
        };
      } else {
        const { dtick, range } = getAxixParams(x);
        this.layout.xaxis = {
          dtick,
          range,
        };
        this.layout.yaxis = {
          type: 'category',
        };
        this.chartData = [
          {
            x: y,
            y: x,
            type: 'bar',
            orientation: 'h',
          },
        ];
      }
    },
  },
};
</script>
<style lang="scss" scoped>
.bk-table {
  .other-value-color {
    color: #979ba5;
  }
  td {
    height: 28px;
  }
  .text-right {
    text-align: right;
    padding-right: 30px !important;
  }
}
.chart-container {
  align-items: flex-start;
}
</style>

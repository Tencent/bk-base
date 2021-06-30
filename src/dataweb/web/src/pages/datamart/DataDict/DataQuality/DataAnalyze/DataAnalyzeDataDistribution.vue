

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
      :title="$t('数据分布')"
      :tipsContent="$t('当前选中字段的数值分布')"
      :isShowLeft="false"
      :isPaddding="false">
      <template slot="title-right">
        <div class="bk-form">
          <div class="bk-form-item">
            <label class="bk-label pr5">
              <span
                v-bk-tooltips="$t('字符串类型的字段暂无数据分布')"
                class="mr5 icon-question-circle-shape-delete" />{{ $t('字段名称') }}：
            </label>
            <div class="bk-form-content">
              <bkdata-selector
                :selected.sync="selectedFeild"
                :searchable="true"
                :list="fieldList"
                :settingKey="'name'"
                :displayKey="'name'"
                :searchKey="'name'"
                :placeholder="placeHolder"
                @change="fieldChange" />
              <!-- :disabled="!fieldList.length " -->
            </div>
          </div>
        </div>
      </template>
      <div v-bkloading="{ isLoading }"
        class="content">
        <div v-if="histogramData.length"
          class="chart-container">
          <div v-if="Object.keys(distributionData).length"
            class="chart-explain">
            μ = {{ distributionData.mean }}, σ = {{ distributionData.std }}
          </div>
          <Chart
            :isLoading="isChartLoading"
            :chartData="histogramData"
            :chartConfig="config"
            :chartLayout="layout"
            :title="$t(`字段（${field}）属性值的数据分布`)"
            :isShowZero="true"
            :tableHead="$t('数据量（条）')"
            :isModifyAxis="false"
            :no-data-text="$t('系统判断到当前字段属性值为字符型，暂无数据分布')" />
        </div>
        <div v-else
          class="no-data">
          {{ $t(`系统判断到当前字段（${field}）属性值为字符型，暂无数据分布`) }}
        </div>
        <div
          v-if="Object.keys(distributionData).length && histogramData.length && tableData.length"
          class="table-container">
          <table class="bk-table bk-table-outer-border"
            border="1"
            cellspacing="0"
            cellpadding="0">
            <tr>
              <th>
                {{ $t('分析项') }}
              </th>
              <th>
                {{ $t('分析值') }}
              </th>
            </tr>
            <tbody>
              <tr v-for="(item, index) in tableData"
                :key="index">
                <td>
                  <span
                    v-bk-tooltips="{
                      content: labelTips[item.label],
                      placements: ['top-end'],
                    }">
                    {{ fieldAlias[item.label] }}
                  </span>
                </td>
                <td class="text-overflow text-right"
                  :title="item.value">
                  {{ item.value }}
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
import dataAnalyzeMixins from './common/mixins.js';

export default {
  components: {
    Chart,
    HeaderType,
  },
  mixins: [dataQualityMixins, dataAnalyzeMixins],
  props: {
    distributionData: {
      type: Object,
      default: () => ({}),
    },
    isLoading: Boolean,
    fieldAlias: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      labelTips: {
        mean: this.$t('数据平均值，数据之和除以数据点的个数，表示数据集的平均大小'),
        var: this.$t('离均差（将数据与均值之差）的平方和的均值，反映数据的离散程度'),
        std: this.$t('反映数据的离散程度，在数学上定义为方差的算术平方根'),
        skew: this.$t(
          '统计数据分布偏斜方向和程度的度量，非对称程度的数字特征，亦称偏态、偏态系数；正态分布的偏度为0，两侧尾部长度对称，偏度<0称分布具有负偏离或左偏态，否则正偏离或右偏态'
        ),
        kurt: this.$t(
          '反映概率密度分布曲线在平均值处峰值高低的特征数，又称峰态系数；样本的峰度是和正态分布相比较而言统计量，正态分布的峰度为3，如果峰度>3，峰的形状比较尖，比正态分布峰要陡峭，反之亦然'
        ),
      },
      config: {},
      layout: {
        width: 800,
        height: 340,
        showlegend: false,
        xaxis: {
          tickfont: {
            size: 12,
          },
        },
        yaxis: {
          title: this.$t('概率'),
        },
        margin: {
          l: 50,
        },
      },
      isChartLoading: false,
      chartData: [],
    };
  },
  computed: {
    tableData() {
      const fieldSortObj = {
        mean: 1,
        var: 2,
        std: 3,
        skew: 4,
        kurt: 5,
      };
      const keyArr = Object.keys(this.distributionData).filter(item => this.fieldAlias[item]);
      if (keyArr.length) {
        return keyArr
          .map(item => {
            return {
              label: item,
              value: this.distributionData[item],
            };
          })
          .sort((a, b) => fieldSortObj[a.label] - fieldSortObj[b.label]);
      }
      return [];
    },
    histogramData() {
      if (!Object.keys(this.distributionData).length) return [];
      let finallyData = [];
      if (Object.keys(this.distributionData.distribution_hist).length) {
        // eslint-disable-next-line vue/no-side-effects-in-computed-properties
        this.layout.xaxis.title = this.field;
        let width = 0;
        if (this.distributionData.distribution_hist.bins.length > 2) {
          width = this.distributionData.distribution_hist.bins[1] - this.distributionData.distribution_hist.bins[0];
        }
        finallyData = [
          ...finallyData,
          {
            type: 'bar',
            x: this.distributionData.distribution_hist.bins,
            y: this.distributionData.distribution_hist.n,
            width,
            name: this.$t('概率'),
          },
          {
            type: 'scatter',
            x: this.distributionData.distribution_hist.extend_bins,
            y: this.distributionData.distribution_hist.prob,
            name: this.$t('核密度估计'),
          },
        ];
      }
      return finallyData;
    },
    placeHolder() {
      return this.fieldList.findIndex(item => item.name === this.field) > -1 ? $t('请选择') : $t('当前无可用字段');
    },
  },
};
</script>
<style lang="scss" scoped>
.bk-table {
  td {
    height: 55px;
  }
  td,
  th {
    text-align: center;
  }
  .text-right {
    text-align: right;
    padding-right: 53px !important;
  }
}
.chart-container {
  position: relative;
  .chart-explain {
    position: absolute;
    top: 71px;
    font-size: 13px;
    z-index: 1;
  }
}
.no-data {
  color: #cfd3dd;
}
</style>

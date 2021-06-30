

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
  <div class="value-cost-container">
    <HeaderType :title="$t('价值成本')"
      :tipsContent="$t('数据价值评估两个综合指标：价值评分，价值成本比，及占用的总存储量')">
      <div class="chart-container clearfix">
        <ChartPart :chartTitle="$t('数据价值评分分布')"
          :titleTips="$t('价值评分tips')">
          <BubbleChart id="valueRate"
            ref="valueRate"
            :isLoading="loading.valueRateLoading" />
        </ChartPart>
        <ChartPart :chartTitle="$t('数据价值等级分布')">
          <PieWidthTable ref="valueLevel"
            :subTitle="$t('数据价值详情分布')"
            :levelDistributionType="'asset_value'"
            :levelScoreType="'asset_value'" />
        </ChartPart>
        <ChartPart :chartTitle="$t('数据占用存储成本分布')">
          <div class="chart-part">
            <div v-bkloading="{ isLoading: isStorageLoading }"
              class="storage-chart">
              <div class="chart-wrapper">
                <div v-if="isShowPie"
                  class="chart-inner-text">
                  <span class="text-overflow"
                    :title="totalStorageCapacity">
                    {{ totalStorageCapacity }}
                  </span>
                  <p>{{ $t('总储存成本') }}</p>
                </div>
                <Chart
                  v-bkloading="{ isLoading: loading.valueCostLevelLoading }"
                  :isShowContent="!!isShowPie"
                  :isRendering="storageDistributionData.isRendering"
                  class="mr10"
                  cssClasses="height-style"
                  :chartData="storageDistributionData"
                  :chartType="'doughnut'"
                  :options="{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                      legend: {
                        display: false,
                      },
                    },
                  }" />
              </div>
              <Chart v-bkloading="{ isLoading: loading.storageTrendLoading }"
                class="storage-bar-chart"
                :isShowContent="storageTrendData.labels.length"
                :isRendering="storageTrendData.isRendering"
                :height="'300px'"
                :chartData="storageTrendData"
                :chartType="'bar'"
                :options="storageTrendOption" />
            </div>
          </div>
        </ChartPart>
        <ChartPart :chartTitle="$t('收益比（价值/成本）分布')"
          :titleTips="$t('收益比tips')">
          <BubbleChart id="valueCost"
            ref="valueCost"
            :isLoading="loading.valueCostLoading" />
        </ChartPart>
        <ChartPart :chartTitle="$t('收益比(价值/成本)等级分布')">
          <PieWidthTable ref="valueCostRatio"
            :subTitle="$t('收益比(价值/成本)详情分布')"
            :levelDistributionType="'assetvalue_to_cost'"
            :levelScoreType="'assetvalue_to_cost'" />
        </ChartPart>
      </div>
    </HeaderType>
  </div>
</template>

<script lang="ts" src="./ValueCost.ts"></script>

<style lang="scss" scoped>
.chart-part {
  width: 100%;
  position: relative;
  overflow-x: auto;
  .storage-chart {
    position: relative;
    display: flex;
    flex-wrap: nowrap;
    align-items: center;
    .storage-bar-chart {
      width: calc(100% - 150px);
    }
    .chart-wrapper {
      position: relative;
      width: 150px;
      .chart-inner-text {
        position: absolute;
        text-align: center;
        font-weight: 550;
        font-size: 12px;
        z-index: 1;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
        .text-overflow {
          display: inline-block;
          width: 55px;
        }
      }
    }
  }
}
</style>

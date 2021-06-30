

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
  <div class="data-importance-container">
    <HeaderType :title="$t('数据重要度')"
      :tipsContent="$t('数据重要度表示数据应用的重要程度详细说明')">
      <div class="chart-container">
        <ChartPart :chartTitle="$t('数据重要度评分分布')">
          <BubbleChart id="importantRate"
            ref="importantRate"
            :isLoading="loading.importanceLoading" />
        </ChartPart>
        <ChartPart :chartTitle="$t('数据重要度等级分布')">
          <PieWidthTable ref="importanceDistributed"
            :subTitle="$t('数据重要度详情分布')"
            levelDistributionType="importance"
            :levelScoreType="'importance'" />
        </ChartPart>
        <ChartPart :chartTitle="$t('影响数据重要度的各项指标分布')">
          <div class="chart-part chart-index-part">
            <div v-bkloading="{ isLoading: sensitivityData.isLoading }"
              class="pie-chart">
              <div class="pie-title">
                <p>数据分布 | 敏感级别</p>
              </div>
              <Chart :width="pieWidth"
                no-data-height="135px"
                :height="pieHeight"
                :chartData="sensitivityData"
                :isShowContent="sensitivityData.datasets[0].data.length"
                :chartType="'pie'"
                :options="getPieTitleOption('数据敏感度分布', sensitivityData, 'sensitive')"
                cssClasses="height-style" />
            </div>
            <div v-bkloading="{ isLoading: activeData.isLoading }"
              class="pie-chart">
              <div class="pie-title">
                <p>数据分布 | 项目状态</p>
              </div>
              <Chart :width="pieWidth"
                no-data-height="135px"
                :height="pieHeight"
                :chartData="activeData"
                :isShowContent="activeData.datasets[0].data.length"
                :chartType="'pie'"
                :options="getPieTitleOption('项目运营状态', activeData, 'proj')"
                cssClasses="height-style" />
            </div>
            <div v-bkloading="{ isLoading: appImportantLevelNameData.isLoading }"
              class="pie-chart">
              <div class="pie-title">
                <p>数据分布 | 业务级别</p>
              </div>
              <Chart
                :width="pieWidth"
                no-data-height="135px"
                :height="pieHeight"
                :chartData="appImportantLevelNameData"
                :isShowContent="appImportantLevelNameData.datasets[0].data.length"
                :chartType="'pie'"
                :options="getPieTitleOption($t('业务重要级别分布'), appImportantLevelNameData, 'biz')"
                cssClasses="height-style" />
            </div>
          </div>
        </ChartPart>
        <ChartPart :chartTitle="$t('影响数据重要度的各项指标分布')">
          <div v-bkloading="{ isLoading: loading.heatMapLoading }"
            class="chart-part">
            <div class="pie-title">
              <p>数据分布 | 业务星级&运营状态</p>
            </div>
            <PlotlyChart v-if="heatMapData[0].x.length > 0"
              :chartConfig="{ operationButtons: ['resetScale2d', 'select2d'], responsive: true }"
              :chartData="heatMapData"
              :chartLayout="heatMapLayout" />
            <NoData v-else />
          </div>
        </ChartPart>
      </div>
    </HeaderType>
  </div>
</template>

<script lang="ts" src="./DataImportance.ts"></script>

<style lang="scss" scoped>
.chart-part {
  width: 100%;
  .pie-title {
    font-size: 12px;
    p {
      font-weight: 540;
      color: #737987;
    }
  }
}
.chart-index-part {
  display: grid !important;
  grid-template-columns: 250px 250px;
  grid-gap: 15px;
}
</style>

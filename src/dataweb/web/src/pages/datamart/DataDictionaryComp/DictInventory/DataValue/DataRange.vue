

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
  <div class="data-range-container">
    <HeaderType :title="$t('数据广度')"
      :tipsContent="$t('数据广度表示数据应用的广泛程度详细说明')">
      <div class="chart-container">
        <ChartPart :chartTitle="$t('数据广度评分分布')">
          <BubbleChart id="rangeRate"
            ref="rangeRate"
            :isLoading="loading.isRangeRateLoading" />
        </ChartPart>
        <ChartPart :chartTitle="$t('数据广度等级分布')">
          <PieWidthTable ref="rangeDistributed"
            :subTitle="$t('数据广度详情分布')"
            :levelDistributionType="'range'"
            :levelScoreType="'range'" />
        </ChartPart>
        <ChartPart :chartTitle="$t('应用业务个数分布')">
          <Chart v-bkloading="{ isLoading: loading.isAppBizLoading }"
            :isShowContent="applyNodeData.labels.length"
            :height="'380px'"
            :chartData="applyNodeData"
            :chartType="'bar'"
            :options="applyNodeDataOptions"
            cssClasses="height-style" />
        </ChartPart>

        <ChartPart :chartTitle="$t('应用项目个数分布')">
          <Chart v-bkloading="{ isLoading: loading.isAppProjLoading }"
            :isShowContent="applyProjData.labels.length"
            :height="'380px'"
            :chartData="applyProjData"
            :chartType="'bar'"
            :options="applyProjDataOptions"
            cssClasses="height-style" />
        </ChartPart>

        <ChartPart :chartTitle="$t('应用APP个数分布')">
          <Chart v-bkloading="{ isLoading: loading.isAppLoading }"
            :isShowContent="applyAppData.labels.length"
            :height="'380px'"
            :chartData="applyAppData"
            :chartType="'bar'"
            :options="applyAppDataOptions"
            cssClasses="height-style" />
        </ChartPart>
      </div>
    </HeaderType>
  </div>
</template>

<script lang="ts" src="./DataRange.ts"></script>

<style lang="scss" scoped></style>

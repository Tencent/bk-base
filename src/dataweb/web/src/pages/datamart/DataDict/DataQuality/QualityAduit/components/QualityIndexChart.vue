

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
    <bkdata-dialog v-model="dialogSetting.isShow"
      width="1200"
      :draggable="false"
      :showFooter="false"
      theme="primary"
      @cancel="cancel">
      <div slot="tools"
        v-bkloading="{ isLoading }"
        class="container">
        <section class="header">
          当前时段（{{ momentStartTime }} ~ {{ momentEndTime }}）内该事件相关指标的详情
        </section>
        <div v-if="plotlyChartData.length"
          class="chart-part clearfix">
          <PlotlyChart v-for="(child, index) in plotlyChartData"
            :key="index"
            :chartData="child"
            :chartConfig="plotlyChartConfig"
            :chartLayout="plotlyLayout[index]" />
          <!-- <chart
                            :isLoading="isLoading"
                            :chartData="chartData[child]"
                            :chartType="'line'"
                            :options="chartOptions[index]"
                            width="550px"
                        /> -->
        </div>
        <NoData v-else />
      </div>
    </bkdata-dialog>
  </div>
</template>

<script lang="ts" src="./QualityIndexChart.ts"></script>

<style lang="scss" scoped>
.container {
  min-height: 300px;
  .header {
    line-height: 42px;
    height: 42px;
    text-indent: 2em;
    background-color: rgb(245, 245, 245);
  }
  .chart-part {
    display: grid;
    grid-template-columns: 450px 450px;
    justify-content: space-around;
    grid-gap: 35px;
    padding: 20px;
    > div {
      float: left;
    }
  }
}
</style>

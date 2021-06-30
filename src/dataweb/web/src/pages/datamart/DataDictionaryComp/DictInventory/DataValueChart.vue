

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
  <div class="data-value-container">
    <ValueCost ref="valueCost" />
    <DataHeat ref="dataHeat" />
    <DataRange ref="dataRange"
      :commonChartOptions="commonChartOptions" />
    <DataImportance ref="dataImportance" />
    <DictOveriew ref="dictOveriew" />
  </div>
</template>

<script lang="ts" src="./DataValueChart.ts"></script>

<style lang="scss" scoped>
::v-deep .overiew-container {
  margin-top: 20px;
}
::v-deep .bubble-class {
  max-width: 450px;
  height: 400px;
}
::v-deep .pt40 {
  padding-top: 40px;
}
::v-deep .height-style {
  height: 100%;
}
::v-deep .chart-container {
  display: grid !important;
  grid-template-columns: 450px 450px 450px;
  justify-content: space-around;
  grid-gap: 15px;
  .part-title {
    font-size: 14px;
    color: #63656e;
    line-height: 46px;
    text-indent: 2em;
    font-weight: 550;
  }
  .chart-width-table {
    display: flex;
    flex-wrap: nowrap;
  }
}
@media screen and (max-width: 1678px) {
  ::v-deep .chart-container {
    grid-template-columns: 600px 600px;
  }
}
@media screen and (min-width: 1678px) and (max-width: 1800px) {
  ::v-deep .chart-container {
    grid-template-columns: 480px 480px 480px;
  }
}
@media screen and (min-width: 1800px) and (max-width: 1900px) {
  ::v-deep .chart-container {
    grid-template-columns: 520px 520px 520px;
  }
}
@media screen and (min-width: 1900px) {
  ::v-deep .chart-container {
    grid-template-columns: 540px 540px 540px;
  }
}
@media screen and (max-width: 1500px) {
  ::v-deep .chart-container {
    grid-template-columns: 530px 530px;
  }
}
</style>

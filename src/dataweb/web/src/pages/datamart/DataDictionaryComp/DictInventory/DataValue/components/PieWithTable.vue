

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
  <div v-bkloading="{ isLoading }"
    class="wrapper">
    <div class="left-part">
      <div class="chart-wrapper">
        <div v-if="isShowPie"
          class="chart-inner-text">
          <span class="text-overflow"
            :title="`${tranNumber(innerText)}${$t('个')}`">
            {{ tranNumber(innerText) }}{{ $t('个') }}
          </span>
          <p>{{ $t('数据表总数') }}</p>
        </div>
        <Chart :isShowContent="isShowPie"
          :chartData="pieChartData"
          :width="'160px'"
          :chartType="'doughnut'"
          :options="chartOptions" />
      </div>
    </div>
    <div class="right-part">
      <div class="right-title">
        最近7日 | 详情分布
      </div>
      <bkdata-table extCls="custom-table-style"
        :data="valueTrendData">
        <bkdata-table-column width="80"
          :label="$t('价值等级')"
          prop="bk_inst_name">
          <template slot-scope="{ row }">
            <div class="value-level-container"
              :title="row.tips">
              <span class="color-square"
                :style="{ backgroundColor: chartColors[row.index] }" />
              {{ row.level }}
            </div>
          </template>
        </bkdata-table-column>
        <bkdata-table-column :className="'column-align'"
          :width="130"
          :label="$t('数据个数（占比）')"
          :showOverflowTooltip="true">
          <div slot-scope="{ row }"
            class="data-count-container">
            <span class="data-count text-overflow">
              {{ row.nums }}
            </span>
            <span class="data-percent text-overflow"> （{{ row.percent }}） </span>
          </div>
        </bkdata-table-column>
        <bkdata-table-column :className="'column-style'"
          :label="$t('变化趋势')"
          :minWidth="80">
          <template slot-scope="{ row }">
            <Chart :height="'60px'"
              cssClasses="trend-chart-class"
              :chartData="row.trendData"
              :chartType="'line'"
              :options="row.options" />
          </template>
        </bkdata-table-column>
      </bkdata-table>
    </div>
  </div>
</template>

<script lang="ts" src="./PieWithTable.ts"></script>

<style lang="scss" scoped>
::v-deep .data-count-container {
  width: 100%;
  display: flex;
  align-items: center;
  justify-content: space-between;
}
::v-deep .custom-table-style {
  overflow: inherit;
  .bk-table-body-wrapper {
    overflow: inherit;
  }
}
::v-deep .column-style {
  .cell {
    overflow: inherit;
    padding: 0;
  }
}
::v-deep .column-align {
  .cell {
    display: flex;
    justify-content: space-between;
    text-align: right;
    .data-count {
      max-width: 35px;
    }
    .data-percent {
      max-width: 65px;
    }
  }
}
.wrapper {
  position: relative;
  width: 100%;
  height: 100%;
  display: flex;
  flex-wrap: nowrap;
  align-items: center;
  justify-content: space-between;
  .chart-title {
    position: absolute;
    left: 0;
    top: 0;
    right: 0;
    height: 40px;
    line-height: 40px;
    color: #666;
    font-size: 14px;
    text-align: center;
  }
  .right-title {
    height: 36px;
    line-height: 36px;
    font-size: 12px;
    color: #737987;
  }
  .left-part {
    position: relative;
    display: flex;
    width: 160px;
    height: 100%;
    .chart-wrapper {
      position: relative;
      display: flex;
      align-items: center;
      width: 100%;
      .chart-inner-text {
        position: absolute;
        width: 100%;
        text-align: center;
        font-weight: 550;
        font-size: 12px;
        z-index: 1;
        .text-overflow {
          display: inline-block;
          width: 55px;
        }
      }
    }
  }
  .right-part {
    width: calc(100% - 160px);
  }
  .trend-chart-class {
    margin: 0 -15px;
  }
  .value-level-container {
    display: flex;
    align-items: center;
    justify-content: space-around;
    .color-square {
      display: inline-block;
      width: 20px;
      height: 20px;
      background: yellow;
    }
  }
}
</style>

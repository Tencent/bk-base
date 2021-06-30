

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
    <HeaderType :title="$t('质量事件')">
      <div class="container">
        <div class="time-pick clearfix mt10">
          <bkdata-button :theme="'primary'"
            :title="$t('刷新')"
            class="fr"
            @click="handleTimePick">
            {{ $t('刷新') }}
          </bkdata-button>
          <bkdata-date-picker v-model="timeRange"
            class="mr15 fr"
            :placeholder="$t('选择日期范围')"
            :type="'datetimerange'" />
        </div>
        <div v-bkloading="{ isLoading: isChartLoading }"
          class="chart-part clearfix">
          <div class="pie-chart">
            <chart
              v-if="pieData.datasets.length"
              :isLoading="isChartLoading"
              :chartData="pieData"
              :chartType="'doughnut'"
              :options="{
                responsive: true,
                plugins: {
                  title: {
                    display: true,
                    text: $t('审核规则对应的事件个数'),
                    fontSize: 14,
                    padding: 30,
                  },
                  legend: {
                    position: 'right',
                  },
                }
              }"
              width="400px" />
            <NoData v-else />
          </div>
          <div class="bar-chart">
            <chart v-if="barData.datasets.length"
              :isLoading="isChartLoading"
              :chartData="barData"
              :chartType="'bar'"
              :options="barChartOptions"
              width="600px" />
            <NoData v-else />
          </div>
        </div>
        <div class="search-part clearfix">
          <bkdata-input v-model="searchContent"
            extCls="m15-0"
            :placeholder="$t('请输入任意关键进行事件检索')"
            :clearable="true"
            :rightIcon="'bk-icon icon-search'" />
        </div>
        <DataTable :totalData="tableData"
          :isLoading="isTableLoading"
          :calcPageSize.sync="calcPageSize">
          <template slot="content">
            <!-- <bkdata-table-column
                            type="expand"
                            width="30"
                            align="center"
                        >
                            <template slot-scope="{ row }">
                                <DataTable
                                    extCls="expand-table-style"
                                    :totalData="row.eventInstances"
                                    :isLoading="isTableLoading"
                                    :calcPageSize.sync="expendPageSize"
                                >
                                    <template slot="content">
                                        <bkdata-table-column
                                            width="150"
                                            :label="$t('事件发生时间')"
                                            prop="eventTime"
                                        > </bkdata-table-column>
                                        <bkdata-table-column
                                            :min-width="250"
                                            :label="$t('审核指标当前状态')"
                                            prop="eventStatusAlias"
                                        > </bkdata-table-column>
                                        <bkdata-table-column
                                            :min-width="250"
                                            :label="$t('事件详情')"
                                            prop="eventDetail"
                                        ></bkdata-table-column>
                                    </template>
                                </DataTable>
                            </template>
                        </bkdata-table-column> -->
            <bkdata-table-column minWidth="150"
              :label="$t('事件名称')"
              :showOverflowTooltip="true"
              prop="eventAlias" />
            <bkdata-table-column width="90"
              :label="$t('事件描述')"
              :showOverflowTooltip="true"
              prop="description" />
            <bkdata-table-column width="120"
              :label="$t('事件失效（秒）')"
              :showOverflowTooltip="true"
              prop="eventCurrency" />
            <bkdata-table-column width="80"
              :label="$t('事件极性')"
              :showOverflowTooltip="true"
              prop="eventPolarity">
              <div slot-scope="{ row }">
                {{ eventPolarityMap[row.eventPolarity] }}
              </div>
            </bkdata-table-column>
            <bkdata-table-column width="90"
              :label="$t('事件敏感性')"
              :showOverflowTooltip="true">
              <div slot-scope="{ row }">
                {{ eventSensitivityMap[row.sensitivity] }}
              </div>
            </bkdata-table-column>
            <bkdata-table-column width="80"
              :label="$t('规则名称')"
              :showOverflowTooltip="true"
              prop="rule.ruleName" />
            <bkdata-table-column width="230"
              :label="$t('规则内容')"
              :showOverflowTooltip="true"
              prop="rule.ruleConfigAlias" />
            <bkdata-table-column width="80"
              :label="$t('是否告警')"
              :showOverflowTooltip="true">
              <div slot-scope="{ row }">
                {{ row.inEffect ? $t('是') : $t('否') }}
              </div>
            </bkdata-table-column>
            <bkdata-table-column width="200"
              :label="$t('告警方式')"
              :showOverflowTooltip="true"
              prop="object_description">
              <div slot-scope="{ row }"
                class="notify-container">
                <img v-for="(item, index) in getNotifyIcon(row.notifyWays)"
                  :key="index"
                  :src="item.src"
                  :title="item.title">
              </div>
            </bkdata-table-column>
            <bkdata-table-column width="150"
              :label="$t('告警接收人')"
              :showOverflowTooltip="true"
              prop="receivers" />
            <bkdata-table-column width="80"
              :label="$t('更新人')"
              :showOverflowTooltip="true"
              prop="updatedBy" />
            <bkdata-table-column width="150"
              :label="$t('更新时间')"
              prop="updatedAt" />
            <bkdata-table-column fixed="right"
              width="200"
              :label="$t('操作')">
              <template slot-scope="{ row }">
                <operationGroup>
                  <a href="javascript:void(0);"
                    @click.stop="editRuleConfig(row)">
                    {{ $t('编辑') }}
                  </a>
                  <a href="javascript:void(0);"
                    @click.stop="lookForIndex(row)">
                    {{ $t('查看指标') }}
                  </a>
                  <a href="javascript:void(0);"
                    @click.stop="openDialog(row)">
                    {{ $t('查看事件实例') }}
                  </a>
                </operationGroup>
              </template>
            </bkdata-table-column>
          </template>
        </DataTable>
      </div>
    </HeaderType>
    <QualityIndexChart v-if="isShowIndex"
      ref="qualityIndexChart"
      :ruleId="ruleId"
      :startTime="timestampRange[0]"
      :endTime="timestampRange[1]"
      @close="isShowIndex = false" />
    <bkdata-dialog v-model="isShowExpandTable"
      :width="eventInstances.length ? 1050 : 700"
      :showFooter="false"
      @cancel="hiddenDialog">
      <DataTable :totalData="eventInstances"
        :calcPageSize.sync="expendPageSize">
        <template slot="content">
          <bkdata-table-column :showOverflowTooltip="true"
            :width="150"
            :label="$t('事件发生时间')"
            prop="eventTime" />
          <bkdata-table-column :showOverflowTooltip="true"
            :minWidth="250"
            :label="$t('审核指标当前状态')"
            prop="eventStatusAlias" />
          <bkdata-table-column :showOverflowTooltip="true"
            :minWidth="250"
            :label="$t('事件详情')"
            prop="eventDetail" />
        </template>
      </DataTable>
    </bkdata-dialog>
  </div>
</template>

<script lang="ts" src="./QualityEvent.ts"></script>

<style lang="scss" scoped>
.chart-part {
  display: flex;
  flex-wrap: nowrap;
  margin: 15px 0;
  height: 350px;
  .pie-chart,
  .bar-chart {
    display: flex;
    align-items: center;
    border: 1px solid #dfe0e5;
  }
  .pie-chart {
    width: 400px;
    border-right: none;
  }
  .bar-chart {
    display: flex;
    justify-content: center;
    width: calc(100% - 400px);
  }
}
.m15-0 {
  margin: 15px 0;
  float: right;
  width: 210px;
}
.notify-container {
  display: flex;
  align-items: center;
  img {
    width: 25px;
  }
}
::v-deep .bk-table-small {
  .bk-table {
    .bk-table-header-wrapper {
      thead > tr > th {
        font-size: 12px;
      }
      .bk-table-header {
        th {
          height: 42px;
          background-color: #fafbfd !important;
          .cell {
            height: 42px;
            line-height: 42px;
          }
        }
      }
    }

    .bk-table-body-wrapper {
      .bk-table-row {
        td {
          height: 42px;
        }
      }
    }
  }
}
::v-deep .expand-table-style {
  width: 1035px;
  // max-height: 463px;
  // .bk-table-body-wrapper {
  //     max-height: 420px;
  //     overflow: scroll;
  //     overflow-x: hidden;
  // }
}
</style>

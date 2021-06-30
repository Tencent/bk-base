

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
  <div class="index-design-layout">
    <div class="index-design">
      <div class="left-tree-menu">
        <IndexTree ref="indexTree"
          :data="treeData"
          @on-selected-node="handleNodeClick" />
      </div>
      <div class="right-table bk-scroll-y"
        @scroll="scrollEvent">
        <div class="table-name-container">
          <div class="table-name">
            <span :class="[getIcon(tableInfoValueMap.type)]" />
            {{ tableInfoData.displayName }}
            <template v-if="tableInfoValueMap.type === 'indicator'">
              <a
                v-if="tableInfoData.data.existedInStage"
                href="javascript:void(0);"
                style="font-size: 12px; line-height: 24px;"
                :title="$t('去编辑')"
                @click="handleGoEditIndexDesign('indicator', tableInfoData.data)">
                {{ $t('去编辑') }}
              </a>
              <a
                v-else
                v-bk-tooltips="$t('该指标在草稿中已被删除')"
                href="javascript:void(0);"
                style="font-size: 12px; line-height: 24px;"
                class="bk-button-normal bk-button-text is-disabled">
                {{ $t('去编辑') }}
              </a>
            </template>
          </div>
          <div v-if="tableInfoValueMap.type === 'calculation_atom'"
            class="indicator-table-explain">
            聚合逻辑：<span class="bold-color">{{ tableInfoData.data.calculationFormulaStrippedComment }}</span>
            <span class="delimiter">|</span>更新人：<span class="bold-color">{{ tableInfoData.data.updatedBy }}</span>
            <span class="delimiter">|</span>更新时间：<span class="bold-color">
              {{
                tableInfoData.data.updatedAt
              }}
            </span>
          </div>
          <DataDetailFold
            v-if="tableInfoValueMap.type === 'indicator'"
            :data-list="indicatorDetailData"
            :isAlwaysOpen="true"
            @on-change-status="handleChangeStatus" />
        </div>
        <template v-if="tableInfoValueMap.type === 'master_table'">
          <div class="index-table">
            <bkdata-table
              key="master_table_key"
              v-bkloading="{ isLoading }"
              :data="calculationAtomTableData"
              :maxHeight="660">
              <bkdata-table-column
                label="指标统计口径名称"
                prop="calculationAtomName"
                :showOverflowTooltip="true"
                :minWidth="200">
                <template slot-scope="{ row }">
                  <div class="index-container">
                    <span class="bk-icon icon-statistic-caliber" />
                    <div class="index-name">
                      <a
                        href="javascript:void(0);"
                        class="index-en-name text-overflow"
                        @click="goToCalculationAtomDetail(row)">
                        {{ row.calculationAtomName }}
                      </a>
                      <div class="index-zh-name text-overflow">
                        {{ row.calculationAtomAlias }}
                      </div>
                    </div>
                  </div>
                </template>
              </bkdata-table-column>
              <bkdata-table-column label="聚合逻辑"
                :showOverflowTooltip="true"
                :minWidth="150">
                <template slot-scope="{ row }">
                  {{ row.calculationFormulaStrippedComment || '--' }}
                </template>
              </bkdata-table-column>
              <bkdata-table-column label="指标数量"
                :showOverflowTooltip="true"
                :width="100">
                <template slot-scope="{ row }">
                  <a class="index-num-right"
                    href="javascript:void(0);"
                    @click="handleIndexDetail(row)">
                    {{ row.indicatorCount }}
                  </a>
                </template>
              </bkdata-table-column>
              <bkdata-table-column label="更新人"
                prop="updatedBy"
                :showOverflowTooltip="true"
                width="130" />
              <bkdata-table-column label="更新时间"
                prop="updatedAt"
                :showOverflowTooltip="true"
                width="180" />
              <bkdata-table-column label="操作"
                width="180"
                fixed="right">
                <template slot-scope="{ row }">
                  <a
                    v-if="row.existedInStage"
                    href="javascript:void(0);"
                    :title="$t('去编辑')"
                    @click="handleGoEditIndexDesign('calculationAtom', row)">
                    {{ $t('去编辑') }}
                  </a>
                  <a
                    v-else
                    v-bk-tooltips="$t('该统计口径在草稿中已被删除')"
                    href="javascript:void(0);"
                    class="bk-button-normal bk-button-text is-disabled">
                    {{ $t('去编辑') }}
                  </a>
                </template>
              </bkdata-table-column>
            </bkdata-table>
          </div>
        </template>
        <!-- <template v-else-if="['calculation_atom', 'indicator'].includes(tableInfoValueMap.type)"> -->
        <template v-else-if="['calculation_atom'].includes(tableInfoValueMap.type)">
          <div class="index-table">
            <bkdata-table
              key="other_table_key"
              v-bkloading="{ isLoading: isIndexLoading }"
              :data="indexTableData"
              :maxHeight="maxHeight">
              <bkdata-table-column :label="$t('指标')"
                :showOverflowTooltip="true"
                prop="indicatorName"
                :minWidth="200">
                <div slot-scope="{ row }"
                  class="index-container"
                  min-width="300">
                  <span class="bk-icon icon-quota" />
                  <div class="index-name">
                    <a
                      href="javascript:void(0);"
                      class="index-en-name text-overflow"
                      @click="goToIndexDetail(row.indicatorName)">
                      {{ row.indicatorName }}
                    </a>
                    <div class="index-zh-name text-overflow">
                      {{ row.indicatorAlias }}
                    </div>
                  </div>
                </div>
              </bkdata-table-column>
              <bkdata-table-column
                :label="$t('聚合字段')"
                prop="aggregationFieldsStr"
                :showOverflowTooltip="true"
                :minWidth="120" />
              <bkdata-table-column :label="$t('过滤条件')"
                :showOverflowTooltip="true"
                :minWidth="115">
                <template slot-scope="{ row }">
                  {{ row.filterFormulaStrippedComment || '--' }}
                </template>
              </bkdata-table-column>
              <bkdata-table-column
                :label="$t('窗口配置')"
                prop="indicatorCount"
                :showOverflowTooltip="true"
                width="260">
                <div slot-scope="{ row }"
                  class="window-config-container">
                  <span class="mb5">{{ windowTypeMap[row.schedulingContent.windowType] }}</span>
                  <div class="group-list">
                    <span class="group-item-container">
                      <span class="config-name">统计频率：</span>
                      <div>
                        <span class="window-length-value">{{ row.schedulingContent.countFreq }}</span>
                        <span class="config-value-unit">
                          {{
                            row.schedulingContent.schedulePeriod
                              ? unitMap[getUnitFirstWord(row.schedulingContent.schedulePeriod)]
                              : $t('秒')
                          }}
                        </span>
                      </div>
                    </span>
                    <span class="group-item-container">
                      <span class="config-name">窗口长度：</span>
                      <div>
                        <span class="window-length-value">{{ row.schedulingContent.formatWindowSize }}</span>
                        <span class="config-value-unit">{{ unitMap[row.schedulingContent.formatWindowSizeUnit] }}</span>
                      </div>
                    </span>
                  </div>
                </div>
              </bkdata-table-column>
              <bkdata-table-column label="更新人"
                prop="updatedBy"
                :showOverflowTooltip="true"
                width="95" />
              <bkdata-table-column label="更新时间"
                prop="updatedAt"
                :showOverflowTooltip="true"
                width="155" />
              <bkdata-table-column label="操作"
                width="110"
                fixed="right">
                <template slot-scope="{ row }">
                  <a
                    v-if="row.existedInStage"
                    href="javascript:void(0);"
                    :title="$t('去编辑')"
                    @click="handleGoEditIndexDesign('indicator', row)">
                    {{ $t('去编辑') }}
                  </a>
                  <a
                    v-else
                    v-bk-tooltips="$t('该指标在草稿中已被删除')"
                    href="javascript:void(0);"
                    class="bk-button-normal bk-button-text is-disabled">
                    {{ $t('去编辑') }}
                  </a>
                </template>
              </bkdata-table-column>
            </bkdata-table>
          </div>
        </template>
      </div>
    </div>
  </div>
</template>
<script lang="tsx" src="./IndexDesignView.tsx"></script>
<style lang="scss" scoped>
.index-design-layout {
  height: 782px;
}
.data-detail-container {
  margin-left: 34px;
}
::v-deep .operation-group {
  display: flex;
  align-items: center;
}
.window-config-container {
  display: flex;
  flex-direction: column;
  justify-content: center;
  .group-list {
    display: flex;
    align-items: center;
  }
  .group-item-container {
    display: flex;
    align-items: center;
    min-width: 100px;
    margin-right: 5px;
    .config-name {
      color: #979ba5;
    }
    .config-value {
      float: right;
      text-align: right;
    }
  }
}
.index-design {
  display: flex;
  flex-wrap: nowrap;
  height: 100%;
  background-color: white;
  .left-tree-menu {
    width: 420px;
    padding-top: 20px;
    background: #fcfdff;
    border-right: 1px solid #dfe0e5;
  }
  .right-table {
    width: calc(100% - 420px);
    padding: 30px;
    background-color: white;
    .table-name-container {
      .table-name {
        text-align: left;
        font-size: 16px;
        color: #313238;
        display: flex;
        align-items: center;
        [class^='icon-'] {
          font-size: 24px;
          margin-right: 10px;
          color: #979ba5;
        }
        .option-icon {
          font-size: 16px;
          color: #979ba5;
          cursor: pointer;
        }
        .icon-edit-big {
          margin-right: 8px;
        }
      }
      .indicator-table-explain {
        font-size: 12px;
        text-align: left;
        margin-top: 14px;
        color: #979ba5;
        padding-left: 34px;
        color: #979ba5;
        .bold-color {
          color: #63656e;
        }
        .delimiter {
          color: #c4c6cc;
          margin: 0 10px;
        }
      }
      ::v-deep .operate-container {
        padding: 20px 0;
        display: flex;
        flex-wrap: nowrap;
        justify-content: space-between;
        align-content: center;
        margin-top: 20px;
        border-top: 1px solid #f0f1f5;
        .width500 {
          width: 44%;
          max-width: 500px;
        }
        .bk-button {
          div {
            margin-left: -8px;
          }
        }
      }
    }
  }
}
.index-container {
  font-size: 12px;
  display: flex;
  justify-content: flex-start;
  align-items: flex-start;
  padding: 5px 0;
  .bk-icon {
    line-height: 26px;
    margin-right: 6px;
    font-size: 16px;
    color: #c4c6cc;
  }
  .index-name {
    width: 100%;
    font-size: 12px;
    text-align: left;
    color: #63656e;
    line-height: 20px;
    .index-en-name {
      display: block;
    }
    .index-zh-name {
      color: #979ba5;
    }
  }
}
.index-num-right {
  display: inline-block;
  width: 100%;
  text-align: right;
  padding-right: 23px;
}
.index-table {
  margin-top: 20px;
  padding-top: 16px;
  border-top: 1px solid #f0f1f5;
  ::v-deep table {
    .bk-table-row td {
      height: 55px;
    }
  }
}
</style>

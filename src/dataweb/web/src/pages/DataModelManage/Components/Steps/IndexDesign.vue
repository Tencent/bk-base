

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
  <div class="height100">
    <div class="index-design">
      <div class="left-tree-menu">
        <IndexTree
          ref="indexTree"
          v-bkloading="{ isLoading }"
          :data="treeData"
          :showAddBtns="true"
          @on-selected-node="handleNodeClick"
          @on-add-caliber="changeIndexType"
          @on-add-indicator="changeIndexType" />
      </div>
      <div class="right-table bk-scroll-y"
        @scroll="scrollEvent">
        <div class="table-name-container"
          :style="{ top: `${headerTop}px` }">
          <div class="table-name">
            <span :class="[getIcon(tableInfoValueMap.type)]" />
            {{ tableInfoData.displayName }}
            <template v-if="tableInfoValueMap.type !== 'master_table'">
              <span
                v-bk-tooltips="getTableInfoEditTips"
                :title="$t('编辑')"
                :class="[
                  {
                    disabled: tableInfoValueMap.type === 'calculation_atom' && !tableInfoData.data.editable,
                  },
                ]"
                class="option-icon icon-edit-big"
                @click="editOption" />
              <span
                v-bk-tooltips="getTableInfoDeleTips"
                :class="[{ disabled: isCalculationDelete || isIndicatorDelete }]"
                :title="$t('删除')"
                class="option-icon icon-delete"
                @click="deleteIndex" />
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
            ref="detailsFold"
            :isAlwaysOpen="true"
            :data-list="indicatorDetailData"
            @on-change-status="handleChangeStatus" />
          <!-- <div
                        class="operate-container"
                        v-if="['master_table', 'calculation_atom', 'indicator'].includes(tableInfoValueMap.type)"
                    > -->
          <div v-if="['master_table', 'calculation_atom'].includes(tableInfoValueMap.type)"
            class="operate-container">
            <bkdata-button
              v-if="tableInfoValueMap.type === 'master_table'"
              theme="primary"
              :title="addButtonText"
              icon="plus"
              class="mr10"
              @click="openAddCaliber">
              {{ addButtonText }}
            </bkdata-button>
            <bkdata-button
              v-else-if="['calculation_atom', 'indicator'].includes(tableInfoValueMap.type)"
              theme="primary"
              :title="addButtonText"
              icon="plus"
              class="mr10"
              @click="addIncatior">
              {{ addButtonText }}
            </bkdata-button>
            <bkdata-input
              v-model.trim="searchText"
              extCls="width500"
              clearable
              :placeholder="searchPlaceholder"
              :rightIcon="'bk-icon icon-search'"
              @change="searchTextChange" />
          </div>
        </div>
        <template v-if="tableInfoValueMap.type === 'master_table'">
          <bkdata-table
            key="master_table_key"
            v-bkloading="{ isLoading }"
            extCls="index-table"
            :data="calculationAtomTableData"
            :maxHeight="appHeight - 421">
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
                      @click="goToCalculationAtomDetail(row.calculationAtomName)">
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
                  v-bk-tooltips="getCalculationAtomsEditTips(row.editable ? '' : $t('该指标统计口径引用自数据集市'))"
                  class="mr10"
                  href="javascript:void(0);"
                  :class="[{ disabled: !row.editable }]"
                  :title="$t('编辑')"
                  @click="calculationAtomDetail(row)">
                  {{ $t('编辑') }}
                </a>
                <a class="mr10"
                  href="javascript:void(0);"
                  :title="$t('新增指标')"
                  @click="addIndex(row)">
                  {{ $t('新增指标') }}
                </a>
                <a
                  v-bk-tooltips="getCalculationAtomsTips(row)"
                  href="javascript:void(0);"
                  :class="[{ disabled: !row.deletable }]"
                  :title="$t('删除')"
                  @click="handleDeleteCalculation(row.calculationAtomName, row.deletable)">
                  {{ $t('删除') }}
                </a>
              </template>
            </bkdata-table-column>
          </bkdata-table>
        </template>
        <!-- <template v-else-if="['calculation_atom', 'indicator'].includes(tableInfoValueMap.type)"> -->
        <template v-else-if="['calculation_atom'].includes(tableInfoValueMap.type)">
          <bkdata-table
            key="other_key"
            v-bkloading="{ isLoading: isIndexLoading }"
            extCls="index-table"
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
            <bkdata-table-column :label="$t('窗口配置')"
              prop="indicatorCount"
              :showOverflowTooltip="true"
              width="270">
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
                <a class="mr10"
                  href="javascript:void(0);"
                  :title="$t('编辑')"
                  @click="editIndicator(row)">
                  {{ $t('编辑') }}
                </a>
                <!-- <a href="javascript:void(0);" @click="addIndex(row.calculationAtomName)">{{$t('新增指标')}}</a> -->
                <a
                  v-bk-tooltips="handleDeleteIndicatorTips(row)"
                  href="javascript:void(0);"
                  :class="[{ disabled: !row.deletable }]"
                  :title="$t('删除')"
                  @click="handleDeleteIndicator(row.indicatorName, row.deletable)">
                  {{ $t('删除') }}
                </a>
              </template>
            </bkdata-table-column>
          </bkdata-table>
        </template>
      </div>
    </div>
    <!-- 指标统计口径侧边栏 -->
    <AddCaliber
      ref="addCaliber"
      :aggregationLogicList="aggregationLogicList"
      :isAllLoading="isAllLoading"
      :addMethod="addMethod"
      :calculationAtomDetailData="calculationAtomDetailData"
      @sendReport="sendUserActionData({ name: '确定【指标统计口径】' })"
      @reFresh="getCalculationAtoms"
      @reSetData="calculationAtomDetailData = {}"
      @updatePublishStatus="handleUpdatePublishStatus" />
    <!-- 创建、编辑指标侧边栏 -->
    <AddIndicator
      ref="addIndicator"
      :indexInfo="calculationAtomProp"
      :calculationAtomsList="calculationAtomsInfo.results"
      @sendReport="sendUserActionData({ name: '确定【指标】' })"
      @closeSlider="isAddIndicator = false"
      @updateIndexList="updateIndexInfo"
      @updatePublishStatus="handleUpdatePublishStatus" />
  </div>
</template>
<script lang="tsx" src="./IndexDesign.tsx"></script>
<style lang="scss" scoped>
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
.height100 {
  height: 100%;
  overflow: hidden;
}
.index-design {
  display: flex;
  flex-wrap: nowrap;
  height: calc(100% + 1px);
  background-color: white;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.16);
  .left-tree-menu {
    width: 420px;
    text-align: left;
    padding-top: 20px;
    background: #fcfdff;
    border-right: 1px solid #dfe0e5;
  }
  .right-table {
    position: relative;
    width: calc(100% - 420px);
    padding: 22px 32px 0;
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
  ::v-deep table {
    &.bk-table-header th .cell {
      height: 41px;
    }
    .bk-table-row td {
      height: 55px;
    }
  }
}
</style>

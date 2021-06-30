

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
  <div class="biz-diff-contents-layout">
    <DiffContents
      class="diff-wrapper"
      :data="renderDiffList"
      :diffResult="diffResult"
      :onlyShowDiff.sync="localOnlyShowDiff"
      :isLoading.sync="localIsLoading"
      :showDiffResult="showResult">
      <span slot="before-verison-desc">
        <slot name="before-verison-desc">【源版本】{{ versionInfo }}</slot>
      </span>
      <span slot="after-verison-desc"><slot name="after-verison-desc">【发布版本】</slot></span>

      <template slot="before-collapse-title"
        slot-scope="{ data }">
        <template v-if="data.originContent.collapseTitle === '--'">
          --
        </template>
        <template v-else-if="data.originContent.objectType === 'model'">
          <span class="custom-collapse-title">
            【{{ $t('数据模型') }}】
            <span class="name">{{ data.originContent.modelName }}</span>
          </span>
        </template>
        <template v-else-if="data.originContent.objectType === 'master_table'">
          <span class="custom-collapse-title">
            【{{ $t('主表') }}】
            <span class="name">{{ data.originContent.modelName }}</span>
          </span>
        </template>
        <template v-else-if="data.originContent.objectType === 'calculation_atom'">
          <span class="custom-collapse-title">
            【{{ $t('指标统计口径') }}】
            <span class="name">
              {{
                `${data.originContent.calculationAtomName} (${data.originContent.calculationAtomAlias})`
              }}
            </span>
          </span>
        </template>
        <template v-else-if="data.originContent.objectType === 'indicator'">
          <span class="custom-collapse-title">
            【{{ $t('指标') }}】
            <span class="name">{{ `${data.originContent.indicatorName} (${data.originContent.indicatorAlias})` }}</span>
          </span>
        </template>
      </template>
      <template slot="after-collapse-title"
        slot-scope="{ data }">
        <template v-if="data.newContent.collapseTitle === '--'">
          --
        </template>
        <template v-else-if="data.newContent.objectType === 'model'">
          <span class="custom-collapse-title">
            【{{ $t('数据模型') }}】
            <span class="name">{{ data.newContent.modelName }}</span>
          </span>
        </template>
        <template v-else-if="data.newContent.objectType === 'master_table'">
          <span class="custom-collapse-title">
            【{{ $t('主表') }}】
            <span class="name">{{ data.newContent.modelName }}</span>
            <div class="diff-result">
              <span class="result-text">{{ $t('主表对比结果') }}：</span>
              <span class="result-item">
                <i class="icon-block" />
                <span class="result-item-text">{{ $t('新增') }}</span>
                <span class="result-item-num">{{ tableDiffResult.create || 0 }}</span>
              </span>
              <span class="result-item item-delete">
                <i class="icon-block" />
                <span class="result-item-text">{{ $t('删除') }}</span>
                <span class="result-item-num">{{ tableDiffResult.delete || 0 }}</span>
              </span>
              <span class="result-item item-update">
                <i class="icon-block" />
                <span class="result-item-text">{{ $t('更新') }}</span>
                <span class="result-item-num">{{ tableDiffResult.update || 0 }}</span>
              </span>
              <span class="result-item item-update mr0">
                <i class="icon-arrows-v" />
                <span class="result-item-text">{{ $t('顺序调整') }}</span>
                <span class="result-item-num">{{ tableDiffResult.fieldIndexUpdate || 0 }}</span>
              </span>
            </div>
          </span>
        </template>
        <template v-else-if="data.newContent.objectType === 'calculation_atom'">
          <span class="custom-collapse-title">
            【{{ $t('指标统计口径') }}】
            <span class="name">
              {{
                `${data.newContent.calculationAtomName} (${data.newContent.calculationAtomAlias})`
              }}
            </span>
          </span>
        </template>
        <template v-else-if="data.newContent.objectType === 'indicator'">
          <span class="custom-collapse-title">
            【{{ $t('指标') }}】
            <span class="name">{{ `${data.newContent.indicatorName} (${data.newContent.indicatorAlias})` }}</span>
          </span>
        </template>
      </template>

      <!-- 模型基础信息 -->
      <template slot="diff-list-text-tags"
        slot-scope="{ data }">
        <template v-if="!data.tags.length">
          --
        </template>
        <template v-else>
          <div class="tags"
            :class="{ 'is-update': getItemStatus(data, ['tags']) }">
            <template v-for="tag in data.tags">
              <span :key="tag.tagCode"
                class="tag">
                {{ tag.tagAlias }}
              </span>
            </template>
          </div>
        </template>
      </template>

      <!-- 指标、统计口径通用字段 -->
      <template slot="diff-list-text-calculationDisplayName"
        slot-scope="{ data }">
        <span :class="{ 'is-update': getItemStatus(data, ['calculationAtomName', 'calculationAtomAlias']) }">
          {{
            `${data.calculationAtomName} (${data.calculationAtomAlias})`
          }}
        </span>
      </template>
      <template slot="diff-list-text-aggregationFieldsStr"
        slot-scope="{ data }">
        <span :class="{ 'is-update': getItemStatus(data, ['aggregationFields']) }">
          {{
            getAggregationFieldsStr(data)
          }}
        </span>
      </template>
      <template slot="diff-list-text-filterFormula"
        slot-scope="{ data, item }">
        <span :class="{ 'is-update': getItemStatus(data, ['filterFormula']) }">
          <template v-if="!data.filterFormula">--</template>
          <template v-else>
            <bkdata-button class="details-btn"
              text>
              <i class="icon-icon-audit"
                @click="handleShowFilterFormula(data, item)" />
            </bkdata-button>
          </template>
        </span>
      </template>
      <template slot="diff-list-text-schedulingType"
        slot-scope="{ data }">
        <span :class="{ 'is-update': getItemStatus(data, ['schedulingType']) }">
          {{
            dataMap.schedulingType[data.schedulingType] || '--'
          }}
        </span>
      </template>
      <template slot="diff-list-text-schedulingContent.windowType"
        slot-scope="{ data }">
        <span :class="{ 'is-update': getItemStatus(data, ['schedulingContent.windowType']) }">
          {{
            dataMap.windowType[data.schedulingContent.windowType] || '--'
          }}
        </span>
      </template>

      <!-- 指标变动字段 -->
      <template slot="diff-list-text-schedulingContent.windowLateness.allowedLateness"
        slot-scope="{ data }">
        <span
          :class="{
            'is-update': getItemStatus(data, [
              'schedulingContent.windowLateness.allowedLateness',
              'schedulingContent.windowLateness',
            ]),
          }">
          {{ dataMap.booleanMap[data.schedulingContent.windowLateness.allowedLateness] }}
        </span>
        <template v-if="data.schedulingContent.windowLateness.allowedLateness">
          <span class="diff-dividing">|</span>
          <div class="diff-child-item">
            <span class="child-item-label">{{ $t('延迟时间') }}：</span>
            <span
              class="child-item-text"
              :class="{
                'is-update': getItemStatus(data, [
                  'schedulingContent.windowLateness.latenessTime',
                  'schedulingContent.windowLateness',
                ]),
              }">
              {{ dataMap.timeformatter(data.schedulingContent.windowLateness.latenessTime, 'h') }}
            </span>
          </div>
          <span class="diff-dividing">|</span>
          <div class="diff-child-item">
            <span class="child-item-label">{{ $t('统计频率') }}：</span>
            <span
              class="child-item-text"
              :class="{
                'is-update': getItemStatus(data, [
                  'schedulingContent.windowLateness.latenessCountFreq',
                  'schedulingContent.windowLateness',
                ]),
              }">
              {{ dataMap.timeformatter(data.schedulingContent.windowLateness.latenessCountFreq, 'h') }}
            </span>
          </div>
        </template>
      </template>
      <template slot="diff-list-text-schedulingContent.windowTime"
        slot-scope="{ data }">
        <span :class="{ 'is-update': getItemStatus(data, ['schedulingContent.windowTime']) }">
          {{ dataMap.timeformatter(data.schedulingContent.windowTime, 'min') }}
        </span>
      </template>
      <template slot="diff-list-text-schedulingContent.countFreq"
        slot-scope="{ data }">
        <span
          :class="{
            'is-update': getItemStatus(data, ['schedulingContent.countFreq', 'schedulingContent.schedulePeriod']),
          }">
          {{ dataMap.timeformatter(data.schedulingContent.countFreq, data.schedulingContent.schedulePeriod || 's') }}
        </span>
      </template>
      <template slot="diff-list-text-schedulingContent.waitingTime"
        slot-scope="{ data }">
        <span :class="{ 'is-update': getItemStatus(data, ['schedulingContent.waitingTime']) }">
          {{ dataMap.timeformatter(data.schedulingContent.waitingTime, 's') }}
        </span>
      </template>
      <template slot="diff-list-text-schedulingContent.delay"
        slot-scope="{ data }">
        <span :class="{ 'is-update': getItemStatus(data, ['schedulingContent.delay']) }">
          {{ dataMap.timeformatter(data.schedulingContent.delay, 'h') }}
        </span>
      </template>
      <template slot="diff-list-text-schedulingContent.dataStart"
        slot-scope="{ data }">
        <span :class="{ 'is-update': getItemStatus(data, ['schedulingContent.dataStart']) }">
          {{ dataStartList[data.schedulingContent.dataStart] && dataStartList[data.schedulingContent.dataStart].name }}
        </span>
      </template>
      <template slot="diff-list-text-schedulingContent.dataEnd"
        slot-scope="{ data }">
        <span :class="{ 'is-update': getItemStatus(data, ['schedulingContent.dataEnd']) }">
          {{ dataStartList[data.schedulingContent.dataEnd] && dataStartList[data.schedulingContent.dataEnd].name }}
        </span>
      </template>
      <template slot="diff-list-text-schedulingContent.unifiedConfig.windowSize"
        slot-scope="{ data }">
        <span
          :class="{
            'is-update': getItemStatus(data, [
              'schedulingContent.unifiedConfig.windowSize',
              'schedulingContent.unifiedConfig.windowSizePeriod',
              'schedulingContent.unifiedConfig',
            ]),
          }">
          {{
            dataMap.timeformatter(
              data.schedulingContent.unifiedConfig.windowSize,
              data.schedulingContent.unifiedConfig.windowSizePeriod
            )
          }}
        </span>
      </template>
      <template slot="diff-list-text-schedulingContent.unifiedConfig.dependencyRule"
        slot-scope="{ data }">
        <span
          :class="{
            'is-update': getItemStatus(data, [
              'schedulingContent.unifiedConfig.dependencyRule',
              'schedulingContent.unifiedConfig',
            ]),
          }">
          {{ dependencyRule[data.schedulingContent.unifiedConfig.dependencyRule] }}
        </span>
      </template>
      <template slot="diff-list-text-schedulingContent.advanced.recoveryEnable"
        slot-scope="{ data }">
        <span
          :class="{
            'is-update': getItemStatus(data, [
              'schedulingContent.advanced.recoveryEnable',
              'schedulingContent.advanced',
            ]),
          }">
          {{ dataMap.booleanMap[data.schedulingContent.advanced.recoveryEnable] }}
        </span>
        <template v-if="data.schedulingContent.advanced.recoveryEnable">
          <span class="diff-dividing">|</span>
          <div class="diff-child-item">
            <span class="child-item-label">{{ $t('重试次数') }}：</span>
            <span
              class="child-item-text"
              :class="{
                'is-update': getItemStatus(data, [
                  'schedulingContent.advanced.recoveryTimes',
                  'schedulingContent.advanced',
                ]),
              }">
              {{ dataMap.timeformatter(data.schedulingContent.advanced.recoveryTimes, 'h') }}
            </span>
          </div>
          <span class="diff-dividing">|</span>
          <div class="diff-child-item">
            <span class="child-item-label">{{ $t('调度间隔') }}：</span>
            <span
              class="child-item-text"
              :class="{
                'is-update': getItemStatus(data, [
                  'schedulingContent.advanced.recoveryInterval',
                  'schedulingContent.advanced',
                ]),
              }">
              {{ dataMap.timeformatter(parseInt(data.schedulingContent.advanced.recoveryInterval), 'min') }}
            </span>
          </div>
        </template>
      </template>

      <!-- table column -->
      <template slot="prepend-table-column"
        slot-scope="{ itemData }">
        <bkdata-table-column label=""
          width="30"
          className="icon-column-cls"
          :resizable="false">
          <template slot-scope="{ row }">
            <i
              :class="[
                'bk-icon',
                {
                  'icon-left-join': row.isJoinField,
                  'is-update': getItemStatus(row, ['isJoinField', 'relatedModelId', 'relatedFieldName']),
                },
              ]"
              @mouseover.stop="handleShowRelationDiff($event, itemData, row)"
              @mouseleave.stop="handleHideRelationDiff" />
          </template>
        </bkdata-table-column>
      </template>
      <template slot="diff-table-text-fieldName"
        slot-scope="{ data }">
        <span :class="{ 'is-update': getItemStatus(data, ['fieldName']) }">
          {{
            data.fieldName === '__time__' ? '--' : data.fieldName
          }}
        </span>
      </template>
      <template slot="diff-table-text-fieldType"
        slot-scope="{ data }">
        <span :class="{ 'is-update': getItemStatus(data, ['fieldType']) }">
          {{
            data.fieldName === '__time__' ? '--' : data.fieldType
          }}
        </span>
      </template>
      <template slot="diff-table-text-fieldCategory"
        slot-scope="{ data }">
        <span :class="{ 'is-update': getItemStatus(data, ['fieldCategory']) }">
          {{
            data.fieldName === '__time__' ? '--' : fieldCategoryMap[data.fieldCategory]
          }}
        </span>
      </template>
      <template slot="diff-table-text-fieldConstraintContent"
        slot-scope="{ data, item }">
        <span :class="{ 'is-update': getItemStatus(data, ['fieldConstraintContent']) }">
          <template v-if="!data.fieldConstraintContent">--</template>
          <template v-else>
            <i
              class="icon-icon-audit details-btn"
              @mouseover.stop="handleShowContentDiff($event, item, data)"
              @mouseleave.stop="handleHideContentDiff" />
          </template>
        </span>
      </template>
      <template slot="diff-table-text-fieldCleanContent"
        slot-scope="{ data, item }">
        <span :class="{ 'is-update': getItemStatus(data, ['fieldCleanContent']) }">
          <template v-if="!data.fieldCleanContent">--</template>
          <template v-else>
            <bkdata-button class="details-btn"
              text>
              <i class="icon-icon-audit"
                @click.stop="handleShowFieldCleanContent(data, item)" />
            </bkdata-button>
          </template>
        </span>
      </template>
      <template slot="diff-table-text-isPrimaryKey"
        slot-scope="{ data, isBefore }">
        <div class="custom-text">
          <template v-if="data.isPrimaryKey">
            <bkdata-checkbox
              class="mr10"
              :value="data.isPrimaryKey"
              :disabled="true"
              :class="{ 'is-update': getItemStatus(data, ['isPrimaryKey']) }" />
          </template>
          <!-- 空行占位 -->
          <span v-else
            class="checkbox-empty" />
          <template v-if="!isBefore && data.fieldIndexUpdate">
            <i :class="['index-update-icon', `icon-arrows-${data.fieldIndexUpdate}`]" />
          </template>
          <!-- 空行占位 -->
          <span v-else />
        </div>
      </template>
    </DiffContents>

    <!-- 字段加工逻辑diff -->
    <bkdata-dialog v-model="diffCodeDialog.isShow"
      headerPosition="left"
      renderDirective="if"
      :width="1334">
      <div slot="header"
        class="field-clean-content-title">
        {{ diffCodeDialog.title }} <span>- {{ diffCodeDialog.subTitle }}</span>
      </div>
      <div class="field-clean-content bk-scroll-y bk-scroll-x">
        <div class="version-diff">
          <span class="origin">【源版本】{{ versionInfo }}</span>
          <span class="target">【目标版本】</span>
        </div>
        <div class="version-code">
          <bk-diff
            theme="dark"
            format="side-by-side"
            :context="20"
            :oldContent="diffCodeDialog.originCode"
            :newContent="diffCodeDialog.targetCode" />
        </div>
      </div>
      <div slot="footer">
        <bkdata-button theme="default"
          @click="diffCodeDialog.isShow = false">
          {{ $t('关闭') }}
        </bkdata-button>
      </div>
    </bkdata-dialog>

    <!-- diff 值约束 -->
    <popContainer ref="diffConditions"
      class="diff-conditions-layout">
      <div class="origin-conditions">
        <span class="title">{{ $t('变更前') }}</span>
        <div class="contents"
          :class="[diffConditionsContent.cls]">
          <template v-if="!diffConditionsContent.origin">
            --
          </template>
          <template v-else>
            <div
              v-for="(group, index) in diffConditionsContent.origin"
              :key="index + '_group'"
              class="input-condition-group">
              <span v-if="diffConditionsContent.origin.length > 1">(</span>
              <template v-for="(item, childIndex) in group.items">
                <div :key="childIndex + '_item'"
                  class="input-condition-content">
                  <span class="condition-item">{{ item.content }}</span>
                  <span v-if="childIndex < group.items.length - 1"
                    class="input-item-op">
                    {{ item.op }}
                  </span>
                </div>
              </template>
              <span v-if="diffConditionsContent.origin.length > 1">)</span>
              <span v-if="index < diffConditionsContent.origin.length - 1"
                class="input-group-op">
                {{ group.op }}
              </span>
            </div>
          </template>
        </div>
      </div>
      <div class="target-conditions">
        <span class="title">{{ $t('变更后') }}</span>
        <div class="contents"
          :class="[diffConditionsContent.cls]">
          <template v-if="!diffConditionsContent.target">
            --
          </template>
          <template v-else>
            <div
              v-for="(group, index) in diffConditionsContent.target"
              :key="index + '_group'"
              class="input-condition-group">
              <span v-if="diffConditionsContent.target.length > 1">(</span>
              <template v-for="(item, childIndex) in group.items">
                <div :key="childIndex + '_item'"
                  class="input-condition-content">
                  <span class="condition-item">{{ item.content }}</span>
                  <span v-if="childIndex < group.items.length - 1"
                    class="input-item-op">
                    {{ item.op }}
                  </span>
                </div>
              </template>
              <span v-if="diffConditionsContent.target.length > 1">)</span>
              <span v-if="index < diffConditionsContent.target.length - 1"
                class="input-group-op">
                {{ group.op }}
              </span>
            </div>
          </template>
        </div>
      </div>
    </popContainer>

    <!-- diff 关联关系 tips -->
    <popContainer ref="diffRelation"
      class="diff-conditions-layout">
      <div class="origin-conditions">
        <span class="title">{{ $t('变更前') }}</span>
        <div class="contents">
          <div class="relation-item mb5">
            <span class="relation-item-label">{{ $t('关联表') }}：</span>
            <span class="relation-item-text"
              :class="[diffRelationContent.modelCls]">
              {{ diffRelationContent.origin.relatedModelName || '--' }}
            </span>
          </div>
          <div class="relation-item">
            <span class="relation-item-label">{{ $t('关联字段') }}：</span>
            <span class="relation-item-text"
              :class="[diffRelationContent.fieldCls]">
              {{ diffRelationContent.origin.relatedFieldName || '--' }}
            </span>
          </div>
        </div>
      </div>
      <div class="target-conditions">
        <span class="title">{{ $t('变更后') }}</span>
        <div class="contents">
          <div class="relation-item mb5">
            <span class="relation-item-label">{{ $t('关联表') }}：</span>
            <span class="relation-item-text"
              :class="[diffRelationContent.modelCls]">
              {{ diffRelationContent.target.relatedModelName || '--' }}
            </span>
          </div>
          <div class="relation-item">
            <span class="relation-item-label">{{ $t('关联字段') }}：</span>
            <span class="relation-item-text"
              :class="[diffRelationContent.fieldCls]">
              {{ diffRelationContent.target.relatedFieldName || '--' }}
            </span>
          </div>
        </div>
      </div>
    </popContainer>
  </div>
</template>
<script lang="ts" src="./BizDiffContents.ts"></script>
<style lang="scss" scoped>
.biz-diff-contents-layout {
  height: 100%;
  .diff-wrapper {
    height: 100%;
    margin-top: 20px;
    padding: 24px;
    background-color: #ffffff;
    .custom-collapse-title {
      .name {
        font-weight: normal;
      }
      .diff-result {
        float: right;
        display: flex;
        align-items: center;
        font-weight: normal;
        .result-text {
          color: #313238;
          margin-right: 8px;
        }
        .result-item {
          display: flex;
          align-items: center;
          color: #63656e;
          margin-right: 16px;
          &.item-delete {
            .icon-block {
              border-color: #ffd2d2;
              background-color: #ffeded;
            }
            .result-item-num {
              color: #ea3636;
            }
          }
          &.item-update {
            .icon-block {
              border-color: #ffdfac;
              background-color: #fff4e2;
            }
            .result-item-num {
              color: #ff9c01;
            }
          }
          .icon-block {
            width: 12px;
            height: 12px;
            border: 1px solid #c6e2c3;
            background-color: #e8fae6;
          }
          .icon-arrows-v {
            font-size: 14px;
            color: #ff9c01;
            margin-right: -4px;
          }
          .result-item-text {
            padding: 0 5px;
          }
          .result-item-num {
            color: #3fc06d;
            font-weight: bold;
          }
        }
      }
    }
    .is-update {
      color: #ff9c01;
      ::v-deep .bk-checkbox {
        border-color: #ff9c01 !important;
        background-color: #ff9c01 !important;
      }
      .details-btn {
        color: #ff9c01;
      }
      .icon-link-2 {
        color: #ff9c01;
      }
    }
    .tags {
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      &.is-update {
        .tag {
          color: #ff9c01;
          background-color: #ffefd6;
        }
      }
      .tag {
        padding: 0 6px;
        color: #63656e;
        background-color: #f0f1f5;
        margin-right: 6px;
        margin-bottom: 4px;
      }
    }
    .diff-dividing {
      display: inline-block;
      vertical-align: middle;
      padding: 0 16px;
    }
    .child-item-label {
      color: #979ba5;
    }
  }
  .details-btn {
    font-size: 16px;
    color: #979ba5;
  }
}
.field-clean-content-title {
  color: #313238;
  font-size: 20px;
  span {
    color: #63656e;
    font-size: 16px;
  }
}
.field-clean-content {
  max-height: 680px;
  background-color: #292929;
  &.bk-scroll-y {
    overflow-y: auto;
  }
  ::v-deep .d2h-file-wrapper {
    border: none;
    margin-right: 4px;
  }
  .version-diff {
    position: sticky;
    top: 0;
    display: flex;
    align-items: center;
    width: 100%;
    height: 42px;
    line-height: 42px;
    font-size: 12px;
    color: #c4c6cc;
    background-color: #323232;
    z-index: 99;
    span {
      flex: 1 1 50%;
      padding: 0 20px;
      &.target {
        border-left: 1px solid #3d3d3d;
      }
    }
  }
  .version-code {
    position: relative;
  }
}
::v-deep {
  th {
    background-color: #ffffff;
    & :hover {
      background-color: #ffffff;
    }
  }
  td.cell-border-bottom-none {
    border-bottom-color: transparent;
  }
  tr {
    &.is-extended-row {
      background-color: #fafbfd;
    }
    &.is-inner-row {
      background-color: #f0f1f5;
    }
    &.is-hover-row {
      background-color: #f0f1f5;
    }
    &.create-row {
      background-color: #f6fdf5;
      &:hover {
        background-color: #ecf3eb;
      }
    }
    &.update-row {
      background-color: #fffbf3;
      &:hover,
      &.is-hover-row {
        background-color: #f5f1e9;
      }
    }
    &.delete-row {
      background-color: #fff8f8;
      color: #979ba5;
      text-decoration: line-through;
      &:hover {
        background-color: #f5eeee;
      }
    }
  }
  .icon-column-cls {
    .cell {
      padding: 0;
      text-align: right;
    }
    .bk-icon {
      color: #939496;
    }
  }
  .custom-text {
    display: flex;
    align-items: center;
    justify-content: space-between;
    .checkbox-empty {
      width: 16px;
      height: 16px;
      margin-right: 10px;
    }
    .index-update-icon {
      font-size: 14px;
      color: #ff9c01;
      font-weight: bold;
    }
  }
}
.diff-conditions-layout {
  width: 568px;
  display: flex;
  .is-update {
    .input-condition-content .condition-item {
      color: #ff9c01;
      background-color: #ffefd6;
    }
  }
  .origin-conditions,
  .target-conditions {
    position: relative;
    flex: 0 0 50%;
    font-size: 12px;
    padding: 10px 20px 20px 16px;
    .title {
      display: inline-block;
      font-weight: 500;
      color: #63656e;
      margin-bottom: 10px;
    }
  }
  .target-conditions::before {
    content: '';
    position: absolute;
    left: 0;
    top: 0;
    width: 1px;
    height: 100%;
    background-color: #dcdee5;
  }
  .input-condition-group {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
  }
  .input-group-op {
    margin: 0 4px;
  }
  .input-condition-content {
    .condition-item {
      display: inline-block;
      color: #63656e;
      background-color: #e6e8f0;
      padding: 2px 4px;
      border-radius: 2px;
      margin: 0 2px 4px;
    }
  }
  .relation-item {
    display: flex;
    flex-wrap: wrap;
    word-break: break-word;
    .relation-item-label {
      color: #979ba5;
    }
    .relation-item-text {
      flex: 1;
      &.is-update {
        color: #ff9c01;
      }
    }
  }
}
</style>
<style lang="scss">
.diff-conditions-theme {
  padding: 0;
}
</style>

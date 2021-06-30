

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
  <section v-bkloading="{ isLoading: isLocalLoading }"
    class="main-table-design">
    <div class="table-btn-group">
      <bkdata-button
        theme="default"
        :title="$t('加载结果表的结构')"
        icon="load-s"
        class="mr10"
        @click="handleLoadRtTable">
        {{ $t('加载结果表的结构') }}
      </bkdata-button>
      <bkdata-button
        class="associate-btn"
        theme="default"
        :title="$t('关联维度表数据模型')"
        icon="left-join"
        @click="handleReferDimensionTable">
        {{ $t('关联维度表数据模型') }}
      </bkdata-button>
    </div>
    <DragTable
      :data="tableData"
      :isEmpty="!tableData.list"
      :isResizeColumn="true"
      dragHandle=".drag-handle-icon"
      @drag-end="handleDragEnd">
      <DragTableRow
        v-for="(item, rowIndex) in tableData.list"
        :key="`${item.uid || item.id}`"
        :isChild="item.isExtendedField"
        :data="item"
        :class="{ selected: activeFieldItem.index === rowIndex, 'is-group-row': item.isJoinField }"
        :style="{ backgroundColor: item._disabled ? '#F0F1F5' : '' }"
        @mousedown.native="() => handleRowClick(item, rowIndex)">
        <template v-if="item.isJoinField">
          <div class="row-group">
            <template v-for="(col, columnIndex) in columns">
              <DragTableColumn :key="`${item.fieldId}-${columnIndex}`"
                :width="getColumnsWidth(col)">
                <customRender :renderFunc="col.renderFn || renderText"
                  :params="[col, item, rowIndex, columnIndex]" />
              </DragTableColumn>
            </template>
          </div>
          <template v-if="item.extends && item.extends.length">
            <ExtendRows
              class="row-group-child"
              :rows="item.extends"
              :columns="tableData.columns"
              :getColumnsWidth="getColumnsWidth" />
          </template>
        </template>
        <template v-else>
          <template v-for="(col, columnIndex) in columns">
            <DragTableColumn :key="`${item.fieldId}-${columnIndex}`"
              :width="getColumnsWidth(col)">
              <customRender :renderFunc="col.renderFn || renderText"
                :params="[col, item, rowIndex, columnIndex]" />
            </DragTableColumn>
          </template>
        </template>
      </DragTableRow>
      <!-- slot(header-label)需要放到DragTableRow后面，否则拖动的时候index会多1 -->
      <template slot="header-label"
        slot-scope="{ column }">
        <template v-if="column.props === 'isPrimaryKey'">
          <span>{{ column.label }}</span>
          <i class="icon-key-line"
            style="color: #979ba5; font-size: 14px; margin: 1px 0 0 2px" />
        </template>
        <template v-else>
          {{ column.label }}
        </template>
      </template>
    </DragTable>

    <bkdata-sideslider
      :isShow.sync="isLoadRtTableSliderShow"
      :title="$t('加载结果表的结构')"
      :width="941"
      :quickClose="false">
      <div slot="content">
        <LoadRTTable @save-table="handleSaveLoadTable"
          @cancel-table="isLoadRtTableSliderShow = false" />
      </div>
    </bkdata-sideslider>

    <bkdata-sideslider
      :isShow.sync="isReferDimensionTableShow"
      :width="963"
      :quickClose="false"
      :title="$t('关联维度表数据模型')">
      <div slot="content">
        <ReferDimensionTable
          :activeModel="activeModelTabItem"
          :parentMstTableInfo="masterTableInfo"
          :relatedField="relatedField"
          :isEdit="isReferDimensionTableEdit"
          @on-save="handleReferDimensionSave"
          @on-cancel="isReferDimensionTableShow = false"
          @create-model-table="handleCreateNewModelTable" />
      </div>
    </bkdata-sideslider>

    <!---字段加工逻辑---->
    <bkdata-sideslider :isShow.sync="fieldProcessingLogicSlider.isShow"
      :width="941"
      :quickClose="false">
      <div slot="header"
        class="custom-sideslider-title">
        {{ $t('字段加工逻辑') }} <span>- {{ fieldProcessingLogicSlider.title }}</span>
      </div>
      <div slot="content">
        <FieldProcessingLogic
          :activeModel="activeModelTabItem"
          :fieldList="masterTableInfo.fields"
          :field="fieldProcessingLogicSlider.field"
          :editable="fieldProcessingLogicSlider.editable"
          @on-save="handleSaveFieldProcessingLogic"
          @on-cancel="fieldProcessingLogicSlider.isShow = false" />
      </div>
    </bkdata-sideslider>
  </section>
</template>
<script lang="ts" src="./MainTableDesign.ts"></script>
<style lang="scss" scoped>
.main-table-design {
  width: 100%;
  height: 100%;
  text-align: left;
  padding-bottom: 26px;

  .master-table-content {
    min-height: 300px;
  }
  .table-btn-group {
    margin-bottom: 20px;
    ::v-deep .bk-icon {
      margin-left: -4px;
    }
    .associate-btn {
      ::v-deep .icon-left-join {
        font-size: 12px;
        vertical-align: middle;
        margin-bottom: 2px;
        color: #979ba5;
        transform: scale(0.8);
      }
    }
  }
  .column-icon {
    margin-right: 3px;
    color: #c4c6cc;
    font-size: 14px;
  }
  .icon-group {
    color: #c4c6cc;
    font-size: 18px;
    cursor: pointer;
    ::v-deep .opt-icon {
      margin-right: 8px;
      &:hover {
        color: #979ba5;
      }
      &.error {
        color: #ea3636;
      }
    }
  }

  .drag-table {
    height: calc(100% - 54px);
    ::v-deep .drag-table-body {
      height: calc(100% - 44px) !important;

      .drag-table-row.ghost {
        border-top-color: #3a84ff;
      }

      .extend-rows .drag-table-row .input-ellipsis {
        background-color: #fafbfd;
        border-color: #fafbfd;
      }
    }
  }

  ::v-deep .drag-table-column {
    padding-right: 10px;
    display: flex;
    align-items: center;
    flex: 1 1 var(--width);

    &.drag-table-header:first-child {
      padding-left: 10px;
    }

    .join-field-icon {
      position: absolute;
      left: 0;
      top: 50%;
      transform: translateY(-50%);
    }

    > div {
      width: 100%;
    }
    .bkdata-form-control {
      .bkdata-input-text {
        height: 28px;
        line-height: 28px;

        input.bkdata-form-input {
          height: 28px;
          line-height: 28px;
        }
      }
    }

    .pointer-cell {
      cursor: pointer;
      width: 100%;
      height: 28px;
      display: flex;
      align-items: center;
      border-radius: 2px;
      padding: 0 10px;

      &.selected {
        background: #fff;
      }
    }

    .category-selector {
      .bk-select-name {
        padding-left: 29px;
      }
    }

    .category-icon {
      position: absolute;
      top: 50%;
      left: 10px;
      transform: translateY(-50%);
      font-size: 14px;
      color: #c4c6cc;
      z-index: 1;
    }
  }
}
.custom-sideslider-title {
  font-size: 16px;
  color: #313238;
  font-weight: normal;
  span {
    font-size: 12px;
    color: #63656e;
  }
}
</style>

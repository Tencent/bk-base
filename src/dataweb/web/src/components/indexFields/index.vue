

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
  <div class="config-drag-table"
    :style="dragTableStyle">
    <div class="drag-table-header">
      <span>{{ $t('主键顺序') }}</span>
      <span class="sub-header-des">
        （已选<b class="field-count"> {{ indexFieldsLength }} </b>个主键）
      </span>
      <span class="text-overflow"><i class="icon-info tips-icon" /> {{ $t('主键的配置提醒') }}</span>
    </div>
    <DragTable
      :data="indexTableData"
      :showHeader="false"
      :isEmpty="!indexFieldsLength"
      :bodyHeight="bodyHeight"
      dragHandle=".drag-handle-icon"
      @drag-end="handleDragEnd">
      <template v-for="(item, rowIndex) in indexTableData.list">
        <DragTableRow :key="`${item.physical_field}_${rowIndex}`"
          :data-x="rowIndex"
          :data="item">
          <DragTableColumn>{{ item.physical_field }}</DragTableColumn>
          <DragTableColumn :width="`20px`">
            <span v-if="!readonly"
              class="bk-rm-index-field"
              @click="() => handleRemoveIndexField(item)">
              <i class="bk-icon icon-close" />
            </span>
          </DragTableColumn>
        </DragTableRow>
      </template>
    </DragTable>
  </div>
</template>
<script lang="ts" src="./index.ts"></script>
<style lang="scss" scoped>
.config-drag-table {
  background: #f5f6fa;
  padding: 0 15px 15px;

  ::v-deep .drag-table {
    height: auto;
  }

  .drag-table-header {
    height: 42px;
    display: flex;
    align-items: center;

    .sub-header-des {
      font-size: 12px;
      color: #979ba5;
      line-height: 17px;
      height: 17px;
      .field-count {
        color: #3a84ff;
      }
    }
  }

  ::v-deep .drag-table-top {
    border: none;
    background: transparent;
    padding-left: 0;
  }

  ::v-deep .drag-table-view {
    border: none;
    background: transparent;

    .drag-table-row {
      border-top: none;
      height: 32px;
      margin-top: 5px;

      .row-icon {
        line-height: 32px;
        &:hover {
          color: #a3c5fd;
        }
      }

      .drag-table-column {
        height: 32px;
        line-height: 32px;
      }
    }
  }

  .bk-rm-index-field {
    cursor: pointer;
    font-size: 16px;
    &:hover {
      color: #a3c5fd;
    }
  }

  .text-overflow {
    color: #ea3636;
    font-size: 12px;
    i {
      padding: 2px;
    }
  }
}
</style>

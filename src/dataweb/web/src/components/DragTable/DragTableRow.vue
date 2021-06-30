// 表格横行


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
  <div :class="['drag-table-row', { 'unable-drag': !data.isDrag || isChild }]"
    @click="handleRowClick">
    <i :class="`row-icon drag-handle-icon ${data.isDrag ? 'bk-dmodel-data icon-grag-fill' : 'disable-icon'}`" />
    <slot />
  </div>
</template>
<script lang="ts" src="./DragTableRow.ts"></script>
<style lang="scss" scoped>
.drag-table-row {
  display: flex;
  width: 100%;
  height: 42px;
  background-color: #fff;
  border-top: 1px solid #dcdee5;
  position: relative;
  padding-left: 30px;

  &.is-group-row {
    flex-direction: column;
    height: auto;
    background-color: #fafbfd;
    .row-group {
      background-color: #ffffff;
    }
    .drag-handle-icon {
      height: 43px;
      border-bottom: 1px solid #dcdee5;
      background-color: #ffffff;
    }
    &:hover {
      ::v-deep .extend-rows .drag-table-row .bk-form-control {
        input {
          background: #f5f6fa;
        }
      }
    }
    .extend-rows .drag-table-row {
      background-color: #fafbfd;
      ::v-deep .bk-form-control {
        input {
          background: #fafbfd;
        }
      }
      &:not(:last-child) {
        .drag-table-column:last-child {
          position: relative;
          overflow: unset;
          z-index: 2;
          &::after {
            content: '';
            position: absolute;
            bottom: 0;
            left: 0;
            width: 100%;
            height: 1px;
            background-color: #fafbfd;
          }
        }
      }
      ::v-deep .condition-selector-input {
        background-color: transparent;
      }
    }
  }

  .row-group {
    display: flex;
    width: 100%;
  }

  ::v-deep .bk-form-control {
    input {
      border: transparent;
    }
  }

  ::v-deep .bk-select {
    border: transparent;

    i.bk-select-angle {
      display: none;
    }
  }

  &:hover {
    // background: rgba(58, 132, 255, 0.1);

    ::v-deep .bk-form-control {
      input {
        background: #f5f6fa;
        height: 28px;
      }

      &.control-active {
        input {
          border: 1px solid #3a84ff;
        }
      }
    }

    ::v-deep .bk-select {
      background: #f5f6fa;
      height: 28px;
      i.bk-select-angle {
        display: block;
      }

      &.is-focus {
        background: #fff;
        border: 1px solid #3a84ff;
      }
    }

    ::v-deep .drag-table-column {
      .pointer-cell {
        background: #f5f6fa;

        &.selected {
          border: 1px solid #3a84ff;
        }
      }
    }
    ::v-deep .condition-selector-input {
      background: #f5f6fa;
      .input-ellipsis {
        background: #f5f6fa;
        border-color: #f5f6fa;
      }
    }
  }

  .row-icon {
    width: 30px;
    color: #c4c6cc;
    line-height: 42px;
    position: absolute;
    cursor: move;
    left: 0;
  }
  .disable-icon {
    cursor: default;
    border-bottom: 1px solid #fff;
    display: none;
  }
}
.unable-drag {
  cursor: default;
}
</style>

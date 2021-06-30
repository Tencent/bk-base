// 表格竖列


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
  <div class="drag-table-column"
    :style="style">
    <span v-bk-tooltips="text">{{ text }}</span>
    <slot />
  </div>
</template>
<script lang="ts" src="./DragTableColumn.ts"></script>
<style lang="scss" scoped>
.drag-table-column {
  width: 100%;
  height: 42px;
  line-height: 42px;
  display: inline-block;
  text-align: left;
  font-size: 12px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  .row-column-container {
    position: relative;
    // padding-left: 13px;
    width: 100%;

    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;

    &.offset-left13 {
      padding-left: 13px;
    }

    &.text {
      padding-left: 10px;
      &.offset-left13 {
        padding-left: 23px;
      }
    }
    &.is-validate-fail {
      ::v-deep .bk-form-control {
        input {
          border: 1px solid #ea3636;
          background: #fff;
        }
      }

      ::v-deep .bk-select {
        background: #fff;
        border: 1px solid #ea3636;
      }
    }
    ::v-deep {
      .bk-form-input,
      .bk-select-name,
      .bk-select {
        height: 28px;
        line-height: 28px;
      }
    }
    ::v-deep .validate-msg-icon {
      position: absolute;
      right: 5px;
      top: 50%;
      transform: translateY(-50%);
      color: #ea3636;
      cursor: pointer;
    }
  }

  ::v-deep .codition-selector-wrapper {
    .condition-selector-input {
      height: 28px;
      line-height: 26px;
      border-color: transparent;
    }
  }

  ::v-deep .bk-input-text {
    display: flex;
    align-items: center;
  }
}
</style>

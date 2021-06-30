

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
  <div v-bkloading="{ isLoading: isPreviewLoading }"
    class="data-model-preview">
    <div :id="uid"
      class="dataModelManager-preview-canvas" />
  </div>
</template>
<script lang="ts" src="./DataModelPreviewView.ts"></script>

<style lang="scss" scoped>
.data-model-preview {
  height: 100%;
}
</style>

<style lang="scss">
.dataModelManager-preview-canvas {
  height: 100%;
  .bkflow-group-item:not(.splitor) {
    line-height: 24px;
    &:first-child {
      justify-content: flex-end !important;
      span {
        padding: 0 242px;
      }
    }
  }
  .node-wrapper {
    box-shadow: 0 1px 2px 0 rgba(0, 0, 0, 0.16);
    &:hover {
      box-shadow: 0 2px 8px 2px rgba(0, 0, 0, 0.1);
      .toggle-icon {
        display: block !important;
      }
    }
    &.calculation_atom .node-item::before {
      left: 0;
    }
    .type {
      display: block;
      height: 28px;
      line-height: 28px;
      font-size: 12px;
      color: #c4c6cc;
      background-color: #313238;
      padding-left: 20px;
    }
    .node-header {
      display: flex;
      align-items: center;
      padding: 8px 20px 10px;
      color: #dcdee5;
      background-color: #63656e;
      .info {
        flex: 1;
        width: 0;
      }
      .header-icon {
        font-size: 42px;
        margin-right: 12px;
      }
      .info-text {
        display: block;
        line-height: 20px;
      }
      .toggle-icon {
        font-size: 20px;
        display: none;
      }
    }
    .node-item {
      position: relative;
      display: flex;
      align-items: center;
      height: 44px;
      line-height: 44px;
      font-size: 12px;
      color: #63656e;
      padding: 0 20px;
      background-color: #ffffff;
      border-left: 6px solid var(--color);
      border-right: 6px solid var(--color);
      &::before,
      &:first-child::after {
        content: '';
        position: absolute;
        left: -6px;
        width: 320px;
        height: 1px;
        background-color: #f0f1f5;
        z-index: 1;
      }
      &::before {
        top: 0;
      }
      &::after {
        bottom: 0;
      }
      &.collapse-item {
        display: flex;
        align-items: center;
        &::before,
        &::after {
          display: none;
        }
        span {
          flex: 1;
        }
        .item-icon {
          color: #c4c6cc;
        }
        .toggle-icon {
          color: #c4c6cc;
          font-size: 20px;
          display: none;
        }
      }
      .item-icon {
        font-size: 16px;
        margin-right: 20px;
        color: var(--color);
      }
    }
    .expand-node-item {
      border-top: 1px solid #f0f1f5;
      background-color: #f7f8fa;
      .node-item {
        background-color: transparent;
        &::before,
        &:last-child::after {
          display: none;
        }
        &::after {
          width: 268px;
          left: 50%;
          transform: translateX(-50%);
        }
        &.extend-item .item-icon {
          color: #c4c6cc;
        }
      }
    }
  }
}
.bk-data-model-preview {
  .tippy-tooltip {
    padding: 7px 10px 6px;
    font-size: 12px;
  }
}
</style>

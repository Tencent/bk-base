

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
  <div class="single-collapse-layout"
    :class="[collapseThemeCls]">
    <div class="single-collapse-trigger"
      :class="{ 'is-show': isShow }"
      @click="toggle">
      <i class="collapse-arrow bk-icon icon-right-shape" />
      <span class="collapse-text text-overflow">
        <slot name="title">{{ label }}</slot>
      </span>
    </div>
    <bkdata-transition name="collapse">
      <div v-show="isShow"
        class="collapse-content">
        <slot />
      </div>
    </bkdata-transition>
  </div>
</template>
<script lang="ts" src="./SingleCollapse.ts"></script>
<style lang="scss" scoped>
.single-collapse-layout {
  border-radius: 2px;
  border: 1px solid #ebecf0;
  &.collapse-update {
    border-color: #ffdfac;
    .single-collapse-trigger {
      background-color: #fff4e2;
      &.is-show {
        border-bottom-color: #ffdfac;
      }
    }
  }
  &.collapse-create {
    border-color: #c6e2c3;
    .single-collapse-trigger {
      background-color: #e8fae6;
      &.is-show {
        border-bottom-color: #c6e2c3;
      }
    }
  }
  &.collapse-delete {
    border-color: #ffd2d2;
    .single-collapse-trigger {
      background-color: #ffeded;
      &.is-show {
        border-bottom-color: #ffd2d2;
      }
      .collapse-text {
        text-decoration: line-through;
      }
    }
  }
  .single-collapse-trigger {
    display: flex;
    align-items: center;
    height: 40px;
    line-height: 40px;
    padding: 0 14px;
    color: #63656e;
    font-size: 12px;
    border-bottom: 1px solid transparent;
    background-color: #fafbfd;
    cursor: pointer;
    &.is-show {
      border-bottom-color: #ebecf0;
      .collapse-arrow {
        transform: rotate(90deg);
      }
    }
    .collapse-arrow {
      color: #979ba5;
      font-size: 16px;
      transition: transform 0.2s ease-in-out;
    }
    .collapse-text {
      display: inline-block;
      width: 100%;
      padding: 0 4px;
      font-weight: bold;
    }
  }
  .collapse-content {
    height: calc(100% - 40px);
    background-color: #ffffff;
  }
}
</style>

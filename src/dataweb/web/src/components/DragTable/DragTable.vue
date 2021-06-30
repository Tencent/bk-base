// 可拖拽表格


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
  <section class="drag-table">
    <div class="drag-table-top">
      <template v-if="showHeader">
        <template v-for="(item, ind) in syncData.columns">
          <DragTableColumn :key="ind"
            class="drag-table-header"
            :width="item.width || widthNum">
            <slot name="header-label"
              :column="item">
              {{ item.label }}
            </slot>
            <label v-if="item.require"
              class="require-icon">
              *
            </label>
          </DragTableColumn>
        </template>
      </template>
      <slot name="header" />
    </div>
    <div class="drag-table-body bk-scroll-y"
      :style="{ height: bodyHeight }">
      <draggable
        v-if="!isEmpty"
        v-model="syncData.list"
        class="drag-table-view"
        v-bind="dragOptions"
        :handle="dragHandle"
        :move="onMove"
        @start="start"
        @end="handleDragEnd">
        <transition-group type="transition"
          :name="'flip-list'">
          <slot />
        </transition-group>
      </draggable>
      <div v-else
        class="drag-table-empty">
        <EmptyView />
      </div>
    </div>
  </section>
</template>
<script lang="ts" src="./DragTable.ts"></script>
<style lang="scss" scoped>
.drag-table {
  width: 100%;
  height: 100%;
  border-radius: 2px;

  .drag-table-top {
    padding-left: 40px;
    background: #fafbfd;
    color: #313238;
    display: flex;
    border: 1px solid #dcdee5;
    margin-right: 4px;
  }
  .drag-table-body {
    width: 100%;
    color: #939496;
  }
  .flip-list-move {
    transition: transform 0.5s;
  }

  .no-move {
    transition: transform 0s;
  }

  .ghost {
    opacity: 0.5;
    background: #ffffff;
    box-shadow: 0px 1px 5px 0px rgba(0, 0, 0, 0.5);
    border-radius: 2px;
  }

  .drag-table-view {
    width: 100%;
    min-height: 20px;
    line-height: 32px;
    text-align: center;
    background: #ffffff;
    border: 1px solid #dcdee5;
    border-top: 0;
    ::v-deep > span > .drag-table-row:first-child {
      border-top: 0;
    }
  }
  .drag-table-empty {
    width: 100%;
    background: #fff;
    height: 100%;
  }
  .require-icon {
    color: #ea3636;
    margin-left: 3px;
    font-size: 14px;
  }
}
</style>

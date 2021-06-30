

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
  <div v-bk-clickoutside="clickOutside"
    class="select-combination-wrapper">
    <bkdata-select
      v-model="initType"
      extCls="select-combination front-select"
      :style="{ width: width + 'px' }"
      :clearable="false"
      @change="typeChange">
      <bkdata-option v-for="(item, index) in typeList"
        :id="item.id"
        :key="index"
        :name="item.name" />
    </bkdata-select><bkdata-select
      v-if="initType === 'field'"
      v-model="initSelected"
      extCls="select-combination behind-select"
      :clearable="clearable"
      :popoverWidth="250"
      :searchable="true"
      @selected="secondSelectedItem"
      @change="changeStatus">
      <bkdata-option-group v-for="(group, index) in list"
        :key="index"
        :name="group.group">
        <bkdata-option v-for="(item, childIndex) in group.children"
          :id="item.id"
          :key="childIndex"
          :name="item.name" />
      </bkdata-option-group>
    </bkdata-select><bkdata-input v-else
      v-model="initInput"
      extCls="select-combination input" />
  </div>
</template>

<script lang="ts" src="./index.ts"></script>

<style lang="scss" scoped>
.select-combination-wrapper {
  width: 100%;
  display: flex;
  padding: 0 10px;
  ::v-deep .select-combination {
    display: inline-block;
    .bk-select-angle {
      z-index: 2;
    }
    &.front-select {
      border-radius: 2px 0 0 2px;
      .bk-select-name {
        padding: 0 20px 0 10px;
        background-color: #fafbfd;
      }
    }
    &.behind-select,
    &.input {
      margin-left: -1px;
      border-radius: 0 2px 2px 0;
      flex: 1;
      .bk-form-control input {
        height: 32px;
        border: 1px solid #c4c6cc;
        &:hover {
          height: 32px;
          border: 1px solid #c4c6cc;
        }
      }
    }
  }
}
</style>

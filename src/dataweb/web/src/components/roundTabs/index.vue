

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
  <ul class="tab-row">
    <li
      v-for="(item, index) in panels"
      :key="index"
      :class="{ selected: selected === item.name, disabled: item.disabled }"
      @click="tabClick(item)">
      {{ item.label }}
    </li>
  </ul>
</template>

<script lang="ts">
import { Component, Prop, PropSync, Vue } from 'vue-property-decorator';

interface iTab {
  name: string;
  disabled?: boolean;
}

@Component({})
export default class RoundTabs extends Vue {
  @Prop(Array) panels: Array<object>;
  @PropSync('selected', { default: '' }) selectedSync: string;

  tabClick(tab: iTab) {
    if (tab.disabled) return;
    this.selectedSync = tab.name;
  }
}
</script>

<style lang="scss" scoped>
.tab-row {
  text-align: left;
  list-style: none;
  overflow: hidden;
  margin: 0;
  padding: 0;
  line-height: 24px;
  position: relative;
}
.tab-row li {
  cursor: pointer;
  margin: 0 10px;
  padding: 0 10px;
  background: #fcfdff;
  display: inline-block;
  color: #8c9099;
  position: relative;
  z-index: 0;
  &:first-of-type {
    margin-left: 20px;
  }
}
.tab-row li.selected {
  background: #fff;
  color: #63656e;
  font-weight: 500;
  border: 1px solid #dcdee5;
  z-index: 2;
  border-bottom-color: #fff;
}

.tab-row:after {
  position: absolute;
  content: '';
  width: 100%;
  bottom: 0;
  left: 0;
  border-bottom: 1px solid #dcdee5;
  z-index: 1;
}

.tab-row:before {
  z-index: 1;
}

.tab-row li.selected:before,
.tab-row li.selected:after {
  position: absolute;
  bottom: -1px;
  width: 5px;
  height: 5px;
  content: ' ';
}
.tab-row li:after,
.tab-row li:before {
  border: 1px solid #dcdee5;
}
.tab-row li:before {
  left: -6px;
  border-bottom-right-radius: 2px;
  border-width: 0 1px 1px 0;
}
.tab-row li:after {
  right: -6px;
  border-bottom-left-radius: 2px;
  border-width: 0 0 1px 1px;
}

.tab-row li:before {
  box-shadow: 2px 2px 0 #fcfdff;
}
.tab-row li:after {
  box-shadow: -2px 2px 0 #fcfdff;
}
.tab-row li.selected:before {
  box-shadow: 2px 2px 0 #fff;
}
.tab-row li.selected:after {
  box-shadow: -2px 2px 0 #fff;
}
</style>

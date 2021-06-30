

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
  <section class="model-tab">
    <!-- <div class="tab-title"></div> -->
    <ul :class="`${type}-tab`">
      <template v-for="item in list">
        <li
          :key="item.name"
          :class="[`${type}-tab-item`, { active: syncActiveTabName === item.name, disabled: item.disabled }]"
          @click.stop="!item.disabled && changeTab(item.name)">
          <label v-if="!item.icon"
            v-bk-tooltips.bottom="item.tips || defaultTips">
            {{ item.label }}
          </label>
          <i v-else
            v-bk-tooltips.right="item.label"
            :class="`bk-icon icon-${item.icon}`" />
        </li>
      </template>
    </ul>
  </section>
</template>
<script lang="ts" src="./ModelTab.ts"></script>
<style lang="scss" scoped>
.model-tab {
  width: 100%;
  height: 100%;
}
.tab-title {
  color: #babcc2;
  padding-top: 8px;
  height: 50px;
  line-height: 44px;
  text-align: center;
  font-size: 12px;
  border-bottom: 1px solid #dcdee5;
}
.top-tab,
.left-tab {
  width: 100%;
  height: 44px;
  .top-tab-item,
  .left-tab-item {
    line-height: 44px;
    text-align: center;
    background: #fafbfd;
    cursor: pointer;
    position: relative;
    border-bottom: 1px solid #dcdee5;

    &.disabled {
      cursor: not-allowed;
      label {
        cursor: not-allowed;
      }
    }
  }
  label {
    display: block;
    cursor: pointer;
  }
  .active {
    background: #fff;
    color: #3a84ff;
    &::before,
    &::after {
      content: '';
      display: inline-block;
      background: #3a84ff;
      position: absolute;
      top: 0;
    }
    &::before {
      left: 0;
    }
  }
}
.top-tab {
  .active {
    border-bottom: 1px solid #fff;
    &::before {
      width: 100%;
      height: 4px;
    }
  }
}
.top-tab-item {
  float: left;
  width: 50%;
  border-right: 1px solid #dcdee5;
  &:last-child {
    border-right: none;
  }
}
.left-tab {
  .active {
    &:last-child {
      border-bottom: 1px solid #dcdee5;
    }
    border-right: 1px solid #fff;
    &::before {
      height: 100%;
      width: 4px;
    }
    &::after {
      height: 100%;
      width: 1px;
      right: -2px;
      background: #fff;
    }
  }
}
.left-tab-item {
  font-size: 18px;
  &:last-child {
    border-bottom: none;
  }
}
</style>

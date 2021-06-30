

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
  <section class="mode-left-menu">
    <div class="mode-left-menu-top">
      <ModelTab :list="topTabList"
        :activeTabName.sync="syncTopTabName"
        @change="handleTypeChanged" />
      <div v-if="isProject"
        class="top-select">
        <bkdata-selector
          ref="projectSelector"
          :selected.sync="syncProjectId"
          :list="showProjectList"
          :settingKey="'project_id'"
          :displayKey="'displayName'"
          class="top-selector"
          :showOnInit="!syncProjectId"
          :searchable="true"
          :clearable="false"
          :placeholder="$t('请选择项目')"
          @change="handleChange">
          <template slot="bottom-option">
            <div slot="extension"
              class="extension-btns">
              <span class="icon-btn"
                @click="handlePermissionApply">
                <i class="icon-apply-2 icon-item" />
                {{ $t('申请权限') }}
              </span>
              <span class="icon-btn"
                @click="handleNewProject">
                <i class="icon-plus-circle icon-item" />
                {{ $t('创建项目') }}
              </span>
            </div>
          </template>
        </bkdata-selector>
      </div>
    </div>
    <div :class="['mode-left-menu-bottom', { 'is-hash-select': isProject }]">
      <div class="mode-left-menu-bottom-tab">
        <div class="tab-title">
          模型
        </div>
        <ModelTab
          :list="leftTabList"
          type="left"
          :activeTabName.sync="syncLeftTabName"
          @change="handleModelTypeChanged" />
      </div>
      <div class="mode-left-menu-bottom-panel bk-scroll-y">
        <slot />
      </div>
    </div>
    <new-project ref="newproject"
      @createNewProject="handleCreatedProject" />
    <permission-apply ref="permissionApply"
      :defaultSelectValue="defaultSelectedValue()" />
  </section>
</template>
<script lang="ts" src="./ModelLeftMenu.ts"></script>
<style lang="scss" scoped>
.mode-left-menu {
  width: 100%;
  height: 100%;
  background: #fff;
}
.mode-left-menu-top {
  min-height: 44px;
  width: 100%;
  .top-select {
    padding: 9px 20px;
    border-bottom: 1px solid #dcdee5;
    .top-selector {
      background: #f0f1f5;
      border-color: transparent;
      &.is-focus {
        background: #ffffff;
        border-color: #3a84ff;
      }
    }
  }
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
.mode-left-menu-bottom {
  display: flex;
  width: 100%;
  height: calc(100% - 44px);
  .mode-left-menu-bottom-tab {
    width: 52px;
    height: 100%;
    background: #fafbfd;
    border-right: 1px solid #dcdee5;
  }
  .mode-left-menu-bottom-panel {
    width: calc(100% - 52px);
    height: 100%;
    &.bk-scroll-y {
      overflow-y: auto;
      &::-webkit-scrollbar-thumb {
        background-color: transparent;
      }
      &:hover {
        &::-webkit-scrollbar-thumb {
          background-color: #dcdee5;
          &:hover {
            background-color: #c4c6cc;
          }
        }
      }
    }
  }
}
.is-hash-select {
  height: calc(100% - 97px);
}
.extension-btns {
  display: flex;
  align-items: center;
  .icon-btn {
    flex: 1;
    height: 32px;
    line-height: 32px;
    color: #63656e;
    cursor: pointer;
    &:hover {
      color: #3a84ff;
    }
    .icon-item {
      font-size: 14px;
    }
  }
}
</style>

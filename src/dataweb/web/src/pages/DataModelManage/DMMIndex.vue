

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
  <section class="data-model-manager">
    <div :class="['data-model-left', { 'is-hide-menu': !isShowLeftMenu }]">
      <ModelLeftMenu
        :isProject="isProject"
        :projectId.sync="pProjectId"
        :projectList="projectList"
        :topTabList="topTabList"
        :leftTabList="leftTabList"
        :topTabName.sync="activeTypeName"
        :leftTabName.sync="activeModelType"
        @projectChanged="handleProjectChanged"
        @activeTypeChanged="handleActiveTypeChanged"
        @modelTypeChanged="handleModelTypeChanged">
        <div class="bottom-panel-top">
          <bkdata-input v-model="searchValue"
            extCls="item-inp"
            placeholder="搜索"
            :rightIcon="'bk-icon icon-search'" />
          <i
            v-bk-tooltips="iconList[activeTypeName].label"
            :class="`bk-icon icon-${iconList[activeTypeName].icon} item-icon`"
            @click="handleNewModel" />
        </div>
        <div v-bkloading="{ isLoading: isPDatasourceLoad || isSDatasourceLoad }">
          <template v-if="pinedList && pinedList.length">
            <MenuItemList
              :list="pinedList"
              :isShowIcon="!isProject"
              :isShowDelete="!!isProject"
              @onClick="clickList"
              @onDelete="handleDeleteModel"
              @topChanged="item => handleTopData(item, false)" />
            <div v-if="!isProject"
              class="item-line" />
          </template>
          <template v-if="unPinedList && unPinedList.length">
            <MenuItemList
              :list="unPinedList"
              :isShowIcon="!isProject"
              :isShowDelete="!!isProject"
              type="urge"
              @onClick="clickList"
              @onDelete="handleDeleteModel"
              @topChanged="item => handleTopData(item, true)" />
          </template>
          <template v-if="!searchList || !searchList.length">
            <EmptyView class="empty-data"
              tips="暂无模型">
              <p style="margin-top: 6px;">
                <bkdata-button theme="primary"
                  text
                  style="font-size: 12px;"
                  @click.stop="handleNewModel">
                  {{ $t('新建模型') }}
                </bkdata-button>
              </p>
            </EmptyView>
          </template>
        </div>
      </ModelLeftMenu>
      <i :class="['toggle-icon', expandIcon]"
        @click.stop="handleToggleLeftMenu" />
    </div>
    <ModeRight class="data-model-right"
      @activeTabChanged="handleActiveTabChanged" />

    <!-- 无权限申请 -->
    <PermissionApplyWindow
      ref="projectApply"
      :objectId="pProjectId"
      :showWarning="true"
      :defaultSelectValue="{ objectClass: 'project' }" />
  </section>
</template>
<script lang="ts" src="./DMMIndex.ts"></script>
<style lang="scss" scoped>
.test {
  width: 100%;
  min-height: calc(100% - 500px);
  margin-top: 50px;
  padding: 0 30px;
}
.data-model-manager {
  width: 100%;
  height: 100%;
  display: flex;
  .data-model-left {
    position: relative;
    width: 320px;
    height: 100%;
    border-right: 1px solid #dcdee5;
    transition: all 0.1s;
    &.is-hide-menu {
      width: 0;
      .mode-left-menu {
        visibility: hidden;
      }
    }
    .toggle-icon {
      position: absolute;
      top: 40%;
      right: -18px;
      font-size: 80px;
      color: rgba(151, 155, 165, 0.5);
      cursor: pointer;
      z-index: 99;
      &:hover {
        color: rgba(151, 155, 165, 0.8);
      }
    }
  }
  .data-model-right {
    flex: 1;
    width: calc(100vw - 320px);
    height: 100%;
    background: #f5f6fa;
  }
  .bottom-panel-top {
    padding: 16px;
    position: relative;
    .item-inp {
      width: 204px;
    }
    .item-icon {
      cursor: pointer;
      color: #979ba5;
      position: absolute;
      right: 16px;
      top: 50%;
      transform: translateY(-50%);
      font-size: 16px;
      &:hover {
        color: #3a84ff;
      }
    }
  }
  .item-line {
    width: calc(100% - 32px);
    height: 1px;
    margin: 10px 16px;
    background: #dcdee5;
  }
  .empty-data {
    width: 100%;
    height: 250px;
    ::v-deep .empty-img {
      margin-bottom: 0;
    }
    ::v-deep .empty-text {
      font-size: 14px;
      color: #63656e;
    }
  }
}
</style>

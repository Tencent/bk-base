

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
  <div class="flowside-bar">
    <section v-if="activeTabName !== SIDEBAR.FLOWSEARCH"
      class="navi-search-bar">
      <span class="bk-icon"
        :class="[`icon-${(isNavCollapsed && 'indent') || 'dedent'}`]"
        @click="handleCollapsed" />
      <template v-if="!isNavCollapsed">
        <bkdata-input
          v-model="searchValue"
          :placeholder="$t('搜索')"
          class="nvi-search-input"
          :rightIcon="'bk-icon icon-search'"
          :class="{ 'full-width': !isNodeListOptionShow }" />
        <template v-if="isNodeListOptionShow">
          <span
            class="bk-icon icon-list list-type"
            :class="{ active: naviListType === NODE_LIST_TYPE.LIST }"
            @click="naviListType = NODE_LIST_TYPE.LIST" />
          <span
            class="bk-icon icon-block-shape list-type"
            :class="{ active: naviListType === NODE_LIST_TYPE.TABLE }"
            @click="naviListType = NODE_LIST_TYPE.TABLE" />
        </template>
      </template>
    </section>
    <bkdata-tab tabPosition="left"
      :active.sync="activeTabName"
      @tab-change="handleTabChange">
      <bkdata-tab-panel :name="SIDEBAR.TASKLIST">
        <span slot="label"
          :title="$t('任务列表')">
          <i class="bkdata-icon icon-task" />
        </span>
        <div class="panel-div project-list">
          <ProjectList :searchValue="searchValue"
            @viewTaskClick="handleTaskChanged" />
        </div>
      </bkdata-tab-panel>
      <bkdata-tab-panel :name="SIDEBAR.COMPONENT">
        <span slot="label"
          :title="$t('组件库')">
          <i class="bkdata-icon icon-apps-shape" />
        </span>
        <div class="panel-div">
          <NodesPanel :listType="naviListType"
            :searchValue="searchValue" />
        </div>
      </bkdata-tab-panel>
      <bkdata-tab-panel :name="SIDEBAR.OUTLINE">
        <span slot="label"
          title="Outline">
          <i class="bkdata-icon icon-list" />
        </span>
        <div class="panel-div">
          <FlowOutline :searchValue="searchValue" />
        </div>
      </bkdata-tab-panel>
      <bkdata-tab-panel :name="SIDEBAR.FUNCTION"
        style="height: 100%">
        <span slot="label"
          :title="$t('函数库')">
          <i class="bkdata-icon icon-udf" />
        </span>
        <div class="panel-div tree-menu">
          <Library :searchValue="searchValue" />
        </div>
      </bkdata-tab-panel>
      <bkdata-tab-panel :name="SIDEBAR.FLOWSEARCH">
        <span slot="label"
          :title="$t('任务搜索')">
          <i class="bkdata-icon icon-search" />
        </span>
        <RtSelect :graphPanel="true"
          @taskRtSearchSelected="searchFLow" />
        <div
          v-bkloading="{ isLoading: isLoading }"
          class="panel-div project-list"
          style="transform: translateY(-40px); min-height: 350px">
          <FlowListPanel :flowList="flowList" />
        </div>
      </bkdata-tab-panel>
    </bkdata-tab>
  </div>
</template>

<script>
import { SIDEBAR } from '@/pages/DataGraph/Common/constant';
import RtSelect from '@/pages/userCenter/components/RtSelect.vue';
import NodesPanel from './NodesPanel.vue';
import Library from '@/components/tree/function.vue';
import FlowOutline from './FlowOutline';
import ProjectList from './project/projectList';
import { NODE_LIST_TYPE } from '../constant.js';
import { postMethodWarning } from '@/common/js/util.js';
import FlowListPanel from './project/FlowListSearchByRT';
export default {
  name: 'Graph-Navi',
  components: { NodesPanel, Library, FlowOutline, ProjectList, RtSelect, FlowListPanel },
  data() {
    return {
      SIDEBAR: SIDEBAR,
      NODE_LIST_TYPE: NODE_LIST_TYPE,
      activeTabName: SIDEBAR.TASKLIST,
      searchValue: '',
      naviListType: NODE_LIST_TYPE.TABLE,
      /** 左侧是否收起 */
      isNavCollapsed: false,
      flowList: [],
      isLoading: false,
    };
  },

  computed: {
    isNodeListOptionShow() {
      return this.activeTabName === this.SIDEBAR.COMPONENT;
    },
  },
  methods: {
    handleTabChange() {
      this.searchValue = '';
    },
    handleCollapsed() {
      this.$emit('handleCollapsed', !this.isNavCollapsed, this.isNavCollapsed);
      this.isNavCollapsed = !this.isNavCollapsed;
    },
    handleTaskChanged(val) {
      this.activeTabName = this.SIDEBAR.COMPONENT;
    },
    searchFLow(val) {
      this.isLoading = true;
      this.bkRequest
        .httpRequest('dataFlow/getProcessingNodes', {
          query: {
            result_table_id: val,
          },
        })
        .then(res => {
          if (res.result) {
            this.flowList = res.data;
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.isLoading = false;
        });
    },
  },
};
</script>

<style lang="scss" scoped>
.flowside-bar {
  width: 100%;
  height: 100%;
  position: relative;

  .navi-search-bar {
    display: flex;
    align-items: center;
    position: absolute;
    left: 0;
    top: 0;
    right: 0;
    height: 40px;
    z-index: 1;
    background: #efefef;
    padding-left: 3px;

    .nvi-search-input {
      width: 200px;
      margin: 0 10px;

      &.full-width {
        width: 280px;
      }
    }

    span.bk-icon {
      width: 32px;
      height: 32px;
      text-align: center;
      display: flex;
      align-items: center;
      justify-content: center;
      background: #fff;
      cursor: pointer;

      &.icon-list {
        border-top-left-radius: 2px;
        border-bottom-left-radius: 2px;
      }

      &.icon-table {
        border-top-right-radius: 2px;
        border-bottom-right-radius: 2px;
      }

      &.list-type {
        &.active {
          background: #3a84ff;
          color: #fff;
        }
      }
    }
  }
  ::v-deep .position-left {
    padding-top: 40px;
    height: calc(100vh - 110px);
    .bk-tab-header {
      background: #efefef;
      height: 100%;
      padding-top: 40px;
      &::before,
      &::after {
        background: none;
      }
      .bk-tab-label-list {
        width: 35px;
        height: 100%;
        // padding-top: 10px;
        .bk-tab-label-item {
          min-width: 35px;
          line-height: 34px;
          border: none;
          .bk-tab-label {
            height: 50px;
          }
          .bkdata-icon {
            font-size: 13px;
            font-weight: 500;
            line-height: 50px;
            color: #909399;
          }
        }
        .is-first {
          border-top: 1px solid #ddd;
        }
        .active {
          border-right: 0px solid;
          position: relative;
          &::before {
            content: '';
            display: inline-block;
            width: 3px;
            position: absolute;
            background: #3a84ff;
            top: 0;
            bottom: 0;
            left: 0;
          }
          .bk-icon {
            font-size: 13px;
            font-weight: 600;
            color: #3a84ff;
          }
        }
      }
      .bk-tab-label-wrapper {
        &::after {
          border: none;
        }
        .bk-tab-label-list-has-bar:after {
          background: none;
        }
      }
    }
    .bk-tab-section {
      width: calc(100% - 35px);
      height: 100%;
      padding: 0;
      .bk-tab-content {
        height: 100%;
      }

      .panel-div {
        &:not(.project-list) {
          height: 100%;
          overflow-y: scroll;
          &.tree-menu {
            height: calc(100% - 50px);
          }
          &::-webkit-scrollbar {
            width: 6px;
            background-color: transparent;
          }
          &::-webkit-scrollbar-thumb {
            border-radius: 6px;
            background-color: #a0a0a0;
          }
        }

        ::v-deep .bk-collapse {
          width: 100%;
        }
      }
    }
  }
}
</style>

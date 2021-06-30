

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
  <section class="tree-layout">
    <bkdata-input
      v-model="filter"
      class="tree-search"
      :placeholder="$t('搜索指标名称')"
      :clearable="true"
      :rightIcon="'bk-icon icon-search'" />
    <bkdata-big-tree
      ref="tree"
      v-bkloading="{ isLoading, opacity: 1 }"
      class="index-tree"
      selectable
      defaultExpandAll
      :nodeHeight="36"
      :expandOnClick="false"
      :filterMethod="handleFilterMethod"
      @select-change="handleSelectedNode">
      <template slot-scope="{ data }">
        <div :class="['node-item-info', { 'has-btns': showAddBtns }]"
          :title="`${data.name}（${data.alias}）`">
          <i :class="['node-custom-icon', data.icon]" />
          <div class="node-display-name text-overflow">
            <span class="alias">{{ data.alias }}</span>
            <span class="name">（{{ data.name }}）</span>
          </div>
          <div class="node-right">
            <span v-if="!!data.count"
              class="node-count">
              {{ data.count }}
            </span>
            <template v-if="data.type === 'master_table' && showAddBtns">
              <bkdata-button
                v-bk-tooltips="getBtnTips($t('添加指标统计口径'))"
                theme="primary"
                class="node-btn"
                @click="handleAddCaliber(data)">
                <i class="icon-add-9" />
                <i class="btn-icon icon-statistic-caliber" />
              </bkdata-button>
            </template>
            <!-- <template v-else-if="['calculation_atom', 'indicator'].includes(data.type) && showAddBtns"> -->
            <template v-else-if="['calculation_atom'].includes(data.type) && showAddBtns">
              <bkdata-button
                v-bk-tooltips="getBtnTips($t('添加指标'))"
                theme="primary"
                class="node-btn"
                @click="handleAddIndicator(data)">
                <i class="icon-add-9" />
                <i class="btn-icon icon-quota" />
              </bkdata-button>
            </template>
          </div>
        </div>
      </template>
      <template slot="empty">
        <EmptyView v-if="isEmpty"
          class="empty-data" />
        <bkException v-else
          class="empty-data"
          type="search-empty"
          scene="part" />
      </template>
    </bkdata-big-tree>
  </section>
</template>
<script lang="ts" src="./IndexTree.ts"></script>
<style lang="scss" scoped>
.tree-layout {
  height: 100%;
  .tree-search {
    width: auto;
    margin: 0 20px 20px;
  }
  .index-tree {
    height: calc(100% - 52px) !important;
    padding-bottom: 20px;
    ::v-deep {
      .bk-big-tree-node {
        height: 36px;
        line-height: 36px;
        &:not(.is-root) {
          .node-options {
            padding-left: 12px;
          }
        }
      }
      .node-options {
        padding-left: 20px;
      }
      .node-folder-icon {
        color: #c4c6cc;
      }
      .bk-big-tree-node {
        &:hover {
          background-color: #ebf2ff;
          .node-item-info.has-btns {
            .node-count {
              display: none;
            }
            .node-btn {
              display: block;
            }
          }
        }
        &.is-selected {
          background-color: #e1ecff;
          .node-folder-icon {
            color: #3a84ff;
          }
          .node-item-info {
            color: #699df4;
            .alias {
              color: #3a84ff;
            }
            .node-count {
              color: #ffffff;
              background-color: #a2c5fd;
            }
          }
        }
      }
    }
    .node-item-info {
      display: flex;
      align-items: center;
      color: #979ba5;
      padding-right: 20px;
      .node-custom-icon {
        font-size: 18px;
      }
      .node-display-name {
        flex: 1;
        padding: 0 5px;
        .alias {
          color: #63656e;
        }
      }
      .node-count {
        padding: 0 5px;
        font-size: 12px;
        background-color: #f0f1f5;
      }
      .node-btn {
        display: none;
        min-width: 38px;
        height: 18px;
        line-height: 16px;
        font-size: 0;
        padding: 0;
        ::v-deep div > span {
          display: flex;
          align-items: center;
          justify-content: center;
        }
        .icon-add-9 {
          font-size: 12px;
          margin-right: 2px;
        }
        .btn-icon {
          font-size: 14px;
        }
      }
    }
    .empty-data {
      min-height: 300px;
    }
  }
}
</style>

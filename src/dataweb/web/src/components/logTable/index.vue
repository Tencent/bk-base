

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
  <div class="data-log-wrapper">
    <bkdata-table
      ref="logTable"
      v-bkloading="{ isLoading: loading }"
      :data="calTableData"
      :maxHeight="maxHeight"
      extCls="log-table"
      @filter-change="filterChange"
      @cell-dblclick="cellDbClickHandle"
      @cell-mouse-leave="cellMouseLeave"
      @cell-mouse-enter="cellMouseEnter">
      <bkdata-table-column :label="$t('时间')"
        prop="time"
        :minWidth="timeWidth" />
      <bkdata-table-column
        :label="$t('等级')"
        columnKey="level"
        width="100"
        prop="level"
        :filterMultiple="!isModelLog"
        :filters="filterListData"
        :filterMethod="levelFilterMethod">
        <template slot-scope="props">
          <span :class="props.row.level">{{ props.row.level }}</span>
        </template>
      </bkdata-table-column>
      <bkdata-table-column
        :label="$t(modelName)"
        prop="origin"
        :minWidth="originWidth"
        :showOverflowTooltip="true"
        :renderHeader="renderHeader" />
      <bkdata-table-column :label="$t('日志内容')"
        minWidth="630">
        <template slot-scope="props">
          <!--eslint-disable vue/no-v-html-->
          <div style="white-space: break-spaces"
            v-html="props.row.log" />
          <div
            v-if="props.row.isOver"
            v-bk-tooltips="{
              content: $t('双击日志可快捷展开收起'),
              theme: 'light',
            }"
            class="icon-group"
            @click="foldIconClick(props.row, $event)">
            <span v-if="props.row.fold"
              class="icon-angle-double-down" />
            <span v-else
              class="icon-angle-double-up" />
          </div>
        </template>
      </bkdata-table-column>
    </bkdata-table>
    <div v-if="isShowPage"
      class="page-view">
      <p class="page-view-count">
        {{ $t('共') }} {{ pagination.count }} {{ $t('条日志') }}
      </p>
      <bkdata-pagination
        align="right"
        extCls="log-page-main"
        size="small"
        :current.sync="pagination.current"
        :count.sync="pagination.count"
        :showLimit="false"
        :limit="10"
        @change="pageChange" />
    </div>
  </div>
</template>

<script>
import mixin from './mixin.js';
export default {
  mixins: [mixin],
  props: {
    search: {
      type: String,
      default: '',
    },
    loading: {
      type: Boolean,
      default: false,
    },
    maxHeight: {
      type: Number,
      default: 500,
    },
    data: {
      type: Array,
      default: () => [],
    },
    logType: {
      type: String,
      default: '',
    },
    pagination: {
      type: Object,
      default: () => {},
    },
    filterList: {
      type: Array,
      default: () => [],
    },
    originWidth: {
      type: Number,
      default: 270,
    },
    timeWidth: {
      type: Number,
      default: 225,
    },
  },
};
</script>

<style lang="scss" scoped>
.page-view {
  width: 100%;
  margin-top: 10px;
  display: flex;
  .page-view-count {
    width: 200px;
    display: inline-block;
    height: 32px;
    line-height: 32px;
    margin-left: 5px;
  }
  .log-page-main {
    flex: 1;
  }
}
.data-log-wrapper {
  ::v-deep .bk-table {
    .is-last {
      cursor: pointer;
    }
    .cell {
      .custom-header-cell {
        color: #313238;
        font-weight: 400;
        position: relative;
        div {
          width: 100%;
        }
        .icon {
          margin-left: 5px;
          font-size: 14px;
          color: #c4c6cc;
          cursor: pointer;
        }
      }
      padding-right: 35px;
      .ERROR {
        color: #ea3938;
      }
      .WARN {
        color: #fb9f13;
      }
      &.log-unfold {
        display: flex;
        align-items: center;
      }
      .icon-group {
        position: absolute;
        top: 50%;
        right: 12px;
        font-size: 18px;
        transform: translateY(-50%);
      }
      .icon-active {
        color: #3a84ff;
      }
    }
    #table-bottom-info {
      width: 1236px;
      height: 42px;
      line-height: 42px;
      text-align: center;
      font-size: 12px;
      color: #979ba5;
    }
  }
}
</style>

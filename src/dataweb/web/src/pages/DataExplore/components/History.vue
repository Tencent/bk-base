

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
  <div v-if="visible"
    class="data-history">
    <!-- <div class="data-history-head">查询历史<i class="bk-icon icon-close close" @click="close"></i></div> -->
    <div v-bkloading="{ isLoading: isLoading }"
      class="data-history-main">
      <bkdata-table
        class="history-table query-table"
        stripe
        :colBorder="false"
        :showHeader="true"
        :pagination="{
          current: current,
          count: total,
          limit: limit,
        }"
        :data="dataList"
        @page-limit-change="limitChange"
        @page-change="handlePageChange">
        <bkdata-table-column :label="$t('状态')"
          width="50"
          align="center">
          <template slot-scope="props">
            <span v-bk-tooltips="props.row.query_state"
              class="status">
              <i v-if="props.row.query_state === 'finished'"
                class="bk-icon icon-check-circle-shape status-running" />
              <i v-else-if="props.row.query_state === 'failed'"
                class="bk-icon icon-close-circle-shape status-error" />
              <i v-else
                class="bk-icon icon-point-loading status-loading" />
            </span>
          </template>
        </bkdata-table-column>
        <bkdata-table-column :label="$t('耗时') + 'ms'"
          prop="cost_time"
          width="80"
          align="center" />
        <bkdata-table-column :label="$t('查询时间')"
          prop="query_start_time"
          width="180" />
        <bkdata-table-column :label="$t('查询SQL')"
          prop="sql_text">
          <cell-click
            slot-scope="{ row }"
            v-bk-overflow-tips="{ content: $t('双击查看更多'), interactive: false }"
            class="table-cell-click"
            :content="row.sql_text"
            :html="false" />
        </bkdata-table-column>
        <bkdata-table-column :label="$t('记录数')"
          prop="total_records"
          width="80"
          align="center" />
      </bkdata-table>
    </div>
  </div>
</template>
<script>
import { bkOverflowTips } from 'bk-magic-vue';
import cellClick from '@/components/cellClick/cellClick.vue';
export default {
  components: {
    cellClick,
  },
  directives: {
    bkOverflowTips,
  },
  props: {
    visible: {
      type: Boolean,
      default: false,
    },
    dataList: {
      type: Array,
      default: () => [],
    },
    total: Number,
    isLoading: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      current: 1,
      limit: 10,
    };
  },
  mounted() {},
  methods: {
    close() {
      this.$emit('update:visible', false);
    },
    limitChange(limit) {
      this.limit = limit;
      this.$emit('pageChange', this.current, limit);
    },
    handlePageChange(page) {
      this.current = page;
      this.$emit('pageChange', page, this.limit);
    },
  },
};
</script>
<style lang="scss" scoped>
.data-history {
  width: 100%;
  height: 100%;
  background: #ffffff;
  .data-history-main {
    width: 100%;
    height: 100%;
    .bk-table {
      border: none;
    }
    ::v-deep .table-cell-click {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .status {
      font-size: 12px;
    }
    .status-running {
      color: #52c41a;
    }
    .status-error {
      color: #f5222d;
    }
    .status-loading {
      font-size: 14px;
      color: #3a84ff;
    }
  }
  .history-table {
    ::v-deep {
      .bk-table-body-wrapper {
        height: calc(100% - 160px);
      }
      .bk-page-count-left {
        margin-top: 2px;
        .bk-select .bk-select-name {
          height: 24px;
          line-height: 24px;
        }
        .bk-select .bk-select-angle {
          top: 2px;
        }
      }
      .bk-table-pagination-wrapper {
        padding: 5px 15px;
      }
    }
  }
}
</style>

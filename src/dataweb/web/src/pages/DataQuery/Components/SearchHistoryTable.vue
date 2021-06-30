

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
  <div v-bkloading="{ isLoading: searchLoading }"
    class="search-table">
    <bkdata-table
      :data="queryHistoryList.results"
      :stripe="true"
      :pagination="pagination"
      :emptyText="$t('暂无数据')"
      @page-change="handlePageChange"
      @page-limit-change="handlePageLimitChange">
      <bkdata-table-column :label="$t('查询SQL')">
        <div slot-scope="props"
          class="bk-table-inlineblock">
          <p v-if="props.row.sql"
            class="beyond">
            {{ props.row.sql }}
          </p>
          <p v-else
            class="no-keyword">
            {{ $t('无关键字') }}
          </p>
        </div>
      </bkdata-table-column>
      <bkdata-table-column width="200"
        :label="$t('查询时间')"
        prop="time" />
      <bkdata-table-column width="120"
        :label="$t('耗时_秒')"
        prop="time_taken" />
      <bkdata-table-column width="100"
        :label="$t('结果条数')"
        prop="total" />
      <bkdata-table-column width="100"
        :label="$t('操作')">
        <div slot-scope="props"
          class="bk-table-inlineblock">
          <a
            href="javascript:;"
            class="check-again"
            :class="{ disable: storageLoading }"
            @click="checkAgain(props.row)">
            {{ $t('再次查询') }}
          </a>
        </div>
      </bkdata-table-column>
    </bkdata-table>
  </div>
</template>
<script>
import { mapState, mapGetters } from 'vuex';
import Bus from '@/common/js/bus.js';
export default {
  props: {
    queryHistoryList: {
      type: Object,
      default: () => {
        {
        }
      },
    },
  },
  data() {
    return {
      storageLoading: false,
      tableDataTotal: 1,
      pageCount: 10,
      curPage: 1,
    };
  },
  computed: {
    ...mapState({
      searchLoading: state => state.dataQuery.search.historyLoading,
    }),
    pagination() {
      return {
        count: this.queryHistoryList.count,
        limit: this.pageCount,
        current: this.curPage || 1,
      };
    },
  },

  methods: {
    handlePageChange(page) {
      this.curPage = page;
      Bus.$emit('changeSearchResult', 'pageChange', this.curPage);
    },
    handlePageLimitChange(pageSize) {
      this.curPage = 1;
      this.pageCount = pageSize;
      Bus.$emit('changeSearchResult', 'pageSizeChange', this.pageCount);
    },
    checkAgain(item) {
      this.$emit('sqlHistoryQueryAgain', item);
    },
  },
};
</script>

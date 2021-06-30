

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
  <div>
    <Layout
      v-if="!isOnlyTable"
      class="container-with-shadow"
      :crumbName="$t('操作历史')"
      :withMargin="false"
      :headerMargin="false"
      :headerBackground="'inherit'"
      :collspan="true"
      height="auto">
      <div class="operation-history">
        <bkdata-table
          :stripe="true"
          :showHeader="false"
          :data="logListView"
          :outerBorder="false"
          :pagination="calcPagination"
          :emptyText="$t('暂无数据')"
          @page-change="handlePageChange"
          @page-limit-change="handlePageLimitChange">
          <bkdata-table-column prop="created_at"
            width="180" />
          <bkdata-table-column>
            <div slot-scope="props"
              class="bk-table-inlineblock">
              <span>
                <b>{{ `${props.row.operation_type_alias}` }} - </b>
                {{ $t('操作人') }}：{{ props.row.created_by }}
                {{ props.row.log }}
              </span>
            </div>
          </bkdata-table-column>
        </bkdata-table>
      </div>
    </Layout>
    <bkdata-table
      v-else
      :stripe="true"
      :showHeader="false"
      :data="logListView"
      :outerBorder="false"
      :pagination="calcPagination"
      :emptyText="$t('暂无数据')"
      @page-change="handlePageChange"
      @page-limit-change="handlePageLimitChange">
      <bkdata-table-column prop="created_at"
        width="180" />
      <bkdata-table-column>
        <div slot-scope="props"
          class="bk-table-inlineblock">
          <span>
            <b>{{ `${props.row.operation_type_alias}` }} - </b> {{ $t('操作人') }}：{{ props.row.created_by }}
            {{ props.row.log }}
          </span>
        </div>
      </bkdata-table-column>
    </bkdata-table>
  </div>
</template>
<script>
import Layout from '@/components/global/layout';
export default {
  components: {
    Layout,
  },
  props: {
    storageItem: {
      type: Object,
      default: () => ({}),
    },
    isOnlyTable: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      curPage: 1,
      pageCount: 10,
      totalCount: 0,
      logList: [],
    };
  },
  computed: {
    calcPagination() {
      return {
        current: Number(this.curPage),
        count: this.totalCount,
        limit: this.pageCount,
      };
    },
    logListView() {
      return this.logList.slice(this.pageCount * (this.curPage - 1), this.pageCount * this.curPage);
    },
  },
  watch: {
    storageItem: {
      deep: true,
      immediate: true,
      handler(val) {
        val && Object.keys(val).length && this.getHistoryLog(val);
      },
    },
  },
  methods: {
    handlePageChange(page) {
      this.curPage = page;
    },
    handlePageLimitChange(limit) {
      this.pageCount = limit;
      this.curPage = 1;
    },
    getHistoryLog(item) {
      const query = {
        data_type: item.data_type,
        storage_type: item.storage_type,
        result_table_id: item.result_table_id,
        raw_data_id: item.raw_data_id,
        storage_cluster: item.storage_cluster,
      };
      this.bkRequest.httpRequest('dataStorage/getInStorageLog', { query: query }).then(res => {
        if (res.result) {
          this.logList = res.data;
          this.totalCount = this.logList.length;
        }
      });
    },
  },
};
</script>
<style lang="scss">
.operation-history {
  width: 100%;
}
</style>

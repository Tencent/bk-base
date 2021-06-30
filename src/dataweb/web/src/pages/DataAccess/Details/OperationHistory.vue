

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
  <Layout
    class="container-with-shadow"
    :crumbName="$t('操作历史')"
    :withMargin="false"
    :headerMargin="false"
    :headerBackground="'inherit'"
    :collspan="true"
    height="auto">
    <div v-if="operationListView.length"
      class="operation-history-container">
      <bkdata-table
        :stripe="true"
        :showHeader="false"
        :data="operationListView"
        :outerBorder="false"
        :pagination="calcPagination"
        :emptyText="$t('暂无数据')"
        @page-change="handlePageChange"
        @page-limit-change="handlePageLimitChange">
        <bkdata-table-column prop="date"
          width="160" />
        <bkdata-table-column>
          <div slot-scope="props"
            class="bk-table-inlineblock">
            <span>
              {{ `${props.row.log} - ${$t('操作人')}：
                            ${props.row.updated_by}，${$t('备注')}：${props.row.content}` }}
            </span>
          </div>
        </bkdata-table-column>
      </bkdata-table>
    </div>
  </Layout>
</template>

<script>
import Layout from '@/components/global/layout';
export default {
  components: { Layout },
  data() {
    return {
      operationList: [],
      curPage: 1,
      pageCount: 10,
      totalCount: 0,
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
    totalPage() {
      return Math.ceil(this.operationList.length / this.pageCount) || 1;
    },
    operationListView() {
      return this.operationList.slice(this.pageCount * (this.curPage - 1), this.pageCount * this.curPage);
    },
  },
  mounted() {
    this.$nextTick(() => {
      this.getExcuteHistory();
    });
  },
  methods: {
    handlePageChange(page) {
      this.curPage = page;
    },
    handlePageLimitChange(limit) {
      this.pageCount = limit;
      this.curPage = 1;
    },
    getExcuteHistory() {
      this.$store
        .dispatch('getDeployHistory', this.$route.params.did)
        .then(res => {
          if (res.result) {
            this.operationList = res.data.map(item => {
              return {
                updated_by: item.updated_by,
                date: item['updated_at'] || item['created_at'],
                log: item.args.log,
                content: item.description || item.args.description,
              };
            });
          }
        })
        ['finally'](() => {
          this.loading = false;
          this.totalCount = this.operationList.length;
        });
    },
  },
};
</script>

<style lang="scss" scoped>
.operation-history-container {
  width: 100%;
  .bk-table {
    background: #f5f5f5;
    border-left: 1px solid #e6e6e6;
    border-right: 1px solid #e6e6e6;
  }
}
.no_data {
  height: 120px;
  display: flex;
  align-items: center;
  flex-direction: column;
  justify-content: center;
}
</style>



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
  <div class="storage-container">
    <HeaderType :title="$t('相似数据集')">
      <div slot="title-right"
        class="bk-button-group">
        <bkdata-button :class="getSelectedStyle(10)"
          @click="forwardData = 10">
          Top10
        </bkdata-button>
        <bkdata-button :class="getSelectedStyle(100)"
          @click="forwardData = 100">
          Top100
        </bkdata-button>
        <bkdata-button :class="getSelectedStyle(1000)"
          @click="forwardData = 1000">
          Top1000
        </bkdata-button>
      </div>
      <div v-bkloading="{ isLoading }"
        class="storage-tabel">
        <bkdata-table
          :pagination="pagination"
          :data="tableDataList"
          @page-change="handlePageChange"
          @page-limit-change="handlePageLimitChange">
          <bkdata-table-column :label="'id'">
            <div slot-scope="props"
              class="text-overflow"
              :title="props.row.reference_result_table_ids">
              <a href="jacascript: void(0);"
                @click="linkToDetail(props.row.reference_result_table_ids)">
                {{ props.row.reference_result_table_ids }}
              </a>
            </div>
          </bkdata-table-column>
          <bkdata-table-column :label="$t('相似度')"
            prop="final_goal" />
          <bkdata-table-column :label="$t('Schema相似度')"
            prop="schema" />
          <bkdata-table-column :label="$t('名称相似度')"
            prop="result_table_name" />
          <bkdata-table-column :label="$t('描述相似度')"
            prop="description" />
        </bkdata-table>
      </div>
    </HeaderType>
  </div>
</template>
<script>
import Bus from '@/common/js/bus.js';
import HeaderType from '@/pages/datamart/common/components/HeaderType';

export default {
  components: {
    HeaderType,
  },
  data() {
    return {
      isLoading: false,
      storageList: [],
      currentPage: 1,
      isShow: false,
      similarityData: [],
      tableDataTotal: 1,
      pageCount: 5,
      page: 1,
      forwardData: 5,
    };
  },
  computed: {
    pagination() {
      return {
        count: this.tableDataTotal,
        limit: this.pageCount,
        current: this.page,
      };
    },
    tableDataList() {
      return this.similarityData.slice((this.page - 1) * this.pageCount, this.page * this.pageCount);
    },
  },
  watch: {
    forwardData(num) {
      if (num > 5) {
        this.getDictionaryStorageList();
      }
    },
  },
  mounted() {
    this.getDictionaryStorageList();
  },
  methods: {
    getSelectedStyle(topNums) {
      return this.forwardData === topNums ? 'is-selected' : '';
    },
    linkToDetail(rtId) {
      this.$router.push({
        name: 'DataDetail',
        query: {
          dataType: 'result_table',
          result_table_id: rtId,
        },
      });
      Bus.$emit('reloadDataDetail');
    },
    handlePageChange(page) {
      this.page = page;
    },
    handlePageLimitChange(pageSize) {
      this.page = 1;
      this.pageCount = pageSize;
    },
    switchPencent(num) {
      return Math.floor(num * 100) + '%';
    },
    getDictionaryStorageList() {
      this.isLoading = true;
      this.bkRequest
        .httpRequest('dataDict/getDictSimilarity', {
          params: {
            targets: [
              {
                id: this.$route.query.result_table_id,
                type: 'result_table',
              },
            ],
            reference_top_cnt: this.forwardData,
          },
        })
        .then(res => {
          if (res.result) {
            if (!res.data.final_goal.length) return;
            this.similarityData = [];
            res.data.final_goal[0].forEach((item, i) => {
              this.similarityData.push({
                final_goal: this.switchPencent(item),
                result_table_name: this.switchPencent(res.data.detail.name[0][i]),
                description: this.switchPencent(res.data.detail.description[0][i]),
                schema: this.switchPencent(res.data.detail.schema[0][i]),
                reference_result_table_ids: res.data.reference_result_table_ids[0][i],
                order: res.data.final_goal[0][i],
              });
            });
            this.similarityData = this.similarityData.sort((a, b) => b.order - a.order);

            this.tableDataTotal = this.similarityData.length;
            this.page = 1;
          } else {
            this.getMethodWarning(res.message, res.code);
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
.storage-container {
  margin-bottom: 5px;
}
</style>

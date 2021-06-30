

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
    <HeaderType :title="$t('数据应用')">
      <div v-bkloading="{ isLoading }">
        <bkdata-table
          :data="tableData"
          :stripe="true"
          :emptyText="$t('暂无数据')"
          :pagination="calcPagination"
          @page-change="handlePageChange"
          @page-limit-change="handlePageLimitChange">
          <bkdata-table-column minWidth="200"
            :label="$t('操作类型')"
            prop="type" />
          <bkdata-table-column minWidth="200"
            :label="$t('应用名称')"
            prop="applicationName" />
          <!-- <bkdata-table-column width="200"
                        :label="$t('用户')"
                        prop="user">
                    </bkdata-table-column> -->
          <bkdata-table-column
            width="200"
            :label="$t('操作次数（次）')"
            :showOverflowTooltip="true"
            prop="operationNum" />
          <bkdata-table-column width="150"
            :label="$t('时间')"
            prop="time" />
          <!-- <bkdata-table-column width="250"
                        :label="$t('操作')"
                        prop="object_description">
                        <div class="bk-table-inlineblock"
                            slot-scope="props">
                            <span class="bk-text-button">
                                {{ $t('详情') }}
                            </span>
                        </div>
                    </bkdata-table-column> -->
        </bkdata-table>
      </div>
    </HeaderType>
  </div>
</template>

<script>
import { dataTypeIds } from '@/pages/datamart/common/config';
import HeaderType from '@/pages/datamart/common/components/HeaderType';
export default {
  components: {
    HeaderType,
  },
  data() {
    return {
      currentPage: 1,
      pageSize: 10,
      isLoading: false,
      applicationData: [],
    };
  },
  computed: {
    calcPagination() {
      return {
        current: Number(this.currentPage),
        count: this.applicationData.length,
        limit: this.pageSize,
      };
    },
    tableData() {
      return this.applicationData.slice(this.pageSize * (this.currentPage - 1), this.pageSize * this.currentPage);
    },
  },
  mounted() {
    this.getTableData();
  },
  methods: {
    handlePageChange(page) {
      this.currentPage = page;
    },
    handlePageLimitChange(pageSize, preLimit) {
      this.currentPage = 1;
      this.pageSize = pageSize;
    },
    getTableData() {
      const types = {
        query: this.$t('查询'),
      };
      this.isLoading = true;
      this.bkRequest
        .httpRequest('dataDict/getLifeCycleApplication', {
          query: {
            dataset_id: this.$route.query[dataTypeIds[this.$route.query.dataType]],
            dataset_type: this.$route.query.dataType,
          },
        })
        .then(res => {
          if (res.result) {
            for (const key in res.data) {
              res.data[key].forEach(item => {
                this.applicationData.push({
                  type: types[key],
                  applicationName: item.app_code_alias ? `${item.app_code} [ ${item.app_code_alias} ]` : item.app_code,
                  user: item.user || '—',
                  operationNum: item.count,
                  time: item.applied_at || '—',
                });
              });
            }
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

<style lang="scss" scoped></style>

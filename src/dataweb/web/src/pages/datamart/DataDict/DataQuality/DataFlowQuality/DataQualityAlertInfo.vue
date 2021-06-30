

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
    <HeaderType :title="$t('告警信息')">
      <bkdata-table
        style="margin-top: 15px"
        :data="tableData"
        :size="size"
        :outerBorder="false"
        :headerBorder="false"
        :headerCellStyle="{ background: '#fff' }"
        :pagination="pagination"
        @page-change="handlePageChange">
        <bkdata-table-column type="index"
          label="ID"
          width="60" />
        <bkdata-table-column :label="$t('告警类型')"
          prop="ip" />
        <bkdata-table-column :label="$t('告警规则')"
          prop="source" />
        <bkdata-table-column :label="$t('告警等级')"
          prop="status" />
        <bkdata-table-column :label="$t('是否收敛')"
          prop="create_time" />
        <bkdata-table-column :label="$t('告警时间')"
          prop="create_time" />
        <bkdata-table-column :label="$t('操作')"
          width="150">
          成功
        </bkdata-table-column>
      </bkdata-table>
    </HeaderType>
  </div>
</template>

<script>
import { dataTypeIds } from '@/pages/datamart/common/config';

export default {
  components: {
    HeaderType: () => import('@/pages/datamart/common/components/HeaderType'),
  },
  data() {
    return {
      size: 'small',
      tableData: [],
      pagination: {
        current: 1,
        count: 500,
        limit: 15,
      },
    };
  },
  computed: {
    detailId() {
      return this.$route.query[dataTypeIds[this.$route.query.dataType]];
    },
  },
  mounted() {
    this.getTableData();
  },
  methods: {
    handlePageChange(page) {
      this.pagination.current = page;
    },
    getTableData() {
      this.bkRequest
        .httpRequest('dataAccess/getWarningList', {
          query: {
            dimensions: JSON.stringify({
              data_set_id: this.detailId,
            }),
          },
        })
        .then(res => {
          if (res.result) {
            console.log(res);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {});
    },
  },
};
</script>

<style lang="scss" scoped></style>



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
  <div class="removal-progress">
    <div class="removal-task-status">
      {{ $t('当前任务状态为') }}{{ status[detailInfo.list[0].status] }}，{{ $t('时间范围') }}：{{
        `${detailInfo.list[0].start} - ${detailInfo.list[0].end}`
      }}。
    </div>
    <bkdata-table
      v-bkloading="{ isLoading: detailInfo.loading }"
      style="margin-top: 15px"
      :data="tableList"
      :pagination="pagination"
      @page-change="handlePageChange"
      @page-limit-change="handlePageLimitChange">
      <bkdata-table-column :width="270"
        :label="$t('时间段')">
        <div slot-scope="props"
          class="text-overflow test-center">
          {{ `${props.row.start} - ${props.row.end}` }}
        </div>
      </bkdata-table-column>
      <bkdata-table-column :label="input"
        minWidth="110">
        <div slot-scope="props"
          class="text-overflow test-center"
          :title="props.row.input">
          {{ props.row.input }}
        </div>
      </bkdata-table-column>
      <bkdata-table-column :label="output"
        minWidth="110">
        <div slot-scope="props"
          :title="props.row.output"
          class="text-overflow test-center">
          {{ props.row.output }}
        </div>
      </bkdata-table-column>
      <bkdata-table-column :label="$t('创建时间')"
        minWidth="150">
        <div slot-scope="props"
          :title="props.row.created_at"
          class="text-overflow test-center">
          {{ props.row.created_at }}
        </div>
      </bkdata-table-column>
      <bkdata-table-column :label="$t('最后一次更新时间')"
        minWidth="150">
        <div slot-scope="props"
          :title="props.row.updated_at"
          class="text-overflow test-center">
          {{ props.row.updated_at || '' }}
        </div>
      </bkdata-table-column>
      <bkdata-table-column :label="$t('状态')">
        <div slot-scope="props"
          class="text-overflow test-center">
          {{ status[props.row.status] ? status[props.row.status] : status.running }}
        </div>
      </bkdata-table-column>
    </bkdata-table>
  </div>
</template>
<script>
export default {
  props: {
    detailInfo: {
      type: Object,
      default: () => ({}),
    },
    input: {
      type: String,
      default: '',
    },
    output: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      currentPage: 1,
      pageSize: 10,
      status: {
        init: this.$t('准备中'),
        running: this.$t('运行中'),
        finish: this.$t('已完成'),
      },
    };
  },
  computed: {
    pagination() {
      return {
        current: Number(this.currentPage),
        count: this.detailInfo.list.length - 1,
        limit: this.pageSize,
      };
    },
    tableList() {
      // this.detailInfo.list第一项是总的状态
      return this.detailInfo.list.slice(
        (this.currentPage - 1) * this.pageSize + 1,
        this.currentPage * this.pageSize + 1
      );
    },
  },
  methods: {
    handlePageChange(page) {
      this.currentPage = page;
    },
    handlePageLimitChange(limit) {
      this.currentPage = 1;
      this.pageSize = limit;
    },
  },
};
</script>
<style lang="scss" scoped>
.removal-progress {
    ::v-deep .bk-table {
        th > .cell {
            text-align: center;
        }
    }
    .test-center {
        text-align: center;
    }
}
</style>

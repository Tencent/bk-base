

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
  <div v-bkloading="{ isLoading }"
    class="table-container">
    <bkdata-table
      :extCls="extCls"
      :data="tableData"
      :pagination="calcPagination"
      :emptyText="emptyText"
      :maxHeight="maxHeight"
      @expand-change="toggleRowExpansion"
      @page-change="handlePageChange"
      @page-limit-change="handlePageLimitChange"
      @row-click="rowClick"
      @selection-change="selectionChange"
      @select="select"
      @select-all="selectAll">
      <slot name="content" />
    </bkdata-table>
  </div>
</template>

<script>
export default {
  props: {
    isLoading: Boolean,
    isSeverPaging: {
      type: Boolean,
      default: false,
    },
    totalData: {
      type: Array,
      default: () => [],
    },
    calcPageSize: {
      type: Number,
      default: 10,
    },
    extCls: String,
    isShowPage: {
      type: Boolean,
      default: true,
    },
    emptyText: {
      type: String,
      default: $t('暂无数据'),
    },
    maxHeight: [String, Number],
  },
  data() {
    return {
      currentPage: 1,
    };
  },
  computed: {
    calcPagination() {
      if (!this.isShowPage) return {};
      return {
        current: Number(this.currentPage),
        count: this.totalData.length,
        limit: this.calcPageSize,
      };
    },
    tableData() {
      if (this.isSeverPaging) {
        return this.totalData;
      }
      return this.totalData.slice((this.currentPage - 1) * this.calcPageSize, this.currentPage * this.calcPageSize);
    },
  },
  methods: {
    handlePageChange(page) {
      this.currentPage = page;
      this.$emit('handlePageChange', page);
    },
    handlePageLimitChange(data) {
      this.currentPage = 1;
      this.$emit('update:calcPageSize', data);
      this.$emit('handlePageLimitChange', data);
    },
    rowClick(data) {
      this.$emit('rowClick', data);
    },
    toggleRowExpansion(data, isExpand) {
      this.$emit('toggleRowExpansion', data, isExpand);
    },
    selectionChange(data) {
      this.$emit('selectionChange', data);
    },
    select(data) {
      this.$emit('select', data);
    },
    selectAll(data) {
      this.$emit('selectAll', data);
    },
  },
};
</script>

<style lang="scss" scoped></style>

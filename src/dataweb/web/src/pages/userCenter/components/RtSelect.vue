

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
  <div class="select-wrapper"
    :class="{ 'graph-search': graphPanel }">
    <p v-if="graphPanel">
      {{ $t('根据结果数据表搜索任务') }}
    </p>
    <div class="biz-select">
      <bkdata-selector :selected.sync="bizs.selected"
        :placeholder="bizs.placeholder"
        :isLoading="bizs.isLoading"
        :searchable="true"
        :list="bizs.list"
        :searchKey="'bk_biz_name'"
        :settingKey="'bk_biz_id'"
        :displayKey="'bk_biz_name'"
        @item-selected="bizChange" />
    </div>
    <div class="rt-select">
      <bkdata-selector
        :selected.sync="results.selected"
        :placeholder="results.placeholder"
        :allowClear="true"
        :isLoading="results.isLoading"
        :searchable="true"
        :hasChildren="true"
        :list="resultsLists"
        :settingKey="'result_table_id'"
        :displayKey="'name'"
        @item-selected="resultsChange"
        @clear="resultsChange('')" />
    </div>
  </div>
</template>

<script>
import rtSelectMixin from '@/pages/DataQuery/RtSelect.mixin.js';
import Bus from '@/common/js/bus.js';
export default {
  mixins: [rtSelectMixin],
  props: {
    graphPanel: {
      type: Boolean,
      default: false,
    },
  },
  methods: {
    resultsChange(val) {
      this.graphPanel ? this.$emit('taskRtSearchSelected', val) : Bus.$emit('taskRtSearchSelected', val);
    },
  },
};
</script>

<style lang="scss" scoped>
.select-wrapper {
  display: flex;
  &.graph-search {
    flex-direction: column;
    padding: 20px;
    border-bottom: 1px solid #ddd;
    background: #f5f5f5;
    transform: translateY(-40px);
    .biz-select,
    .rt-select {
      width: 257px;
      margin-top: 15px;
    }
  }
  .biz-select {
    width: 180px;
  }
  .rt-select {
    width: 238px;
  }
}
</style>

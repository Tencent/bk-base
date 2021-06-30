

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
  <div class="table-component">
    <DataTable :totalData="tableData"
      @rowClick="rowClick">
      <template v-if="!isTableLoading"
        slot="content"
        :isLoading="isTableLoading">
        <bkdata-table-column :label="$t('变量名')"
          prop="variableName" />
        <bkdata-table-column :label="$t('含义')"
          prop="meaning" />
        <bkdata-table-column :label="$t('示例')"
          prop="example" />
      </template>
    </DataTable>
  </div>
</template>

<script lang="ts">
import { Component, Vue } from 'vue-property-decorator';
import DataTable from '@/pages/datamart/common/components/DataTable.vue';

@Component({
  components: {
    DataTable,
  },
})
export default class DropSelect extends Vue {
  isTableLoading = false;
  tableData: Array<Record<string, any>> = [
    {
      variableName: '${lala}',
      meaning: '事件名称',
      example: 'X事件',
    },
  ];
  rowClick(data) {
    this.$emit('hiddenTable', data);
  }
}
</script>

<style lang="scss" scoped>
.table-component {
  width: 100%;
}
</style>

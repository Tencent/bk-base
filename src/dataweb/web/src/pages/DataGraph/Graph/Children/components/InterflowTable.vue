

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
  <div v-bkloading="{ isLoading: loading }">
    <bkdata-table style="margin-top: 15px"
      :border="true"
      :data="tableDataArray"
      :emptyText="$t('暂无数据')">
      <bkdata-table-column :label="$t('字段名称')"
        prop="fieldName" />
      <bkdata-table-column :label="$t('类型')"
        prop="type" />
      <bkdata-table-column :label="$t('描述')"
        prop="alias" />
      <bkdata-table-column :label="$t('操作')">
        <div slot-scope="props"
          class="bk-table-inlineblock">
          <span class="pointer"
            @click="openFieldDiff(props.row)">
            {{ $t('查看') }}
          </span>
        </div>
      </bkdata-table-column>
    </bkdata-table>
    <bkdata-dialog
      v-model="isFieldDiffShow"
      :content="'component'"
      :hasFooter="false"
      :hasHeader="false"
      :maskClose="false"
      :width="1000"
      @confirm="isFieldDiffShow = !isFieldDiffShow">
      <div v-if="isFieldDiffShow"
        class="debug-list">
        <TableDataCompare :rtids="rtids"
          :fieldData="fieldData" />
      </div>
    </bkdata-dialog>
  </div>
</template>
<script>
import TableDataCompare from './TableDataCompare.vue';
export default {
  components: {
    TableDataCompare,
  },
  props: {
    rtids: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      loading: false,
      tableData: [],
      fieldData: {},
      isFieldDiffShow: false,
      isDisable: false,
    };
  },
  computed: {
    tableDataArray() {
      let arr = [];
      Object.keys(this.tableData).forEach(name => {
        this.tableData[name].fieldName = name;
        this.tableData[name].type = this.getObjectFieldType(this.tableData[name]);
        this.tableData[name].alias = this.getObjectFieldAlias(this.tableData[name]);
        arr.push(this.tableData[name]);
      });
      return arr;
    },
  },
  watch: {
    rtids: {
      immediate: true,
      handler(val, oldVal) {
        val && val.length && this.getTableDiff();
      },
    },
  },
  methods: {
    getObjectFieldType(item) {
      return ((item.attrs.field_type && Object.keys(item.attrs.field_type)) || []).join(',');
    },
    getObjectFieldAlias(item) {
      return ((item.attrs.field_alias && Object.keys(item.attrs.field_alias)) || []).join(',');
    },
    getTableDiff() {
      let options = {
        // mock: true,
        // manualSchema: true,
        params: {
          rtids: this.rtids,
          noCompareArr: ['updated_by', 'created_at', 'updated_at', 'created_by', 'id', 'field_name', 'field_index'],
        },
      };
      this.bkRequest.httpRequest('meta/getMultiResultTables', options).then(data => {
        this.tableData = data;
        this.isDisable = Object.keys(data).some(attr => {
          return data[attr].isAvailable === false;
        });
        if (!this.isDisable) {
          this.$emit('disabled');
        }
      });
    },
    openFieldDiff(item) {
      this.isFieldDiffShow = true;
      this.fieldData.data = item;
      this.fieldData.fieldName = item.fieldName;
    },
  },
};
</script>
<style lang="scss" scoped>
.pointer {
  color: #3a84ff;
  cursor: pointer;
}
</style>

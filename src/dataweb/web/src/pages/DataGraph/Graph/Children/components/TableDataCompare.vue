

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
  <div v-bkloading="{ isLoading: loading }"
    class="table-compare-wrap">
    <label class="filed-name">{{ $t('字段名') }}：{{ fieldData.fieldName }}</label>
    <bkdata-table :border="true"
      :data="tableData"
      :emptyText="$t('暂无数据')">
      <bkdata-table-column :label="$t('结果表ID')"
        prop="id" />
      <bkdata-table-column
        v-for="(name, index) in fieldNames"
        :key="index"
        :label="$t(name)"
        :prop="index.toString()" />
    </bkdata-table>
  </div>
</template>
<script>
export default {
  name: 'TableDataCompare',

  props: {
    rtids: {
      type: Array,
      default: () => [],
    },
    fieldData: {
      type: Object,
      default: () => {
        {
        }
      },
    },
  },
  data() {
    return {
      loading: false,
    };
  },
  computed: {
    fieldNames() {
      return Object.keys(this.fieldData.data.attrs);
    },
    tableData() {
      let arr = [];
      let obj = {};
      Object.keys(this.fieldData.data.attrs).forEach((key, idx) => {
        obj[idx] = this.getAttr(key, this.rtids[0]);
      });
      obj.id = this.rtids[0];
      arr.push(obj);
      return arr;
    },
  },
  methods: {
    getAttr(item, tableId) {
      let flag = Object.keys(this.fieldData.data.attrs[item]).find(child => {
        return this.fieldData.data.attrs[item][child].nodes.includes(tableId);
      });
      if (flag && flag !== 'null') {
        return flag;
      } else {
        return '无';
      }
    },
    attrDiff(item) {
      return Object.keys(this.fieldData.data.attrs[item]).length > 1;
    },
  },
};
</script>
<style scoped>
.table-compare-wrap {
  margin-top: 14px;
}
.filed-name {
  border: 1px solid #ddd;
  height: 40px;
  display: block;
  margin-bottom: -1px;
  line-height: 40px;
  padding-left: 20px;
  font-size: 15px;
  font-weight: bold;
}
td {
  text-align: center;
}
.diff-bg {
  color: red;
}
</style>

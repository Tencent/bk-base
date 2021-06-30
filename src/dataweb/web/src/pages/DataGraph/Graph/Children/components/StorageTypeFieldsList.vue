

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
  <table v-if="showTable"
    v-bkloading="{ isLoading: loading }"
    class="bk-table has-table-bordered bk-demo">
    <thead>
      <tr>
        <th class="num">
          {{ $t('字段名称') }}
        </th>
        <th class="field">
          {{ $t('类型') }}
        </th>
        <th class="des">
          {{ $t('描述') }}
        </th>
      </tr>
    </thead>
    <tbody>
      <tr v-for="(item, index) in fieldList.fields"
        :key="index">
        <td>{{ item.physical_field }}</td>
        <td>{{ item.physical_field_type }}</td>
        <td>{{ item.field_alias }}</td>
      </tr>
    </tbody>
  </table>
</template>
<script>
export default {
  props: {
    isNewform: {
      type: Boolean,
      default: true,
    },
    rtId: {
      type: String,
      default: '',
    },
    clusterType: {
      type: String,
      default: '',
    },
    showTable: {
      type: Boolean,
      default: true,
    },
    forceAll: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      fieldList: {
        fields: [],
      },
      loading: false,
      schemaSql: {},
    };
  },
  watch: {
    rtId: function (val) {
      val && this.getSchemaAndSql();
    },
  },
  async mounted() {
    this.rtId && (await this.getSchemaAndSql());
  },
  methods: {
    async getSchemaAndSql() {
      this.loading = true;
      await this.bkRequest
        .httpRequest('meta/getSchemaAndSqlBase', {
          params: { rtid: this.rtId },
          query: Object.assign({}, (this.forceAll && { flag: 'all' }) || {}),
        })
        .then(res => {
          // this.axios.get(getSchemaAndSqlUrl)
          if (res.result) {
            this.schemaSql = res.data;
            this.fieldList = res.data.storage[this.clusterType];
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.loading = false;
          this.$emit('onSchemaAndSqlComplete', this.fieldList, this.schemaSql);
        });
    },
  },
};
</script>

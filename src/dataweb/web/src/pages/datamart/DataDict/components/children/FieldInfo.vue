

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
  <div v-bkloading="{ isLoading: isTableLoading }">
    <bkdata-table :data="orderData"
      :pagination="pagination"
      :emptyText="emptyText"
      @page-change="handlePageChange"
      @page-limit-change="handlePageLimitChange">
      <bkdata-table-column type="index"
        :label="$t('序号')"
        align="center"
        width="60" />
      <bkdata-table-column minWidth="200"
        :label="$t('字段名')">
        <div v-if="!$route.query.tdw_table_id"
          slot-scope="data"
          :title="`${data.row.field_name}`">
          {{ data.row.field_name }}
        </div>
        <div v-else
          slot-scope="data"
          :title="`${data.row.field_name}`">
          {{ data.row.field_name }}
        </div>
      </bkdata-table-column>
      <bkdata-table-column v-if="!$route.query.tdw_table_id"
        minWidth="200"
        :label="$t('字段中文名')">
        <div slot-scope="data">
          <span v-if="modifyIndex !== data.$index"
            :title="data.row.field_alias || ''">
            {{ data.row.field_alias || '' }}
          </span>
          <bkdata-input v-else
            v-model="dataAlias"
            v-bkloading="{ isLoading: isModifyLoading }" />
        </div>
      </bkdata-table-column>
      <bkdata-table-column minWidth="100"
        :label="$t('类型')"
        prop="field_type">
        <div slot-scope="data"
          :title="data.row.field_type">
          {{ data.row.field_type }}
        </div>
      </bkdata-table-column>
      <!-- 只有结果表或者tdw表的时候才可以修改描述，结果表跳转到另一个页面修改，tdw表使用EditInput组件修改 -->
      <bkdata-table-column v-if="!$route.query.tdw_table_id"
        width="300"
        :label="$t('描述')"
        prop="data">
        <div slot-scope="data"
          class="text-overflow">
          <span v-if="modifyIndex !== data.$index"
            :title="data.row.description">
            {{ data.row.description }}
          </span>
          <bkdata-input v-else
            v-model="dataDes"
            v-bkloading="{ isLoading: isModifyLoading }" />
        </div>
      </bkdata-table-column>
      <bkdata-table-column v-else
        width="300"
        :label="$t('描述')">
        <template slot-scope="{ row }">
          <EditInput :isEdit="isModifyDes"
            :toolTipsContent="modifyDesTips($t('数据描述'))"
            :rowData="row"
            :isCancelFieldLoading="isCancelFieldLoading"
            :inputValue="row.description"
            @change="modifyFieldDes" />
        </template>
      </bkdata-table-column>
      <bkdata-table-column v-if="!$route.query.tdw_table_id"
        width="200"
        :label="$t('最新字段值')"
        prop="newField" />
      <bkdata-table-column v-if="$route.query.result_table_id && isShowOptions"
        width="100"
        :label="$t('操作')">
        <template slot-scope="props">
          <div v-if="modifyIndex === props.$index"
            class="button-container">
            <a href="javascript:void(0);"
              @click.stop="modifyField(props)">
              {{ $t('保存') }}
            </a>
            <a href="javascript:void(0);"
              @click.stop="modifyIndex = null">
              {{ $t('取消') }}
            </a>
          </div>
          <i v-else
            v-bk-tooltips="internalField.includes(props.row.field_name) ? $t('禁止修改系统内部字段') : modifyDesTips()"
            class="bk-icon icon-edit"
            :class="{ 'is-disabled': !isModifyDes || internalField.includes(props.row.field_name) }"
            @click="linkToDes(props)" />
        </template>
      </bkdata-table-column>
    </bkdata-table>
  </div>
</template>
<script>
import Bus from '@/common/js/bus.js';
export default {
  components: {
    EditInput: () => import('./EditInput'),
  },
  props: {
    placeHolder: {
      type: String,
      default: '',
    },
    resultTableInfo: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      isTableLoading: true,
      fieldInfo: [],
      pageCount: 10,
      curPage: 1,
      newField: {},
      emptyText: this.$t('暂无数据'),
      isRequestedFieldInfo: false,
      modifyIndex: null,
      isModifyLoading: false,
      isCancelFieldLoading: false,
      isShowOptions: false,
      internalField: ['localTime', 'thedate', 'dtEventTimeStamp', 'dtEventTime'],
      dataAlias: '',
      dataDes: '',
    };
  },
  computed: {
    pagination() {
      return {
        count: (this.fieldInfo || []).length,
        limit: this.pageCount,
        current: this.curPage || 1,
      };
    },
    orderData() {
      return this.fieldInfo.slice(this.pageCount * (this.curPage - 1), this.pageCount * this.curPage);
    },
    isModifyDes() {
      return this.resultTableInfo.meta_status === 'success' && this.resultTableInfo.has_write_permission;
    },
  },
  watch: {
    newField(val) {
      if (Object.keys(val).length) {
        Object.keys(val).forEach(item => {
          this.$set(
            this.fieldInfo.find(field => field.field_name === item),
            'newField',
            val[item]
          );
        });
      }
    },
    placeHolder: {
      immediate: true,
      handler(val) {
        if (val) {
          this.fieldInfo = [];
          this.emptyText = val;
        } else {
          if (this.$route.query.tdw_table_id) return; // tdw结果表时，不调用下面方法
          this.getDictDetailFieldInfo();
        }
      },
    },
    resultTableInfo: {
      // TDW时，fields传入fieldinfo组件
      immediate: true,
      handler(val) {
        if (this.$route.query.dataType !== 'tdw_table') return;
        if (Object.keys(val).length && val.fields && this.$route.query.dataType === 'tdw_table') {
          this.fieldInfo = val.fields;
          this.isTableLoading = false;
        }
      },
    },
    orderData(val) {
      if (val.length) {
        if (this.isRequestedFieldInfo) {
          let data = [];
          val.forEach(item => {
            data.push(item.field_name);
          });
          Bus.$emit('postFieldInfo', data);
        }
      }
    },
    isModifyDes: {
      immediate: true,
      handler() {
        this.isShowOptions = false;
        this.$nextTick(() => {
          this.isShowOptions = true;
        });
      },
    },
  },
  mounted() {
    Bus.$on('GiveNewField', data => {
      this.newField = data;
    });
    Bus.$on('requestFieldInfo', () => {
      this.isRequestedFieldInfo = true;
      let data = [];
      if (this.orderData.length) {
        this.orderData.forEach(item => {
          data.push(item.field_name);
        });
      }
      Bus.$emit('postFieldInfo', data);
    });
  },
  methods: {
    modifyField(props) {
      if (this.isModifyLoading) return;
      this.isModifyLoading = true;
      this.isCancelFieldLoading = false;
      const field = JSON.parse(JSON.stringify(this.orderData));
      field[props.$index].field_alias = this.dataAlias;
      field[props.$index].description = this.dataDes;
      const options = {
        params: {
          field: field[props.$index],
          result_table_id: this.$route.query.result_table_id,
        },
      };
      this.bkRequest
        .httpRequest('dataDict/modifyFieldInfo', options)
        .then(res => {
          if (res.result) {
            this.orderData[props.$index].field_alias = this.dataAlias;
            this.orderData[props.$index].description = this.dataDes;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.modifyIndex = null;
          this.dataAlias = this.dataDes = '';
          this.isModifyLoading = false;
          this.isCancelFieldLoading = true;
        });
    },
    modifyDesTips(des = '') {
      return this.isModifyDes
        ? `${this.$t('点击修改')}${des}`
        : this.resultTableInfo.no_write_pers_reason || this.$t('权限不足');
    },
    linkToDes(props) {
      if (this.internalField.includes(props.row.field_name) || !this.isModifyDes) return;
      this.modifyIndex = props.$index;
      this.dataAlias = props.row.field_alias;
      this.dataDes = props.row.description;
    },
    modifyFieldDes(value, rowData) {
      const colName = this.resultTableInfo.cols_info.find(item => item.col_index === rowData.col_index).col_name;
      const options = {
        params: {
          cluster_id: this.resultTableInfo.cluster_id,
          db_name: this.resultTableInfo.db_name,
          table_name: this.resultTableInfo.table_name,
          ddls: [
            {
              col_name: colName,
              col_index: rowData.col_index,
              col_comment: value,
            },
          ],
          bk_username: this.$store.getters.getUserName,
        },
      };
      this.bkRequest
        .httpRequest('dataDict/modifyTdwFieldDes', options)
        .then(res => {
          if (res.result) {
            if (res.data === 'ok') {
              Bus.$emit('changeFieldInfo', value, rowData.col_index);
            }
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isCancelFieldLoading = true;
        });
    },
    handlePageChange(page) {
      this.curPage = page;
    },
    handlePageLimitChange(pageSize) {
      this.curPage = 1;
      this.pageCount = pageSize;
    },
    getDictDetailFieldInfo() {
      this.isTableLoading = true;
      const options = {
        params: {
          result_table_id: this.$route.query.result_table_id,
        },
      };
      this.bkRequest.httpRequest('dataDict/getDictDetailFieldInfo', options).then(res => {
        if (res.result) {
          this.fieldInfo = res.data;
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        this.isTableLoading = false;
      });
    },
  },
};
</script>
<style lang="scss" scoped>
.is-disabled {
  color: #666 !important;
  opacity: 0.6 !important;
  cursor: not-allowed !important;
}
.icon-edit {
  margin-right: 10px;
  color: #3a84ff;
  cursor: pointer;
  font-size: 14px;
}
.button-container {
  display: flex;
  justify-content: space-around;
}
</style>

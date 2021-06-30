/** 数据探索 - 结果集 */


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
  <div class="structure-set">
    <div
      v-bkloading="{ isLoading: tableLoading }"
      class="structure-set-table"
      style="background: url(signature.png) repeat">
      <bkdata-table
        v-if="!isEmptyColumns && tableList.length"
        class="bk-table-transparent"
        :data="tableList"
        :pagination="{}">
        <bkdata-table-column v-if="tableList"
          type="index"
          :label="$t('序号')"
          align="center"
          width="60" />
        <template v-for="(item, index) in columns">
          <bkdata-table-column v-if="item.isShow"
            :key="index"
            :label="item.value"
            :prop="item.value">
            <cell-click slot-scope="{ row }"
              :content="row[item.value]" />
          </bkdata-table-column>
        </template>
        <div slot="append"
          :class="['search-result-append', (tableInfo && 'active') || 'deactive']">
          {{ tableInfo }}
        </div>
      </bkdata-table>
      <div v-else
        class="no-result">
        <i class="no-result-img" />
        <div class="larger-font">
          暂无数据
        </div>
      </div>
    </div>
    <div class="structure-set-columns">
      <div class="columns-head">
        <bkdata-checkbox v-model="checkAll"
          :indeterminate="isIndeterminate"
          @change="handleCheckAllChange" />
        {{ $t('字段') }}
      </div>
      <div v-bkloading="{ isLoading: tableLoading }"
        class="columns-list">
        <bkdata-checkbox-group v-model="checkArr"
          @change="chooseColumns">
          <template v-for="(item, index) in columns">
            <bkdata-checkbox :key="index"
              :value="item.value"
              class="list-item">
              {{ item.value }}
            </bkdata-checkbox>
          </template>
        </bkdata-checkbox-group>
      </div>
    </div>
  </div>
</template>
<script>
import cellClick from '@/components/cellClick/cellClick.vue';
export default {
  components: {
    cellClick,
  },
  props: {
    task: { type: Object, default: () => ({}) },
  },
  data() {
    return {
      checkAll: false,
      tableInit: true,
      isIndeterminate: false,
      tableLoading: false,
      tableList: [],
      pagination: {
        current: 1,
        count: 100,
        limit: 15,
      },
      checkArr: [], // checkbox绑定值
      columns: [], // 表格渲染列
      fieldOptions: [], // 字段选项，用于全选
      isEmptyColumns: false,
      tableInfo: '',
    };
  },
  watch: {
    checkArr(newArr, oldArr) {
      /** 防止初始化，表格全选将内容清空 */
      if (this.tableInit) {
        this.tableInit = false;
        return;
      }

      /** 比较当前选项与上次状态的异同，对表格列进行添加/删除 */
      const filterField = newArr.length > oldArr.length ? newArr : oldArr;
      const filterFactor = newArr.length > oldArr.length ? oldArr : newArr;
      const key = filterField.filter(v => filterFactor.indexOf(v) === -1);
      this.columns.forEach(item => {
        if (key.includes(item.value)) {
          item.isShow = !item.isShow;
        }
      });
      /** 如果清空checkbox，isExptyColumns为true，不渲染表格 */
      this.isEmptyColumns = !this.checkArr.length;
    },
  },
  mounted() {
    this.getResultList();
  },
  methods: {
    // 全选
    handleCheckAllChange(val) {
      this.checkArr = val ? this.fieldOptions : [];
      this.isIndeterminate = false;
    },
    // 获取查询结果
    getResultList() {
      this.isEmptyColumns = true;
      this.tableLoading = true;
      this.bkRequest
        .httpRequest('dataExplore/getResultList', {
          params: { query_id: this.task.query_id },
          query: { limit: true },
        })
        .then(res => {
          if (res.code === '00') {
            if (res.data) {
              const { list, timetaken, totalRecords } = res.data;
              const selectFieldsOrder = res.data['select_fields_order'] || [];
              this.tableInfo = res.data.info;
              this.tableList = list;
              this.checkArr = selectFieldsOrder;
              this.checkAll = true;
              this.fieldOptions = Array.from(this.checkArr);
              // this.columns = select_fields_order
              this.columns = selectFieldsOrder.map(item => ({ value: item, isShow: true }));
              this.isEmptyColumns = false;
            }
          } else {
            this.tableList = [];
            this.$bkMessage({
              theme: 'error',
              message: res.message,
            });
          }
        })
        ['finally'](() => {
          this.tableLoading = false;
        });
    },
    // 选择字段
    chooseColumns(value) {
      let checkedCount = value.length;
      this.checkAll = checkedCount === this.columns.length;
      this.isIndeterminate = checkedCount > 0 && checkedCount < this.columns.length;
    },
  },
};
</script>
<style lang="scss" scoped>
.structure-set {
  width: 100%;
  height: 100%;
  display: flex;
  .structure-set-table {
    // max-width: 90%;
    height: 100%;
    flex: 1;
    padding: 25px 15px 8px;
    overflow: auto;
    position: relative;

    ::v-deep .bk-table-body-wrapper {
      height: auto;
      td {
        height: 25px;
      }
      .cell {
        height: 25px;
        line-height: 25px;
        font-size: 12px;
        padding: 0 8px;
      }
    }
  }
  .search-result-append {
    &.active {
      color: red;
      padding: 5px 15px;
      font-weight: 600;
    }

    &.deactive {
      display: none;
    }
  }
  .structure-set-columns {
    width: 320px;
    height: 100%;
    border-left: 1px solid #dcdee5;
    .columns-head {
      height: 32px;
      line-height: 32px;
      padding: 0 16px;
      color: #000000;
      font-size: 12px;
      border-bottom: 1px solid #dcdee5;
      .bk-form-checkbox,
      .bk-form-radio {
        margin-right: 4px;
      }
    }
    .columns-list {
      height: calc(100% - 80px);
      padding-left: 16px;
      padding-top: 10px;
      overflow: auto;
      .bk-form-checkbox .bk-checkbox-text {
        font-size: 12px;
      }
      .list-item {
        width: 90%;
        overflow: hidden;
        margin-bottom: 8px;
      }
    }
  }
}
</style>

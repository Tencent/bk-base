

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
  <!--
        字段设置展示模块
     -->
  <div class="field-config">
    <div class="config-list-table"
      :style="listTableStyle">
      <bkdata-table
        :data="fields"
        :emptyText="$t('暂无数据')"
        :pagination="pagination"
        @page-change="handlePageChange"
        @page-limit-change="handlePageLimitChange">
        <bkdata-table-column :label="$t('序号')"
          width="100"
          prop="field_index" />
        <bkdata-table-column :label="$t('字段名称')"
          width="300"
          prop="fieldName" />
        <bkdata-table-column :label="$t('类型')"
          width="80"
          prop="type" />
        <bkdata-table-column :label="$t('配置项')"
          width="200">
          <div slot-scope="item"
            class="bk-table-inlineblock">
            <template v-for="(conf, _index) in item.row.configs">
              <bkdata-checkbox
                :key="`${item.row.physical_field}${_index}`"
                v-model="item.row[conf.field]"
                :disabled="isEditModeDisabled(conf.key) || conf.isReadonly"
                :name="item.row.physical_field"
                @change="e => handleItemConfChanged(e, conf, item.row, fieldConfigModuleData.opt)">
                {{ conf.value }}
              </bkdata-checkbox>
            </template>
          </div>
        </bkdata-table-column>
        <bkdata-table-column :label="$t('中文名称')"
          prop="field_alias" />
      </bkdata-table>
    </div>
    <div v-if="indexFieldsLength"
      class="config-drag-table"
      :style="dragTableStyle">
      <div class="drag-table-header">
        <span>{{ $t('主键顺序') }}</span><span class="sub-header-des">
          （已选<b class="field-count"> {{ indexFieldsLength }} </b>个主键）
        </span>
      </div>
      <DragTable
        :data="indexTableData"
        :showHeader="false"
        :isEmpty="!indexFieldsLength"
        dragHandle=".drag-handle-icon"
        @drag-end="handleDragEnd">
        <template v-for="(item, rowIndex) in indexTableData.list">
          <DragTableRow :key="`${item.physical_field}_${rowIndex}`"
            :data-x="rowIndex"
            :data="item">
            <DragTableColumn>{{ item.physical_field }}</DragTableColumn>
            <DragTableColumn :width="`20px`">
              <span v-if="mode === 'add'"
                class="bk-rm-index-field"
                @click="() => handleRemoveIndexField(item)">
                <i class="bk-icon icon-close" />
              </span>
            </DragTableColumn>
          </DragTableRow>
        </template>
      </DragTable>
    </div>
  </div>
</template>

<script>
import { DragTableColumn, DragTable, DragTableRow } from '@/components/DragTable/index';
export default {
  components: { DragTableColumn, DragTable, DragTableRow },
  props: {
    type: {
      type: [String, Object],
      default: '',
    },
    backupField: {
      type: Object,
      default: () => ({}),
    },
    mode: {
      type: String,
      default: 'edit',
    },
    fieldConfigModuleData: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      curPage: 1,
      pageCount: 10, // 每一页数据量设置
      tableDataTotal: 1,
      isRending: true,
      dragTableWidth: 320,
    };
  },
  computed: {
    indexFields() {
      return (
        (this.type === 'clickhouse'
          && (this.fieldConfigModuleData.index_fields
            || []).map(field => Object.assign(field, { isDrag: this.mode === 'add' })
          ))
        || []
      );
    },
    indexTableData() {
      return {
        columns: ['physical_field', 'option'].map(col => ({ label: '', props: col })),
        list: (this.indexFields || []).map(field => ({ isDrag: this.mode === 'add', ...field })),
      };
    },
    indexFieldsLength() {
      return this.indexFields.length;
    },

    listTableStyle() {
      return {
        width: `calc(100% - ${this.indexFieldsLength ? this.dragTableWidth : 0}px)`,
      };
    },

    dragTableStyle() {
      return {
        width: `${this.dragTableWidth}px`,
      };
    },

    isDepulicationStorage() {
      return ['es', 'tspider', 'mysql'].includes(this.type) || false;
    },
    fields: {
      get() {
        let tableList = [];
        // eslint-disable-next-line vue/no-side-effects-in-computed-properties
        this.isRending = true;
        if (this.fieldConfigModuleData.fields) {
          tableList = this.fieldConfigModuleData.fields.map(i => {
            i.fieldName = i.physical_field || i.field_name;
            i.type = i.physical_field_type || i.field_type;
            return i;
          });
        }
        // eslint-disable-next-line vue/no-side-effects-in-computed-properties
        this.tableDataTotal = tableList.length;
        this.$nextTick(_ => {
          this.isRending = false;
        });
        return tableList.slice(this.pageCount * (this.curPage - 1), this.pageCount * this.curPage) || [];
      },
    },
    pagination() {
      return {
        count: this.tableDataTotal,
        limit: this.pageCount,
        current: this.curPage || 1,
      };
    },
  },

  mounted() {
    this.isRending = false;
  },
  methods: {
    handleDragEnd(endList) {
      const copyList = (endList[0] || []).map((field, index) => ({ index, physical_field: field.physical_field }));
      this.$set(this.fieldConfigModuleData, 'index_fields', copyList);
      this.$set(this.backupField, 'index_fields', copyList);
    },
    handleRemoveIndexField(field) {
      const originField = this.fields.find(f => f.physical_field === field.physical_field);
      if (originField) {
        const cfg = originField.configs.find(cf => cf.key === 'primary_keys');

        if (cfg) {
          this.$set(originField, cfg.field, false);
          this.handleItemConfChanged(false, cfg, field, 'ck');
        }
      }
    },
    isEditModeDisabled(key) {
      return this.mode === 'edit' && /^mysql|tspider|tcaplus/i.test(this.type) && key !== 'storage_keys';
    },
    handlePageChange(page) {
      this.curPage = page;
    },
    handlePageLimitChange(pageSize) {
      this.curPage = 1;
      this.pageCount = pageSize;
    },
    handleItemConfChanged(checked, conf, item, opt = 'radio') {
      const changedKey = [];
      if (!this.isRending && /radio/i.test(opt)) {
        for (let i = 0; i < item.configs.length; i++) {
          if (item.configs[i].field !== conf.field) {
            if (this.isDepulicationStorage && conf.key === 'storage_keys') break;
            const value =              this.isDepulicationStorage && item.configs[i].key === 'storage_keys'
              ? item[item.configs[i].field]
              : false;
            this.$set(item.configs[i], 'checked', value);
            this.$set(item, item.configs[i].field, value);
            changedKey.push({
              key: item.configs[i].field,
              value: value,
            });
          }
        }
      }

      if (!Object.prototype.hasOwnProperty.call(this.fieldConfigModuleData, 'index_fields')) {
        this.$set(this.fieldConfigModuleData, 'index_fields', []);
      }

      if (checked) {
        // eslint-disable-next-line vue/no-mutating-props
        this.fieldConfigModuleData.index_fields.push({ index: 0, physical_field: item.physical_field });
      } else {
        const index = this.fieldConfigModuleData.index_fields.findIndex(f => f.physical_field === item.physical_field);
        if (index >= 0) {
          // eslint-disable-next-line vue/no-mutating-props
          this.fieldConfigModuleData.index_fields.splice(index, 1);
        }
      }

      this.backupField.fields.forEach(field => {
        if (field.field_name === item.field_name) {
          field[conf.field] = item[conf.field];
        }
        changedKey.forEach(obj => {
          field[obj.key] = obj.value;
        });
      });

      this.$set(this.backupField, 'index_fields', this.fieldConfigModuleData.index_fields);
    },
  },
};
</script>

<style lang="scss" scoped>
.no_data {
  height: 120px;
  display: flex;
  align-items: center;
  flex-direction: column;
  justify-content: center;
  border: 1px solid #e6e6e6;
  border-top: none;
}
.field-config {
  margin: -15px 0;
  padding: 20px 0;
  border-top: 1px solid #ddd;
  width: 100%;
  display: flex;
  table {
    border: 1px solid #e6e6e6;
    // table-layout: fixed;
    > tbody > tr > td {
      vertical-align: middle;
      &.field-name {
        vertical-align: middle;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }
    }

    .field-desc {
      max-width: 300px;
      white-space: nowrap;
      text-overflow: ellipsis;
      overflow: hidden;
    }
  }

  .bk-form-checkbox {
    padding: 2px 0;
  }
}

.center {
  text-align: center;
}

.config-drag-table {
  background: #f5f6fa;
  padding: 0 15px;

  ::v-deep .drag-table {
    height: auto;
  }

  .drag-table-header {
    height: 42px;
    display: flex;
    align-items: center;

    .sub-header-des {
      font-size: 12px;
      color: #979ba5;
      line-height: 17px;
      height: 17px;
      .field-count {
        color: #3a84ff;
      }
    }
  }

  ::v-deep .drag-table-top {
    border: none;
    background: transparent;
    padding-left: 0;
  }

  ::v-deep .drag-table-view {
    border: none;
    background: transparent;

    .drag-table-row {
      border-top: none;
      height: 32px;
      margin-top: 5px;

      .row-icon {
        line-height: 32px;
        &:hover {
          color: #a3c5fd;
        }
      }

      .drag-table-column {
        height: 32px;
        line-height: 32px;
      }
    }
  }

  .bk-rm-index-field {
    cursor: pointer;
    font-size: 16px;
    &:hover {
      color: #a3c5fd;
    }
  }
}
</style>

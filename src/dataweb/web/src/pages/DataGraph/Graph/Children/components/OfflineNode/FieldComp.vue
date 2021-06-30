

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
    class="tdw-jar-fields">
    <Tips :text="$t('请根据SQL填写结果表字段信息')"
      size="small"
      class="mt10 mb10" />
    <bkdata-table :data="fields">
      <bkdata-table-column align="center"
        :label="$t('字段名称')"
        prop="field_name">
        <TableCell
          v-model="props.row.field_name"
          slot-scope="props"
          v-tooltip.notrigger="{
            content: $t('该值不能为空'),
            visible: props.row.validate.field_name,
            class: 'error-red',
          }"
          :canEdit="props.row.edit"
          @inputChange="validateStatus(props.row, 'field_name')">
          <span slot="content"
            :title="props.row.field_name">
            {{ props.row.field_name || $t('点击编辑') }}
          </span>
        </TableCell>
      </bkdata-table-column>
      <bkdata-table-column align="center"
        :label="$t('类型')"
        width="160">
        <template slot-scope="props">
          <bkdata-selector
            v-if="props.row.edit"
            :selected.sync="props.row.field_type"
            :list="fieldTypeList"
            :settingKey="'field_type'"
            :displayKey="'field_type_name'" />
          <span v-else
            :title="props.row.field_name">
            {{ props.row.field_type }}
          </span>
        </template>
      </bkdata-table-column>
      <bkdata-table-column align="center"
        :label="$t('描述')">
        <TableCell
          v-model="props.row.description"
          slot-scope="props"
          v-tooltip.notrigger="{
            content: $t('该值不能为空'),
            visible: props.row.validate.description,
            class: 'error-red',
          }"
          :canEdit="props.row.edit"
          @inputChange="validateStatus(props.row, 'description')">
          <span slot="content"
            :title="props.row.description">
            {{ props.row.description || $t('点击编辑') }}
          </span>
        </TableCell>
      </bkdata-table-column>
      <bkdata-table-column align="center"
        :label="$t('操作')">
        <template slot-scope="props">
          <span class="click-item"
            @click.stop="removeField(props.row, props.$index)">
            {{ $t('删除') }}
          </span>
        </template>
      </bkdata-table-column>
    </bkdata-table>
    <div
      v-tooltip.notrigger="{ content: validator.message, visible: validator.status, class: 'error-red' }"
      class="btn-adds dotted-btn"
      @click.stop="addHandle">
      <i class="bk-icon left-icon icon-plus icons" />{{ $t('添加') }}
    </div>
    <slot />
  </div>
</template>
<script>
import TableCell from '@/pages/DataGraph/Graph/Components/UDF/components/editTableCell.vue';
import Tips from '@/components/TipsInfo/TipsInfo.vue';
export default {
  components: {
    TableCell,
    Tips,
  },
  props: {
    fields: {
      type: Array,
      default: () => [],
    },
    hasDefaultField: {
      type: Boolean,
      default: true,
    },
    defaultFields: {
      type: Array,
      default: () => {
        return [];
      },
    },
  },
  data() {
    return {
      loading: false,
      fieldTypeList: [],
      validator: {
        status: false,
        message: this.$t('请添加字段配置'),
      },
    };
  },
  mounted() {
    this.getTypeList();
  },
  methods: {
    clearStatus() {
      this.validator.status = false;
      if (this.fields.length) {
        this.fields.forEach(item => {
          item.validate[key] = false;
        });
      }
    },
    validateForm() {
      if (this.fields.length === 0) {
        this.validator.status = true;
        return Promise.reject(false);
      }

      const validateField = ['field_name', 'field_type', 'description'];
      let result = true;
      this.fields.forEach(item => {
        validateField.forEach(key => {
          if (item[key] === '' || item[key] === undefined) {
            item.validate[key] = true;
            result = false;
          } else {
            item.validate[key] = false;
          }
        });
      });
      this.$forceUpdate();
      return result ? Promise.resolve(true) : Promise.reject(false);
    },
    validateStatus(validator, key) {
      if (validator[key]) {
        validator.validate[key] = false;
      } else {
        validator.validate[key] = true;
      }
    },
    getTypeList() {
      this.bkRequest.httpRequest('/tdw/getTypeListJar').then(res => {
        if (res.result) {
          this.fieldTypeList = res.data;
        }
      });
    },
    addHandle() {
      this.validator.status = false;
      this.$emit('addFieldHandle', {
        field_name: '',
        field_type: 'double',
        description: '',
        validate: {
          field_name: false,
          field_type: false,
          description: false,
        },
        edit: true,
      });
    },
    removeField(item, index) {
      this.$emit('removeFieldHandle', item, index);
    },
  },
};
</script>

<style lang="scss">
.tdw-jar-fields {
  display: flex;
  flex-direction: column;
  ::v-deep .bk-table-empty-block {
    display: none;
  }
  button {
    margin: 10px auto;
  }
  .click-item {
    cursor: pointer;
    color: #3a84ff;
  }
  .dotted-btn {
    width: 100%;
    margin-top: 8px;
    border-radius: 2px;
    text-align: center;
    line-height: 28px;
    border: 1px dashed #979ba5;
    cursor: pointer;
    font-size: 12px;
    .icons {
      font-size: 12px;
      color: #63656e;
      margin-right: 5px;
    }
    &.emphasize {
      background-color: #e1ecff;
      border-color: #3a84ff;
      color: #3a84ff;
      .icons {
        color: #3a84ff;
      }
    }
    &:hover {
      border-color: #699df4;
      color: #3a84ff;
      .icons {
        color: #3a84ff;
      }
    }
  }
}
</style>

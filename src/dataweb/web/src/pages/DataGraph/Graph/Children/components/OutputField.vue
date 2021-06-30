

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
  <div class="output-field-wrapper">
    <bkdata-table :data="fields"
      size="small"
      :rowBorder="rowBorder"
      :outerBorder="rowBorder">
      <bkdata-table-column :label="$t('名称')">
        <TableCell
          v-model="props.row.field_name"
          slot-scope="props"
          :canEdit="!readOnly"
          @inputChange="inputChangeHandle(props.row)">
          <span
            slot="content"
            v-tooltip.notrigger="{
              content: props.row.validate.name.errorMsg,
              visible: props.row.validate.name.status,
              class: 'error-red',
            }"
            :title="props.row.field_name">
            {{ props.row.field_name || $t('点击编辑') }}
          </span>
        </TableCell>
      </bkdata-table-column>
      <bkdata-table-column :label="$t('类型')">
        <template slot-scope="props">
          <span v-if="readOnly">{{ props.row.field_type }}</span>
          <bkdata-selector
            v-else
            :selected.sync="props.row.field_type"
            :popoverOptions="{
              appendTo: rightPanelDom,
            }"
            :list="typeList"
            :settingKey="'field_type'"
            :displayKey="'field_type'" />
        </template>
      </bkdata-table-column>
      <bkdata-table-column :label="$t('中文名称')">
        <TableCell v-model="props.row.field_alias"
          slot-scope="props"
          @inputChange="inputChangeHandle(props.row)">
          <span
            slot="content"
            v-tooltip.notrigger="{
              content: props.row.validate.alias.errorMsg,
              visible: props.row.validate.alias.status,
              class: 'error-red',
            }"
            :title="props.row.alias">
            {{ props.row.field_alias || $t('点击编辑') }}
          </span>
        </TableCell>
      </bkdata-table-column>
      <bkdata-table-column v-if="openTimeOption"
        :renderHeader="timeColumn"
        minWidth="50"
        align="center">
        <template slot-scope="props">
          <bkdata-checkbox
            v-model="props.row.event_time"
            v-bk-tooltips="{ content: $t('指定为时间字段'), placement: 'left' }"
            @change="configChecked(props)" />
        </template>
      </bkdata-table-column>
      <bkdata-table-column v-if="!readOnly"
        :label="$t('操作')"
        width="60">
        <template slot-scope="props">
          <span class="del"
            @click.stop="removeField(props.row, props.$index)">
            {{ $t('删除') }}
          </span>
        </template>
      </bkdata-table-column>
    </bkdata-table>
    <div v-if="fields.length && openTimeOption"
      class="box-tip-wrapper">
      <Tips
        :text="$t('数据时间字段必须为13位时间戳_如不选择_则以当前时间作为数据时间')"
        :size="'small'"
        style="margin-top: 8px" />
    </div>
    <div v-if="!readOnly"
      class="field-item btn-adds"
      @click.stop="addHandle">
      <i class="icon-plus" />{{ $t('添加') }}
    </div>
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
    readOnly: {
      type: Boolean,
      default: false,
    },
    openTimeOption: {
      type: Boolean,
      default: true,
    },
    rowBorder: {
      type: Boolean,
      default: true,
    },
  },
  data() {
    return {
      typeList: [], // 字段类型
      fieldTypeList: [],
    };
  },
  computed: {
    rightPanelDom() {
      return document.getElementsByClassName('right-column')[0];
    },
    aliasWidth() {
      return this.readOnly ? 200 : 70;
    },
  },
  created() {
    this.getOutputTypeList();
  },
  methods: {
    /*
            获取输出字段类型
        */
    getOutputTypeList() {
      return this.bkRequest.httpRequest('/dataFlow/getSdkTypeList').then(res => {
        if (res.result) {
          this.typeList = res.data;
        }
      });
    },
    timeColumn() {
      const tip = this.$t('用于系统内部数据流转的时间');
      return <span class="icon-time" v-bk-tooltips={tip}></span>;
    },
    validateFieldFormat(val, reg, reg1) {
      if (reg.test(val) || reg1.test(val)) {
        return true;
      }
      return false;
    },
    inputChangeHandle(item) {
      if (item.validate.name.status === false && item.validate.alias.status === false) return;
      this.$emit('vailidateField');
      console.log('validate', item);
    },
    configChecked(item) {
      // 配置项只允许其中其中一条被选择
      const value = item.row.event_time;
      const index = item.$index;
      if (value) {
        this.fields.forEach((item, idx) => {
          if (idx !== index) {
            item.event_time = false;
          }
        });
      }
    },
    addHandle() {
      const newItem = {
        field_name: '',
        field_type: 'int',
        field_alias: '',
        event_time: false,
        validate: {
          alias: {
            status: false,
            errorMsg: this.validator.message.required,
          },
          name: {
            status: false,
            errorMsg: this.validator.message.required,
          },
        },
      };
      // eslint-disable-next-line vue/no-mutating-props
      this.fields.push(newItem);
      this.$emit('addField');
    },
    removeField(item, index) {
      // eslint-disable-next-line vue/no-mutating-props
      this.fields.splice(index, 1);
    },
  },
};
</script>

<style lang="scss" scoped>
.output-field-wrapper {
  width: 100%;
  .bk-table {
    ::v-deep .bk-table-empty-block {
      display: none;
      .bk-table-empty-text div {
        line-height: 1;
      }
    }
    ::v-deep .cell {
      padding-left: 10px !important;
      padding-right: 10px !important;
      .bk-form-checkbox {
        margin-right: 0;
        outline: none;
      }
      .del {
        color: #3a84ff;
        font-size: 12px;
        cursor: pointer;
      }
    }
  }
  .btn-adds {
    margin-top: 8px;
    text-align: center;
    line-height: 28px;
    border: 1px dashed #979ba5;
    cursor: pointer;
    .icons {
      font-size: 12px;
      color: #63656e;
      margin-right: 5px;
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

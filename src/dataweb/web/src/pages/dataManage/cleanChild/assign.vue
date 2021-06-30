

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
  <div class="clean-assign">
    <div
      v-if="innerParam.assign_method !== 'direct' && innerParam.assign_method !== 'json'"
      class="clearfix moreParams">
      <bkdata-button size="small"
        class="mr10"
        @click="$refs.dataImport.isShow = true">
        {{ $t('批量导入') }}
      </bkdata-button>
      <bkdata-dropdown-menu @show="isDropdownShow = true"
        @hide="isDropdownShow = false">
        <bkdata-button slot="dropdown-trigger"
          size="small"
          class="mr10">
          <span>{{ $t('批量导出') }}</span>
          <i :class="['bk-icon icon-angle-down', { 'icon-flip': isDropdownShow }]" />
        </bkdata-button>
        <ul slot="dropdown-content"
          class="bk-dropdown-list">
          <li>
            <a href="javascript:;"
              @click="exportToJson">
              {{ $t('导出为json') }}
            </a>
          </li>
          <li>
            <a href="javascript:;"
              @click="exportToCsvFile">
              {{ $t('导出为csv') }}
            </a>
          </li>
        </ul>
      </bkdata-dropdown-menu>
    </div>
    <div
      v-for="(item, idx) in innerParam.fields"
      :key="idx"
      class="clearfix moreParams"
      :class="{ 'active-highlight': item.highlight }">
      <div
        :class="[
          'delimiter-key',
          'clearfix',
          'fl',
          'mr20',
          'line',
          { 'json-wrapper': innerParam.assign_method === 'json' },
        ]">
        <div class="delimiter-title fl">
          <bkdata-selector
            :class="{ 'long-selector': innerParam.assign_method === 'direct' }"
            :placeholder="operatorKey.placehoder"
            :list="operatorKey.list"
            :selected="innerParam.assign_method"
            @item-selected="selectOperator" />
        </div>
        <div
          v-if="innerParam.assign_method !== 'direct' && innerParam.assign_method !== 'json'"
          class="delimiter-select fl">
          <div class="bk-form-content"
            :class="{ error: error['fields#' + idx + '#location'] }">
            <bkdata-combobox
              v-if="innerParam && innerParam.assign_method === 'key'"
              :list="calcOperatorParam.keys ? calcOperatorParam.keys : []"
              :extCls="repeatIndex === idx ? 'disabled-color' : ''"
              :placeholder="hoder.keyHoder"
              :value.sync="item.location"
              :showOpen="false"
              :showClear="false"
              :showEmpty="false"
              :bypass="true"
              @input="checkFieldLocation(idx, item.location)" />
            <bkdata-combobox
              v-else-if="innerParam && innerParam.assign_method === 'index'"
              :list="calcOperatorParam.length ? generateLengthList() : []"
              :extCls="repeatIndex === idx ? 'disabled-color' : ''"
              :placeholder="hoder.keyHoder"
              :value.sync="item.location"
              :showOpen="false"
              :showClear="false"
              :showEmpty="false"
              :bypass="true"
              @input="checkFieldLocation(idx, item.location)" />
            <bkdata-input
              v-else
              v-model="item.location"
              :disabled="repeatIndex === idx"
              :placeholder="hoder.keyHoder"
              @focus="focusActive"
              @input="checkFieldLocation(idx, item.location)" />
            <div v-show="error['fields#' + idx + '#location']"
              class="error-msg">
              {{ error['fields#' + idx + '#location'] }}
            </div>
          </div>
        </div>
        <div
          v-if="innerParam.assign_method === 'json'"
          class="json-key"
          :class="{ error: error['fields#' + idx + '#location'] }">
          <span> key </span>
          <bkdata-input
            v-model="item.location"
            style="width: 100px"
            @focus="focusActive"
            @input="checkFieldLocation(idx, item.location)" />
          <div v-show="error['fields#' + idx + '#location']"
            class="error-msg">
            {{ error['fields#' + idx + '#location'] }}
          </div>
        </div>
      </div>
      <div class="clearfix fl mr20 line">
        <div class="operator-param clearfix fl">
          <div class="param-select fl title">
            <span>{{ $t('字段') }}</span>
          </div>
          <div
            class="bk-form-content param-input fl"
            :class="{
              error: error['fields#' + idx + '#assign_to'] || error['fields#' + idx + '#repeat'],
            }">
            <bkdata-input
              v-model="item.assign_to"
              v-focus="item.highlight"
              :disabled="repeatIndex === idx"
              :placeholder="hoder.keyHoder"
              @focus="focusActive(item.id)"
              @blur="answerBlur(item.id)"
              @input="checkFieldAssignTo(idx, item.assign_to)" />
            <div v-if="error['fields#' + idx + '#assign_to']"
              class="error-msg">
              {{ error['fields#' + idx + '#assign_to'] }}
            </div>
            <div
              v-else-if="error['fields#' + idx + '#repeat'] && !error['fields#' + idx + '#assign_to']"
              class="error-msg">
              {{ error['fields#' + idx + '#repeat'] }}
            </div>
          </div>
        </div>
        <div class="operator-type fl"
          :class="{ error: error['fields#' + idx + '#type'] }">
          <!-- <template> -->
          <div class="type-title fl">
            {{ $t('类型') }}
          </div>
          <div class="type-select fl">
            <bkdata-selector
              :placeholder="characterType.placehoder"
              :disabled="repeatIndex === idx"
              :list="dataIdDisplay"
              :selected.sync="item.type"
              :settingKey="'field_type'"
              :displayKey="'type_name'"
              @item-selected="checkFieldType(idx, item.type)" />
            <div v-show="error['fields#' + idx + '#type']"
              class="error-msg">
              {{ error['fields#' + idx + '#type'] }}
            </div>
          </div>
          <!-- </template> -->
        </div>
      </div>
      <OperateRow
        v-if="shouldShowOperator"
        class="line-left fl ml20"
        :isAdd="!isAdd"
        :isDelete="innerParam.fields.length === 1"
        @add-row="addParams(idx + 1, item)"
        @delete-row="delectParams(idx, item)" />
    </div>
    <cleandata-import ref="dataImport"
      :innerParams="innerParam"
      @importData="changeInnerParam" />
  </div>
</template>
<script>
import { JValidator } from '@/common/js/check';
import { generateId } from '@/common/js/util.js';
import ChildBase from './child';
import Vue from 'vue';
import cleandataImport from './cleanDataImport';
// import mixin from './mixin';
import OperateRow from '@/pages/dataManage/cleanChild/components/OperateRow.vue';
// import DefaultValue from './components/defaultValue';

export default {
  name: 'vAssign',
  components: {
    cleandataImport,
    // mixin,
    OperateRow,
    // DefaultValue,
  },
  directives: {
    focus: {
      // 指令的定义
      inserted: function (el, binding, vnode) {
        if (binding.value) {
          el.focus();
        } else {
          el.blur();
        }
      },
      update: function (el, binding, vnode) {
        if (binding.value) {
          el.focus();
        } else {
          el.blur();
        }
      },
    },
  },
  mixins: [ChildBase],
  props: {
    operatorParam: {
      type: Object,
    },
    index: {
      type: Number,
    },
    dataIdSet: {
      type: Array,
      default: () => [
        {
          field_type: 'string',
          type_name: 'string',
        },
        {
          field_type: 'int',
          type_name: 'int',
        },
        {
          field_type: 'long',
          type_name: 'long',
        },
      ],
    },
  },
  data() {
    return {
      isDropdownShow: false,
      importingShow: false,
      hoder: {
        keyHoder: this.$t('请输入'),
      },
      operatorKey: {
        placehoder: this.$t('请选择'),
        list: [
          {
            id: 'key',
            name: 'key',
          },
          {
            id: 'index',
            name: 'index',
          },
          {
            id: 'direct',
            name: 'direct',
          },
          {
            id: 'json',
            name: 'json',
          },
        ],
      },
      jsonType: [
        {
          disabled: false,
          id: 1,
          isDisabled: false,
          field_type: 'string',
          type_name: 'string(512)',
        },
        {
          disabled: 0,
          id: 2,
          isDisabled: false,
          field_type: 'text',
          type_name: 'text',
        },
      ],
      repeatError: [],
      characterType: {
        // 类型
        placehoder: this.$t('请选择类型'),
        list: [
          {
            id: 'string',
            name: 'string',
          },
          {
            id: 'int',
            name: 'int',
          },
          {
            id: 'long',
            name: 'long',
          },
        ],
      },
      innerParam: {
        assign_method: 'index',
        fields: [
          {
            id: generateId('field'),
            type: '',
            assign_to: '',
            location: '',
            highlight: false,
            // 'default_value': '',
            // 'default_type': 'null'
          },
        ],
      },
      error: {},
      repeatIndex: null,
      isAdd: true,
    };
  },
  computed: {
    shouldShowOperator() {
      if (this.innerParam.assign_method === 'json') {
        const len = this.innerParam.fields.length;
        for (let i = 0; i < len; i++) {
          if (this.innerParam.fields[i].location === '__all_keys__') {
            this.deleteAllFiled(i);
            return false;
          }
        }
        return true;
      } else if (this.innerParam.assign_method !== 'direct') {
        return true;
      }
      return false;
    },
    configToExport() {
      if (this.innerParam.fields.length === 0) {
        return [];
      }
      let retConfig = this.innerParam.fields.map(obj => {
        let rObj = {};
        rObj.assign_method = this.innerParam.assign_method;
        rObj.location = obj.location;
        rObj.assign_to = obj.assign_to;
        rObj.type = obj.type;
        rObj.field_alias = obj.field_alias;
        return rObj;
      });
      return retConfig;
    },
    calcOperatorParam() {
      return this.operatorParam || {};
    },
    dataIdDisplay() {
      let backupDataIdSet = [
        {
          disabled: 0,
          id: 1,
          isDisabled: false,
          field_type: 'string',
          type_name: 'string',
        },
        {
          field_type: 'int',
          disabled: 0,
          id: 2,
          isDisabled: false,
          type_name: 'int',
        },
        {
          field_type: 'long',
          disabled: 0,
          id: 3,
          isDisabled: false,
          type_name: 'long',
        },
      ];
      if (this.innerParam.assign_method === 'json') {
        return this.jsonType;
      } else if (this.dataIdSet) {
        if (this.dataIdSet instanceof Array) {
          if (this.dataIdSet.length > 0) {
            return this.dataIdSet.map((field, index) => {
              return {
                field_type: field.field_type,
                disabled: !field.active,
                id: index + 1,
                isDisabled: !field.active,
                type_name: field.field_type_name,
              };
            });
          }
        }
      }
      return backupDataIdSet;
    },
    isIncludeAssignMethod() {
      return ['key', 'index'].includes(this.innerParam.assign_method);
    },
  },
  watch: {
    innerParam: {
      handler(newVal) {
        this.$emit('operatorParam', this.formatInnerParam(newVal), this.index);
      },
      deep: true,
    },
    'innerParam.assign_method': function (val) {
      for (let key of Object.keys(this.error)) {
        if (key.match('location') !== null) {
          this.error[key] = '';
        }
      }
      if (val === 'json' && this.innerParam.fields[0].location === '') {
        this.innerParam.fields[0].location = '__all_keys__';
      }
    },
    'innerParam.fields'() {
      if (
        this.innerParam.fields !== null
        && this.innerParam.fields !== undefined
        && !(this.innerParam.fields instanceof Array)
      ) {
        this.innerParam.fields = [this.innerParam.fields];
      }
      if (this.innerParam.fields.length <= 0) {
        this.addParams(0);
      }
      for (let i = 0; i < this.innerParam.fields.length; i++) {
        if (this.innerParam.fields[i].id === undefined) {
          this.innerParam.fields[i].id = generateId('field');
        }
      }
    },
  },
  mounted() {
    this.updateInnerParam();
  },
  methods: {
    formatInnerParam(val = {}) {
      if (val.assign_method === 'direct') {
        return Object.assign({}, val, { result: val.fields[0].assign_to });
      }

      return val;
    },
    deleteAllFiled(index) {
      for (let i = 0; i < this.innerParam.fields.length; i++) {
        Vue.set(this.error, 'fields#' + i + '#assign_to', '');
      }
      let copyFields = Array.from(this.innerParam.fields);
      this.innerParam.fields = copyFields.splice(index, 1);
    },
    parseJSONToCSVStr(data) {
      if (data.length === 0) {
        return '';
      }

      const keys = Object.keys(data[0]);

      const columnDelimiter = ',';
      const lineDelimiter = '\n';

      const csvColumnHeader = keys.join(columnDelimiter);
      let csvStr = csvColumnHeader + lineDelimiter;

      data.forEach(item => {
        keys.forEach((key, index) => {
          csvStr += item[key];
          if (index < keys.length - 1) {
            csvStr += columnDelimiter;
          }
        });
        csvStr += lineDelimiter;
      });

      return encodeURIComponent(csvStr);
    },
    exportToCsvFile() {
      const data = this.configToExport;
      const csvStr = this.parseJSONToCSVStr(data);
      let dataUri = 'data:text/csv;charset=utf-8,' + csvStr;

      let exportFileDefaultName = 'data.csv';

      let linkElement = document.createElement('a');
      linkElement.setAttribute('href', dataUri);
      linkElement.setAttribute('download', exportFileDefaultName);
      linkElement.click();
    },
    exportToJson() {
      if (this.innerParam.fields.length === 0) {
        return;
      }
      const dataStr = JSON.stringify(this.configToExport);
      const dataUri = 'data:application/json;charset=utf-8,' + encodeURIComponent(dataStr);

      const exportFileDefaultName = 'data.json';

      const linkElement = document.createElement('a');
      linkElement.setAttribute('href', dataUri);
      linkElement.setAttribute('download', exportFileDefaultName);
      linkElement.click();
    },
    changeInnerParam(data) {
      this.innerParam = JSON.parse(JSON.stringify(data));
    },
    changeHighlight(fieldId, highlight) {
      for (let i = 0; i < this.innerParam.fields.length; i++) {
        if (this.innerParam.fields[i].id === fieldId) {
          this.innerParam.fields[i].highlight = highlight;
        } else {
          this.innerParam.fields[i].highlight = false;
        }
      }
      this.$forceUpdate();
    },
    answerBlur(fieldId) {
      for (let i = 0; i < this.innerParam.fields.length; i++) {
        if (this.innerParam.fields[i].id === fieldId) {
          this.innerParam.fields[i].highlight = false;
        }
      }
    },
    /**
     * 校验重复内容
     */
    checkRepeatInfo() {
      for (let i = 0; i < this.innerParam.fields.length; i++) {
        if (this.repeatError.includes(i)) {
          Vue.set(
            this.error,
            'fields#' + i + '#repeat',
            this.$t('重复的内容') + ': ' + this.innerParam.fields[i].assign_to
          );
        } else {
          Vue.set(this.error, 'fields#' + i + '#repeat', '');
        }
      }
    },
    /**
     * 显示报错信息
     */
    showErrorMessage(indexList) {
      this.repeatError = indexList;
      this.checkRepeatInfo();
    },
    selectOperator(key, data) {
      if (key !== this.innerParam.assign_method) {
        this.innerParam.fields = [
          {
            id: generateId('field'),
            type: key === 'json' ? 'text' : '',
            assign_to: '',
            location: '',
            highlight: false,
            // 'default_value': '',
            // 'default_type': 'null'
          },
        ];
      }
      this.innerParam.assign_method = key;
    },
    focusActive(fieldId) {
      for (let i = 0; i < this.innerParam.fields.length; i++) {
        if (this.innerParam.fields[i].id !== fieldId) {
          this.innerParam.fields[i].highlight = false;
        } else {
          this.innerParam.fields[i].highlight = true;
        }
      }
      this.$emit('focus', this.index);
    },
    /*
     * 点击添加算子参数
     */
    addParams(index, item) {
      if (!this.isAdd) return;
      let val = {
        id: generateId('field'),
        type: '',
        assign_to: '',
        location: '',
        highlight: false,
      };
      this.innerParam.fields.splice(index, 0, val); // (索引位置, 要删除元素的数量, 元素)
    },
    /*
     * 点击删除算子参数
     */
    delectParams(index, item) {
      console.log(index);
      for (let i = 0; i < this.innerParam.fields.length; i++) {
        Vue.set(this.error, 'fields#' + i + '#assign_to', '');
      }
      if (this.innerParam.fields.length > 1) {
        this.innerParam.fields.splice(index, 1);
      }
      if (
        this.isIncludeAssignMethod
        && this.innerParam.fields.filter(child => child.location === item.location).length === 1
      ) {
        this.repeatIndex = null;
        this.isAdd = true;
      }
    },
    check() {
      let ret = true;

      // 基本参数校验
      this.innerParam.fields.forEach((item, index) => {
        if (!this.checkFieldLocation(index, item.location)) {
          ret = false;
        }
        if (!this.checkFieldAssignTo(index, item.assign_to)) {
          ret = false;
        }
        if (!this.checkFieldType(index, item.type)) {
          ret = false;
        }
      });
      return ret;
    },
    checkFieldLocation(index, value) {
      // 赋值操作时，需要避免key或index值出现重复的情况
      if (this.isIncludeAssignMethod && this.innerParam.fields.filter(item => item.location === value).length === 2) {
        this.$bkMessage({
          message: this.$t(`${this.innerParam.assign_method}${this.$t('不能重复添加')}`),
          theme: 'warning',
          delay: 0,
        });
        this.repeatIndex = index;
        this.isAdd = false;
        return;
      } else {
        this.repeatIndex = null;
        this.isAdd = true;
      }
      if (this.innerParam.assign_method === 'direct') {
        return true;
      }
      let errKey = `fields#${index}#location`;
      this.$set(this.error, errKey, '');

      let validator = new JValidator({
        required: true,
      });
      if (this.innerParam.assign_method === 'index') {
        validator = new JValidator({
          required: true,
          numFormat: true,
        });
      }
      if (!validator.valid(value)) {
        this.$set(this.error, errKey, validator.errMsg);
        return false;
      }
      return true;
    },
    checkFieldAssignTo(index, value) {
      let errKey = `fields#${index}#assign_to`;
      this.$set(this.error, errKey, '');

      let validator = new JValidator({
        required: true,
        wordUnderlineBefore: true,
      });
      if (!validator.valid(value)) {
        this.$set(this.error, errKey, validator.errMsg);
        return false;
      }
      return true;
    },
    checkFieldType(index, value) {
      let errKey = `fields#${index}#type`;
      this.$set(this.error, errKey, '');

      let validator = new JValidator({
        required: true,
      });
      if (!validator.valid(value)) {
        this.$set(this.error, errKey, validator.errMsg);
        return false;
      }
      return true;
    },
    updateInnerParam() {
      if (this.operatorParam !== null) {
        this.innerParam = Object.assign({}, this.formatInnerParam(this.operatorParam));
      }
    },
  },
};
</script>
<style lang="scss" scoped>
::v-deep .disabled-color {
  .bkdata-combobox-input {
    border-color: #ff0000 !important;
  }
}
</style>
<style lang="scss">
.highlight {
  border: 1px solid red;
}
.clean-assign {
  margin: -5px -10px;
  width: 100%;
  .bkdata-combobox-input {
    height: 32px;
  }
  .json-wrapper {
    width: 231px !important;
    .json-key {
      display: flex;
      justify-content: flex-start;
      padding-left: 10px;
      span {
        margin-right: 10px;
      }
    }
  }
  .title {
    padding: 0 10px;
  }
  > .moreParams {
    padding: 5px 10px;
  }
  .long-selector {
    width: 180px;
  }
  .operator-type {
    border-left: none;
    margin-left: 10px;
    .type-title {
      padding: 0 9px;
      border: 1px solid #c3cdd7;
      background: #fafafa;
      line-height: 30px;
    }
    .type-select {
      width: 100px;
      margin-left: -1px;
    }
  }
  .line-left {
    position: relative;
    &:after {
      content: '';
      width: 1px;
      height: 24px;
      background: #dbe1e7;
      position: absolute;
      left: -11px;
      top: 50%;
      -webkit-transform: translate(0, -50%);
      transform: translate(0, -50%);
    }
  }
  .import-btn {
    position: absolute;
    right: -2px;
    bottom: 16px;
  }
  .error {
    position: relative;
    input {
      border-color: #f00;
    }
    .error-msg {
      max-width: 300px;
      position: absolute;
      top: -33px;
      left: 50%;
      padding: 5px 5px;
      font-size: 12px;
      white-space: nowrap;
      background: #212232;
      color: #fff;
      border-radius: 2px;
      line-height: 14px;
      transform: translate(-50%, 0);
      &:after {
        content: '';
        position: absolute;
        /* right: 70%; */
        left: 50%;
        bottom: -10px;
        border: 5px solid;
        border-color: #212232 transparent transparent transparent;
      }
    }
  }
}
</style>

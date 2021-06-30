

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
  <tr>
    <th>
      {{ config[arg_name] }}
      <i class="fa fa-question-circle f20 fr"
        aria-hidden="true"
        data-toggle="tooltip"
        :title="config.description" />
    </th>
    <td :class="{ 'has-error': has_error }">
      <div>
        <p v-for="(rt, index) in invalid_result_tables"
          :key="index">
          <span class="text-danger">{{ rt.invalidMsg }}</span>
        </p>
      </div>

      <div v-if="valid_result_tables.length > 0">
        <select v-model="selected_result_table_id"
          class="bk-form-select">
          <template v-for="(rt, index) in valid_result_tables">
            <option :key="index"
              :value="rt.result_table_id">
              {{ rt.result_table_id }} ({{ rt.description }})
            </option>
          </template>
        </select>
      </div>
    </td>
  </tr>
</template>

<script>
import { JValidator } from '@/common/js/check';
import Bus from '@/common/js/bus.js';
import mixin from './prop.mixin.js';
export default {
  mixins: [mixin],
  data() {
    return {
      result_tables: [],
      selected_result_table_id: '',
      has_error: false,
    };
  },
  computed: {
    valid_result_tables() {
      return this.result_tables.filter(function (rt) {
        return rt.isValid;
      });
    },
    invalid_result_tables() {
      return this.result_tables.filter(function (rt) {
        return !rt.isValid;
      });
    },
    arg_name() {
      return `arg_${this.$getLocale() === 'en' ? 'en' : 'zh'}_name`;
    },
  },
  watch: {
    selected_result_table_id(cur, prev) {
      Bus.$emit('input_result_table', this.get_result_table(cur));
    },
  },
  mounted() {
    // 用于记录所有的rt
    var resultTableIds = [];
    for (var i = 0; i < this.fromResultTables.length; i++) {
      var rt = this.fromResultTables[i];
      this.result_tables.push(this.clean_result_table(rt));
      // 记录这个rt
      resultTableIds.push(rt.result_table_id);
    }

    var val = this.tools().get_args_value(this.config);

    // 需要有这个配置项, 而且是在结果表中, 才可以使用
    if (val && resultTableIds.includes(val)) {
      this.set_value(val);
    } else if (this.valid_result_tables.length > 0) {
      this.set_value(this.valid_result_tables[0].result_table_id);
    }
  },
  methods: {
    get_result_table(rtId) {
      for (var i = 0; i < this.result_tables.length; i++) {
        var rt = this.result_tables[i];
        if (rt.result_table_id === rtId) {
          return rt;
        }
      }
      return null;
    },
    clean_result_table(rt) {
      var dimFields = [];
      var metricFields = [];
      var nameFields = [];
      for (var i = 0; i < rt.fields.length; i++) {
        var field = rt.fields[i];
        if (field.is_dimension) {
          dimFields.push(field.field);
        } else {
          metricFields.push(field.field);
        }
        nameFields.push(field.field);
      }
      if (dimFields.length === 0) {
        dimFields = metricFields;
      }
      var isValid = true;
      var invalidMsg = '';

      return {
        // 基本信息
        result_table_id: rt.result_table_id,
        description: rt.description,

        // 字段列表
        nameFields: nameFields,
        dimFieldOptions: dimFields,
        metricFieldOptions: metricFields,

        // 是否作为模型输入表
        isValid: isValid,
        invalidMsg: invalidMsg,
      };
    },
    set_value(val) {
      this.selected_result_table_id = val;
    },
    get_value() {
      return {
        category: this.category,
        nodeid: this.nodeid,
        arg: this.config.arg_en_name,
        value: this.selected_result_table_id,
      };
    },
    check() {
      if (this.valid_result_tables.length === 0) {
        this.has_error = true;
        return false;
      }
      var rules = { required: true };
      var validator = new JValidator(rules);
      if (!validator.valid(this.selected_result_table_id)) {
        this.has_error = true;
        return false;
      }

      this.has_error = false;
      return true;
    },
  },
};
</script>

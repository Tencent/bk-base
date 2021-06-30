

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
      <div v-if="choices_length == 0">
        {{ $t('该输入数据表无字段可选用') }}
      </div>
      <div v-else>
        <bkdata-selector
          :selected.sync="choices_fields"
          :placeholder="$t('请选择')"
          :searchable="true"
          :list="calcChoicesFields"
          :settingKey="'field_name'"
          :displayKey="'field_name'"
          :searchKey="'field_name'"
          :multiSelect="true"
          @item-selected="check()" />
      </div>
      <div class="text-danger mt5">
        {{ err_msg }}
      </div>
    </td>
  </tr>
</template>

<script>
import { JValidator } from '@/common/js/check';
import mixin from './prop.mixin.js';
export default {
  mixins: [mixin],
  data: function () {
    return {
      // 字段映射表
      choices_fields: [],
      // 验证失败信息
      err_msg: '',
    };
  },
  computed: {
    calcChoicesFields() {
      return (this.fromResultTables && this.fromResultTables[0] && this.fromResultTables[0].fields) || [];
    },
    has_error: function () {
      return this.err_msg.length > 0;
    },
    choices_length: function () {
      return this.calcChoicesFields.length;
    },
    arg_name() {
      return `arg_${this.$getLocale() === 'en' ? 'en' : 'zh'}_name`;
    },
  },
  mounted() {
    // 恢复原本的配置
    let val = this.tools().get_args_value(this.config);
    if (val !== null) {
      this.set_value(val);
    }
  },
  methods: {
    check: function () {
      var rules = { required: false };
      var validator = new JValidator(rules);

      if (!validator.valid(this.choices_fields)) {
        this.err_msg = validator.errMsg;
        return false;
      }

      this.err_msg = '';
      return true;
    },
    set_value: function (val) {
      this.choices_fields = val;
    },
    get_value: function () {
      return {
        category: this.category,
        nodeid: this.nodeid,
        arg: this.config.arg_en_name,
        value: this.choices_fields,
      };
    },
  },
};
</script>

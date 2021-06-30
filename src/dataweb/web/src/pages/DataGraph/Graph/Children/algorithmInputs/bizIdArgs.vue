

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
      <select v-model="value"
        class="bk-form-select">
        <template v-for="biz in fromBizList">
          <option :key="biz.biz_id"
            :value="biz.biz_id">
            [{{ biz.biz_id }}] {{ biz.biz_name }}
          </option>
        </template>
      </select>
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
      value: '',
      errMsg: '',
    };
  },
  computed: {
    has_error: function () {
      return this.errMsg.length > 0;
    },
    arg_name() {
      return `arg_${this.$getLocale() === 'en' ? 'en' : 'zh'}_name`;
    },
  },
  watch: {
    value: function (cur, prev) {
      // 业务选择成功，通知父组件
      if (this.check()) {
        this.$emit('udpate-biz-id', cur);
      }
    },
  },
  mounted() {
    var val = this.tools().get_args_value(this.config);

    if (val !== null) {
      this.set_value(val);
    } else if (this.fromBizList.length > 0) {
      this.value = this.fromBizList[0].biz_id;
    }
  },
  methods: {
    check: function () {
      var rules = { required: true };
      var validator = new JValidator(rules);
      if (!validator.valid(this.value)) {
        this.errMsg = validator.errMsg;
        return false;
      } else {
        this.errMsg = '';
      }
      return true;
    },

    set_value: function (val) {
      this.value = val;
    },
    get_value: function () {
      return {
        category: this.category,
        nodeid: this.nodeid,
        arg: this.config.arg_en_name,
        value: this.value,
      };
    },
  },
};
</script>



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
    <td>
      <div class="fields-form-box">
        <div
          v-for="(value, key) in m_field"
          :key="key"
          class="field-group mb10"
          :class="{ 'has-error': fieldErrors[key] }">
          <label class="field-label">{{ key }}：</label>
          <div class="field-selector">
            <bkdata-selector
              :selected.sync="m_field[key]"
              :placeholder="$t('请选择')"
              :searchable="true"
              :list="calcChoicesFields"
              :settingKey="'field_name'"
              :displayKey="'field_name'"
              :searchKey="'field_name'"
              @item-selected="checkField(key)" />
            <div v-show="fieldErrors[key]"
              class="text-danger mt5">
              {{ fieldErrors[key] }}
            </div>
          </div>
          <div class="clearfix" />
        </div>
        <div v-if="m_field_length == 0"
          class="mb20">
          {{ $t('该模型不需要配置字段映射') }}
        </div>
      </div>
    </td>
  </tr>
</template>

<script>
import { JValidator } from '@/common/js/check';
import mixin from './prop.mixin.js';
export default {
  mixins: [mixin],
  data() {
    return {
      // 所有可以选择的字段列表
      all_choices: [],
      // 可选择的字段列表
      choices: [],
      // 字段映射表
      m_field: {},
      // 判断该组件是否已经初始化, 用于自动匹配serving字段和结果表字段
      has_init: false,
      fieldErrors: {},
    };
  },
  computed: {
    calcChoicesFields() {
      return (this.fromResultTables && this.fromResultTables[0] && this.fromResultTables[0].fields) || [];
    },
    m_field_length() {
      var count = 0;
      for (var f in this.m_field) {
        count += 1;
      }
      return count;
    },
    arg_name() {
      return `arg_${this.$getLocale() === 'en' ? 'en' : 'zh'}_name`;
    },
  },
  watch: {
    configTemplate: {
      immediate: true,
      deep: true,
      handler: function (val) {
        val && this.initJsonMap(val);
      },
    },
  },
  methods: {
    initJsonMap(val) {
      const fields = val.input[this.nodeid].node_args['serving_use_fields'].value || [];
      fields.forEach(field => {
        if (!this.m_field[field]) {
          this.$set(this.m_field, field, (this.config.value && this.config.value[field]) || '');
        }
      });
    },
    set_value(val) {
      this.m_field = val;
    },
    get_value() {
      return {
        category: this.category,
        nodeid: this.nodeid,
        arg: this.config.arg_en_name,
        value: this.m_field,
      };
    },
    check() {
      for (var f in this.m_field) {
        if (!this.checkField(f)) {
          return false;
        }
      }
      return true;
    },
    checkField(f) {
      var rules = { required: !/timestamp/i.test(f) };
      var validator = new JValidator(rules);
      if (!validator.valid(this.m_field[f])) {
        this.$set(this.fieldErrors, f, validator.errMsg);
        return false;
      }

      let values = Object.values(this.m_field);
      let keys = Object.keys(this.m_field);

      // 判断是否有重复选择的
      if (values.indexOf(this.m_field[f]) !== keys.indexOf(f)) {
        this.$set(this.fieldErrors, f, this.$t('映射的字段已被选择_请选择其他字段'));
        return false;
      }

      this.$set(this.fieldErrors, f, '');
      return true;
    },
  },
};
</script>

<style media="screen" lang="scss">
.fields-form-box {
  padding: 5px 10px;
  background: #efefef;

  .field-group {
    .field-label {
      width: 25%;
      display: inline-block;
      text-align: right;
      vertical-align: top;
      margin-top: 8px;
    }
    .field-selector {
      width: 74%;
      display: inline-block;
    }
  }
}
</style>

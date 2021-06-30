

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
  <div class="clearfix clean-json">
    <div>
      <div class="operator-param clearfix fl mr20 line">
        <div class="param-select fl title">
          {{ $t('正则表达式') }}
        </div>
        <div class="bk-form-content param-input"
          style="width: 328px"
          :class="{ error: error['regex'] }">
          <bkdata-input
            v-model="innerParam.regex"
            :placeholder="placeholder['regex']"
            @focus="focusActive"
            @input="checkParams('regex')" />
          <div v-show="error['regex']"
            class="error-msg">
            {{ error['regex'] }}
          </div>
        </div>
      </div>
      <div class="operator-param clearfix fr mr20 line">
        <div class="param-select fl title">
          {{ $t('中间结果') }}
        </div>
        <div class="bk-form-content param-input fl"
          :class="{ error: error['result'] }">
          <bkdata-input
            v-model="innerParam.result"
            :placeholder="placeholder['result']"
            @focus="focusActive"
            @input="checkParams('result')" />
          <div v-show="error['result']"
            class="error-msg">
            {{ error['result'] }}
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
<script>
import { JValidator } from '@/common/js/check';
import mixin from './mixin';
// import DefaultValue from './components/defaultValue';

export default {
  name: 'vRegx',
  components: {
    // DefaultValue
  },
  mixins: [mixin],
  props: {
    operatorParam: {
      type: Object,
    },
    index: {
      type: Number,
    },
  },
  data() {
    return {
      innerParam: {
        result: '',
        regex: '',
        // default_value: '',
        // default_type: 'null'
      },
      placeholder: {
        result: this.$t('请输入中间结果表名'),
        regex: this.$t('请输入正确的正则表达式'),
      },
      error: {
        result: '',
        regex: '',
      },
    };
  },
  watch: {
    innerParam: {
      handler(newVal) {
        this.$emit('operatorParam', newVal, this.index);
      },
      deep: true,
    },
  },
  mounted() {
    if (this.operatorParam !== null) {
      this.innerParam = Object.assign({}, this.operatorParam);
    }
  },
  methods: {
    focusActive() {
      this.$emit('focus', this.index);
    },
    checkParams(key) {
      let validator = new JValidator({
        required: true,
      });
      if (!validator.valid(this.innerParam[key])) {
        this.error[key] = validator.errMsg;
        return false;
      }

      this.error[key] = '';
      return true;
    },
    check() {
      return this.checkParams('regex') && this.checkParams('result');
    },
  },
};
</script>

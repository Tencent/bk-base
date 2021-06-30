

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
  <div>
    <div class="delimiter clearfix fl mr20 line">
      <div class="delimiter-title fl">
        {{ $t('分割符') }}
      </div>
      <div :class="{ error: error.delimiter }"
        class="delimiter-select fl">
        <bkdata-combobox
          :displayKey="'delimiter_alias'"
          :idKey="'delimiter'"
          :list="delimiterList"
          :placeholder="hoder.delimiter"
          :value.sync="innerParam.delimiter"
          @focus="focusActive" />
        <div v-show="error.delimiter"
          class="error-msg">
          {{ error.delimiter }}
        </div>
      </div>
    </div>
    <div class="clearfix fl">
      <div class="operator-param clearfix fl mr20 line">
        <div class="param-select fl title">
          {{ $t('中间结果') }}
        </div>
        <div :class="{ error: error.result }"
          class="bk-form-content param-input fl">
          <bkdata-input
            v-model="innerParam.result"
            :placeholder="hoder.result"
            type="text"
            @focus="focusActive"
            @input="checkParam(innerParam.result, 'result')" />
          <div v-show="error.result"
            class="error-msg">
            {{ error.result }}
          </div>
        </div>
      </div>
      <div class="operator-param clearfix fl">
        <div class="param-select fl title">
          {{ $t('切分次数') }}
        </div>
        <div class="bk-form-content param-input fl"
          style="width: 80px">
          <bkdata-input v-model="innerParam.limit"
            :placeholder="hoder.limit"
            type="number"
            :min="0" />
        </div>
      </div>
    </div>
  </div>
</template>
<script>
import { JValidator } from '@/common/js/check';
import { getDelimiter } from '@/pages/DataAccess/NewForm/FormItems/Components/fun.js';
import mixin from './mixin';
export default {
  name: 'vSplit',
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
      delimiterList: [],
      hoder: {
        delimiter: this.$t('分割符'),
        result: this.$t('请输入中间结果表名'),
        limit: this.$t('切分次数'),
      },
      innerParam: {
        result: '',
        delimiter: '',
        limit: '',
      },
      error: {
        delimiter: '',
        result: '',
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
  created() {
    getDelimiter().then(res => {
      this.delimiterList = res.data.map(d => {
        d.delimiter_alias = `${d.delimiter}(${d.delimiter_alias})`;
        return d;
      });
    });
  },
  methods: {
    focusActive() {
      this.$emit('focus', this.index);
    },
    check() {
      return (
        (/\n|\t|\s/.test(this.innerParam.delimiter) || this.checkParam(this.innerParam.delimiter, 'delimiter'))
        && this.checkParam(this.innerParam.result, 'result')
      );
    },
    checkParam(params, error) {
      let validator = new JValidator({
        required: true,
      });
      if (!validator.valid(params)) {
        this.error[error] = validator.errMsg;
        return false;
      }

      this.error[error] = '';
      return true;
    },
  },
};
</script>

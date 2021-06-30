

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
  <div class="splitkv">
    <div class="delimiter clearfix fl mr20 line">
      <div class="delimiter-title fl">
        {{ $t('列表A') }}
      </div>
      <div :class="{ error: error.list_key_a.visible }"
        class="delimiter-select fl">
        <bkdata-input
          v-model="innerParam.list_key_a"
          v-tooltip.notrigger="error.list_key_a"
          @focus="focusActive"
          @input="checkParam(innerParam.list_key_a, 'list_key_a')" />
      </div>
    </div>
    <div class="delimiter clearfix fl mr20 line">
      <div class="delimiter-title fl">
        {{ $t('列表B') }}
      </div>
      <div :class="{ error: error.list_key_b.visible }"
        class="delimiter-select fl">
        <bkdata-input
          v-model="innerParam.list_key_b"
          v-tooltip.notrigger="error.list_key_b"
          @focus="focusActive"
          @input="checkParam(innerParam.list_key_b, 'list_key_b')" />
      </div>
    </div>
    <div class="clearfix fl">
      <div class="operator-param clearfix fl">
        <div class="param-select fl title">
          {{ $t('中间结果') }}
        </div>
        <div :class="{ error: error.result.visible }"
          class="bk-form-content param-input fl">
          <bkdata-input
            v-model="innerParam.result"
            v-tooltip.notrigger="error.result"
            :placeholder="hoder.result"
            type="text"
            @focus="focusActive"
            @input="checkParam(innerParam.result, 'result')" />
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
      },
      innerParam: {
        result: '',
        list_key_a: '',
        list_key_b: '',
      },
      error: {
        list_key_a: { content: '', visible: false, class: 'error-red' },
        list_key_b: { content: '', visible: false, class: 'error-red' },
        result: { content: '', visible: false, class: 'error-red' },
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
        this.checkParam(this.innerParam.list_key_a, 'list_key_a')
        && this.checkParam(this.innerParam.list_key_b, 'list_key_b')
        && this.checkParam(this.innerParam.result, 'result')
      );
    },
    checkParam(params, error) {
      let validator = new JValidator({
        required: true,
      });
      if (!validator.valid(params)) {
        this.error[error].content = validator.errMsg;
        this.error[error].visible = true;
        return false;
      }

      this.error[error].visible = false;
      return true;
    },
  },
};
</script>
<style lang="scss" scoped>
.splitkv {
  .delimiter {
    width: 180px !important;
    .delimiter-title {
      width: 90px !important;
    }
    .delimiter-select {
      width: calc(100% - 90px) !important;
    }
  }
}
</style>

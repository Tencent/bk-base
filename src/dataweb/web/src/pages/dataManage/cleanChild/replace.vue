

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
    <div class="replace clearfix fl mr20 line">
      <div class="replace-title fl">
        {{ $t('字符替换') }}
      </div>
      <div class="replace-select fl">
        <div class="bk-form-content replace-input fl"
          :class="{ error: error.from }">
          <bkdata-input
            v-model="innerParam.from"
            :placeholder="hoder.from"
            @focus="focusActive"
            @input="checkParam(innerParam.from, 'from')" />
          <div v-show="error.from"
            class="error-msg">
            {{ error.from }}
          </div>
        </div>
      </div>
      <div class="replace-title fl">
        <i class="bk-icon icon-angle-double-right" />
      </div>
      <div class="replace-select fl">
        <div class="bk-form-content replace-input fl"
          :class="{ error: error.to }">
          <bkdata-input v-model="innerParam.to"
            :placeholder="hoder.to"
            @focus="focusActive" />
          <!-- <div class="error-msg"
                        v-show="error.to">{{ error.to }}</div> -->
        </div>
      </div>
    </div>
    <div class="clearfix fl">
      <div class="operator-param clearfix fl">
        <div class="param-select fl title">
          {{ $t('中间结果') }}
        </div>
        <div class="bk-form-content param-input fl"
          :class="{ error: error.result }">
          <bkdata-input
            v-model="innerParam.result"
            :placeholder="hoder.result"
            @focus="focusActive"
            @input="checkParam(innerParam.result, 'result')" />
          <div v-show="error.result"
            class="error-msg">
            {{ error.result }}
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
<script>
import { JValidator } from '@/common/js/check';
import mixin from './mixin';
export default {
  name: 'vReplace',
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
      hoder: {
        from: this.$t('将替换字符'),
        to: this.$t('替换字符'),
        result: this.$t('请输入中间结果表名'),
      },
      innerParam: {
        result: '',
        from: '',
        to: '',
      },
      error: {
        from: '',
        to: '',
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
  methods: {
    focusActive() {
      this.$emit('focus', this.index);
    },
    check() {
      return this.checkParam(this.innerParam.from, 'from') && this.checkParam(this.innerParam.result, 'result');
    },
    checkParam(param, error) {
      let validator = new JValidator({
        required: true,
      });
      if (!validator.valid(param)) {
        this.error[error] = validator.errMsg;
        return false;
      }
      this.error[error] = '';
      return true;
    },
  },
};
</script>

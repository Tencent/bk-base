

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
      <div class="operator-param clearfix fl">
        <div class="param-select fl title">
          {{ $t('中间结果') }}
        </div>
        <div class="bk-form-content param-input fl"
          :class="{ error: error }">
          <bkdata-input v-model="innerParam.result"
            :placeholder="placeholder"
            @focus="focusActive"
            @input="check" />
          <div v-show="error"
            class="error-msg">
            {{ error }}
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
  name: 'vUrl',
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
      placeholder: this.$t('请输入中间结果表名'),
      innerParam: {
        result: '',
      },
      error: '',
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
      let validator = new JValidator({
        required: true,
      });
      if (!validator.valid(this.innerParam.result)) {
        this.error = validator.errMsg;
        return false;
      }

      this.error = '';
      return true;
    },
  },
};
</script>
<style media="screen" lang="scss"></style>

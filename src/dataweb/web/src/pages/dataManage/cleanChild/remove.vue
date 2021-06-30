

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
    <div class="delimiter-key clearfix fl mr20 line">
      <div class="delimiter-title fl">
        <bkdata-selector
          :placeholder="operatorKey.placehoder"
          :list="operatorKey.list"
          :selected.sync="innerParam.assign_method" />
      </div>
      <div class="delimiter-select fl">
        <div class="bk-form-content fl"
          :class="{ error: error.type }">
          <bkdata-input
            v-model="innerParam.type"
            :placeholder="hoder.type"
            @focus="focusActive"
            @input="checkParam(innerParam.type, 'type')" />
          <div v-show="error.type"
            class="error-msg">
            {{ error.type }}
          </div>
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
  name: 'vRemove',
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
        type: this.$t('请输入'),
        result: this.$t('请输入中间结果表名'),
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
        ],
      },
      innerParam: {
        assign_method: 'index',
        type: '',
        result: '',
      },
      error: {
        type: '',
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
      return this.checkParam(this.innerParam.type, 'type') && this.checkParam(this.innerParam.result, 'result');
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
<style lang="scss">
.delimiter-key {
  .bkdata-combobox-input {
    height: 32px;
  }
}
</style>

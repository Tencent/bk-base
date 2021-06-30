

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
  <div class="iteration clearfix clean-json">
    <div>
      <div class="delimiter-key clearfix fl mr20 line">
        <bkdata-selector
          :placeholder="'选择遍历方式'"
          :list="iterationTypes"
          :selected.sync="innerParam.iteration_type"
          @item-selected="handleTypechange()" />
        <div v-show="error['iteration_type']"
          class="error-msg">
          {{ error['iteration_type'] }}
        </div>
      </div>
      <div class="operator-param clearfix fl">
        <div class="param-select fl title">
          {{ $t('中间结果') }}
        </div>
        <div class="bk-form-content param-input fl"
          :class="{ error: error['result'] }">
          <bkdata-input
            v-model="innerParam.result"
            :placeholder="placeholder"
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
export default {
  name: 'vJson',
  mixins: [mixin],
  props: {
    operatorParam: {
      type: Object,
    },
    index: {
      type: Number,
    },
    label: {
      type: String,
    },
  },
  data() {
    return {
      iterationTypes: [
        { id: 'list', name: window.$t('列表遍历') },
        { id: 'hash', name: window.$t('哈希遍历') },
      ],
      placeholder: this.$t('请输入中间结果表名'),
      innerParam: {
        result: '',
        iteration_type: 'list',
      },
      error: {
        iteration_type: '',
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
    handleTypechange() {
      this.error = {
        iteration_type: '',
        result: '',
      };
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
      if (this.innerParam.iteration_type === 'list') {
        return this.checkParams('result');
      } else {
        return this.checkParams('iteration_type') && this.checkParams('result');
      }
    },
  },
};
</script>
<style media="screen" lang="scss" scoped>
.iteration {
  .delimiter {
    width: 90px !important;

    ::v-deep .bkdata-selector-input {
      padding-right: 15px;
    }
  }

  .operator-param {
    .param-select {
      min-width: 50px;
    }
  }
}
.delimiter-title {
  width: 120px;
}

/*.param-select{*/
/*min-width: 50px!important;*/
/*}*/

/*.operator-type {*/
/*.param-input {*/
/*width: 65px!important;*/
/*}*/
/*}*/
</style>

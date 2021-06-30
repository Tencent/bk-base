

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
          :selected="innerParam.assign_method"
          @item-selected="changeType" />
      </div>
      <div class="delimiter-select fl">
        <div class="bk-form-content"
          :class="{ error: error.location }">
          <bkdata-combobox
            v-if="innerParam && innerParam.assign_method === 'key'"
            :list="calcOperatorParam.keys ? calcOperatorParam.keys : []"
            :placeholder="hoder.location"
            :value.sync="innerParam.location"
            :showOpen="false"
            :showClear="false"
            :showEmpty="false"
            :bypass="true"
            @input="checkParam(innerParam.location, 'location')" />
          <bkdata-combobox
            v-else-if="innerParam && innerParam.assign_method === 'index'"
            :list="calcOperatorParam.length ? generateLengthList() : []"
            :placeholder="hoder.location"
            :value.sync="innerParam.location"
            :showOpen="false"
            :showClear="false"
            :showEmpty="false"
            :bypass="true"
            @input="checkParam(innerParam.location, 'location')" />
          <input
            v-else
            v-model="innerParam.location"
            type="text"
            :placeholder="hoder.location"
            @focus="focusActive"
            @input="checkParam(innerParam.location, 'location')">
          <div v-show="error.location"
            class="error-msg">
            {{ error.location }}
          </div>
        </div>
      </div>
    </div>
    <div class="clearfix fl mr20 line">
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
    <DefaultValue
      :defaultValue.sync="innerParam.default_value"
      :defaultType.sync="innerParam.default_type"
      @focus="focusActive" />
  </div>
</template>
<script>
import { JValidator } from '@/common/js/check';
import ChildBase from './child';
import mixin from './mixin';
import DefaultValue from './components/defaultValue';
export default {
  name: 'vRemove',
  components: { DefaultValue },
  mixins: [ChildBase, mixin],
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
        location: this.$t('请输入'),
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
        location: '',
        result: '',
        default_value: '',
        default_type: 'null',
      },
      error: {
        location: '',
        result: '',
      },
    };
  },
  computed: {
    calcOperatorParam() {
      return this.operatorParam || {};
    },
  },
  watch: {
    innerParam: {
      handler(newVal) {
        this.$emit('operatorParam', newVal, this.index);
      },
      deep: true,
    },
    'innerParam.assign_method': function (val) {
      this.error.location = '';
    },
  },
  mounted() {
    if (this.operatorParam !== null) {
      this.innerParam = Object.assign({}, this.operatorParam);
    }
  },
  methods: {
    changeType(key, data) {
      this.innerParam.assign_method = key;
      this.innerParam.location = '';
    },
    focusActive() {
      this.$emit('focus', this.index);
    },
    check() {
      return this.checkParam(this.innerParam.location, 'location') && this.checkParam(this.innerParam.result, 'result');
    },
    checkParam(param, error) {
      let validator = new JValidator({
        required: true,
      });
      if (error === 'location' && this.innerParam.assign_method === 'index') {
        validator = new JValidator({
          required: true,
          numFormat: true,
        });
      }
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
<style lang="scss" scoped>
.operator-param {
  .title {
    border-right: none !important;
  }
}
</style>

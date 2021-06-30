

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
  <div class="token-comfirm">
    <form class="bk-form">
      <div class="bk-form-item is-required mb20">
        <label class="bk-label pr15">{{ $t('申请理由') }}</label>
        <div class="bk-form-content">
          <textarea
            v-model="reason"
            :class="`normal-width bk-form-textarea ${isEmpty ? 'error-status' : ''}`"
            rows="3"
            @blur="checkValid" />
          <p v-if="isEmpty"
            class="bk-tip-text">
            {{ $t('申请理由不能为空') }}
          </p>
        </div>
      </div>

      <div class="bk-form-item is-required mb20">
        <label class="bk-label pr15">{{ $t('有效期') }}</label>
        <div class="bk-form-content">
          <bkdata-selector
            class="normal-width"
            :selected.sync="selectedExpire"
            :list="expireList"
            :searchable="true"
            :settingKey="'value'"
            :displayKey="'name'"
            :searchKey="'name'" />
        </div>
      </div>
    </form>
  </div>
</template>

<script>
import { mapState } from 'vuex';

export default {
  props: {
    scopes: {
      type: Array,
    },
    permissions: {
      type: Array,
    },
  },
  data() {
    return {
      isEmpty: false,
      selectedExpire: 365, // 必填项，给默认值
      reason: '',
      expireList: [
        {
          name: `1 ${this.$t('年')}`,
          value: 365,
        },
        {
          name: `30 ${this.$t('天s')}`,
          value: 30,
        },
        {
          name: `7 ${this.$t('天s')}`,
          value: 7,
        },
      ],
    };
  },
  computed: {
    ...mapState({
      objectTypeList: state => state.auth.scopes,
    }),
  },
  watch: {
    permissions(val) {
      console.log(val);
    },
    reason(val) {
      this.$emit('setReason', val);
    },
    selectedExpire(val) {
      this.$emit('setExpire', val);
    },
  },

  methods: {
    checkValid() {
      this.isEmpty = !this.reason.trim();
    },
  },
};
</script>

<style lang="scss" scoped>
.token-comfirm {
  .error-status {
    border-color: #ff5656;
    background-color: #fff4f4;
    color: #ff5656;
  }

  .bk-tip-text {
    font-size: 12px;
    color: #ff5656;
    margin-top: 5px;
  }

  .normal-width {
    width: 60%;
  }
}
</style>

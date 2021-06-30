

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
  <div class="token-app-step">
    <form class="bk-form">
      <div class="bk-form-item is-required">
        <label class="bk-label pr15">{{ $t('蓝鲸应用') }}</label>
        <div class="bk-form-content"
          style="display: flex; align-items: center">
          <bkdata-selector
            class="app-selector"
            :disabled="disabled"
            :selected.sync="selectedApp"
            :list="blueKingAppList"
            :placeholder="$t('请选择')"
            :searchable="true"
            :settingKey="'bk_app_code'"
            :displayKey="'bk_app_name'"
            :searchKey="'bk_app_name'" />
          <i v-tooltip="$t('授权需要制定访问的蓝鲸应用')"
            class="bk-icon icon-exclamation-circle" />
        </div>
      </div>
    </form>
  </div>
</template>

<script>
import { retrieveTokenDetail } from '@/common/api/auth.js';
import { showMsg } from '@/common/js/util';

export default {
  props: {
    value: {
      type: String,
    },
    disabled: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      blueKingAppList: [],
      selectedApp: '',
      tokenInfo: {},
    };
  },
  watch: {
    selectedApp(val) {
      if (val) {
        this.$emit('input', val);
      }
    },
    value(val) {
      if (val) {
        this.selectedApp = val;
      }
    },
  },
  created() {
    if (this.$route.name === 'TokenEdit') {
      this.initTokenInfo(this.$route.params.token_id);
    }
  },
  async mounted() {
    this.blueKingAppList = await this.$store.dispatch('updateBlueKingAppList');
  },
  methods: {
    initTokenInfo(tokenId) {
      retrieveTokenDetail(tokenId).then(resp => {
        if (resp.result) {
          this.tokenInfo = resp.data;
          this.selectedApp = resp.data.data_token_bk_app_code;
        } else {
          showMsg(resp.message, 'error');
        }
      });
    },
  },
};
</script>

<style lang="scss" scoped>
.token-app-step {
  .app-selector {
    display: inline-block;
    width: 60%;
    vertical-align: 2px;
    margin-right: 5px;
  }

  .bk-form {
    .bk-form-item {
      .bk-form-content {
        input,
        select {
          width: 100%;
        }
      }
    }
  }

  .icon-exclamation-circle {
    font-size: 16px;
  }
}
</style>

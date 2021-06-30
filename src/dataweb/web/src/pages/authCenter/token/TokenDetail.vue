

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
  <div v-bkloading="{ isLoading: isLoading }"
    class="token-detail">
    <div class="bkdata-parts">
      <div class="part-title">
        <span class="square" />{{ $t('基本信息') }}
      </div>
      <div class="part-content">
        <table class="bkdata-info-table">
          <tbody>
            <tr>
              <td>{{ $t('授权编号：') }}</td>
              <td>{{ tokenId }}</td>
            </tr>
            <tr style="vertical-align: top">
              <td>{{ $t('授权码：') }}</td>
              <td v-if="!showPlainCode">
                {{ tokenInfo.data_token }}
                <i
                  class="bk-icon icon-eye-shape preview-permission-code"
                  :title="$t('预览授权码：')"
                  @click="previewPermissionCode" />
              </td>
              <td v-else>
                <p class="plain-code-text">
                  {{ plainCode }}
                </p>
                <i
                  class="bk-icon icon-eye-slash-shape preview-permission-code"
                  :title="$t('隐藏授权码：')"
                  @click="showPlainCode = false" />
              </td>
            </tr>
            <tr>
              <td>{{ $t('应用名称：') }}</td>
              <td>{{ tokenInfo.data_token_bk_app_code }}</td>
            </tr>
            <tr>
              <td>{{ $t('授权状态：') }}</td>
              <td>{{ tokenInfo.status_display }}</td>
            </tr>
            <tr>
              <td>{{ $t('创建时间：') }}</td>
              <td>{{ tokenInfo.created_at }}</td>
            </tr>
            <tr>
              <td>{{ $t('创建人：') }}</td>
              <td>{{ tokenInfo.created_by }}</td>
            </tr>
            <tr>
              <td>{{ $t('过期时间：') }}</td>
              <td>{{ tokenInfo.expired_at }}</td>
            </tr>
            <tr>
              <td>{{ $t('用途描述：') }}</td>
              <td>{{ tokenInfo.description }}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="part-title">
        <span class="square" />{{ $t('授权内容') }}
      </div>
      <div class="part-content">
        <TokenObjectScopes
          :isEditable="false"
          :selectedScopes="tokenInfo.scopes"
          :selectedPermissions="tokenInfo.permissions" />
      </div>
    </div>
  </div>
</template>

<script>
import { showMsg } from '@/common/js/util';
import { getClearPermissionCode, retrieveTokenDetail } from '@/common/api/auth';

import TokenObjectScopes from './components/TokenObjectScopes';

export default {
  components: {
    TokenObjectScopes,
  },
  props: {
    tokenId: {
      type: Number,
      required: true,
    },
  },
  data() {
    return {
      isLoading: false,
      tokenInfo: {
        id: 0,
        data_token: '',
        status: '',
        status_display: '',
        created_at: '',
        created_by: '',
        data_token_bk_app_code: '',
        data_token_bk_app_name: '',
        description: null,
        expired_at: '',
        permissions: [],
        scopes: [],
      },
      showPlainCode: false,
      plainCode: '---',
    };
  },
  watch: {
    tokenId: {
      handler(val) {
        this.loadTokenDetail();
      },
      immediate: true,
    },
  },
  methods: {
    loadTokenDetail() {
      this.isLoading = true;
      retrieveTokenDetail(this.tokenId).then(resp => {
        if (resp.result) {
          this.tokenInfo = resp.data;
        } else {
          showMsg(resp.message, 'error');
        }
        this.isLoading = false;
      });
    },
    /**
     * 预览授权码
     */
    previewPermissionCode() {
      getClearPermissionCode(this.tokenId).then(resp => {
        if (resp.result) {
          this.plainCode = resp.data;
          this.showPlainCode = true;
        } else {
          showMsg(resp.message, 'error');
        }
        this.isLoading = false;
      });
    },
  },
};
</script>

<style lang="scss" scoped>
.bkdata-parts {
  margin-left: 30px;
  .part-title {
    background: #f5f6fa;
    width: 500px;
    height: 28px;
    line-height: 28px;
    font-size: 14px;
    font-family: MicrosoftYaHei, MicrosoftYaHei-Bold;
    font-weight: 700;
    text-align: left;
    color: #63656e;
    padding: 0 0 0 10px;
    position: relative;
    .square {
      display: inline-block;
      width: 3px;
      height: 14px;
      background: #3a84ff;
      position: absolute;
      top: 50%;
      left: 0;
      transform: translateY(-50%);
    }
  }
  .part-content {
    padding: 10px 0;
  }
}
table.bkdata-info-table tr td:first-child {
  color: #979ba5;
  font-weight: 400;
}
</style>

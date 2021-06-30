

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
  <div class="field-processing-logic">
    <bkdata-alert
      type="info"
      :title="$t('字段加工逻辑：使用 SQL 加工字段，例如使用 IFNULL 做 空值处理，CASE WHEN 做 条件判断')" />
    <div class="monaco-wrapper"
      :style="{ height: errorMessage ? '' : 'calc(100% - 52px)' }">
      <Monaco
        height="100%"
        :code="code"
        :options="options"
        :tools="monacoSetting.tools"
        :language="'universal-sql'"
        @codeChange="handleChangeCode" />
      <div v-if="errorMessage"
        class="error-msg bk-scroll-y">
        <i class="icon-exclamation-circle" />
        <span class="msg">
          {{ errorMessage }}
          <br>
          {{ completeSql }}
        </span>
      </div>
    </div>
    <div v-if="editable"
      class="footer">
      <bkdata-button class="mr5"
        theme="primary"
        :loading="isVerifying"
        @click="handleVerifyCode">
        {{ $t('保存') }}
      </bkdata-button>
      <bkdata-button theme="default"
        :disabled="isVerifying"
        @click="handleClose">
        {{ $t('取消') }}
      </bkdata-button>
      <bkdata-button class="clear-btn"
        theme="default"
        :disabled="isVerifying"
        @click="handleShowConfirm">
        {{ $t('删除') }}
      </bkdata-button>
      <pop-container ref="clearConfirmPop"
        v-bkClickoutside="handleHideConfirm"
        class="clear-confirm">
        <div class="clear-tips">
          <span>{{ $t('确认删除字段加工逻辑？') }}</span>
          <p>{{ $t('删除操作无法撤回，请谨慎操作！') }}</p>
          <div class="btns">
            <bkdata-button class="mr5"
              theme="primary"
              size="small"
              @click="handleSubmitClear">
              {{ $t('确定') }}
            </bkdata-button>
            <bkdata-button theme="default"
              size="small"
              @click="handleHideConfirm">
              {{ $t('取消') }}
            </bkdata-button>
          </div>
        </div>
      </pop-container>
    </div>
  </div>
</template>
<script lang="ts" src="./FieldProcessingLogic.ts"></script>
<style lang="scss" scoped>
.field-processing-logic {
  position: relative;
  height: calc(100vh - 60px);
  padding: 20px 30px 52px;
  overflow: hidden;
  .monaco-wrapper {
    height: calc(100% - 130px);
    margin-top: 20px;
    .error-msg {
      height: 76px;
      font-size: 14px;
      padding: 12px 20px 20px;
      color: #ff5a5a;
      background-color: #1d1d1d;
      border-left: 3px solid #ff5a5a;
      .icon-exclamation-circle {
        float: left;
        font-size: 16px;
        margin-top: 2px;
      }
      .msg {
        width: calc(100% - 16px);
        float: left;
        line-height: 22px;
      }
    }
  }
  .footer {
    position: absolute;
    left: 0;
    bottom: 0;
    width: 100%;
    height: 52px;
    line-height: 52px;
    border-top: 1px solid #f0f1f5;
    padding: 0 30px;
    background-color: #ffffff;
    .bk-button {
      min-width: 86px;
    }
    .clear-btn {
      float: right;
      margin-top: 10px;
    }
  }
}
.clear-confirm {
  width: 260px;
  .clear-tips {
    padding: 12px 10px;
    span {
      font-size: 16px;
      color: #313238;
    }
    p {
      font-size: 12px;
      color: #63656e;
      margin: 10px 0 16px;
    }
    .btns {
      text-align: right;
    }
    .bk-button {
      min-width: 60px;
      height: 24px;
      line-height: 22px;
    }
  }
}
</style>

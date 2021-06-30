

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
  <div v-bkloading="{ isLoading: isDiffLoading, opacity: 1 }"
    class="data-model-release">
    <template v-if="!hasDiff">
      <EmptyView class="empty-data"
        width="200px"
        height="120px"
        :tips="$t('暂无变更，无需发布')">
        <p style="line-height: 22px; margin-top: 6px;">
          {{ $t('前往') }}
          <bkdata-button style="font-size: 12px;"
            text
            @click.stop="handleGoApplication">
            {{ $t('Dataflow 应用') }}
          </bkdata-button>
        </p>
      </EmptyView>
    </template>
    <template v-else>
      <BizDiffContents
        :onlyShowDiff.sync="onlyShowDiff"
        :diff="diffData"
        :newContents="newContents"
        :origContents="origContents"
        :fieldContraintConfigList="fieldContraintConfigList" />
    </template>

    <!-- 发布确认 -->
    <bkdata-dialog
      v-model="comfirmRelease.isShow"
      headerPosition="left"
      :maskClose="false"
      :width="480"
      :title="$t('发布')"
      @cancel="handleCancelRelease">
      <div class="comfirm-release">
        <bkdata-alert type="info"
          :title="releaseTips" />
        <bkdata-form ref="modelInfoForm"
          formType="vertical"
          :model="formData"
          :rules="rules">
          <bkdata-form-item class="mt10"
            label="发布描述"
            property="log"
            :required="true">
            <bkdata-input v-model="formData.log"
              type="textarea"
              maxlength="50" />
          </bkdata-form-item>
        </bkdata-form>
      </div>
      <div slot="footer">
        <bkdata-button class="mr5"
          theme="primary"
          :loading="comfirmRelease.isSubmiting"
          @click="handleConfirmRelease">
          {{ $t('确定') }}
        </bkdata-button>
        <bkdata-button theme="default"
          @click="handleCancelRelease">
          {{ $t('取消') }}
        </bkdata-button>
      </div>
    </bkdata-dialog>
  </div>
</template>
<script lang="ts" src="./DataModelRelease.ts"></script>
<style lang="scss" scoped>
.data-model-release {
  height: 100%;
  text-align: left;
  margin: 0 auto;
  ::v-deep .bk-textarea-wrapper {
    background-color: #ffffff;
  }
  .empty-view {
    height: 100%;
    background-color: #ffffff;
    ::v-deep .empty-img {
      margin-bottom: 0;
    }
    ::v-deep .empty-text {
      font-size: 14px;
      color: #63656e;
    }
  }
  .biz-diff-contents-layout {
    height: 100%;
    ::v-deep .diff-contents-layout {
      margin-top: 0;
    }
  }
}
</style>

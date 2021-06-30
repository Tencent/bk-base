

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
  <div class="model-info-view-layout">
    <div class="info-list">
      <div class="info-item model-type-item">
        <span class="info-label">{{ $t('模型类型') }}：</span>
        <span class="info-text">{{ modelType[data.modelType] }}</span>
      </div>
      <div class="info-item">
        <span class="info-label">{{ $t('英文名称') }}：</span>
        <span class="info-text">{{ data.modelName }}</span>
      </div>
      <div class="info-item">
        <span class="info-label">{{ $t('发布状态') }}：</span>
        <span class="info-text">{{ publishStatus[data.publishStatus] }}</span>
      </div>
      <div class="info-item">
        <span class="info-label">{{ $t('中文名称') }}：</span>
        <span class="info-text">{{ data.modelAlias }}</span>
      </div>
      <div class="info-item">
        <span class="info-label">{{ $t('发布时间') }}：</span>
        <span class="info-text">{{ data.releaseCreatedAt || '--' }}</span>
      </div>
      <div class="info-item">
        <span class="info-label">{{ $t('数据标签') }}：</span>
        <span class="info-text">
          <div class="tags">
            <template v-for="tag in tags">
              <span :key="tag.tagCode"
                class="tag">
                {{ tag.tagAlias }}
              </span>
            </template>
          </div>
        </span>
      </div>
      <div class="info-item">
        <span class="info-label">{{ $t('发布者') }}：</span>
        <span class="info-text">{{ data.releaseCreatedBy || '--' }}</span>
      </div>
      <div class="info-item">
        <span class="info-label">{{ $t('模型备注') }}：</span>
        <span class="info-text">{{ data.description || '--' }}</span>
      </div>
      <div class="info-item">
        <span class="info-label">{{ $t('发布描述') }}：</span>
        <span class="info-text">{{ data.versionLog || '--' }}</span>
      </div>
    </div>
  </div>
</template>
<script lang="ts" src="./ModelInfoView.ts"></script>
<style lang="scss" scoped>
.model-info-view-layout {
  padding: 14px 36px 20px;
  .info-list {
    display: flex;
    flex-wrap: wrap;
    font-size: 12px;
    line-height: 24px;
    .info-item {
      display: flex;
      padding-bottom: 8px;
      &:nth-child(n) {
        flex: 0 0 60%;
      }
      &:nth-child(2n) {
        flex: 0 0 40%;
      }
      &.model-type-item {
        flex: 0 0 100%;
      }
    }
    .info-label {
      min-width: 64px;
      text-align: right;
      color: #979ba5;
    }
    .info-text {
      flex: 1;
      word-break: break-word;
      color: #63656e;
      padding-right: 10px;
    }
    .tags {
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      .tag {
        padding: 0 6px;
        color: #63656e;
        background-color: #f0f1f5;
        margin-right: 6px;
        margin-bottom: 4px;
      }
    }
  }
}
</style>

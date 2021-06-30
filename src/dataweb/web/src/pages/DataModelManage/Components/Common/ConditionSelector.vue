

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
  <div class="codition-selector-wrapper">
    <div
      :ref="conditionInputId"
      :class="['condition-selector-input', { 'is-focus': isShow }]"
      @click="handleShowCondition">
      <span v-if="!renderInputContent.length"
        style="color: #c4c6cc">
        {{ $t('未设置') }}
      </span>
      <template v-for="(group, index) in renderInputContent">
        <div :key="index + '_group'"
          class="input-condition-group">
          <span v-if="renderInputContent.length > 1">(</span>
          <template v-for="(item, childIndex) in group.items">
            <div :key="childIndex + '_item'"
              class="input-condition-content">
              <span class="condition-item">{{ item.content }}</span>
              <span v-if="childIndex < group.items.length - 1"
                class="input-item-op">
                {{ item.op }}
              </span>
            </div>
          </template>
          <span v-if="renderInputContent.length > 1">)</span>
          <span v-if="index < renderInputContent.length - 1"
            class="input-group-op">
            {{ group.op }}
          </span>
        </div>
      </template>
      <span v-if="isShowInputEllipsis"
        class="input-ellipsis" />
      <i
        v-if="renderInputContent.length && !readonly"
        class="clear-btn icon-close-circle-shape"
        @click.stop="handleClear" />
    </div>
    <bkdata-sideslider
      :isShow.sync="isShow"
      :width="720"
      :quickClose="false"
      :transfer="true"
      @shown="handleSidesliderShow"
      @hidden="handleSidesliderHidden">
      <div slot="header"
        class="custom-sideslider-title">
        {{ $t('值约束设置') }} <span>- {{ fieldDisplayName }}</span>
      </div>
      <template slot="content">
        <BkConditionSelector
          ref="conditionSelector"
          class="condition-main bk-scroll-y"
          trigger="hover"
          :constraintContent="constraintContent"
          :constraintList="constraintList"
          :readonly="readonly" />
        <div v-if="!readonly"
          :class="['condition-selector-operation', { 'sticky-bottom': hasScrollbar }]">
          <bkdata-button extCls="mr5"
            theme="primary"
            @click="handleSubmit">
            {{ $t('确定') }}
          </bkdata-button>
          <bkdata-button extCls="mr5"
            theme="default"
            @click="isShow = false">
            {{ $t('取消') }}
          </bkdata-button>
        </div>
      </template>
    </bkdata-sideslider>

    <div v-show="conditionPop.isShow"
      ref="conditionPopEl"
      v-bkClickoutside="handleHideConditionPop">
      <BkConditionSelector
        class="condition-main condition-pop bk-scroll-y"
        :constraintContent="constraintContent"
        :constraintList="constraintList"
        :readonly="readonly" />
    </div>
  </div>
</template>
<script lang="ts" src="./ConditionSelector.ts"></script>
<style lang="scss" scoped>
.codition-selector-wrapper {
  .condition-selector-input {
    position: relative;
    width: 100%;
    height: 32px;
    line-height: 30px;
    display: flex;
    align-items: center;
    flex-wrap: nowrap;
    white-space: nowrap;
    overflow: hidden;
    font-size: 12px;
    color: #939496;
    border-radius: 2px;
    padding: 0 10px;
    border: 1px solid #c4c6cc;
    cursor: pointer;
    &.is-focus {
      border-color: #3a84ff;
    }
    &:hover {
      .clear-btn {
        display: flex;
      }
    }
    .input-condition-group {
      display: flex;
      align-items: center;
    }
    .input-group-op {
      margin: 0 4px;
    }
    .input-condition-content {
      .condition-item {
        color: #63656e;
        background-color: #e6e8f0;
        padding: 2px 4px;
        border-radius: 2px;
        margin: 0 2px;
      }
    }
    .input-ellipsis {
      position: absolute;
      top: 0;
      right: 0;
      display: flex;
      align-items: center;
      height: 100%;
      border-left: 10px solid #ffffff;
      background-color: #ffffff;
      &::after {
        content: '...';
        height: 20px;
        line-height: 20px;
        background-color: #e6e8f0;
        padding: 0 6px;
        border-radius: 2px;
        margin-right: 30px;
      }
    }
    .clear-btn {
      position: absolute;
      top: 50%;
      right: 6px;
      transform: translateY(-50%);
      display: none;
      align-items: center;
      justify-content: center;
      font-size: 14px;
      color: #c4c6cc;
      border-radius: 50%;
      &:hover {
        color: #979ba5;
      }
    }
  }
}
.condition-main {
  max-height: calc(100vh - 162px);
  margin: 20px 0 25px;
  padding: 0 24px;
  &.condition-pop {
    max-height: 400px;
    ::v-deep .condition-selector-form .condition-item {
      max-width: 140px;
    }
  }
}
.condition-selector-operation {
  position: sticky;
  bottom: 0;
  padding: 0 24px;
  &.sticky-bottom {
    width: 100%;
    height: 52px;
    line-height: 52px;
    background-color: #fafbfd;
    border-top: 1px solid #f0f1f5;
  }
}
.custom-sideslider-title {
  font-size: 16px;
  color: #313238;
  font-weight: normal;
  span {
    font-size: 12px;
    color: #63656e;
  }
}
</style>

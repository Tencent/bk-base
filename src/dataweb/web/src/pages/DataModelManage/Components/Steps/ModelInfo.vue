

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
  <div class="step-model-info">
    <div class="message-info">
      <i class="icon icon-info" />
      <span class="text">为了加快数据开发的效率，建议先在</span>
      <a href="http://"
        target="_blank"
        rel="noopener noreferrer">
        数据集市
      </a>
      <span class="text"> 中查询你需要的数据模型。</span>
    </div>
    <bkdata-form extCls="mb20"
      :labelWidth="80"
      :model="formData"
      :rules="rules">
      <bkdata-form-item label="模型类型"
        :required="true"
        :property="'model_type'">
        <div class="model-type-items">
          <div
            class="model-type-item"
            :class="{ selected: formData.model_type === 'fact_table', readonly: !isNewForm }"
            @click.stop="() => isNewForm && handleModelTypeSelected('fact_table')">
            <div class="type-item-icon">
              <i class="icon icon-fact-model" />
            </div>
            <div class="type-item-content">
              <div class="content-title">
                事实表数据模型
              </div>
              <div class="content-description text-overflow">
                描述业务活动的细节，如：登录流水
              </div>
            </div>
            <div class="header-icon">
              <i class="icon-status icon-check-1" />
            </div>
          </div>
          <div
            class="model-type-item"
            :class="{ selected: formData.model_type === 'dimension_table', readonly: !isNewForm }"
            @click.stop="() => isNewForm && handleModelTypeSelected('dimension_table')">
            <div class="type-item-icon">
              <i class="icon icon-dimension-model" />
            </div>
            <div class="type-item-content">
              <div class="content-title">
                维度表数据模型
              </div>
              <div class="content-description text-overflow">
                定义事实表数据模型中的维度，如：定义登录流水表中的用户维度（性别、年龄等字段）
              </div>
            </div>
            <div class="header-icon">
              <i class="icon-status icon-check-1" />
            </div>
          </div>
        </div>
      </bkdata-form-item>
    </bkdata-form>
    <bkdata-form ref="modelInfoForm"
      :labelWidth="80"
      :model="formData"
      :rules="rules">
      <bkdata-form-item label="英文名称"
        :required="true"
        :property="'model_name'"
        :errorDisplayType="'normal'">
        <bkdata-input
          v-model="formData.model_name"
          :maxlength="50"
          :readonly="!isNewForm"
          :placeholder="$t('由英文字母_下划线和数字组成_且字母开头')" />
      </bkdata-form-item>
      <bkdata-form-item label="中文名称"
        :required="true"
        :property="'model_alias'"
        :errorDisplayType="'normal'">
        <bkdata-input
          v-model="formData.model_alias"
          :maxlength="50"
          :placeholder="$t('请输入')"
          @change="handleFormItemChanged" />
      </bkdata-form-item>
      <bkdata-form-item label="数据标签"
        :required="true"
        :property="'tags'"
        :errorDisplayType="'normal'">
        <div class="data-tag-input">
          <bkdata-tag-input
            v-model="formData.tags"
            style="width: 540px"
            :maxData="10"
            :placeholder="$t('请输入')"
            :allowCreate="true"
            :allowAutoMatch="true"
            :hasDeleteIcon="true"
            displayKey="alias"
            saveKey="code"
            :searchKey="['alias', 'code']"
            :list="tagList"
            :createTagValidator="handleCreateTagValidator"
            :filterCallback="handleTagFilterCallback"
            @change="handleFormItemChanged" />
          <p class="limit-box">
            {{ tagCount }}/<span>10</span>
          </p>
        </div>
      </bkdata-form-item>
      <bkdata-form-item label="模型备注"
        :errorDisplayType="'normal'">
        <bkdata-input v-model="formData.description"
          type="textarea"
          :maxlength="100"
          @change="handleFormItemChanged" />
      </bkdata-form-item>
    </bkdata-form>
  </div>
</template>
<script lang="ts" src="./ModelInfo.ts"></script>
<style lang="scss" scoped>
.step-model-info {
  width: 620px;
  margin: 0 auto 26px;

  .message-info {
    width: 100%;
    height: 32px;
    background: #f0f8ff;
    border: 1px solid #c5daff;
    border-radius: 2px;
    display: flex;
    align-items: center;
    padding: 8px 10px;
    margin-bottom: 20px;
    i {
      margin-right: 8px;
      color: #3a84ff;
    }

    .text {
      height: 16px;
      font-size: 12px;
      text-align: left;
      color: #63656e;
      line-height: 16px;
      padding: 0 2px;
    }

    a {
      font-size: 12px;
      color: rgba(58, 132, 255, 1);
      padding: 0 2px;
    }
  }

  ::v-deep .bk-form {
    .bk-label {
      font-weight: normal;
    }
    .bk-textarea-wrapper {
      background-color: #ffffff;
    }
    .bk-form-content {
      display: block;
      text-align: left;
    }
  }

  .model-type-items {
    .model-type-item {
      width: 540px;
      height: 64px;
      background: #ffffff;
      border: 1px solid #c4c6cc;
      border-radius: 2px;
      margin-bottom: 15px;
      position: relative;
      display: flex;
      cursor: pointer;

      &.readonly {
        border-color: #dcdee5;
        cursor: not-allowed;
        .type-item-icon {
          color: #c4c6cc;
          border-color: #dcdee5;
        }
        .type-item-content {
          .content-title {
            color: #979ba5;
          }
          .content-description {
            color: #c4c6cc;
          }
        }
      }

      .type-item-icon {
        display: flex;
        justify-content: center;
        align-items: center;
        width: 50px;
        background: #fafbfd;
        border-right: 1px solid #c4c6cc;
        border-radius: 2px 0px 0px 2px;
        i {
          font-size: 32px;
        }
      }

      .type-item-content {
        width: 490px;
        background: #ffffff;
        text-align: left;
        padding: 10px 8px 10px 14px;

        .content-title {
          font-size: 14px;
          text-align: left;
          color: #313238;
          line-height: 19px;
          letter-spacing: 1px;
        }

        .content-description {
          height: 20px;
          font-size: 12px;
          text-align: left;
          color: #979ba5;
          line-height: 20px;
        }
      }

      .header-icon {
        display: none;
      }

      &.selected {
        border-color: #3a84ff;

        .type-item-icon {
          border-right-color: #3a84ff;
          color: #3a84ff;
          background: #e1ecff;
        }

        .header-icon {
          display: block;
          position: absolute;
          right: 0px;
          top: 0;
          width: 36px;
          height: 36px;
          .icon-status {
            position: absolute;
            right: 0px;
            top: 2px;
            color: #fff;
            font-size: 24px;
          }
          &::before {
            content: '';
            left: 0;
            top: 0;
            position: absolute;
            transform: rotateZ(225deg) translate(6px);
            border-style: solid;
            border-width: 30px 30px 0 30px;
            border-color: #3a84ff transparent transparent transparent;
          }

          &.checked {
            &::before {
              border-bottom-color: #3a84ff !important;
            }
          }
        }
      }
    }
  }
  .data-tag-input {
    position: relative;
    ::v-deep .bk-tag-input {
      .clear-icon {
        align-self: flex-start;
        margin-top: 8px;
        margin-right: 32px;
      }
    }
    .limit-box {
      position: absolute;
      top: 6px;
      right: 6px;
      font-size: 12px;
      margin: 0;
      padding: 0;
      color: #979ba5;
      line-height: 20px;
      transform: scale(0.8);
      z-index: 2;
    }
  }
}
</style>

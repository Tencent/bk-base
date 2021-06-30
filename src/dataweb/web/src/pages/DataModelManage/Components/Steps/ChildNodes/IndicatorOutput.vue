

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
  <div class="output-wrapper bk-scroll-y">
    <div class="title">
      {{ $t('数据输出') }}
    </div>
    <div class="content">
      <bkdata-form ref="baseForm"
        :labelWidth="200"
        formType="vertical"
        :rules="rules"
        :model="indicatorParams">
        <bkdata-form-item
          style="margin-bottom: 8px;"
          :label="$t('指标英文名称')"
          :property="'indicator_name'"
          :required="true"
          :iconOffset="iconDistance">
          <div class="indicator-name-container"
            :class="{ 'is-disabled': isRestrictedEdit }">
            <bkdata-input
              v-model="params.calculation_atom_name"
              style="width: 120px"
              extCls="border-right-none none-error"
              disabled />
            <bkdata-input
              v-model="indicatorParams.indicator_name"
              extCls="none-radius"
              style="flex: 1"
              :disabled="isRestrictedEdit" />
            <span ref="windowTypeEl"
              class="window-type-style">
              {{ windowTypeLength }}
            </span>
          </div>
        </bkdata-form-item>
        <bkdata-form-item>
          <bkdata-input v-model="indicatorName"
            class="display-input"
            disabled />
        </bkdata-form-item>
        <bkdata-form-item
          style="margin-bottom: 8px;"
          :label="$t('指标中文名称')"
          :required="true"
          :property="'indicator_alias'">
          <div class="compose-box">
            <div v-if="!!preAliasName"
              class="pre-text">
              {{ preAliasName }}
            </div>
            <bkdata-input v-model="indicatorParams.indicator_alias"
              class="alias-input" />
          </div>
        </bkdata-form-item>
        <bkdata-form-item v-if="!!preAliasName">
          <bkdata-input :value="indicatorAlias"
            class="display-input"
            disabled />
        </bkdata-form-item>
        <bkdata-form-item :label="$t('指标描述')">
          <bkdata-input v-model="indicatorParams.description"
            type="textarea"
            :maxlength="100" />
        </bkdata-form-item>
        <bkdata-form-item :label="$t('字段信息')">
          <bkdata-table :data="indicatorTableFields"
            :maxHeight="300">
            <bkdata-table-column prop="fieldName"
              :label="$t('字段名')" />
            <bkdata-table-column prop="fieldAlias"
              :label="$t('字段中文名')" />
            <bkdata-table-column prop="fieldCategory"
              :label="$t('字段角色')"
              :width="80" />
            <bkdata-table-column prop="fieldType"
              :label="$t('类型')"
              :width="70" />
            <bkdata-table-column prop="description"
              :label="$t('描述')" />
          </bkdata-table>
        </bkdata-form-item>
      </bkdata-form>
    </div>
  </div>
</template>

<script lang="ts" src="./IndicatorOutput.ts"></script>

<style lang="scss" scoped>
@import '~@/pages/DataGraph/Graph/Children/components/scss/dataOutput.scss';
.indicator-explain {
  height: 16px;
  font-size: 12px;
  color: #979ba5;
  line-height: 16px;
}
::v-deep .bk-label {
  font-weight: normal;
}
.indicator-name-container {
  display: flex;
  flex-wrap: nowrap;
  &.is-disabled {
    .window-type-style {
      border-color: #dcdee5;
    }
    .border-right-none {
      ::v-deep .bk-form-input:disabled {
        border-color: #dcdee5 !important;
      }
    }
  }
  .window-type-style {
    min-width: 32px;
    padding: 0 8px;
    color: #63656e;
    background-color: #fafbfd;
    border: 1px solid #c4c6cc;
    border-left: none;
    border-radius: 0 2px 2px 0;
  }
}
.border-left-none {
  ::v-deep .bk-form-input {
    border-left: none;
  }
}
.none-radius {
  ::v-deep .bk-form-input {
    border-radius: 0;
  }
}
.border-right-none {
  &.none-error {
    ::v-deep input {
      color: #63656e !important;
    }
  }
  ::v-deep .bk-form-input {
    border-right: none;
    border-radius: 2px 0 0 2px;
    border-color: #c4c6cc !important;
  }
}
.compose-box {
  display: flex;
  align-items: center;
  .pre-text {
    height: 32px;
    line-height: 30px;
    font-size: 12px;
    border: 1px solid #c4c6cc;
    border-right: 0;
    padding: 0 10px;
    background-color: #fafbfd;
    border-radius: 2px 0 0 2px;
  }
  .alias-input {
    flex: 1;
    ::v-deep .bk-form-input {
      border-radius: 0 2px 2px 0;
    }
  }
}
.display-input {
  ::v-deep .bk-form-input {
    border: none;
    background-color: #f0f1f5 !important;
  }
}
</style>

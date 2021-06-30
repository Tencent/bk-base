

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
  <div class="index-config-container bk-scroll-y">
    <div class="header">
      {{ $t('指标配置') }}
    </div>
    <bkdata-alert class="mb20"
      type="info">
      <div slot="title">
        基于指标统计口径，快速创建相应指标。
      </div>
    </bkdata-alert>
    <bkdata-form
      id="add-indicator-sql"
      ref="baseForm"
      extCls="bk-common-form"
      :labelWidth="200"
      :rules="rules"
      :model="params">
      <bkdata-form-item
        v-if="isChildIndicator"
        :label="$t('父指标')"
        :required="true"
        :property="'parent_indicator_name'">
        <bkdata-input :value="parentIndicatorDisplayName"
          :disabled="true"
          style="width: 496px" />
      </bkdata-form-item>
      <bkdata-form-item
        :label="$t('指标统计口径')"
        :required="true"
        :property="'calculation_atom_name'"
        style="margin-bottom: 8px;">
        <bkdata-selector
          :style="{ width: '496px' }"
          :selected.sync="params.calculation_atom_name"
          :filterable="true"
          :searchable="true"
          :list="calculationAtomsList"
          :settingKey="'calculationAtomName'"
          :displayKey="'displayName'"
          :disabled="isSaveNode || isChildIndicator"
          searchKey="calculationAtomName" />
      </bkdata-form-item>
      <bkdata-form-item :required="true">
        <bkdata-input v-model="calculationFormula"
          extCls="border-none"
          readonly />
      </bkdata-form-item>
      <bkdata-form-item :label="$t('聚合字段')">
        <bkdata-select
          v-model="params.aggregation_fields"
          style="width: 100%"
          :placeholder="$t('请选择聚合字段')"
          :disabled="isRestrictedEdit"
          clearable
          searchable
          multiple
          showSelectAll>
          <bkdata-option
            v-for="option in aggregateFieldList"
            :id="option.fieldName"
            :key="option.fieldName"
            :name="option.displayName" />
        </bkdata-select>
      </bkdata-form-item>
      <bkdata-form-item :label="$t('过滤条件')"
        extCls="sql-container">
        <Monaco
          ref="monacoEditor"
          :code="params.filter_formula"
          :height="'240px'"
          :options="{ fontSize: '14px' }"
          :tools="{
            guidUrl: $store.getters['docs/getPaths'].realtimeSqlRule,
            toolList: {
              font_size: true,
              full_screen: true,
              event_fullscreen_default: true,
              editor_fold: true,
            },
            title: 'SQL',
          }"
          @codeChange="changeCode" />
      </bkdata-form-item>
      <bkdata-form-item :label="$t('计算类型')"
        :required="true"
        :property="'name'">
        <div class="bk-button-group">
          <bkdata-button
            :class="nodeType === 'stream' ? 'is-selected' : ''"
            :disabled="isRestrictedEdit"
            @click="changeNodeType('stream')">
            {{ $t('实时计算') }}
          </bkdata-button>
          <bkdata-button
            :class="nodeType === 'batch' ? 'is-selected' : ''"
            :disabled="isRestrictedEdit"
            @click="changeNodeType('batch')">
            {{ $t('离线计算') }}
          </bkdata-button>
        </div>
      </bkdata-form-item>
    </bkdata-form>
    <div class="second-section">
      <bkdata-tab :active.sync="activeName"
        type="unborder-card">
        <bkdata-tab-panel
          v-for="(panel, index) in tabConfig"
          :key="index"
          v-bind="panel"
          :disabled="isPanelDisabled(panel.disabled)">
          <component
            :is="panel.component"
            :ref="panel.name"
            :params.sync="params.scheduling_content"
            :filteWindowTypes="['none', 'session']"
            :nodeType="nodeType"
            :isShowStartTime="false"
            :isShowSelfDependency="false"
            :isShowDependencyConfig="false"
            :isSetDisable="isRestrictedEdit"
            :parentConfig="[
              {
                node_type: 'batch',
              },
            ]" />
        </bkdata-tab-panel>
      </bkdata-tab>
    </div>
  </div>
</template>

<script lang="ts" src="./IndexConfig.ts"></script>

<style lang="scss" scoped>
.border-none {
  width: 496px;
  ::v-deep .bk-form-input {
    border: none;
    background-color: #f0f1f5 !important;
  }
}
.index-config-container {
  height: 100%;
  padding: 20px;
  background-color: white;
  border-left: 1px solid #dcdee5;
  border-right: 1px solid #dcdee5;
  .header {
    width: 64px;
    height: 22px;
    font-size: 16px;
    font-family: PingFangSC, PingFangSC-Medium;
    font-weight: 500;
    text-align: left;
    color: #313238;
    line-height: 22px;
    margin-bottom: 30px;
  }
}
.bk-tag-selector {
  width: 100%;
}
#add-indicator-sql {
  .bk-form-item {
    width: 100%;
    margin-bottom: 20px;
    ::v-deep .bk-label {
      font-weight: normal;
    }
    ::v-deep .bk-form-content {
      width: calc(100% - 100px);
      .bk-monaco-editor {
        width: 100%;
      }
    }
  }
  .mb2 {
    margin-bottom: 2px;
  }
}

::v-deep .second-section {
  .bk-tab-header {
    .bk-tab-label-list {
      li {
        padding: 0;
        min-width: auto;
        margin-right: 20px;
      }
      .active::after {
        left: 0;
        width: 100%;
      }
    }
  }
  .bk-tab-section {
    padding: 20px 0;
  }
}
::v-deep .bk-common-form {
  & > .bk-form-item {
    width: auto !important;
    & > .bk-form-content {
      width: 496px;
      margin-left: 100px !important;
      .number-select-width {
        width: 300px;
      }
      .unit-select-width {
        border-left: none;
        width: 196px;
      }
    }
  }
}
</style>

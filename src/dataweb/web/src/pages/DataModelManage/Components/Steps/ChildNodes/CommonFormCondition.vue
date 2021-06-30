

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
  <div class="index-statistical-caliber">
    <bkdata-form ref="dimensionForm"
      class="caliber-form mt20"
      :labelWidth="80"
      :model="params"
      :rules="rules">
      <bkdata-form-item :label="$t('来源主表')">
        <bkdata-input style="width: 320px"
          :value="activeTabName"
          :disabled="true" />
      </bkdata-form-item>
      <div class="bk-form-item is-required">
        <label class="bk-label"
          style="width: 80px">
          <span class="bk-label-text">{{ $t('聚合逻辑') }}</span>
        </label>
        <div class="bk-form-content"
          style="margin-left: 80px">
          <template v-if="isSelectorModel">
            <bkdata-form-item class="content-no-ml mr10"
              :property="'calculation_content.content.calculation_function'">
              <bkdata-selector
                style="width: 120px"
                :disabled="isRestrictedEdit"
                :dropDownWidth="230"
                :isLoading="isSqlFuncLoading"
                :placeholder="$t('请选择聚合函数')"
                allowClear
                class="normal-width"
                searchable
                displayKey="functionName"
                :settingKey="'functionName'"
                :selected.sync="params.calculation_content.content.calculation_function"
                :list="sqlFuncsList"
                @item-selected="sqlFuncChange" />
            </bkdata-form-item>
            <bkdata-form-item class="content-no-ml mt0"
              :property="'calculation_content.content.calculation_field'">
              <bkdata-select
                v-model="params.calculation_content.content.calculation_field"
                :disabled="isRestrictedEdit"
                :loading="isSqlFieldLoading"
                :popoverWidth="230"
                style="width: 190px"
                searchable
                @toggle="selectToggle">
                <bkdata-option-group v-for="(group, index) in fieldNameGroupList"
                  :key="index"
                  :name="group.name">
                  <bkdata-option
                    v-for="option in group.children"
                    :id="option.id"
                    :key="option.id"
                    v-bk-tooltips="{
                      disabled: !option.disabled,
                      content: $t('当前指标统计口径已经存在') + `：${option.name}`,
                    }"
                    :name="option.name"
                    :disabled="option.disabled" />
                </bkdata-option-group>
              </bkdata-select>
            </bkdata-form-item>
            <bkdata-form-item class="content-no-ml mt0">
              <bkdata-button theme="primary"
                size="small"
                text
                :disabled="isRestrictedEdit"
                @click="handleChangeModel">
                {{ $t('自定义') }}
              </bkdata-button>
            </bkdata-form-item>
          </template>
          <template v-else>
            <Monaco
              class="text-left"
              height="400px"
              :code="code"
              :options="monacoSetting.options"
              :tools="monacoSetting.tools"
              @codeChange="handleChangeCode" />
          </template>
        </div>
      </div>
      <bkdata-form-item style="width: 400px"
        required
        :label="$t('字段类型')"
        :property="'field_type'">
        <bkdata-selector
          class="normal-width"
          :disabled="isRestrictedEdit"
          :isLoading="isFieldTypeLoading"
          searchable
          displayKey="fieldType"
          :settingKey="'fieldType'"
          :selected.sync="params.field_type"
          :list="fieldTypeList" />
      </bkdata-form-item>
      <div class="custom-inline-item mt20 mb20">
        <div class="inline-item-content mr40">
          <bkdata-form-item required
            :label="$t('英文名称')"
            :property="'calculation_atom_name'">
            <bkdata-input v-model="params.calculation_atom_name"
              class="normal-width"
              :disabled="mode === 'edit'" />
          </bkdata-form-item>
          <p class="name-tips">
            {{ $t('对于费用、收入的统计，推荐以 amt 结尾') }}
          </p>
        </div>
        <bkdata-form-item required
          :label="$t('中文名称')"
          :property="'calculation_atom_alias'">
          <bkdata-input v-model="params.calculation_atom_alias"
            class="normal-width" />
        </bkdata-form-item>
      </div>
      <bkdata-form-item :label="$t('描述')"
        :property="'description'">
        <bkdata-input v-model="params.description"
          class="area-input"
          type="textarea"
          :maxlength="100" />
      </bkdata-form-item>
      <bkdata-form-item>
        <bkdata-button class="mr10 btn-width"
          :loading="isLoading"
          theme="primary"
          @click="handleSubmit">
          {{ $t('提交') }}
        </bkdata-button>
        <bkdata-button class="btn-width"
          theme="default"
          @click="$emit('cancel')">
          {{ $t('取消') }}
        </bkdata-button>
      </bkdata-form-item>
    </bkdata-form>
  </div>
</template>
<script lang="ts" src="./CommonFormCondition.ts"></script>
<style lang="scss" scoped>
::v-deep .bk-select-name {
  text-align: left;
}
.text-left {
  text-align: left;
}
.index-statistical-caliber {
  .normal-width {
    width: 320px;
  }
  .btn-width {
    width: 86px;
  }
  .caliber-form {
    .content-no-ml {
      ::v-deep .bk-form-content {
        margin-left: 0 !important;
      }
    }
    .custom-inline-item {
      display: flex;
      justify-content: space-between;
    }
    .name-tips {
      font-size: 12px;
      line-height: 16px;
      margin-left: 80px;
      color: #979ba5;
      margin-top: 4px;
      text-align: left;
    }
    .area-input {
      ::v-deep .bk-form-textarea {
        min-height: 38px;
      }
    }
  }
}
</style>

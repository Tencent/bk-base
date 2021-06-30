

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
  <div class="refer-dimension-table">
    <div ref="mainContent"
      class="main bk-scroll-y">
      <bkdata-alert type="info"
        :title="$t('通过关联维度表数据模型，可以扩展主表中的维度。')" />
      <bkdata-form ref="dimensionForm"
        class="dimension-form mt20"
        :labelWidth="84"
        :model="formData">
        <div class="combination-item mb10">
          <bkdata-form-item :label="$t('关联模型')">
            <div class="compose-item">
              <div class="compose-text is-disabled">
                {{ $t('主表') }}
              </div>
              <bkdata-input style="width: 298px"
                :value="mstTableDisplayName"
                :disabled="true" />
            </div>
          </bkdata-form-item>
          <span class="connection-label icon-label">
            <i class="icon-left-join" />
          </span>
          <bkdata-form-item
            ref="dimensionModelNode"
            class="no-label"
            :required="true"
            :rules="dimensionModelIdRules"
            property="dimensionModelId">
            <div class="compose-item">
              <div :class="['compose-text', { 'is-disabled': isDisabled }]">
                {{ $t('关联表') }}
              </div>
              <bkdata-select
                v-model="formData.dimensionModelId"
                v-bk-tooltips="disabledTips"
                style="width: 298px"
                :clearable="false"
                :disabled="isDisabled"
                :loading="isModelListLoading"
                :popoverOptions="{
                  boundary: selectorBoundary,
                }">
                <li style="overflow: hidden">
                  <ModelTab :list="modelPanels"
                    :activeTabName.sync="activeSelectedTab" />
                </li>
                <li v-if="displayDimensionModelList.length"
                  class="select-search-wrapper">
                  <i class="icon-search bk-icon left-icon" />
                  <input v-model="searchKey"
                    type="text"
                    placeholder="输入关键字搜索"
                    class="bk-select-search-input">
                </li>
                <bkdata-option
                  v-for="item in searchDimensionModelList"
                  :id="item.model_id"
                  :key="item.model_id"
                  v-bk-tooltips="{
                    content: $t('当前维度表无扩展字段，建议完善维度表字段'),
                    disabled: !(item.has_extended_fields === false && formData.dimensionModelId !== item.model_id),
                    interactive: false,
                  }"
                  :name="`${item.model_name} (${item.model_alias})`"
                  :disabled="item.has_extended_fields === false && formData.dimensionModelId !== item.model_id" />
                <template slot="extension">
                  <bkdata-button text
                    size="small"
                    style="padding: 0"
                    @click="handleCreateNewModelTable">
                    {{ $t('新建维度表数据模型') }}
                  </bkdata-button>
                </template>
              </bkdata-select>
            </div>
          </bkdata-form-item>
        </div>
        <div class="combination-item mb20">
          <bkdata-form-item :required="true"
            :rules="rules.mustSelect"
            property="masterFieldName">
            <div class="compose-item">
              <div :class="['compose-text', { 'is-disabled': isEdit }]">
                {{ $t('字段') }}
              </div>
              <bkdata-select
                v-model="formData.masterFieldName"
                style="width: 298px"
                :clearable="false"
                :searchable="true"
                :disabled="isEdit"
                :placeholder="$t('请选择主表字段')"
                :popoverOptions="{
                  boundary: selectorBoundary,
                }">
                <bkdata-option
                  v-for="item in mstFields"
                  :id="item.fieldName"
                  :key="item.fieldName"
                  :name="`${item.fieldName} (${item.fieldAlias})`" />
              </bkdata-select>
            </div>
          </bkdata-form-item>
          <span class="connection-label">=</span>
          <bkdata-form-item
            ref="dimensionFieldNameItem"
            class="no-label"
            property="dimensionFieldName"
            :rules="relatedFieldNamedRules">
            <div class="compose-item">
              <div class="compose-text">
                {{ $t('字段') }}
              </div>
              <bkdata-select
                v-model="formData.dimensionFieldName"
                style="width: 298px"
                :clearable="false"
                :searchable="true"
                :loading="isMasterTableLoading"
                :placeholder="$t('请选择关联表字段')"
                :popoverOptions="{
                  boundary: selectorBoundary,
                }">
                <bkdata-option
                  v-for="item in displayDimensionFields"
                  :id="item.fieldName"
                  :key="item.fieldName"
                  :name="`${item.fieldName} (${item.fieldAlias})`" />
              </bkdata-select>
            </div>
          </bkdata-form-item>
        </div>
        <div class="bk-form-item">
          <label class="bk-label"
            style="width: 84px">
            <span class="bk-label-text">{{ $t('扩展字段') }}</span>
          </label>
          <div class="bk-form-content"
            style="margin-left: 84px; flex-wrap: wrap">
            <bkdata-form-item
              ref="expandFieldSelector"
              style="width: 100%"
              class="no-label"
              property="expandFields"
              :rules="rules.selectExpand">
              <bkdata-select
                v-model="selectedExpandFields"
                class="mb10"
                style="width: 100%"
                multiple
                :searchable="true"
                :clearable="false"
                :popoverOptions="{
                  boundary: selectorBoundary,
                }"
                @change="handleSelectedExpandField">
                <template v-for="item in nonPrimaryKeyFields">
                  <bkdata-option
                    :id="item.sourceFieldName"
                    :key="item.sourceFieldName"
                    :name="item.sourceFieldName"
                    :disabled="isReadonly(item)" />
                </template>
              </bkdata-select>
            </bkdata-form-item>
            <div v-if="!!deleteExpandFields.length"
              class="delete-fields">
              {{
                $t(
                  `当前关联表（${curDimensionModelName}）的字段：${deleteExpandFields.join(
                    '，'
                  )}，已被删除，保存后系统后自动删除这些扩展字段`
                )
              }}
            </div>
            <bkdata-table class="fields-table"
              :data="displayExpandFields">
              <bkdata-table-column :label="$t('原字段名称')"
                :showOverflowTooltip="true"
                prop="displayName" />
              <bkdata-table-column :label="$t('字段类型')"
                prop="fieldType"
                width="100" />
              <bkdata-table-column :label="$t('字段英文名')">
                <template slot-scope="{ row, $index }">
                  <span
                    v-if="row.editable === false"
                    v-bk-tooltips="getDeleteTips(row, 'edit')"
                    style="display: block; padding: 0 10px">
                    {{ row.fieldName }}
                  </span>
                  <bkdata-form-item
                    v-else
                    :ref="'fieldFormItem_name_' + $index"
                    :key="$index + '_name'"
                    :rules="row.rules.name"
                    :property="`expandFields.${$index}.fieldName`">
                    <bkdata-input
                      v-model="formData.expandFields[$index].fieldName"
                      class="custom-input"
                      @blur="handleBlur" />
                  </bkdata-form-item>
                </template>
              </bkdata-table-column>
              <bkdata-table-column :label="$t('字段中文名')">
                <template slot-scope="{ row, $index }">
                  <bkdata-form-item
                    :ref="'fieldFormItem_alias_' + $index"
                    :key="$index + '_alias'"
                    :rules="row.rules.alias"
                    :property="`expandFields.${$index}.fieldAlias`">
                    <bkdata-input
                      v-model="formData.expandFields[$index].fieldAlias"
                      class="custom-input"
                      @blur="handleBlur" />
                  </bkdata-form-item>
                </template>
              </bkdata-table-column>
              <bkdata-table-column :label="$t('操作')"
                width="70">
                <template slot-scope="{ row, $index }">
                  <span class="icon-group">
                    <i
                      v-bk-tooltips="getDeleteTips(row, 'delete')"
                      class="bk-icon icon-minus-6"
                      :class="{ disabled: row.deletable === false }"
                      @click="handleRemoveField(row, $index)" />
                  </span>
                </template>
              </bkdata-table-column>
            </bkdata-table>
          </div>
        </div>
      </bkdata-form>
    </div>
    <div :class="['footer-btns', { 'sticky-style': hasScrollBar }]">
      <bkdata-button style="width: 86px"
        extCls="mr10"
        theme="primary"
        @click="handleSave">
        {{ $t('保存') }}
      </bkdata-button>
      <bkdata-button style="width: 86px"
        theme="default"
        @click="handleCancel">
        {{ $t('取消') }}
      </bkdata-button>
      <template v-if="isEdit">
        <span
          v-if="isDisabled"
          v-bk-tooltips="$t('当前关联的维度字段（维度字段名称）已被使用，暂无法删除')"
          class="clear-btn bk-default bk-button-normal bk-button is-disabled">
          {{ $t('删除关联') }}
        </span>
        <bkdata-button v-else
          class="clear-btn"
          theme="default"
          :disabled="isDisabled"
          @click="handleShowConfirm">
          {{ $t('删除关联') }}
        </bkdata-button>
      </template>
      <pop-container ref="clearConfirmPop"
        v-bkClickoutside="handleHideConfirm"
        class="clear-confirm">
        <div class="clear-tips">
          <span>{{ $t('确认删除关联信息？') }}</span>
          <p>{{ $t('删除操作无法撤回，请谨慎操作！') }}</p>
          <div class="btns">
            <bkdata-button class="mr5"
              theme="primary"
              size="small"
              @click="handleConfirmDelete">
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
<script lang="ts" src="./ReferDimensionTable.ts"></script>
<style lang="scss" scoped>
.refer-dimension-table {
  .main {
    padding: 20px 30px;
    max-height: calc(100vh - 112px);
  }
  .dimension-form {
    ::v-deep .bk-label {
      font-weight: normal;
    }
    .combination-item {
      display: flex;
      align-items: center;
    }
    ::v-deep .no-label .bk-form-content {
      margin-left: 0 !important;
    }
    .connection-label {
      width: 59px;
      font-size: 16px;
      color: #81838f;
      text-align: center;
      &.icon-label {
        display: flex;
        align-items: center;
        &::before,
        &::after {
          content: '';
          width: 10px;
          height: 1px;
          background-color: #dcdee5;
        }
      }
      .icon-left-join {
        color: #979ba5;
        font-size: 24px;
      }
    }
    .compose-item {
      display: flex;
      align-items: center;
      .compose-text {
        width: 76px;
        height: 32px;
        line-height: 30px;
        text-align: center;
        border: 1px solid #c4c6cc;
        border-right: none;
        border-radius: 2px 0 0 2px;
        background-color: #f5f6fa;
        &.is-disabled {
          border-color: #dcdee5;
        }
      }
      ::v-deep {
        .bk-form-input,
        .bk-select {
          border-radius: 0 2px 2px 0;
        }
      }
    }
    .delete-fields {
      width: 100%;
      line-height: 18px;
      font-size: 14px;
      color: #ff9c01;
      margin-bottom: 16px;
    }
    .fields-table {
      .bk-select {
        height: 28px;
        line-height: 28px;
        border-color: transparent;
        background-color: #ffffff;
        ::v-deep .bk-select-name {
          height: 28px;
        }
        ::v-deep .icon-angle-down {
          display: none;
        }
      }
      .custom-input {
        ::v-deep .bk-form-input {
          height: 28px;
          line-height: 28px;
          border-color: transparent;
          color: #63656e;
          background-color: #ffffff;
        }
      }
      .icon-group {
        color: #c4c6cc;
        font-size: 18px;
        cursor: pointer;
        .bk-icon {
          &:hover {
            color: #979ba5;
          }
          &.disabled {
            cursor: not-allowed;
          }
        }
      }
      ::v-deep .bk-form-content {
        font-size: 0;
        min-height: 28px;
        margin-left: 0 !important;
      }
      ::v-deep .tooltips-icon {
        top: 7px;
      }
      ::v-deep tbody tr {
        &.hover-row {
          td {
            background-color: #ffffff;
          }
          .custom-input .bk-form-input {
            background-color: #f5f6fa;
          }
          .bk-select {
            background-color: #f5f6fa;
            .icon-angle-down {
              display: block;
            }
          }
        }
      }
    }
  }
  .footer-btns {
    position: sticky;
    bottom: 0;
    padding: 0 30px;
    margin-top: 20px;
    margin-left: 84px;
    &.sticky-style {
      width: 100%;
      height: 52px;
      line-height: 52px;
      margin: 0;
      background-color: #fafbfd;
      border-top: 1px solid #f0f1f5;
      .clear-btn {
        margin-top: 10px;
      }
    }
    .clear-btn {
      min-width: 100px;
      float: right;
    }
  }
}
.select-search-wrapper {
  padding: 0 16px;
  position: relative;
  .icon-search {
    position: absolute;
    font-size: 16px;
    color: #979ba5;
    left: 16px;
    top: 9px;
  }
  .bk-select-search-input {
    padding-left: 22px;
    border-bottom: 1px solid #f0f1f5;
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

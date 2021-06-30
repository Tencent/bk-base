

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
    <bkdata-form
      ref="conditionSelectorForm"
      extCls="condition-selector-form"
      formType="inline"
      :labelWidth="0"
      :model="conditionData">
      <template v-for="(group, index) in conditionData.groups">
        <div :key="group.uid"
          :class="['condition-group', { 'is-error': sameGroupIds.includes(group.uid) }]">
          <i v-bk-tooltips="$t('重复条件组')"
            class="icon-exclamation-circle-shape error-icon" />
          <bkdata-button
            v-if="conditionData.groups.length > 1 && !readonly"
            extCls="group-delete"
            :text="true"
            @click="handleDeleteGroup(index)">
            <i class="icon-delete" />
          </bkdata-button>
          <template v-for="(item, childIndex) in group.items">
            <div :key="item.uid"
              class="condition-group-item">
              <bkdata-form-item :extCls="'connection-item ' + (childIndex === 0 && 'is-hide')"
                label="">
                <bkdata-button class="op-switch-btn"
                  :disabled="readonly"
                  @click="handleChangeItemOp(index, group.op)">
                  {{ group.op }}
                </bkdata-button>
              </bkdata-form-item>
              <bkdata-form-item
                :ref="`validate_id_${index}_${childIndex}`"
                extCls="condition-item ml10"
                label=""
                :property="`groups.${index}.items.${childIndex}.constraintId`"
                :required="true"
                :rules="[validatorMap.required]">
                <bk-cascade
                  v-model="item.constraintId"
                  class="condition-cascade"
                  :trigger="trigger"
                  :list="conditionList"
                  :disabled="readonly"
                  :popoverOptions="{
                    theme: 'condition-selector-cls',
                    boundary: selectorBoundary,
                  }"
                  :options="{
                    idKey: 'constraintId',
                    nameKey: 'constraintName',
                  }"
                  @change="handleConditionChange(...arguments, index, childIndex)">
                  <template slot="prepend"
                    slot-scope="{ node }">
                    <span v-if="node.description"
                      style="color: #c4c6cc; margin-left: 6px">
                      {{ node.description }}
                    </span>
                  </template>
                </bk-cascade>
              </bkdata-form-item>
              <bkdata-form-item
                :ref="`validate_content_${index}_${childIndex}`"
                extCls="content-item ml10"
                label=""
                :property="`groups.${index}.items.${childIndex}.constraintContent`"
                :required="true"
                :rules="item.config.rules">
                <bkdata-input
                  v-model="item.constraintContent"
                  :disabled="!item.config.editable || readonly"
                  :placeholder="item.config.placeholder"
                  @blur="handleItemBlur(index)" />
              </bkdata-form-item>
              <div v-if="!readonly"
                class="btns">
                <bkdata-button
                  v-if="group.items.length > 1"
                  class="ml10"
                  :text="true"
                  @click="handleDeleteItem(index, childIndex)">
                  <i class="icon-minus-circle-shape" />
                </bkdata-button>
                <bkdata-button
                  v-if="group.items.length - 1 === childIndex"
                  class="ml10"
                  :text="true"
                  @click="handleAddItem(index)">
                  <i class="icon-plus-circle-shape" />
                </bkdata-button>
              </div>
            </div>
          </template>
        </div>
        <div
          v-if="conditionData.groups.length > 1 && index !== conditionData.groups.length - 1"
          :key="index + 'group-op'"
          class="group-connection">
          <bkdata-button class="op-switch-btn"
            :disabled="readonly"
            @click="handleChangeGroupOp(conditionData.op)">
            {{ conditionData.op }}
          </bkdata-button>
        </div>
      </template>
      <bkdata-button v-if="!readonly"
        extCls="add-group-btn"
        @click="handleAddGroup">
        <i class="icon-add-9" />
        {{ $t('条件组') }}
      </bkdata-button>
    </bkdata-form>
  </div>
</template>
<script lang="ts" src="./ConditionSelector.ts"></script>
<style lang="scss" scoped>
.codition-selector-wrapper {
  .condition-selector-form {
    width: 100%;
    .condition-group {
      position: relative;
      padding: 20px 38px 10px 16px;
      background-color: #f5f6fa;
      border: 1px solid transparent;
      &.is-error {
        border-color: #ea3636;
        .error-icon {
          display: block;
        }
      }
      .error-icon {
        display: none;
        position: absolute;
        top: 8px;
        right: 34px;
        font-size: 16px;
        color: #ea3636;
        cursor: pointer;
        z-index: 10;
      }
    }
    .condition-group-item {
      display: flex;
      align-items: center;
      font-size: 0;
      margin-bottom: 10px;
      ::v-deep .bk-form-content {
        width: 100%;
      }
    }
    .connection-item {
      flex: 0 0 42px;
      &.is-hide {
        visibility: hidden;
      }
    }
    .condition-item {
      flex: 0 0 240px;
      background-color: #ffffff;
      ::v-deep .bk-select-angle {
        display: none;
      }
    }
    .content-item {
      flex: 1;
      ::v-deep .bk-form-input[disabled] {
        background-color: #fafbfd !important;
      }
    }
    .op-switch-btn {
      font-size: 12px;
      min-width: 42px;
      color: #3a84ff;
      border-color: #c4c6cc;
      padding: 0;
    }
    .group-connection {
      margin: 10px 0;
    }
    ::v-deep .condition-cascade {
      .bk-cascade-name {
        padding: 0 10px;
      }
      .bk-cascade-angle {
        display: none;
      }
    }
    .btns {
      flex: 0 0 56px;
      display: inline-block;
      vertical-align: middle;
      font-size: 0;
      i {
        font-size: 18px;
        color: #c4c6cc;
      }
    }
    .group-delete {
      position: absolute;
      top: 8px;
      right: 10px;
      font-size: 0;
      height: 18px;
      color: #979ba5;
      i {
        font-size: 18px;
      }
    }
    .add-group-btn {
      width: 100%;
      height: 46px;
      line-height: 44px;
      color: #c4c6cc;
      border: 1px dashed #c4c6cc;
      background-color: #fafbfd;
      margin: 10px 0 0;
    }
  }
}
</style>

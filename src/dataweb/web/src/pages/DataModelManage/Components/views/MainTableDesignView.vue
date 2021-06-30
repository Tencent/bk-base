

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
  <div class="main-table-view-layout">
    <bkdata-table
      class="master-table"
      :data="fields"
      :outerBorder="false"
      :maxHeight="462"
      :rowClassName="rowClassName"
      :cellClassName="cellClassName">
      <bkdata-table-column label=""
        width="30"
        className="icon-column-cls"
        :resizable="false">
        <template slot-scope="{ row }">
          <i :class="['bk-icon', { 'icon-link-2': row.isJoinField }]" />
        </template>
      </bkdata-table-column>
      <bkdata-table-column :label="$t('字段名')"
        prop="fieldName"
        width="200"
        :showOverflowTooltip="true">
        <template slot-scope="{ row }">
          <span>{{ row.fieldName === '__time__' ? '--' : row.fieldName }}</span>
        </template>
      </bkdata-table-column>
      <bkdata-table-column :label="$t('字段中文名')"
        prop="fieldAlias"
        width="200"
        :showOverflowTooltip="true" />
      <bkdata-table-column :label="$t('数据类型')"
        prop="fieldType"
        width="120">
        <template slot-scope="{ row }">
          <span>{{ row.fieldName === '__time__' ? '--' : row.fieldType }}</span>
        </template>
      </bkdata-table-column>
      <bkdata-table-column :label="$t('字段角色')"
        prop="fieldCategory"
        width="120">
        <template slot-scope="{ row }">
          <span>{{ row.fieldName === '__time__' ? '--' : fieldCategoryMap[row.fieldCategory] }}</span>
        </template>
      </bkdata-table-column>
      <bkdata-table-column :label="$t('字段描述')"
        prop="description"
        :showOverflowTooltip="true">
        <template slot-scope="{ row }">
          <span>{{ row.description || '--' }}</span>
        </template>
      </bkdata-table-column>
      <bkdata-table-column :label="$t('值约束')"
        prop="fieldConstraintContent">
        <template slot-scope="{ row }">
          <template v-if="!row.fieldConstraintContent">
            --
          </template>
          <condition-selector
            v-else
            :readonly="true"
            :constraintList="getConstraintList(row)"
            :constraintContent="row.fieldConstraintContent" />
        </template>
      </bkdata-table-column>
      <bkdata-table-column :label="$t('字段加工逻辑')"
        prop="fieldCleanContent"
        width="160">
        <template slot-scope="{ row }">
          <template v-if="!row.fieldCleanContent">
            --
          </template>
          <template v-else>
            <bkdata-button
              style="color: #979ba5; font-size: 16px"
              theme="default"
              text
              @click="handleFieldProcessingLogic(row)">
              <i class="icon-icon-audit" />
            </bkdata-button>
          </template>
        </template>
      </bkdata-table-column>
      <bkdata-table-column :label="$t('主键')"
        prop="isPrimaryKey"
        width="80">
        <template slot-scope="{ row }">
          <bkdata-checkbox v-if="row.isPrimaryKey"
            :value="row.isPrimaryKey"
            :disabled="true" />
          <template v-else>
            --
          </template>
        </template>
      </bkdata-table-column>
    </bkdata-table>

    <!---字段加工逻辑---->
    <bkdata-sideslider :isShow.sync="fieldCleanContentSlider.isShow"
      :width="941"
      :quickClose="false">
      <div slot="header"
        class="custom-sideslider-title">
        {{ $t('字段加工逻辑') }} <span>- {{ fieldCleanContentSlider.title }}</span>
      </div>
      <div slot="content">
        <FieldProcessingLogic
          :activeModel="activeModelTabItem"
          :editable="false"
          :fieldList="fields"
          :field="fieldCleanContentSlider.field"
          @on-cancel="fieldCleanContentSlider.isShow = false" />
      </div>
    </bkdata-sideslider>
  </div>
</template>
<script lang="ts" src="./MainTableDesignView.ts"></script>
<style lang="scss" scoped>
.main-table-view-layout {
  .master-table {
    &::before {
      height: 0;
    }
    ::v-deep {
      td.cell-border-bottom-none {
        border-bottom-color: transparent;
      }
      tr {
        &.is-extended-row {
          background-color: #fafbfd;
          .condition-selector-input .input-ellipsis {
            background-color: #fafbfd;
            border-color: #fafbfd;
          }
        }
        &.is-inner-row {
          background-color: #f0f1f5;
        }
        &:hover {
          .condition-selector-input .input-ellipsis {
            background-color: #f0f1f5;
            border-color: #f0f1f5;
          }
        }
      }
      .icon-column-cls {
        .cell {
          padding: 0;
          text-align: right;
        }
      }
      .bk-table-row-last {
        td {
          border-bottom: none;
        }
      }
    }
  }
  ::v-deep .codition-selector-wrapper .condition-selector-input {
    border: none;
    padding: 0;
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
}
</style>

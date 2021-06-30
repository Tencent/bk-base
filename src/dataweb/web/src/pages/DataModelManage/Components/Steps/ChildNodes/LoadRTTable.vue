

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
  <div class="pd30">
    <bkdata-form :labelWidth="200"
      :model="formData"
      formType="vertical">
      <bkdata-form-item :label="$t('业务')"
        :required="true"
        :property="'name'">
        <bkdata-selector
          :selected.sync="formData.bizId"
          :filterable="true"
          :isLoading="isBizLoading"
          :searchable="true"
          :list="bizList"
          :settingKey="'bk_biz_id'"
          :displayKey="'bk_biz_name'"
          :allowClear="false"
          :searchKey="'bk_biz_name'"
          @item-selected="bizChange">
          <template slot="bottom-option">
            <div slot="extension"
              class="extension-btns">
              <span class="icon-btn"
                @click="handleShowApply">
                <i class="icon-apply-2 icon-item" />
                {{ $t('申请业务数据') }}
              </span>
              <span class="icon-btn"
                @click="handleDddDataId">
                <i class="icon-plus-circle icon-item" />
                {{ $t('新接入数据源') }}
              </span>
            </div>
          </template>
        </bkdata-selector>
      </bkdata-form-item>
      <bkdata-form-item :label="$t('结果数据表')"
        :required="true">
        <bkdata-selector
          :selected.sync="formData.resultTableId"
          :filterable="true"
          :isLoading="isResultTableLoading"
          :searchable="true"
          :list="resultTableList"
          :settingKey="'result_table_id'"
          :displayKey="'result_table_id'"
          :allowClear="false"
          :searchKey="'result_table_id'"
          @item-selected="resultTableChange" />
      </bkdata-form-item>
      <bkdata-form-item v-bkloading="{ isLoading: isTableLoading }"
        :label="$t('结果数据表结构预览')">
        <bkdata-table :data="resultTableDetail"
          :maxHeight="appHeight - 356"
          @selection-change="handleSelectonChange">
          <bkdata-table-column
            width="60"
            type="selection"
            :selectable="(row, index) => row.field_name !== '__time__'" />
          <bkdata-table-column width="180"
            :label="$t('字段名')"
            :showOverflowTooltip="true"
            :prop="'field_name'">
            <template slot-scope="{ row }">
              <span>{{ row.field_name === '__time__' ? '--' : row.field_name }}</span>
            </template>
          </bkdata-table-column>
          <bkdata-table-column
            minWidth="250"
            :label="$t('字段中文名')"
            :showOverflowTooltip="true"
            :prop="'field_alias'" />
          <bkdata-table-column width="100"
            :label="$t('数据类型')"
            :showOverflowTooltip="true"
            :prop="'field_type'" />
          <bkdata-table-column
            minWidth="250"
            :label="$t('字段描述')"
            :showOverflowTooltip="true"
            :prop="'description'" />
        </bkdata-table>
      </bkdata-form-item>
      <bkdata-form-item>
        <div class="mt20">
          <bkdata-button
            :theme="'primary'"
            type="submit"
            :title="$t('确定')"
            :disabled="!resultTableDetail.length || !loadFields.length"
            class="mr10"
            @click="confirm">
            {{ $t('确定') }}
          </bkdata-button>
          <bkdata-button :theme="'default'"
            :title="$t('取消')"
            @click="cancel">
            {{ $t('取消') }}
          </bkdata-button>
        </div>
      </bkdata-form-item>
    </bkdata-form>

    <applyBiz v-if="isShowApply"
      ref="applyBiz"
      :bkBizId="formData.bizId"
      @closeApplyBiz="handleCloseApply" />
  </div>
</template>
<script lang="ts" src="./LoadRTTable.ts"></script>
<style lang="scss" scoped>
::v-deep .bk-label {
  font-weight: normal;
}
.pd30 {
  padding: 30px;
}
.extension-btns {
  display: flex;
  align-items: center;
  .icon-btn {
    flex: 1;
    height: 32px;
    line-height: 32px;
    color: #63656e;
    cursor: pointer;
    &:hover {
      color: #3a84ff;
    }
    .icon-item {
      font-size: 14px;
    }
  }
}
</style>

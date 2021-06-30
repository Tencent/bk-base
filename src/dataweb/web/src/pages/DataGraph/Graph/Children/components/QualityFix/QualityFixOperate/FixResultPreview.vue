

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
  <div>
    <bkdata-alert extCls="word-break"
      :type="alertType"
      :title="title"
      closable />
    <div v-bkloading="{ isLoading }">
      <p class="correct-filed">
        全局调试结果【当前已配置<span class="field-nums">{{ Object.keys(correctFieldsMap).length }}</span>个字段修正】：
        {{ debugResultField }}
      </p>
      <DataTable
        :emptyText="$t('请进行数据修正，并查看结果数据')"
        :isLoading="isCorrectLoading"
        :totalData="correctResult"
        :calcPageSize.sync="calcPageSize">
        <template slot="content">
          <bkdata-table-column
            v-for="(label, index) in tableColumKeys"
            :key="index"
            :width="moreWidthKeys.includes(label) ? 150 : 100"
            :label="label"
            :prop="label"
            :showOverflowTooltip="true"
            :renderHeader="renderHeader" />
        </template>
      </DataTable>
    </div>
    <bkdata-button
      v-if="isShowDebug"
      :loading="isCorrectLoading"
      :theme="'primary'"
      :title="$t('修正调试')"
      class="mt10"
      @click="submitCorrectSql">
      {{ $t('修正调试') }}
    </bkdata-button>
  </div>
</template>

<script lang="tsx" src="./FixResultPreview.tsx"></script>

<style lang="scss" scoped>
@import '~@/pages/dataManage/cleanChild/scss/cleanRules.scss';
::v-deep .bk-table-header-label {
  width: 100%;
  .table-head-field {
    .icon-data-amend {
      position: absolute;
      width: 0;
      height: 0;
      right: 0;
      top: 0;
      border: 15px solid #3a84ff;
      border-left-color: rgba(0, 0, 0, 0);
      border-bottom-color: rgba(0, 0, 0, 0);
      background-color: rgba(0, 0, 0, 0);
      &::before {
        position: absolute;
        right: -14px;
        top: -14px;
        color: white;
      }
    }
  }
}
.correct-filed {
  color: #666;
  height: 36px;
  line-height: 36px;
  font-size: 14px;
  font-weight: 530;
  .field-nums {
    color: #3a84ff;
  }
}
.header-table {
  border-bottom: none;
}
.no-data {
  margin-top: 10px;
  border: 1px solid #ddd;
  text-align: center;
  font-size: 14px;
  color: #979ba5;
  height: 100px;
  line-height: 100px;
}
::v-deep .bk-alert-title {
  word-break: break-all;
}
</style>

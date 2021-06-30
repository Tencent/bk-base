

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
  <HeaderType :title="$t('信息总览')">
    <template slot="title-right">
      <span
        v-if="processingType === 'clean'"
        v-bk-tooltips="$t('清洗结果表暂不支持进行数据修正')"
        class="mr10 bk-primary bk-button-normal bk-button is-disabled">
        {{ OperateName }}
      </span>
      <bkdata-button
        v-else
        :theme="'primary'"
        :title="OperateName"
        class="mr10"
        :disabled="processingType === 'clean'"
        @click="operateCorrectInfo">
        {{ OperateName }}
      </bkdata-button>
    </template>
    <section v-bkloading="{ isLoading }">
      <DataTable
        :totalData="tableData"
        :calcPageSize.sync="calcPageSize"
        :emptyText="$t('暂无数据修正信息，请点击“创建修正任务”新建')">
        <template slot="content">
          <bkdata-table-column width="120"
            :label="$t('修正字段')">
            <template slot-scope="{ row }">
              {{ row.correct_configs.length ? row.correct_configs[0].field : '' }}
            </template>
          </bkdata-table-column>
          <bkdata-table-column :minWidth="150"
            :label="$t('修正内容')"
            :showOverflowTooltip="true">
            <template slot-scope="{ row }">
              {{
                `${
                  row.correct_configs[0]
                    ? row.correct_configs[0].correct_config_detail.rules[0].condition.condition_value
                    : ''
                }`
              }}
            </template>
          </bkdata-table-column>
          <bkdata-table-column :minWidth="150"
            :label="$t('描述')"
            prop="description" />
          <bkdata-table-column width="90"
            :label="$t('更新人')"
            prop="updated_by" />
          <bkdata-table-column width="150"
            :label="$t('更新时间')"
            prop="updated_at" />
        </template>
      </DataTable>
    </section>
  </HeaderType>
</template>

<script lang="ts" src="./DataFix.index.ts"></script>



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
    <HeaderType :title="$t('审核规则')">
      <div slot="title-right">
        <bkdata-button theme="primary"
          icon="plus"
          :title="$t('新增审核规则')"
          @click="showConfig">
          {{ $t('新增审核规则') }}
        </bkdata-button>
      </div>
      <DataTable :totalData="tableData"
        :calcPageSize.sync="calcPageSize"
        :isLoading="isLoading">
        <template slot="content">
          <bkdata-table-column width="70"
            :label="$t('规则ID')"
            prop="rule_id" />
          <bkdata-table-column minWidth="150"
            :label="$t('规则名称')"
            :showOverflowTooltip="true"
            prop="rule_name" />
          <bkdata-table-column width="80"
            :label="$t('规则模板')"
            :showOverflowTooltip="true"
            prop="rule_template_alias" />
          <bkdata-table-column minWidth="250"
            :label="$t('规则内容')"
            :showOverflowTooltip="true"
            prop="rule_config_alias" />
          <bkdata-table-column width="70"
            :label="$t('状态')"
            :showOverflowTooltip="true">
            <div slot-scope="{ row }"
              :style="{ color: statusMap[row.audit_task_status].color }">
              {{ statusMap[row.audit_task_status].alias }}
            </div>
          </bkdata-table-column>
          <bkdata-table-column width="90"
            :label="$t('关联事件')"
            :showOverflowTooltip="true"
            prop="event_alias" />
          <bkdata-table-column width="100"
            :label="$t('更新人')"
            :showOverflowTooltip="true">
            <template slot-scope="{ row }">
              {{ row.updated_by || row.created_by }}
            </template>
          </bkdata-table-column>
          <bkdata-table-column width="150"
            :label="$t('更新时间')"
            :showOverflowTooltip="true"
            prop="updated_at" />
          <bkdata-table-column fixed="right"
            width="150"
            :label="$t('操作')">
            <template slot-scope="{ row }">
              <operation-group>
                <a href="javascript:void(0);"
                  @click.stop="editAuditRule(row)">
                  {{ $t('编辑') }}
                </a>
                <a href="javascript:void(0);"
                  :class="{ disabled: row.audit_task_status === 'running' }"
                  @click.stop="deleteRule(row)">
                  {{ $t('删除') }}
                </a>
                <a href="javascript:void(0);"
                  @click.stop="actionRule(row)">
                  {{ row.audit_task_status === 'waiting' ? $t('启动') : $t('停止') }}
                </a>
              </operation-group>
            </template>
          </bkdata-table-column>
        </template>
      </DataTable>
      <template v-if="isShowConfig">
        <QualityRuleConfig ref="QualityRuleConfig"
          @close="closeConfig" />
      </template>
      <bkdata-dialog v-model="process.isShow"
        extCls="bkdata-dialog"
        :loading="process.buttonLoading"
        :hasFooter="false"
        :okText="process.buttonText"
        :cancelText="$t('取消')"
        :theme="dialogTheme"
        :title="process.title"
        @confirm="getActionFunc"
        @cancel="cancel">
        <span style="height: 50px; display: inline-block" />
      </bkdata-dialog>
    </HeaderType>
  </div>
</template>

<script lang="ts" src="./AuditRule.ts"></script>

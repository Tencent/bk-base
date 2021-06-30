

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
  <div class="header-table-container">
    <table :class="['header-table bk-table has-table-bordered', tableSize]">
      <thead>
        <tr>
          <th>{{ $t('修正字段') }}</th>
          <th>{{ $t('修正内容') }}</th>
          <th>{{ $t('操作') }}</th>
        </tr>
      </thead>
      <tbody v-show="correct_configs.length">
        <template v-for="(item, index) in correct_configs">
          <tr :key="item.correct_config_item_id">
            <td>
              <!-- 修正字段 -->
              <div class="td-inner-container">
                <bkdata-selector
                  :displayKey="'field_alias'"
                  :searchable="true"
                  :list="correctFieldList"
                  :selected.sync="item.field"
                  :multiSelect="false"
                  :settingKey="'field_name'"
                  :searchKey="'field_alias'"
                  :dropDownWidth="dropDownWidth"
                  @item-selected="changeSqlFeild"
                  @change="correctConfigChange" />
              </div>
            </td>
            <td>
              <div v-for="(child, idx) in item.correct_config_detail.rules"
                :key="idx"
                class="fix-unit-container">
                <div class="fix-unit-left">
                  <div class="fix-unit clearfix">
                    <span class="fix-unit-title fix-unit-label">{{ $t('修正条件') }} </span>
                    <div
                      class="fix-unit-select"
                      :class="{
                        'width-responsive': child.condition.condition_name !== 'custom_sql_condition',
                      }"
                      @click.stop="getCorrectTarget(item, child.condition)">
                      <bkdata-selector
                        :displayKey="'condition_template_alias'"
                        :list="correctConditionList"
                        :multiSelect="false"
                        :selected.sync="child.condition.condition_name"
                        :searchable="true"
                        :settingKey="'condition_template_name'"
                        :searchKey="'condition_template_alias'"
                        :dropDownWidth="dropDownWidth"
                        @change="correctConfigChange"
                        @item-selected="correctConditionChange" />
                    </div>
                    <div v-if="child.condition.condition_name === 'custom_sql_condition'"
                      class="fix-unit-select">
                      <bkdata-input
                        v-model="child.condition.condition_value"
                        :clearable="true"
                        @change="correctInputChange" />
                    </div>
                  </div>
                  <div class="fix-unit clearfix">
                    <span class="fix-unit-title fix-unit-label"> {{ $t('修正算子') }} </span>
                    <div class="fix-unit-select"
                      @click.stop="getCorrectOperatorTarget(child.handler)">
                      <bkdata-selector
                        :displayKey="'handler_template_alias'"
                        :list="correctFillingsList"
                        :selected.sync="child.handler.handler_name"
                        :multiSelect="false"
                        :searchable="true"
                        :settingKey="'handler_template_name'"
                        :searchKey="'handler_template_alias'"
                        :dropDownWidth="dropDownWidth"
                        @change="correctConfigChange"
                        @item-selected="correctOperatorChange" />
                    </div>
                    <div class="fix-unit-select">
                      <bkdata-input
                        v-model="child.handler.handler_value"
                        :clearable="true"
                        @change="correctInputChange" />
                    </div>
                  </div>
                </div>
                <OperateRow
                  :isDelete="item.correct_config_detail.rules.length === 1"
                  @add-row="addCondition(idx + 1, item.correct_config_detail.rules)"
                  @delete-row="deleteParams(idx, item.correct_config_detail.rules)" />
              </div>
            </td>
            <td :ref="'operating' + index"
              class="operating-button">
              <span class="bk-icon-button"
                @click="addRule(correct_configs, index + 1)">
                <i :title="$t('添加')"
                  class="bk-icon icon-plus" />
              </span>
              <span
                v-if="correct_configs.length !== 1"
                class="bk-icon-button"
                @click="deleteParams(index, correct_configs)">
                <i :title="$t('删除')"
                  class="bk-icon icon-delete bk-icon-new" />
              </span>
            </td>
          </tr>
        </template>
      </tbody>
    </table>
  </div>
</template>

<script lang="ts" src="./QualityFixConfig.ts"></script>

<style lang="scss" scoped>
@import '~@/pages/dataManage/cleanChild/scss/cleanRules.scss';
.min-table {
  .td-inner-container {
    width: 90px;
  }
  .fix-unit-select {
    width: 98px;
  }
  .operating-button {
    width: 100px;
  }
  .width-responsive {
    width: 196px !important;
  }
}
.normal-table {
  .td-inner-container {
    width: 120px;
  }
  .fix-unit-select {
    width: 120px;
  }
  .width-responsive {
    width: 240px !important;
  }
}
.fix-unit-container {
  display: flex;
  .fix-unit-left {
    flex: 1;
    display: flex;
    justify-content: space-between;
  }
  .fix-unit {
    display: flex;
    align-items: center;
    justify-content: flex-start;
    margin-right: 10px;
    .fix-unit-title {
      float: left;
      min-width: 75px;
      max-width: 140px;
      background: #fafafa;
      margin-right: -1px;
    }
    .fix-unit-label {
      font-size: 12px;
      min-width: 75px;
      border: 1px solid #c3cdd7;
      padding: 9px 10px;
      line-height: 12px;
    }
    .fix-unit-select {
      display: flex;
      align-items: center;
      line-height: normal;
    }
  }
}
.header-table-container {
  padding: 10px 0;
  .header-table {
    border-bottom: none;
  }
}
</style>



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
    <bkdata-sideslider :width="941"
      :isShow.sync="isShow"
      :quickClose="true"
      @hidden="reSetData">
      <div slot="header">
        {{ title }}
        <span class="slider-header-tips">（{{ $t('指标统计口径，又称指标的计算逻辑') }}）</span>
      </div>
      <div slot="content"
        v-bkloading="{ isLoading: isAllLoading }"
        class="slider-content">
        <bkdata-form :labelWidth="80">
          <bkdata-form-item :label="$t('添加方式')">
            <div class="bk-button-group">
              <bkdata-button
                v-if="!calculationAtomsInfo.results.length || isEdit"
                v-bk-tooltips="{
                  content: $t('当前集市中暂无相关的指标统计口径'),
                  disabled: isEdit,
                }"
                class="is-disabled">
                {{ $t('从集市引用') }}
              </bkdata-button>
              <bkdata-button
                v-else
                :disabled="!calculationAtomsInfo.results.length"
                :class="groupSetting.selected === 'quote' ? 'is-selected' : ''"
                @click="groupSetting.selected = 'quote'">
                {{ $t('从集市引用') }}
              </bkdata-button>
              <bkdata-button
                :class="groupSetting.selected === 'create' ? 'is-selected' : ''"
                @click="groupSetting.selected = 'create'">
                {{ $t('新建') }}
              </bkdata-button>
            </div>
          </bkdata-form-item>
        </bkdata-form>
        <keep-alive>
          <section v-if="groupSetting.selected === 'quote'"
            class="mt20">
            <div class="indicator-count">
              <span class="indicator-label">
                {{ groupSetting.selected === 'quote' ? $t('从集市引用') : $t('新建') }}指标统计口径
              </span>
            </div>
            <DataTable
              :totalData="calculationAtomsInfo.results"
              :calcPageSize.sync="calcPageSize"
              :isLoading="isLoading"
              :emptyText="$t('暂无指标统计口径，请新建')"
              :maxHeight="appHeight - 248"
              @selectAll="selectAll">
              <template slot="content">
                <bkdata-table-column v-if="calculationAtomsInfo.results.length"
                  :renderHeader="renderHeader"
                  width="60">
                  <template slot-scope="{ row }">
                    <bkdata-checkbox
                      name="custom-check"
                      :value.sync="row.isChecked"
                      @change="changeCheckBox($event, row)" />
                  </template>
                </bkdata-table-column>
                <bkdata-table-column :label="$t('名称')"
                  prop="calculationAtomName" />
                <bkdata-table-column :label="$t('聚合逻辑')"
                  prop="calculationFormula" />
                <bkdata-table-column :label="$t('引用数量')"
                  prop="quotedCount" />
                <bkdata-table-column :label="$t('创建人')"
                  prop="createdBy" />
                <bkdata-table-column :label="$t('更新时间')"
                  prop="updatedAt"
                  width="150" />
              </template>
            </DataTable>
            <div class="option-container">
              <bkdata-button
                theme="primary"
                :loading="isSubmitLoading"
                type="submit"
                :title="$t('提交')"
                :disabled="!calculationAtomsInfo.results.length"
                class="mr10"
                @click="quoteCalculationAtom">
                {{ $t('提交') }}
              </bkdata-button>
              <bkdata-button theme="default"
                :title="$t('取消')"
                @click="isShow = false">
                {{ $t('取消') }}
              </bkdata-button>
            </div>
          </section>
          <template v-else>
            <CommonFormCondition
              :aggregationLogicList="aggregationLogicList"
              :calculationAtomDetailData="calculationAtomDetailData"
              :mode="mode"
              :isLoading="isSubmitLoading"
              :isRestrictedEdit="isRestrictedEdit"
              @submit="handleSubmit"
              @cancel="isShow = false" />
          </template>
        </keep-alive>
      </div>
    </bkdata-sideslider>
  </div>
</template>

<script lang="tsx" src="./AddCaliber.tsx"></script>

<style lang="scss" scoped>
::v-deep .bk-sideslider-title {
  text-align: left;
}
.slider-header-tips {
  font-size: 12px;
  font-weight: normal;
  color: #979ba5;
}
.slider-content {
  padding: 20px 30px;
  .bk-button-group {
    width: 320px;
    .bk-button {
      width: 50%;
    }
  }
}
.add-method {
  display: flex;
  align-items: center;
  margin: 20px 0;
  font-size: 14px;
  .add-text {
    width: 80px;
    color: #63656e;
    font-weight: bold;
    text-align: left;
    padding-right: 24px;
  }
}
.option-container {
  text-align: left;
  margin-top: 20px;
}
.indicator-count {
  height: 40px;
  background: #f0f1f5;
  border: 1px solid #dcdee5;
  border-radius: 2px 2px 0px 0px;
  border-bottom: none;
  text-align: left;
  padding-left: 20px;
  font-weight: 700;
  .indicator-label {
    height: 40px;
    line-height: 40px;
    font-size: 14px;
    color: #63656e;
  }
  .indicator-num {
    color: #3a84ff;
  }
}
</style>

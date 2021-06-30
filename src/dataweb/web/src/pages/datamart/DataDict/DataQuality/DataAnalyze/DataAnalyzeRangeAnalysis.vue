

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
    <HeaderType
      :title="$t('值域分析')"
      :tipsContent="$t('当前数据表中所有字段的描述性分析指标')"
      :isShowLeft="false"
      :isPaddding="false">
      <div v-bkloading="{ isLoading }">
        <bkdata-table
          ref="table"
          :extCls="isHighLightFirstRow ? 'high-light-row' : ''"
          :data="tableData"
          :highlightCurrentRow="true"
          :pagination="calcPagination"
          @row-click="rowClick"
          @page-change="handlePageChange"
          @page-limit-change="handlePageLimitChange">
          <bkdata-table-column width="30">
            <template slot-scope="{ row }">
              <span
                v-bk-tooltips="$t(`向下可查看当前字段（${row.name}）的数据分布、基数分析详情`)"
                :class="['icon-prompt', 'mr5', 'bk-text-success', selectedField === row.name ? '' : 'hidden']" />
            </template>
          </bkdata-table-column>
          <bkdata-table-column :minWidth="120"
            :label="$t('字段名称')"
            prop="name"
            :showOverflowTooltip="true" />
          <bkdata-table-column :label="$t('字段类型')"
            :showOverflowTooltip="true"
            prop="field_type" />
          <bkdata-table-column :label="$t('平均值')"
            :showOverflowTooltip="true"
            prop="mean" />
          <bkdata-table-column :label="$t('最大值')"
            :showOverflowTooltip="true"
            prop="max" />
          <bkdata-table-column :label="$t('最小值')"
            :showOverflowTooltip="true"
            prop="min" />
          <bkdata-table-column :label="$t('极差')"
            :showOverflowTooltip="true"
            prop="range" />
          <bkdata-table-column
            :renderHeader="renderFunc($t('分位值'), $t('下四分位'), true)"
            :showOverflowTooltip="true"
            prop="quantile75" />
          <bkdata-table-column
            prop="median"
            :showOverflowTooltip="true"
            :renderHeader="renderFunc($t('分位值'), $t('中位值'))" />
          <bkdata-table-column
            :showOverflowTooltip="true"
            :renderHeader="renderFunc($t('分位值'), $t('上四分位'), true)"
            prop="quantile25" />
          <bkdata-table-column minWidth="100"
            :renderHeader="renderModes"
            :showOverflowTooltip="true">
            <div slot-scope="{ row }">
              {{ row.modes }}
            </div>
          </bkdata-table-column>
          <bkdata-table-column :label="$t('唯一值个数')"
            :showOverflowTooltip="true"
            width="100"
            prop="unique_count" />
          <bkdata-table-column prop="null_count"
            width="100"
            :showOverflowTooltip="true"
            :renderHeader="renderNullNum">
            <div slot-scope="{ row }">
              {{ `${row.null_count}（${row.null_rate.toFixed(2)}%）` }}
            </div>
          </bkdata-table-column>
          <!-- <bkdata-table-column
                    prop="null_rate"
                    width="100"
                    :show-overflow-tooltip="true"
                    :render-header="renderNullProportion"
                ></bkdata-table-column> -->
        </bkdata-table>
      </div>
    </HeaderType>
  </div>
</template>

<script>
import HeaderType from '@/pages/datamart/common/components/HeaderType';
export default {
  components: {
    HeaderType,
  },
  props: {
    fieldData: {
      type: Array,
      default: () => [],
    },
    isLoading: Boolean,
    field: String,
  },
  data() {
    return {
      isHighLightFirstRow: true,
      selectedField: '',
      currentPage: 1,
      calcPageSize: 10,
    };
  },
  computed: {
    calcPagination() {
      return {
        current: Number(this.currentPage),
        count: this.fieldData.length,
        limit: this.calcPageSize,
      };
    },
    tableData() {
      return this.fieldData.slice((this.currentPage - 1) * this.calcPageSize, this.currentPage * this.calcPageSize);
    },
  },
  watch: {
    fieldData: {
      immediate: true,
      handler(val) {
        if (val.length) {
          // 页面强制刷新时，需要默认选中表格第一行
          this.selectedField = val[0].name;
          this.isHighLightFirstRow = true;
          this.changeField(val[0].name);
        }
      },
    },
    field(val) {
      if (!val) return;
      if (this.fieldData.length) {
        const index = this.fieldData.findIndex(item => item.name === val);
        this.currentPage = Math.floor(index / this.calcPageSize) + 1;
      }
      this.selectedField = val;
      this.changeRowStyle(this.tableData.findIndex(item => item.name === val));
    },
  },
  methods: {
    changeRowStyle(num = 0, isReset = false) {
      this.$nextTick(() => {
        const trArr = Array.from(
          this.$refs.table.$el.getElementsByClassName('bk-table-body-wrapper')[0].getElementsByTagName('tr')
        );
        trArr.forEach((child, index) => {
          Array.from(child.getElementsByTagName('td')).forEach(item => {
            item.style.backgroundColor = index === num && !isReset ? '#ecf5ff' : '#fff';
          });
        });
      });
    },
    changeField(val) {
      this.$emit('update:field', val);
    },
    handlePageChange(page) {
      this.currentPage = page;
    },
    handlePageLimitChange(pageSize) {
      this.currentPage = 1;
      this.calcPageSize = pageSize;
    },
    rowClick(data) {
      this.changeRowStyle(0, true);
      this.isHighLightFirstRow = false;
      this.selectedField = data.name;
      this.changeField(data.name);
    },
    renderFunc(topText, bottomText, isHiddenTop) {
      return (h, data) => (
        <div class="custom-header-cell">
          <span class={isHiddenTop ? 'hidden' : ''}>{topText}</span>
          <span class="text-scale block sub-color">{bottomText}</span>
        </div>
      );
    },
    renderNullNum(h, data) {
      return (
        <div class="custom-header-cell">
          {this.$t('空值')}（<span class="text-scale">{this.$t('比率')}</span>）
        </div>
      );
    },
    // renderNullProportion (h, data) {
    //     return (
    //         <div class="custom-header-cell">
    //             {this.$t('空值')}（<span class="text-scale">{this.$t('比例')}</span>）
    //         </div>
    //     )
    // },
    renderModes(h, data) {
      return (
        <div class="custom-header-cell">
          {this.$t('众数')}
          <span
            class="ml5 icon-question-circle bk-text-primary"
            v-bk-tooltips={$t('众数，若有多个众数使用逗号隔开；')}
          ></span>
        </div>
      );
    },
  },
};
</script>

<style lang="scss" scoped>
::v-deep .custom-header-cell {
  line-height: normal;
  .text-scale {
    transform: scale(0.94);
  }
  .block {
    display: block;
  }
  .sub-color {
    color: #63656e;
  }
}
::v-deep .high-light-row {
  table > tbody {
    tr:first-child {
      background-color: #ecf5ff;
    }
  }
}
::v-deep .bk-table-body-wrapper {
  cursor: pointer;
}
</style>

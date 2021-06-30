

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
    <bkdata-table style="margin: 10px 0px"
      :data="calcData"
      :pagination="pagination"
      :size="size"
      @page-change="handlePageChange"
      @page-limit-change="handlePageLimitChange">
      <bkdata-table-column prop="field_name"
        :label="$t('字段英文名')"
        :width="148">
        <div slot-scope="props"
          class="text-overflow"
          :title="props.row.field_name">
          {{ props.row.field_name }}
        </div>
      </bkdata-table-column>
      <bkdata-table-column prop="field_alias"
        :label="$t('字段中文名')"
        :width="148">
        <div slot-scope="props"
          class="text-overflow"
          :title="props.row.field_alias">
          {{ props.row.field_alias }}
        </div>
      </bkdata-table-column>
      <bkdata-table-column :width="80"
        prop="field_type"
        :label="$t('字段类型')" />
      <bkdata-table-column prop="description"
        :minWidth="desWidth"
        :label="$t('描述')">
        <div slot-scope="props"
          class="text-overflow"
          :title="props.row.description">
          {{ props.row.description }}
        </div>
      </bkdata-table-column>
      <bkdata-table-column v-if="standardInfo.standard_content_type === 'indicator'"
        :label="$t('维度_度量')">
        <template slot-scope="props">
          <span v-if="props.row.is_dimension"
            class="dimension">
            {{ $t('维度') }}
          </span>
          <span v-else
            class="dimension dimension-w">
            {{ $t('度量') }}
          </span>
        </template>
      </bkdata-table-column>
      <bkdata-table-column :width="100"
        :label="$t('单位')">
        <span slot-scope="props">{{ props.row.unit ? props.row.unit_alias : props.row.unit }}</span>
      </bkdata-table-column>

      <bkdata-table-column :width="120"
        :renderHeader="renderHeader"
        fixed="right" />
      <bkdata-table-column :label="$t('值约束')">
        <template v-if="props.row.constraint"
          slot-scope="props">
          <div v-for="(item, index) in props.row.constraint.rule"
            :key="index"
            :title="`${item.type_alias}:${item.value_use}`"
            class="padding5 text-overflow">
            {{ item.type_alias }}:{{ item.value_use }}
          </div>
        </template>
      </bkdata-table-column>
    </bkdata-table>
    <CalcConfig v-if="standardInfo.standard_content_type === 'indicator'"
      ref="calcConfig"
      :standardInfo="standardInfo"
      :code="sql" />
  </div>
</template>
<script>
import CalcConfig from './CalcConfig';

export default {
  components: {
    CalcConfig,
  },
  props: {
    tableData: {
      type: Array,
      default: () => [],
    },
    isShowCalcSet: {
      type: Boolean,
      default: false,
    },
    standardInfo: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      size: 'small',
      currentPage: 1,
      // pageCount: 1,
      pageSize: 10,
      sql: '',
      tipsWordObj: {
        detaildata: $t('明细指标'),
        indicator: $t('原子指标'),
      },
    };
  },
  computed: {
    pageCount() {
      return (this.tableData || []).length;
    },
    pagination() {
      return {
        current: Number(this.currentPage),
        count: this.pageCount,
        limit: this.pageSize,
      };
    },
    calcData() {
      // eslint-disable-next-line vue/no-side-effects-in-computed-properties
      // this.pageCount = this.tableData.length;
      return this.tableData.slice((this.currentPage - 1) * this.pageSize, this.currentPage * this.pageSize);
    },
    desWidth() {
      return this.standardInfo.standard_content_type === 'detaildata' ? 200 : null;
    },
  },
  methods: {
    linkToDict() {
      return () => {
        if (!this.standardInfo.linked_data_count) return;
        const { standard_version_id, standard_name, standard_content_name, id } = this.standardInfo;
        this.$router.push({
          name: 'DataSearchResult',
          params: {
            standard_version_id,
            standard_name,
            standard_content_name,
            standard_content_id: id,
          },
        });
      };
    },
    openSlide(index, isShow) {
      const that = this;
      return e => {
        if (!isShow) return;
        window.event ? (window.event.cancelBubble = true) : e.stopPropagation();
        that.$refs.calcConfig.isShow = true;
        this.sql = this.standardInfo.standard_content_sql;
      };
    },
    handlePageChange(page) {
      this.currentPage = page;
    },
    handlePageLimitChange(preLimit) {
      this.currentPage = 1;
      this.pageSize = preLimit;
    },
    renderHeader(h, { column, $index }) {
      let isShow = true;
      if (!this.standardInfo.standard_content_sql && !Object.keys(this.standardInfo.window_period).length) {
        isShow = false;
      }
      const that = this;
      const linkDataInfo = that.standardInfo.linked_data_count
        ? `当前${that.tipsWordObj[that.standardInfo.standard_content_type]}下，
                对应的标准化数据：${that.standardInfo.linked_data_count}个`
        : `当前${that.tipsWordObj[that.standardInfo.standard_content_type]}下，没有对应的标准化数据`;
      return h(
        'div',
        {
          class: ['icon-container', that.standardInfo.linked_data_count ? 'bk-click-style' : ''],
        },
        [
          h('span', {
            class: ['icon-structure', that.isShowCalcSet ? '' : 'border-right-none'],
            directives: [
              {
                name: 'bkTooltips',
                value: linkDataInfo,
              },
            ],
            on: {
              click: that.linkToDict(),
            },
          }),
          h('span', {
            class: ['icon-cog-shape', that.isShowCalcSet ? '' : 'hidden', isShow ? '' : 'forbid'],
            directives: [
              {
                name: 'bkTooltips',
                value: isShow ? that.$t('计算配置') : that.$t('无计算配置'),
              },
            ],
            on: {
              click: that.openSlide($index, isShow),
            },
          }),
        ]
      );
    },
  },
};
</script>
<style lang="scss" scoped>
::v-deep .bk-table {
  td,
  th {
    height: 36px;
  }
  th {
    .cell {
      height: 36px;
      line-height: 36px;
    }
  }
}
.padding5 {
  padding: 5px;
}
::v-deep .icon-container {
  display: flex;
  align-items: center;
  font-size: 18px;
  height: 36px;
  border: 1px solid #dfe0e5;
  border-top: none;
  border-bottom: none;
  span {
    display: inline-block;
    width: 42px;
    height: 36px;
    line-height: 36px;
    outline: none;
    &:first-child {
      border-right: 1px solid #dfe0e5;
    }
  }
  .icon-cog-shape {
    cursor: pointer;
  }
  .border-right-none {
    border-right: none !important;
    width: 90px;
  }
  .hidden {
    display: none !important;
  }
  .forbid {
    cursor: not-allowed;
  }
}
::v-deep .dimension {
  display: inline-block;
  padding: 5px 8px;
  border: 1px solid #3a84ff;
  color: #3a84ff;
}
::v-deep .dimension-w {
  border-color: #ff9c01;
  color: #ff9c01;
}
</style>

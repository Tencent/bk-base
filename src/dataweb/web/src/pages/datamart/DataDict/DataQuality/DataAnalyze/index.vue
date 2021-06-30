

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
  <div class="data-analyze-wrapper"
    :class="{ 'analyze-left': isShowConfig }">
    <!-- <AnalyzeBar
            :count="totalCount"
            :isSampling="analyzeData.profiling_way === 'sampling'"
            @openAnalyzeConfig="openAnalyzeConfig"
        /> -->
    <HeaderType :isPaddding="false">
      <div slot="title"
        class="title-container">
        <div class="header">
          剖析结果
          <span class="explain">
            共计<span class="primary-color m5">{{ totalCount }}条</span>采样数据参与剖析
          </span>
        </div>
        <bkdata-button theme="primary"
          disabled
          :title="$t('自定义剖析')">
          {{ $t('自定义剖析') }}
        </bkdata-button>
      </div>
      <div :class="{ 'content-container': isShowConfig }">
        <!-- 值域分析 -->
        <DataAnalyzeRangeAnalysis :fieldData="rangeTableData"
          :isLoading="isLoading"
          :field.sync="field" />
        <!-- 数据分布 -->
        <DataAnalyzeDataDistribution
          :fieldList="rangeTableData"
          :fieldAlias="fieldAlias"
          :field.sync="field"
          :isLoading="isLoading"
          :distributionData="distributionData" />
        <!-- 基数分析 -->
        <DataAnalyzeCardinalityAnalysis
          :fieldList="rangeTableData"
          :uniqueCount="uniqueCount"
          :field.sync="field"
          :distributionData="distributionData"
          :totalCount="totalCount"
          :isLoading="isLoading" />
      </div>
    </HeaderType>
    <!-- <DataAnalyzeConfig
            ref="analyzeConfig"
            @close="slideClose"
            :ext-cls="isShowConfig ? 'slide-container' : ''"
        /> -->
  </div>
</template>

<script>
import HeaderType from '@/pages/datamart/common/components/HeaderType';
import DataAnalyzeRangeAnalysis from './DataAnalyzeRangeAnalysis.vue';
import DataAnalyzeDataDistribution from './DataAnalyzeDataDistribution.vue';
import DataAnalyzeCardinalityAnalysis from './DataAnalyzeCardinalityAnalysis.vue';
import { dataTypeIds } from '@/pages/datamart/common/config';

export default {
  components: {
    DataAnalyzeRangeAnalysis,
    DataAnalyzeDataDistribution,
    DataAnalyzeCardinalityAnalysis,
    HeaderType,
  },
  data() {
    return {
      isLoading: false,
      analyzeData: {
        fields: {},
        total_count: 0,
        is_sampling: false,
      },
      field: '',
      rangeTableData: [],
      uniqueCount: 0,
      totalCount: 0,
      fieldAlias: {
        mean: this.$t('平均值'),
        var: this.$t('方差'),
        std: this.$t('标准差'),
        kurt: this.$t('峰度'),
        skew: this.$t('偏度'),
      },
      isShowConfig: false,
      timer: null,
    };
  },
  computed: {
    distributionData() {
      return this.analyzeData.fields[this.field] || {};
    },
  },
  mounted() {
    this.getData();
  },
  beforeDestroy() {
    clearTimeout(this.timer);
  },
  methods: {
    pullData(timeout) {
      if (timeout) {
        this.getData(true, timeout);
      } else {
        this.getData();
      }
    },
    cancelRefresh() {
      clearTimeout(this.timer);
    },
    slideClose() {
      this.isShowConfig = false;
    },
    openAnalyzeConfig() {
      this.isShowConfig = true;
      this.$refs.analyzeConfig.open();
    },
    getData(isPullData, timeout) {
      this.isLoading = true;
      this.bkRequest
        .httpRequest('dataDict/getDataQualitySummary', {
          params: {
            measurement: 'profiling',
          },
          query: {
            data_set_id: this.$route.query[dataTypeIds[this.$route.query.dataType]],
          },
        })
        .then(res => {
          if (res.result) {
            this.analyzeData = res.data;
            const keyArr = Object.keys(this.analyzeData.fields);
            if (keyArr.length) {
              const sortObj = {
                double: 0,
                float: 1,
                int: 2,
                long: 3,
                string: 4,
                text: 5,
                timestamp: 6,
              };
              this.rangeTableData = keyArr
                .map((item, idx) => {
                  for (const key in this.analyzeData.fields[item]) {
                    if (Object.keys(this.fieldAlias).includes(key) && this.analyzeData.fields[item][key]) {
                      this.analyzeData.fields[item][key] = this.analyzeData.fields[item][key].toFixed(2);
                    }
                    let value = this.analyzeData.fields[item][key];
                    // 特殊处理
                    const keys = Object.keys(this.analyzeData.fields[item].distribution);
                    if (this.analyzeData.fields[item].distribution && keys.length) {
                      if (keys.includes('__other_dimensions')) {
                        this.analyzeData.fields[item].distribution[this.$t('其他')] = this.analyzeData.fields[
                          item
                        ].distribution.__other_dimensions;
                      }
                      delete this.analyzeData.fields[item].distribution.__other_dimensions;
                    }
                    if (!value && value !== 0) {
                      this.analyzeData.fields[item][key] = '—';
                    }
                  }
                  this.analyzeData.fields[item].name = item;
                  return this.analyzeData.fields[item];
                })
                .sort((a, b) => sortObj[a.field_type] - sortObj[b.field_type]);
              this.field = keyArr[0];
              this.uniqueCount = this.analyzeData.fields[this.field].unique_count;
              this.totalCount = this.analyzeData.total_count;
            }
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isLoading = false;
          if (isPullData) {
            this.timer = setTimeout(() => {
              this.getData(true, timeout);
            }, timeout);
          }
        });
    },
  },
};
</script>

<style lang="scss" scoped>
.title-container {
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: calc(100% - 15px);
  .header {
    display: inline-block;
    font-size: 14px;
    font-weight: 700;
    color: #63656e;
    line-height: 55px;
    .explain {
      font-weight: normal;
      font-size: 12px;
      margin-left: 10px;
      color: #737987;
    }
    .primary-color {
      color: #3a84ff;
    }
  }
}
.analyze-left {
  left: -437px !important;
  top: 0px;
}
.data-analyze-wrapper {
  position: relative;
  left: 0;
  background-color: white;
  // border: 1px solid rgb(221, 221, 221);
  width: calc(100% - 40px);
  margin: 0 auto;
  // margin-bottom: 20px;
  .content-container {
    max-width: 1103px;
  }
  ::v-deep .content {
    display: flex;
    flex-wrap: nowrap;
    justify-content: center;
    .table-container {
      width: 300px;
      .bk-table {
        border-collapse: collapse;
        border: none;
        table-layout: fixed;
        td {
          min-width: 50px;
          padding: 0 15px;
        }
        th {
          padding: 0 15px;
          max-width: 200px;
        }
        .pad10 {
          padding: 0 10px;
        }
      }
    }
    .chart-container {
      width: calc(100% - 300px);
      display: flex;
      flex-wrap: nowrap;
      justify-content: center;
    }
  }
  ::v-deep .bk-form {
    .bk-form-item {
      .bk-label {
        font-size: 12px;
        font-weight: 400;
        color: #313238;
        .icon-question-circle-shape-delete {
          font-size: 14px;
          color: rgb(254, 156, 0);
          margin-left: 4px;
        }
      }
      .bk-form-content {
        min-width: 150px;
        .bk-select-name {
          color: #2dcb56;
        }
      }
    }
  }
}
</style>

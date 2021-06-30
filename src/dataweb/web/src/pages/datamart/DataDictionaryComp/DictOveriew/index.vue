

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
  <div class="overiew-container">
    <OveriewItem :title="$t('应用最热门的数据')"
      class="overiew-item">
      <div v-bkloading="{ isLoading: loading.heatLoading }">
        <bkdata-table class="overiew-table"
          :data="hotData">
          <bkdata-table-column :minWidth="70"
            :label="$t('数据名称')"
            prop="data_set_id">
            <div slot-scope="item"
              class="text-overflow click-style"
              :class="{ 'max-length': explorer === 'Safari' }"
              :title="getDataName(item.row)"
              @click="linkToDetail(item.row)">
              {{ getDataName(item.row) }}
            </div>
          </bkdata-table-column>
          <bkdata-table-column width="79"
            :label="$t('数据类型')">
            <div slot-scope="item"
              class="text-overflow"
              :title="dataTypeName[item.row.data_set_type]">
              {{ dataTypeName[item.row.data_set_type] }}
            </div>
          </bkdata-table-column>
          <bkdata-table-column width="94"
            :label="$t('热度评分')"
            prop="heat_metric.heat_score" />
        </bkdata-table>
      </div>
    </OveriewItem>
    <OveriewItem :title="$t('应用最广泛的数据')"
      class="overiew-item">
      <div v-bkloading="{ isLoading: loading.rangeLoading }">
        <bkdata-table class="overiew-table"
          :data="rangeData">
          <bkdata-table-column :minWidth="70"
            :label="$t('数据名称')"
            prop="data_set_id">
            <div slot-scope="item"
              class="text-overflow click-style"
              :class="{ 'max-length': explorer === 'Safari' }"
              :title="getDataName(item.row)"
              @click="linkToDetail(item.row)">
              {{ getDataName(item.row) }}
            </div>
          </bkdata-table-column>
          <bkdata-table-column width="79"
            :label="$t('数据类型')">
            <div slot-scope="item"
              class="text-overflow"
              :title="dataTypeName[item.row.data_set_type]">
              {{ dataTypeName[item.row.data_set_type] }}
            </div>
          </bkdata-table-column>
          <bkdata-table-column width="94"
            :label="$t('广度评分')"
            prop="range_metric.normalized_range_score" />
        </bkdata-table>
      </div>
    </OveriewItem>
    <OveriewItem :title="$t('重要度最高的数据')"
      class="overiew-item">
      <div v-bkloading="{ isLoading: loading.importanceLoading }">
        <bkdata-table class="overiew-table"
          :data="importanceData">
          <bkdata-table-column :minWidth="70"
            :label="$t('数据名称')"
            prop="data_set_id">
            <div slot-scope="item"
              class="text-overflow click-style"
              :class="{ 'max-length': explorer === 'Safari' }"
              :title="getDataName(item.row)"
              @click="linkToDetail(item.row)">
              {{ getDataName(item.row) }}
            </div>
          </bkdata-table-column>
          <bkdata-table-column width="79"
            :label="$t('数据类型')">
            <div slot-scope="item"
              class="text-overflow"
              :title="dataTypeName[item.row.data_set_type]">
              {{ dataTypeName[item.row.data_set_type] }}
            </div>
          </bkdata-table-column>
          <bkdata-table-column width="94"
            :label="$t('重要度评分')"
            prop="importance_metric.importance_score" />
        </bkdata-table>
      </div>
    </OveriewItem>
    <OveriewItem :title="$t('存储成本最高的数据')"
      class="overiew-item">
      <div v-bkloading="{ isLoading: loading.storageLoading }">
        <bkdata-table class="overiew-table"
          :data="storageData">
          <bkdata-table-column :minWidth="70"
            :label="$t('数据名称')"
            prop="data_set_id">
            <div slot-scope="item"
              class="text-overflow click-style"
              :class="{ 'max-length': explorer === 'Safari' }"
              :title="getDataName(item.row)"
              @click="linkToDetail(item.row)">
              {{ getDataName(item.row) }}
            </div>
          </bkdata-table-column>
          <bkdata-table-column width="79"
            :label="$t('数据类型')">
            <div slot-scope="item"
              class="text-overflow"
              :title="dataTypeName[item.row.data_set_type]">
              {{ dataTypeName[item.row.data_set_type] }}
            </div>
          </bkdata-table-column>
          <bkdata-table-column width="94"
            className="text-right"
            :label="$t('存储成本')"
            :showOverflowTooltip="true"
            prop="cost_metric.format_capacity">
            <div slot-scope="{ row }"
              class="pr23">
              {{ changeStorageValue(row) }}
            </div>
          </bkdata-table-column>
        </bkdata-table>
      </div>
    </OveriewItem>
    <OveriewItem :title="$t('价值成本收益比最高的数据')"
      class="overiew-item">
      <div v-bkloading="{ isLoading: loading.assetValueCostLoading }">
        <bkdata-table class="overiew-table"
          :data="assetValueCostData">
          <bkdata-table-column :minWidth="70"
            :label="$t('数据名称')"
            prop="data_set_id">
            <div slot-scope="item"
              class="text-overflow click-style"
              :class="{ 'max-length': explorer === 'Safari' }"
              :title="getDataName(item.row)"
              @click="linkToDetail(item.row)">
              {{ getDataName(item.row) }}
            </div>
          </bkdata-table-column>
          <bkdata-table-column width="79"
            :label="$t('数据类型')">
            <div slot-scope="item"
              class="text-overflow"
              :title="dataTypeName[item.row.data_set_type]">
              {{ dataTypeName[item.row.data_set_type] }}
            </div>
          </bkdata-table-column>
          <bkdata-table-column width="94"
            :label="$t('收益比')"
            prop="assetvalue_to_cost" />
        </bkdata-table>
      </div>
    </OveriewItem>
  </div>
</template>

<script>
import OveriewItem from './OveriewItem';
import { getExplorer } from '@/pages/datamart/common/utils.js';
export default {
  components: {
    OveriewItem,
  },
  props: {
    isInitRequest: Boolean,
  },
  data() {
    return {
      isResolvedRequest: false,
      hotData: [],
      rangeData: [],
      importanceData: [],
      storageData: [],
      assetValueCostData: [],
      loading: {
        rangeLoading: false,
        heatLoading: false,
        importanceLoading: false,
        storageLoading: false,
        assetValueCostLoading: false,
      },
      dataTypeName: {
        result_table: this.$t('结果表'),
        raw_data: this.$t('数据源'),
      },
      dataType: {
        raw_data: 'raw_data_alias',
        result_table: 'result_table_name_alias',
      },
      explorer: '', // 浏览器类型
    };
  },
  watch: {
    isInitRequest: {
      immediate: true,
      handler(val) {
        if (val) {
          this.getData();
        }
      },
    },
  },
  methods: {
    changeStorageValue(row) {
      const target = row.cost_metric.format_capacity;
      const num = Math.round(target.slice(0, target.length - 2));
      const unit = target.slice(target.length - 2);
      return `${num}${unit}`;
    },
    getData() {
      if (this.isResolvedRequest) return;
      this.getHotData();
      this.getRangeData();
      this.getImportanceData();
      this.getStorageData();
      this.getAssetValueCostData();
      this.explorer = getExplorer();
      this.isResolvedRequest = true;
    },
    getAppName(data) {
      return data.app_code_alias || data.app_code;
    },
    getDataName(data) {
      return `${data.data_set_id}(${data[this.dataType[data.data_set_type]]})`;
    },
    linkToDetail(detail, tableType) {
      if (tableType) return;

      let id = null;
      if (detail.data_set_type === 'raw_data') {
        id = 'data_id';
      } else if (detail.platform === 'tdw') {
        id = 'tdw_table_id';
      } else {
        id = 'result_table_id';
      }

      const idObj = {
        data_id: detail.data_set_id,
        tdw_table_id: detail.table_id,
        result_table_id: detail.result_table_id,
      };
      this.$router.push({
        name: 'DataDetail',
        query: {
          dataType: detail.data_set_type || 'result_table',
          [id]: idObj[id],
          bk_biz_id: detail.bk_biz_id,
        },
      });
    },
    getHotData() {
      this.loading.heatLoading = true;
      this.bkRequest
        .httpRequest('dataDict/getResultInfo', {
          params: {
            bk_biz_id: null,
            project_id: null,
            tag_ids: [],
            keyword: '',
            tag_code: 'virtual_data_mart',
            me_type: 'tag',
            has_standard: 1,
            cal_type: ['standard'],
            data_set_type: 'all',
            page: 1,
            page_size: 10,
            platform: 'bk_data',
            order_heat: 'desc',
            is_cache_used: 1,
          },
        })
        .then(res => {
          if (res.result) {
            this.hotData = res.data.results;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.loading.heatLoading = false;
        });
    },
    getRangeData() {
      this.loading.rangeLoading = true;
      this.bkRequest
        .httpRequest('dataDict/getResultInfo', {
          params: {
            bk_biz_id: null,
            project_id: null,
            tag_ids: [],
            keyword: '',
            tag_code: 'virtual_data_mart',
            me_type: 'tag',
            has_standard: 1,
            cal_type: ['standard'],
            data_set_type: 'all',
            page: 1,
            page_size: 10,
            platform: 'bk_data',
            order_range: 'desc',
            is_cache_used: 1,
          },
        })
        .then(res => {
          if (res.result) {
            this.rangeData = res.data.results;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.loading.rangeLoading = false;
        });
    },
    getImportanceData() {
      this.loading.importanceLoading = true;
      this.bkRequest
        .httpRequest('dataDict/getResultInfo', {
          params: {
            bk_biz_id: null,
            project_id: null,
            tag_ids: [],
            keyword: '',
            tag_code: 'virtual_data_mart',
            me_type: 'tag',
            has_standard: 1,
            cal_type: ['standard'],
            data_set_type: 'all',
            page: 1,
            page_size: 10,
            platform: 'bk_data',
            order_importance: 'desc',
            is_cache_used: 1,
          },
        })
        .then(res => {
          if (res.result) {
            this.importanceData = res.data.results;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.loading.importanceLoading = false;
        });
    },
    getStorageData() {
      this.loading.storageLoading = true;
      this.bkRequest
        .httpRequest('dataDict/getResultInfo', {
          params: {
            bk_biz_id: null,
            project_id: null,
            tag_ids: [],
            keyword: '',
            tag_code: 'virtual_data_mart',
            me_type: 'tag',
            has_standard: 1,
            cal_type: ['standard'],
            data_set_type: 'all',
            page: 1,
            page_size: 10,
            platform: 'bk_data',
            order_storage_capacity: 'desc',
            is_cache_used: 1,
          },
        })
        .then(res => {
          if (res.result) {
            this.storageData = res.data.results;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.loading.storageLoading = false;
        });
    },
    getAssetValueCostData() {
      this.loading.assetValueCostLoading = true;
      this.bkRequest
        .httpRequest('dataDict/getResultInfo', {
          params: {
            bk_biz_id: null,
            project_id: null,
            tag_ids: [],
            keyword: '',
            tag_code: 'virtual_data_mart',
            me_type: 'tag',
            has_standard: 1,
            cal_type: ['standard'],
            data_set_type: 'all',
            page: 1,
            page_size: 10,
            platform: 'bk_data',
            order_assetvalue_to_cost: 'desc',
            is_cache_used: 1,
          },
        })
        .then(res => {
          if (res.result) {
            this.assetValueCostData = res.data.results;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.loading.assetValueCostLoading = false;
        });
    },
  },
};
</script>

<style lang="scss" scoped>
::v-deep .text-right {
  .cell {
    text-align: right;
  }
  .pr23 {
    padding-right: 13px;
  }
}
::v-deep .bk-table {
  background-color: white;
  td {
    height: 36px;
  }
  th {
    .cell {
      height: 36px;
      line-height: 36px;
    }
  }
}
.click-style {
  cursor: pointer;
  color: #3a84ff;
}
.overiew-container {
  height: 100%;
  min-width: 1250px;
  padding: 20px 10px;
  border: 1px solid #ddd;
  display: grid;
  justify-content: space-around;
  grid-gap: 15px;
  background-color: #f5f5f5;
  .overiew-table {
    margin-top: 10px;
    // min-width: 400px;
    min-height: 404px;
  }
}
.max-length-last {
  max-width: 158px;
}
.max-length {
  max-width: 187px;
}
@media screen and (min-width: 1527px) {
  .overiew-container {
    grid-template-columns: repeat(5, 18%);
  }
}
@media screen and (max-width: 1527px) {
  .overiew-container {
    grid-template-columns: repeat(auto-fill, 250px);
  }
}
</style>

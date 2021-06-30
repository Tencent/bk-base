

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
  <div v-show="isShow"
    class="ip-list-slider container-with-shadow">
    <div class="ip-header">
      <i class="bk-icon icon-close fr"
        @click="$emit('update:isShow', false)" />
    </div>
    <div v-bkloading="{ isLoading: isLoading }"
      class="ip-list mt30">
      <bkdata-table
        :data="tableIpList"
        :emptyText="$t('暂无数据')"
        :pagination="pagination"
        @page-change="handlePageChange"
        @page-limit-change="handlePageLimitChange"
        @sort-change="handleSortChange">
        <bkdata-table-column prop="ip"
          :label="$t('IP地址')" />
        <bkdata-table-column :label="$t('运行状态')"
          sortable>
          <div slot-scope="item"
            class="bk-table-inlineblock">
            <span
              v-bk-tooltips="`${$t('状态最后更新时间')}：${item.row.updated_at}`"
              :class="`status-icon ${item.row.deploy_status}`">
              {{ item.row.deploy_status_display }}
            </span>
          </div>
        </bkdata-table-column>
        <bkdata-table-column label="操作">
          <div slot-scope="item"
            class="bk-table-inlineblock operation-btns center">
            <a
              class="bk-icon icon-history-record mr10"
              :title="$t('执行历史')"
              @click="getExcuteHistoryByIp(item.row)" />
            <a class="bk-icon icon-queue_storage mr10"
              :title="$t('运行日志')"
              @click="getRunningLogByIp(item.row)" />
            <a
              :class="
                `bk-icon icon-restart f13 mr10 ${retryLoading && restartIPLib[item.row.ip]
                  ? 'refresh-loading' : ''}`
              "
              :title="$t('重试')"
              @click="restartIP(item.row, 'retryLoading')" />
          </div>
        </bkdata-table-column>
      </bkdata-table>
    </div>

    <excute-history :isShow.sync="showHistory"
      :baseData="baseData" />
    <IPRunningLog :isShow.sync="showRunningLog"
      :baseData="baseData" />
  </div>
</template>

<script>
import excuteHistory from './excuteHistory';
import IPRunningLog from './IPRunningLog';
export default {
  components: {
    excuteHistory,
    IPRunningLog,
  },
  props: {
    dataScenario: {
      type: String,
      default: '',
    },
    ipList: {
      type: Array,
      default: () => [],
    },
    isShow: {
      type: Boolean,
    },
    isLoading: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      curPage: 1,
      pageCount: 4,
      showHistory: false,
      showRunningLog: false,
      baseData: {
        deploy_plan_id: '',
        bk_cloud_id: '',
        ip: '',
      },
      restoreLoading: false,
      retryLoading: false,
      restartIPLib: {},
      tableDataTotal: 1,
      sortList: [],
      isSort: false,
    };
  },
  computed: {
    tableIpList() {
      const list = this.isSort ? this.sortList : this.ipList;
      return list.slice((this.curPage - 1) * this.pageCount, this.curPage * this.pageCount);
    },
    pagination() {
      return {
        count: this.tableDataTotal,
        limit: this.pageCount,
        current: this.curPage || 1,
        limitList: [5],
        showLimit: false,
      };
    },
  },
  watch: {
    ipList: {
      immediate: true,
      deep: true,
      handler(list) {
        this.tableDataTotal = list.length;
        this.sortList = JSON.parse(JSON.stringify(list));
      },
    },
  },
  methods: {
    handlePageChange(page) {
      this.curPage = page;
    },
    handlePageLimitChange(pageSize) {
      this.curPage = 1;
      this.pageCount = pageSize;
    },
    handleSortChange({ column, prop, order }) {
      if (!column) {
        this.isSort = false;
        return;
      }
      const label = this.$t('运行状态');
      const isSort = label === column.label;
      this.isSort = isSort;
      if (isSort) {
        this.sortList.sort((itemA, itemB) => {
          const fieldA = itemA.deploy_status.toLowerCase();
          const fieldB = itemB.deploy_status.toLowerCase();
          return order === 'ascending' ? fieldB.localeCompare(fieldA) : fieldA.localeCompare(fieldB);
        });
      }
    },
    getExcuteHistoryByIp(item) {
      this.baseData = {
        deploy_plan_id: this.$route.params.did,
        bk_cloud_id: item.bk_cloud_id,
        ip: item.ip,
      };

      this.showHistory = true;
    },
    getRunningLogByIp(item) {
      this.baseData = {
        deploy_plan_id: this.$route.params.did,
        bk_cloud_id: item.bk_cloud_id,
        ip: item.ip,
      };

      this.showRunningLog = true;
    },
    /**
     * ip采集器重试
     */
    restartIP(item, retryLoading) {
      if (retryLoading && this[retryLoading]) {
        return;
      }
      this.restartIPLib[item.ip] = 1;
      retryLoading && (this[retryLoading] = true);
      this.bkRequest
        .httpRequest('dataAccess/startCollector', {
          params: {
            raw_data_id: this.$route.params.did,
            bk_biz_id: this.$store.state.common.bizId,
            config_mode: 'incr',
            data_scenario: this.dataScenario || item.scope_type,
            scope: [
              {
                deploy_plan_id: item.deploy_plan_id,
                hosts: [
                  {
                    bk_cloud_id: item.bk_cloud_id,
                    ip: item.ip,
                  },
                ],
              },
            ],
          },
        })
        .then(res => {
          if (!res.result) {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          retryLoading && (this[retryLoading] = false);
          delete this.restartIPLib[item.ip];
        });
    },
  },
};
</script>

<style lang="scss" scoped>
$normalColor: #30d878;
$stoppedColor: #737987;
$abnormalColor: #ff5656;
$removedColor: #c3cdd7;
$borderColor: #e6e6e6;
$bgColor: #f5f5f5;
$primaryColor: #3a84ff;

.status-icon {
  &.success {
    color: $normalColor;
  }
  &.failure {
    color: $abnormalColor;
  }
}

.bk-icon.disabled {
  cursor: not-allowed;
  opacity: 0.5;
}

.sort {
  cursor: pointer;
}

.ip-list-slider {
  position: absolute;
  left: 0;
  top: 0;
  width: 100%;
  height: 100%;
  background: #fff;

  .ip-header {
    line-height: 36px;
    border-bottom: 1px solid $borderColor;
    padding: 0 10px 0 20px;
  }

  .icon-close {
    margin-top: 6px;
    cursor: pointer;
    font-size: 20px;
    &:hover {
      color: $abnormalColor;
    }
  }

  .center {
    text-align: center;
  }

  .operation-btns {
    a {
      display: inline-block;
      color: $primaryColor;
      font-size: 15px;
      vertical-align: top;
      margin-top: 2px;
      cursor: pointer;

      &:hover {
        font-weight: bold;
      }
    }
  }
}
</style>

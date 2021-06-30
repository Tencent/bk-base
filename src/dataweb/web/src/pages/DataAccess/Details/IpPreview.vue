

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
  <div class="ip-preview-container">
    <div class="ip-info">
      <div class="header">
        <span v-if="details.host_scope"
          class="num">
          {{ details.host_scope.length }}
        </span>{{ $t('个IP') }}

        <span class="search-panel">
          <div class="search-input">
            <bkdata-input v-model="searchKey" />
            <i class="bk-icon icon-search" />
          </div>
        </span>

        <StatusIcons class="status-icons fr"
          :status="statusCount"
          @refresh="refreshModuleIPStatus" />
      </div>

      <bkdata-table
        v-bkloading="{ isLoading }"
        :data="ipListView"
        :outerBorder="false"
        :emptyText="$t('暂无数据')"
        :pagination="pagination"
        @page-change="handlePageChange"
        @page-limit-change="handlePageLimitChange">
        <bkdata-table-column :label="$t('云区域')"
          prop="cloudArea" />
        <bkdata-table-column :label="$t('IP地址')"
          prop="ip" />
        <bkdata-table-column :label="$t('运行状态')"
          prop="status_display">
          <div slot-scope="item"
            class="bk-table-inlineblock">
            <span :class="item.row.deploy_status">{{ item.row.deploy_status_display }}</span>
          </div>
        </bkdata-table-column>
        <bkdata-table-column :label="$t('操作')">
          <div slot-scope="item"
            class="bk-table-inlineblock operation-btns center">
            <a
              href="javascript:;"
              class="bk-icon icon-history-record mr5"
              :title="$t('执行历史')"
              @click="getExcuteHistoryByIp(item.row)" />
            <a
              href="javascript:;"
              class="bk-icon icon-queue_storage mr5"
              :title="$t('运行日志')"
              @click="getRunningLogByIp(item.row)" />
            <a
              href="javascript:;"
              :class="`bk-icon icon-restart f13 ${retryLoading && restartIPLib[item.row.ip] ? 'refresh-loading' : ''}`"
              :title="$t('重试')"
              @click="restartIP(item.row)" />
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
import StatusIcons from './StatusIcons';
import excuteHistory from './excuteHistory';
import IPRunningLog from './IPRunningLog';

export default {
  components: {
    StatusIcons,
    excuteHistory,
    IPRunningLog,
  },
  props: {
    deployPlanId: {
      type: Number,
    },
    details: {
      type: Object,
      default: () => ({}),
    },
    dataScenario: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      searchKey: '',
      curPage: 1,
      sortBy: '',
      ipList: [],
      showHistory: false,
      showRunningLog: false,
      baseData: {
        deploy_plan_id: '',
        bk_cloud_id: '',
        ip: '',
      },
      statusCount: {},
      retryLoading: false,
      restartIPLib: {},
      cloudAreaList: [],
      pageCount: 5, // 每一页数据量设置
      tableDataTotal: 1,
      isLoading: false,
    };
  },
  computed: {
    ipListView() {
      let sourceList = JSON.parse(JSON.stringify(this.sourceList));
      // eslint-disable-next-line vue/no-side-effects-in-computed-properties
      this.tableDataTotal = sourceList.length;
      sourceList.map(item => {
        item.bk_cloud_name = this.cloudAreaList[item.bk_cloud_id] || '';
        item.deploy_status = this.IPLib[item.ip] && this.IPLib[item.ip].deploy_status;
        item.deploy_status_display = this.IPLib[item.ip] && this.IPLib[item.ip].deploy_status_display;
        return item;
      });
      sourceList.forEach(i => {
        i.cloudArea = i.bk_cloud_name || i.bk_cloud_id;
      });
      // eslint-disable-next-line vue/no-side-effects-in-computed-properties
      this.isLoading = false;
      return sourceList.slice(this.pageCount * (this.curPage - 1), this.pageCount * this.curPage);
    },
    sourceList() {
      let sourceList = JSON.parse(JSON.stringify(this.ipList));
      // 若存在排序设置，则先将ipList进行排序
      if (this.sortBy) {
        sourceList.sort((a, b) => a[this.sortBy] < b[this.sortBy]);
      }

      if (this.searchKey.trim()) {
        sourceList = sourceList.filter(item => item.ip.includes(this.searchKey));
      }

      return sourceList;
    },
    IPLib() {
      const ipLib = {};
      this.ipList.forEach(item => {
        if (item.deploy_plan_id === this.details.deploy_plan_id) {
          ipLib[item.ip] = item;
        }
      });

      return ipLib;
    },
    rawDataId() {
      return this.$route.params.did;
    },
    pagination() {
      return {
        count: this.tableDataTotal,
        limit: this.pageCount,
        current: this.curPage || 1,
        limitList: [5],
        showLimit: false,
        type: 'compact',
      };
    },
  },
  watch: {
    deployPlanId: {
      immediate: true,
      handler(id) {
        if (id) {
          // 切换接入对象时需要同步更新IP列表以及刷新接入状态
          this.refreshModuleIPStatus();
        }
      },
    },
  },
  created() {
    this.getAreaLists();
  },
  methods: {
    handlePageChange(page) {
      this.curPage = page;
    },
    handlePageLimitChange(pageSize) {
      this.curPage = 1;
      this.pageCount = pageSize;
    },
    // 获取云区域列表
    getAreaLists() {
      this.$store.dispatch('api/getAreaLists').then(res => {
        if (res.result) {
          this.cloudAreaList = res.data.reduce((pre, next) => {
            return Object.assign(pre, { [next.bk_cloud_id]: next.bk_cloud_name });
          }, {});
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      });
    },
    previewIpList() {
      this.showIpList = true;
    },
    // 表格排序
    sort(key) {
      this.sortBy = this.sortBy === key ? '' : key;
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
     * 获取当前模块的IP列表
     */
    getIpListByActiveModule() {
      this.isLoading = true;
      this.bkRequest
        .httpRequest('dataAccess/queryAccessStatus', {
          params: {
            raw_data_id: this.$route.params.did,
          },
          query: {
            raw_data_id: this.$route.params.did,
            deploy_plan_id: this.deployPlanId,
            bk_obj_id: 'host',
          },
        })
        .then(res => {
          if (res.result) {
            this.ipList = res.data.results || res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
    },
    /**
     * 刷新当前ip采集状态信息
     */
    refreshModuleIPStatus(closeLoadingFn) {
      this.bkRequest
        .httpRequest('dataAccess/getIpStatusInfo', {
          params: {
            raw_data_id: this.rawDataId,
            deploy_plan_id: this.deployPlanId,
          },
          query: {
            deploy_plan_id: this.deployPlanId,
          },
        })
        .then(res => {
          if (res.result) {
            this.statusCount = res.data.summary;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          closeLoadingFn && closeLoadingFn();
        });

      this.getIpListByActiveModule();
    },
    /**
     * ip采集器重试
     */
    restartIP(item) {
      if (this.retryLoading) {
        return;
      }
      this.retryLoading = true;
      this.restartIPLib[item.ip] = 1;
      this.bkRequest
        .httpRequest('dataAccess/startCollector', {
          params: {
            raw_data_id: this.$route.params.did,
            bk_biz_id: this.$store.state.common.bizId,
            config_mode: 'incr',
            data_scenario: this.dataScenario,
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
          this.retryLoading = false;
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

.success {
  color: $normalColor;
}
.stopped {
  color: $stoppedColor;
}
.failure {
  color: $abnormalColor;
}
.removed {
  color: $removedColor;
}

.center {
  text-align: center;
}

.ip-preview-container {
  height: 100%;
  border: 1px solid $borderColor;
}

.ip-info {
  .header {
    line-height: 36px;
    background: $bgColor;
    padding: 0 10px;

    .num {
      font-weight: bold;
      color: $primaryColor;
    }

    .timer {
      font-style: normal;
      color: $primaryColor;
    }

    a {
      color: $primaryColor;

      &:hover {
        text-decoration: underline;
      }
    }
  }

  .search-panel {
    display: inline-block;
    vertical-align: middle;
    line-height: 1;

    .search-input {
      width: 160px;
      position: relative;
      top: -1px;
      margin-left: 10px;
    }

    .icon-search {
      position: absolute;
      right: 5px;
      top: 6px;
    }

    input {
      height: 26px;
      padding: 0 20px 0 10px;
    }
  }

  .status-icons {
    display: inline-block;
    background: none;
    padding: 0 5px 0 0;
  }

  .ip-paging {
    margin: 10px;
  }

  .operation-btns {
    a {
      display: inline-block;
      color: $primaryColor;
      font-size: 16px;
      vertical-align: top;
      margin-top: 2px;
      cursor: pointer;

      &:hover {
        font-weight: bold;
      }
    }
  }
}

.ip-preview-container {
  position: relative;
}

.sort {
  cursor: pointer;
}

::v-deep .bk-table-body td {
  height: 38px;
}

::v-deep .bk-table-pagination-wrapper {
  padding: 10px;
}
</style>

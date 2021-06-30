

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
  <div class="module-preview-container">
    <div class="module-info">
      <div class="header">
        <span class="num">{{ moduleList.length }}</span>{{ $t('个模块') }}
        <StatusIcons class="status-icons fr"
          :status="statusSummary"
          @refresh="refreshModuleStatus" />
      </div>
      <div v-show="!showIpList"
        class="module-status-wrap">
        <bkdata-table
          :data="moduleListView"
          :outerBorder="false"
          :emptyText="$t('暂无数据')"
          :pagination="pagination"
          @page-change="handlePageChange"
          @page-limit-change="handlePageLimitChange">
          <bkdata-table-column :label="$t('模块名')">
            <div slot-scope="props"
              :title="props.row.moduleName">
              {{ props.row.moduleName }}
            </div>
          </bkdata-table-column>
          <bkdata-table-column :label="$t('运行状态')"
            prop="deploy_status_display" />
          <bkdata-table-column :label="$t('接入IP数')"
            prop="summary.total" />
          <bkdata-table-column :label="$t('操作')">
            <div slot-scope="item"
              class="bk-table-inlineblock operation-btns center">
              <a href="javascript:;"
                class="mr10"
                @click.stop="previewIpList(item.row)">
                {{ $t('查看IP列表') }}
              </a>
              <a href="javascript:void(0)"
                @click="refreshModuleItemStatus(item.row)">
                {{ $t('刷新') }}
              </a>
            </div>
          </bkdata-table-column>
        </bkdata-table>
      </div>
    </div>
    <!-- ip info start -->
    <IpListFromModule
      :ipList="activePreviewIPList"
      :isShow.sync="showIpList"
      :isLoading="isIpListLoading"
      :data-scenario="dataScenario" />
    <!-- ip info end -->
  </div>
</template>

<script>
import StatusIcons from './StatusIcons';
import IpListFromModule from './IpListFromModule';
import { mapState } from 'vuex';

export default {
  components: {
    StatusIcons,
    IpListFromModule,
  },
  props: {
    bizid: {
      type: [Number, String],
      default: 0,
    },
    deployPlanId: {
      type: Number,
    },
    details: {
      type: Object,
      default: () => ({}),
    },
    ipList: {
      type: Array,
      default: () => [],
    },
    dataScenario: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      curPage: 1,
      showIpList: false,
      activeModuleInfo: {},
      activePreviewIPList: [],
      activeRefreshModule: '',
      statusSummary: {},
      isIpListLoading: false,
      pageCount: 5, // 每一页数据量设置
      tableDataTotal: 1,
    };
  },
  computed: {
    pagination() {
      return {
        count: this.tableDataTotal,
        limit: this.pageCount,
        current: this.curPage || 1,
        limitList: [5],
        showLimit: false,
      };
    },
    ...mapState({
      accessSummaryInfo: state => state.common.accessSummaryInfo || {},
    }),
    statusSummaryInfo() {
      const matchedItem =        this.accessSummaryInfo.deploy_plans
        && this.accessSummaryInfo.deploy_plans.find(item => item.deploy_plan_id === this.deployPlanId);

      return (matchedItem && matchedItem.summary) || {};
    },
    moduleListView() {
      this.moduleList.forEach(i => {
        i.moduleName = (i.bk_inst_name_list || []).join('->');
      });
      // eslint-disable-next-line vue/no-side-effects-in-computed-properties
      this.tableDataTotal = this.moduleList.length;
      return this.moduleList.slice(this.pageCount * (this.curPage - 1), this.pageCount * this.curPage);
    },
    moduleList() {
      const matchedItem =        this.accessSummaryInfo.deploy_plans
        && this.accessSummaryInfo.deploy_plans.find(item => item.deploy_plan_id === this.deployPlanId && item.module);

      return (matchedItem && matchedItem.module) || [];
    },
    allIpGroupedByModule() {
      const ipLib = {};
      this.ipList.forEach(item => {
        const moduleId = item.scope_object.bk_inst_id;
        ipLib[moduleId] = ipLib[moduleId] || [];
        ipLib[moduleId].push(item);
      });

      return ipLib;
    },
    // statusSummary () {
    //     // 第三种状态待确认后调整
    //     return {
    //         success: this.statusSummaryInfo.success || 0,
    //         error: this.statusSummaryInfo.failure || 0,
    //         warning: this.statusSummaryInfo.total - this.statusSummaryInfo.success
    //     }
    // },
    rawDataId() {
      return this.$route.params.did;
    },
  },
  watch: {
    deployPlanId: {
      immediate: true,
      handler() {
        this.refreshModuleStatus();
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
    linkToCMDB() {
      this.$linkToCC();
    },
    previewIpList(moduleInfo) {
      this.showIpList = true;
      this.activeModuleInfo = moduleInfo;

      this.getIpListByActiveModule();
    },
    moduleStatus(moduleItem) {
      const summaryInfo = moduleItem.summary;
      const statusInfo = {
        class: '',
        name: '',
      };
      if (summaryInfo.failed) {
        return {
          class: 'failed',
          name: '失败',
        };
      } else if (summaryInfo.success === summaryInfo.total) {
        return {
          class: 'success',
          name: '成功',
        };
      }

      return {
        class: 'running',
        name: '接入中',
      };
    },
    getIpListByActiveModule() {
      this.isIpListLoading = true;
      this.bkRequest
        .httpRequest('dataAccess/queryAccessStatus', {
          params: {
            raw_data_id: this.rawDataId,
          },
          query: {
            raw_data_id: this.rawDataId,
            deploy_plan_id: this.deployPlanId,
            bk_inst_id: this.activeModuleInfo.bk_instance_id,
            bk_obj_id: this.activeModuleInfo.bk_object_id,
          },
        })
        .then(res => {
          if (res.result) {
            this.activePreviewIPList = res.data.results || res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](_ => {
          this.isIpListLoading = false;
        });
    },
    /**
     * 刷新当前模块采集状态信息
     */
    refreshModuleStatus(closeLoadingFn) {
      this.bkRequest
        .httpRequest('dataAccess/getAccessModuleSummary', {
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
            this.statusSummary = res.data.summary;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          closeLoadingFn && closeLoadingFn();
        });
    },
    /**
     * 刷新当前单个模块状态信息
     */
    refreshModuleItemStatus(item) {
      const instanceId = item.bk_instance_id;
      // 添加当前行刷新loading
      this.moduleList.forEach((mitem, index) => {
        if (mitem.bk_instance_id === instanceId) {
          this.$set(this.moduleList[index], 'loading', true);
        }
      });
      this.bkRequest
        .httpRequest('dataAccess/getAccessSummary', {
          params: {
            raw_data_id: this.rawDataId,
            deploy_plan_id: this.deployPlanId,
            bk_instance_id: item.bk_instance_id,
          },
          query: {
            deploy_plan_id: this.deployPlanId,
            bk_obj_id: this.bizid,
            bk_inst_id: item.bk_instance_id,
          },
        })
        .then(res => {
          if (res.result) {
            // 刷新同步更新当前刷新得模块信息
            this.moduleList.forEach((item, index) => {
              if (item.bk_instance_id === instanceId) {
                this.$set(this.moduleList, index, Object.assign({}, item, res.data, { loading: false }));
              }
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
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
.abnormal,
.failed {
  color: $abnormalColor;
}
.removed {
  color: $removedColor;
}

.center {
  text-align: center;
}

.module-preview-container {
  height: 100%;
}

.module-info {
  height: 100%;
  border: 1px solid $borderColor;

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

    .link-cmdb {
      cursor: pointer;
      color: $primaryColor;

      &:hover {
        text-decoration: underline;
      }
    }
  }

  .status-icons {
    display: inline-block;
    background: none;
    padding: 0 5px 0 0;
  }

  .module-paging {
    margin: 10px;
  }

  .operation-btns {
    a {
      display: inline-block;
      color: $primaryColor;
    }
  }
}

.module-preview-container {
  position: relative;
}

.status-icon {
  position: relative;
  left: -9px;

  &.success {
    color: $normalColor;
  }

  &.failure {
    color: $abnormalColor;
  }
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



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
  <div class="pre-container">
    <div class="pre-view">
      <bkdata-tab :active="activeTabName"
        @tab-change="tabChanged">
        <bkdata-tab-panel name="1"
          :disabled="isOfflineFile"
          :label="$t('数据预览')">
          <div style="padding: 0 15px">
            <template v-if="activeTabName === '1'">
              <data-detail ref="dataDetail" />
            </template>
          </div>
        </bkdata-tab-panel>
        <bkdata-tab-panel name="2"
          :label="$t('接入详情')">
          <div class="p20">
            <template v-if="activeTabName === '2'">
              <AccessDetails
                :details="details"
                :accessSummaryStatus="accessSummaryStatus"
                :data-scenario="dataScenario"
                :lists="lists" />
            </template>
          </div>
        </bkdata-tab-panel>
        <bkdata-tab-panel name="3"
          :disabled="isOfflineFile"
          :label="$t('数据清洗')">
          <div class="data-access-component">
            <template v-if="activeTabName === '3'">
              <component
                :is="clean.currentComponent"
                :ref="clean.currentComponent"
                :details="details"
                :itemObj="itemObj"
                :isAddClean="isAddClean"
                :rid="clean.rid"
                @detail="cleanDetail"
                @backList="cleanList" />
            </template>
          </div>
        </bkdata-tab-panel>
        <bkdata-tab-panel name="4"
          :disabled="isOfflineFile"
          :label="$t('数据入库')">
          <div class="data-access-component">
            <template v-if="activeTabName === '4'">
              <dataStorage :details="details" />
            </template>
          </div>
        </bkdata-tab-panel>
        <template v-if="isShowAlertCount">
          <bkdata-tab-panel name="5"
            :disabled="isOfflineFile"
            :label="$t('监控告警')">
            <div v-if="alertCount"
              slot="label"
              class="custom-label">
              {{ $t('监控告警') }}
              <span class="dmonitor-num">{{ alertCount }}</span>
            </div>
            <div class="p20">
              <section class="content clearfix">
                <template v-if="activeTabName === '5'">
                  <MonitoringConfig
                    :submitLoading="submitLoading"
                    :isFirstLoading="isFirstLoading"
                    :alertConfigInfo="alertConfigInfo"
                    :isRefresh="dataChannel.includes(details.bk_app_code)"
                    @submit="alertConfigSubmit" />
                  <MonitoringList />
                </template>
              </section>
            </div>
          </bkdata-tab-panel>
          <bkdata-tab-panel
            v-if="$modules.isActive('lifecycle')"
            name="6"
            :disabled="isOfflineFile"
            :label="$t('数据迁移')">
            <div class="data-access-component">
              <section class="content clearfix">
                <template v-if="activeTabName === '6'">
                  <DataRemoval :details="details" />
                </template>
              </section>
            </div>
          </bkdata-tab-panel>
        </template>
      </bkdata-tab>
    </div>
  </div>
</template>
<script>
// 原有逻辑 start
// import ipTable from '@/bkdata-ui/magicbox/bk-magic/bkdata-components/bkdata-table/bkdata-table'
import cleanList from '@/pages/dataManage/datadetailChildren/dataCleanList';
import cleanDetail from '@/pages/dataManage/datadetailChildren/cleanDetail';
import { postMethodWarning, showMsg } from '@/common/js/util.js';
import dataStorage from '@/pages/DataAccess/Details/DataStorage/index.vue';
import DataRemoval from '@/pages/DataAccess/Details/DataRemoval/index.vue';
import Bus from '@/common/js/bus';
import { mapState, mapGetters } from 'vuex';
// 原有逻辑 end
import dataDetail from '@/pages/dataManage/dataDetail';
import AccessDetails from './AccessDetailIndex';

import MonitoringList from '@/pages/DataAccess/Details/components/Monitoring/MonitoringList';
import MonitoringConfig from '@/pages/DataAccess/Details/components/Monitoring/MonitoringConfig';

import mixin from './mixin';
export default {
  components: {
    dataDetail,
    cleanList,
    cleanDetail,
    dataStorage,
    // ipTable,
    MonitoringList,
    MonitoringConfig,
    AccessDetails,
    DataRemoval,
  },
  mixins: [mixin],
  props: {
    details: {
      type: Object,
      default() {
        return {
          scopes: [],
          bk_app_code: 'dataweb',
        };
      },
    },
    delpoyedList: {
      type: Object,
      default() {
        return {};
      },
    },
  },
  data() {
    return {
      page: {
        cur: 1,
        totalPage: 1,
        unit: 10,
      },
      pageCur: 1,
      validDataScenario: this.$store.getters.getValidDataScenario,
      clean: {
        currentComponent: 'cleanList', // 清洗列表组件切换
        rid: '',
      },
      planIdAlias: {},
      deployeStatus: [],
      params: {
        deploye_status: '',
        bk_cloud_id: '',
        scope_type: 1,
        ip: '',
      },
      isShowAlertCount: true,
      // thead: [
      //     {
      //         name: 'index',
      //         text: this.$t('接入对象'),
      //         width: '120px',
      //         sortable: true
      //     },
      //     {
      //         name: 'bk_cloud_name',
      //         text: this.$t('云区域名称'),
      //         width: '180px',
      //         sortable: true
      //     },
      //     {
      //         name: 'scope_type_display',
      //         text: this.$t('接入范围'),
      //         width: '180px',
      //         sortable: true
      //     },
      //     {
      //         name: 'ip',
      //         text: this.$t('服务器IP'),
      //         width: '144px',
      //         sortable: true
      //     },
      //     {
      //         name: 'deploy_status',
      //         text: this.$t('接入状态'),
      //         width: '100px',
      //         sortable: false
      //     },
      //     {
      //         name: 'updated_at',
      //         text: this.$t('最新接入时间'),
      //         width: '200px'
      //     },
      //     {
      //         name: 'operation',
      //         text: this.$t('操作')
      //     }
      // ],
      timerId: '',
      lists: {
        deployStateList: [],
        // cloudAreaList: [],
        deployTypeList: [],
        Iplists: [],
        logList: [],
      },
      checkedList: [],
      filter: {
        objId: -1,
        instId: -1,
        type: '',
      },
      modulePlanId: -1,
      planId: -1,
      itemObj: {},
      isAddClean: false,
      dataChannel: ['dataweb', 'data'], // 允许编辑的接入渠道
      alertConfigInfo: {
        active: false,
        monitor_config: {
          no_data: {},
          data_drop: {},
          data_trend: {},
          data_time_delay: {},
          process_time_delay: {},
          data_interrupt: {},
          task: {},
        },
        trigger_config: {
          alert_threshold: 1,
          duration: 1,
        },
        convergence_config: {},
        notify_config: [],
        receivers: [],
      },
      isFirstLoading: true,
      submitLoading: false,
      // accessSummaryStatus: {},
      alertCount: 0, // 数据源告警数量
    };
  },
  computed: {
    ...mapState({
      activeTabName: state => state.accessDetail.activeTabName,
      alertData: state => state.accessDetail.alertData,
      accessSummaryStatus: state => state.accessDetail.accessSummaryStatus,
    }),
    logFilterList() {
      return this.lists.logList.slice((this.page.cur - 1) * 10, this.page.cur * 10);
    },
    showAccess() {
      return !this.dataScenarioHasAccessDetail.includes(this.dataScenario) || !this.details.data_scenario;
    },
    dataScenarioHasAccessDetail() {
      return this.$store.getters.getDataScenarioHasAccessDetail;
    },
    dataScenario: function () {
      // 消息队列下有kafka类型的数据源
      return this.details.data_scenario === 'queue'
        ? this.details.access_conf_info.resource.type
        : this.details.data_scenario;
    },
    dataId() {
      return this.details.data_id || this.$route.params.did;
    },
  },
  watch: {
    details(val) {
      this.updateIpsIndex();
      if (Object.keys(val).length) {
        this.isAddClean = val.access_raw_data.permission === 'access_only';
      }
    },
    '$route.params.tabid': {
      handler(val) {
        this.$store.commit('accessDetail/setActiveTabName', (val && val + '') || '2');
      },
      immediate: true,
    },
    alertData: {
      immediate: true,
      handler(val) {
        if (Object.keys(val).length) {
          this.getAlertCount();
        }
      },
    },
  },
  created() {
    Bus.$on('filterIpsList', data => {
      this.activeTabName = 'second';
      this.params.deploye_status = data;
      this.params.scope_type = 1;
      this.params.ip = '';
      this.changeReduction('0');
    });
  },
  beforeDestroy() {
    // clearTimeout(this.timerId)
  },
  mounted() {
    this.getStatusSummary();
    this.getDefaultData();
  },
  methods: {
    getDefaultData() {
      const params = {
        raw_data_id: this.$route.params.did,
      };
      return this.bkRequest.httpRequest('dataAccess/getAlertConfigIds', { params }).then(res => {
        if (res && res.result) {
          this.alertConfigInfo = res.data;
        } else {
          res && this.getMethodWarning(res.message, res.code);
        }
        this.isFirstLoading = false;
      });
    },
    alertConfigSubmit(options) {
      this.submitLoading = true;
      this.bkRequest.httpRequest('dmonitorCenter/updateAlertConfig', options).then(res => {
        if (res.result) {
          showMsg(this.$t('保存告警配置成功'), 'success', { delay: 1500 });
        } else {
          postMethodWarning(res.message, res.code);
        }
        this.submitLoading = false;
      });
    },
    getAlertCount() {
      const query = {
        alert_config_ids: this.alertData.id,
      };
      this.bkRequest
        .httpRequest('dataAccess/getAlertCount', { query: query })
        .then(res => {
          if (res.result) {
            if (res.data) {
              this.alertCount = res.data.alert_count;
            }
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isShowAlertCount = false;
          this.$nextTick(() => {
            this.isShowAlertCount = true;
          });
        });
    },
    /**
     * 获取状态数据统计
     */
    getStatusSummary() {
      this.bkRequest
        .httpRequest('dataAccess/getAccessSummary', {
          params: {
            raw_data_id: this.$route.params.did,
          },
        })
        .then(res => {
          if (res.result) {
            this.$store.commit('accessDetail/setAccessSummaryStatus', res.data);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
    },
    tabChanged(val) {
      this.$router.push(`/data-access/data-detail/${this.dataId}/${val}`);
    },
    pageChange(val) {
      this.$router.push({
        name: 'data_detail',
        params: { did: this.dataId },
        hash: this.$route.hash,
        query: { page: val },
      });
    },
    filterByPlanId(detail) {
      this.planId = detail.deploy_plan_id;
      this.filter = {
        instId: -1,
        objId: -1,
        type: '',
      };

      this.params.scope_type = 1;
      this.scrollToIplist();
    },
    scrollToIplist() {
      let h = document.getElementById('accessed-ip-list').offsetTop;
      document.getElementsByTagName('html')[0].scrollTop = h;
    },
    changeReduction(id, item) {
      // '0' 表示从接入总览点击过滤
      if (id === '0') {
        this.filter.instId = -1;
        this.filter.objId = -1;
        this.planId = -1;
        this.filter.type = '';
      }
    },
    changeDataId() {
      this.$router.push({ name: 'updateDataid', params: { did: this.details.data_id }, hash: '#obj' });
    },
    /*
     * 清洗列表点击查看详情
     * */
    cleanDetail(item) {
      this.clean.rid = item.processing_id;
      this.itemObj = item;
      this.clean.currentComponent = 'cleanDetail';
    },
    /*
     * 清洗详情点击返回列表
     * */
    cleanList() {
      this.clean.currentComponent = 'cleanList';
      this.$nextTick(() => {
        this.$refs[this.clean.currentComponent].init();
      });
    },
    getPlanIdIndex(id) {
      return (
        (this.details.access_conf_info
          && this.details.access_conf_info.resource.scope.findIndex(scop => scop.deploy_plan_id === id) + 1)
        || ''
      );
    },
    updateIpsIndex() {
      this.lists.Iplists.forEach(ip => {
        this.$set(ip, 'index', this.getPlanIdIndex(ip.deploy_plan_id));
      });
    },
  },
};
</script>

<style lang="scss">
.container-with-shadow {
  -webkit-box-shadow: 2px 3px 5px 0 rgba(33, 34, 50, 0.15);
  box-shadow: 2px 3px 5px 0 rgba(33, 34, 50, 0.15);
  border-radius: 2px;
  border: 1px solid rgba(195, 205, 215, 0.6);
}

.compact-table {
  > thead > tr > th,
  > thead > tr > td,
  > tbody > tr > th,
  > tbody > tr > td {
    padding: 6px 10px;
  }
}

.mini-paging {
  text-align: right;
  margin: 10px 0;

  .bk-page.bk-page-small .page-item {
    height: 28px;
    min-width: 28px;
    line-height: 28px;

    .page-button {
      line-height: 28px;
    }
  }
}

.refresh-loading {
  display: inline-block;
  -webkit-animation: rotating 1s linear infinite;
  -moz-animation: rotating 1s linear infinite;
  -ms-animation: rotating 1s linear infinite;
  -o-animation: rotating 1s linear infinite;
  animation: rotating 1s linear infinite;
}

.source-data-tooltip {
  padding: 10px;
  color: #fff;

  h3 {
    font-size: 15px;
    font-weight: normal;
  }

  p {
    padding: 2px 20px 5px;
    font-size: 14px;
  }
}

@-webkit-keyframes rotating /* Safari and Chrome */ {
  from {
    -webkit-transform: rotate(0deg);
    -o-transform: rotate(0deg);
    transform: rotate(0deg);
  }
  to {
    -webkit-transform: rotate(360deg);
    -o-transform: rotate(360deg);
    transform: rotate(360deg);
  }
}
@keyframes rotating {
  from {
    -ms-transform: rotate(0deg);
    -moz-transform: rotate(0deg);
    -webkit-transform: rotate(0deg);
    -o-transform: rotate(0deg);
    transform: rotate(0deg);
  }
  to {
    -ms-transform: rotate(360deg);
    -moz-transform: rotate(360deg);
    -webkit-transform: rotate(360deg);
    -o-transform: rotate(360deg);
    transform: rotate(360deg);
  }
}
</style>

<style lang="scss" scoped>
.dmonitor-num {
  min-width: 18px;
  height: 18px;
  line-height: 16px;
  padding: 0 2px;
  border-radius: 18px;
  background-color: #ff5656;
  font-size: 12px;
  text-align: center;
  color: white;
}
.data-access-component {
  padding: 5px 5px 20px 5px;
}
.pre-container {
  margin: 0 auto;
  min-width: 900px;
  .pre-view {
    background: #f5f5f5;
    ::v-deep .bk-tab-section {
      padding: 0;
      background: #fff;
    }

    .content {
      width: 100%;
      margin: 0 auto;
      display: flex;
      flex-direction: column;
      .shadows {
        box-shadow: 2px 3px 5px 0 rgba(33, 34, 50, 0.15);
        border-radius: 2px;
        border: solid 1px rgba(195, 205, 215, 0.6);
      }
    }
  }
}
</style>

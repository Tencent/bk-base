

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
  <Layout
    :collspan="true"
    :crumbName="$t('告警列表')"
    :headerBackground="'inherit'"
    :headerMargin="false"
    :isOpen="true"
    :withMargin="false"
    class="container-with-shadow"
    height="auto">
    <div class="filter-box">
      <div class="date-box">
        <bkdata-date-picker
          v-model="params.searchTime"
          :endDate="endDate"
          :type="'date'"
          :timer="false"
          :placeholder="$t('请选择')" />
      </div>
      <div class="monitor-select">
        <bkdata-selector
          :allowClear="true"
          :list="list"
          :displayKey="'name'"
          :settingKey="'id'"
          :placeholder="$t('告警类型')"
          :selected.sync="params.searchType" />
      </div>
      <div class="monitor-select">
        <bkdata-selector
          :displayKey="'name'"
          :allowClear="true"
          :list="alertLevelList"
          :placeholder="$t('告警级别')"
          :selected.sync="params.selectedAlertLevel"
          :settingKey="'key'" />
      </div>
      <div class="monitor-select">
        <bkdata-selector
          :displayKey="'name'"
          :allowClear="true"
          :list="alertStatusList"
          :placeholder="$t('告警状态')"
          :selected.sync="params.selectedAlertStatus"
          :settingKey="'key'" />
      </div>
      <div class="identification-input">
        <bkdata-input v-model="params.searchContent"
          :placeholder="$t('告警内容_回车直接检索')"
          type="text" />
      </div>
    </div>
    <div id="monitorList"
      class="table-content">
      <div v-bkloading="{ isLoading: tableLoading }">
        <bkdata-table
          :expandRowKeys="openRow"
          :rowKey="rowKey"
          :outerBorder="false"
          :headerBorder="false"
          :headerCellStyle="{ background: '#fff' }"
          :data="warningListView"
          :emptyText="$t('暂无数据')"
          @row-click="rowClick">
          <bkdata-table-column width="48px"
            type="expand">
            <div slot-scope="props"
              class="bk-table-inlineblock">
              <div class="expand-content">
                <bkdata-form formType="inline"
                  name="props.row.id">
                  <bkdata-form-item style="margin-bottom: 0">
                    {{ props.row[fullMessageKey] }}
                  </bkdata-form-item>
                </bkdata-form>
              </div>
            </div>
          </bkdata-table-column>
          <bkdata-table-column :label="$t('告警时间')"
            prop="alert_time"
            width="160" />
          <bkdata-table-column :label="$t('告警类型')"
            prop="alert_type_display"
            width="120" />
          <bkdata-table-column :label="$t('接收人')"
            prop="receivers_display"
            width="200" />
          <bkdata-table-column :label="$t('告警内容')">
            <div slot-scope="props"
              class="bk-table-inlineblock">
              <span
                v-bk-tooltips.bottom="alertRuleDescription[props.row.alert_code]"
                class="cursor-pointer alert-message-title">
                {{ alertCodeDisplay(props.row.alert_code) }}
              </span>
              <span
                class="alert-message-content"
                :title="`【${alertCodeDisplay(props.row.alert_code)}】${props.row[fullMessageKey]}`">
                {{ props.row[fullMessageKey] }}
              </span>
            </div>
          </bkdata-table-column>
          <bkdata-table-column :label="$t('告警状态')"
            prop="alert_status_display"
            width="80">
            <div slot-scope="scope"
              class="bk-table-inlineblock">
              <span
                v-if="scope.row['alert_send_status'] === 'partial_error' || scope.row['alert_send_status'] === 'error'"
                v-bk-tooltips="formatToolTip(scope.row)"
                :class="scope.row['alert_send_status']">
                {{ scope.row['alert_status_display'] }}
              </span>
              <span
                v-if="scope.row['alert_send_status'] !== 'partial_error' && scope.row['alert_send_status'] !== 'error'"
                :class="scope.row['alert_send_status']">
                {{ scope.row['alert_status_display'] }}
              </span>
            </div>
          </bkdata-table-column>
        </bkdata-table>
      </div>
      <div class="table-pagination">
        <bkdata-pagination
          :current.sync="defaultPaging.page"
          :showLimit="false"
          :count="warningList.length"
          :limit="defaultPaging.limit" />
      </div>
    </div>
  </Layout>
</template>

<script>
import moment from 'moment';
import Layout from '@/components/global/layout';
import Bus from '@/common/js/bus';
import {
  alertCodeMappings,
  alertTypeMappings,
  alertRuleDescription,
  alertSendStatusMappings,
  alertStatusMappings,
} from '@/common/js/dmonitorCenter.js';
import { mapState } from 'vuex';

export default {
  name: 'MonitoringList',
  components: { Layout },
  data() {
    return {
      tableLoading: false,
      openRow: [],
      defaultPaging: {
        page: 1,
        totalPage: 1,
        limit: 10,
      },
      params: {
        searchContent: '',
        searchTime: '',
        searchType: '',
        selectedAlertLevel: '',
        selectedAlertStatus: 'alerting',
      },
      list: [
        {
          id: 'task_monitor',
          name: window.$t('任务监控'),
        },
        {
          id: 'data_monitor',
          name: window.$t('数据监控'),
        },
      ],
      warningListSource: [],
      warningList: [],
      tableShowData: [],

      alertTypeMappings: alertTypeMappings,
      alertCodeMappings: alertCodeMappings,
      alertRuleDescription: alertRuleDescription,
      alertLevelList: [
        {
          name: this.$t('警告'),
          key: 'warning',
        },
        {
          name: this.$t('严重'),
          key: 'danger',
        },
      ],
      alertStatusList: [
        {
          name: this.$t('告警中'),
          key: 'alerting',
        },
        {
          name: this.$t('已屏蔽'),
          key: 'shielded',
        },
        {
          name: this.$t('已收敛'),
          key: 'converged',
        },
      ],
    };
  },

  computed: {
    ...mapState({
      alertData: state => state.accessDetail.alertData,
    }),
    fullMessageKey() {
      return `full_message${this.$getLocale() === 'en' ? '_en' : ''}`;
    },
    endDate() {
      return moment(new Date()).format('YYYY-MM-DD');
    },
    rawDataId() {
      return this.$route.params.did;
    },
    warningListView() {
      const startIndex = (this.defaultPaging.page - 1) * 10;
      const endIndex = this.defaultPaging.page * 10;

      return this.warningList.slice(startIndex, endIndex);
    },
  },
  watch: {
    'params.searchTime': {
      handler: function (timeSet) {
        if (typeof timeSet === 'object') {
          timeSet = moment(timeSet).format('YYYY-MM-DD');
        }
        const startTime = `${timeSet} 00:00:00`;
        const endTime = `${timeSet} 23:59:59`;

        this.getWarningList({
          start_time: new Date(startTime).getTime(),
          end_time: new Date(endTime).getTime(),
        });
      },
      deep: true,
    },
    // 过滤条件发生变化时需重新获取最新告警列表
    'params.searchType'() {
      this.getFilterResultList();
    },
    'params.selectedAlertLevel'() {
      this.getFilterResultList();
    },
    'params.selectedAlertStatus'() {
      this.getFilterResultList();
    },
    'params.searchContent'() {
      this.getFilterResultList();
    },
    alertData: {
      immediate: true,
      handler(val) {
        if (Object.keys(val).length) {
          this.getWarningList();
        }
      },
    },
  },
  beforeDestroy() {
    Bus.$off('alertConfigsLoaded');
  },
  methods: {
    formatToolTip(item) {
      return {
        appendTo: document.body,
        placement: 'top',
        content: `<div style="max-width:300px;max-height:300px; overflow: auto;">
                        <pre style="white-space: pre-wrap; word-break: break-all;">${item['alert_send_error']}</pre>
                  </div>`,
      };
    },
    rowClick(row, event, column) {
      this.openRow.includes(row.monitorId)
        ? this.openRow.splice(
          this.openRow.findIndex(item => item === row.monitorId),
          1
        )
        : this.openRow.push(row.monitorId);
    },
    rowKey(row) {
      return row.id;
    },
    /**
     * 获取告警列表
     */
    getWarningList(timeSet = {}) {
      this.tableLoading = true;
      const query = Object.assign(
        {},
        {
          flow_id: `rawdata${this.rawDataId}`,
          alert_config_ids: [this.alertData.id],
        },
        timeSet
      );
      this.bkRequest
        .httpRequest('dataAccess/getWarningList', {
          params: {
            raw_data_id: this.rawDataId,
          },
          query: query,
        })
        .then(res => {
          if (res.result) {
            const warningTypeMap = {
              task: this.$t('任务异常'),
              data_trend: this.$t('数据波动'),
              no_data: this.$t('数据断流'),
              data_loss: this.$t('数据丢失'),
              data_delay: this.$t('数据延迟'),
            };

            const warningStatusMap = {
              init: this.$t('准备发送'),
              success: this.$t('已发送'),
              partial_error: this.$t('部分发送失败'),
              error: this.$t('发送失败'),
            };
            this.warningList =              res.data.map(item => {
              item['alert_type_display'] = this.alertTypeMappings[item.alert_type];
              item['receivers_display'] = item.receivers.join(',');
              item['alert_status_display'] = warningStatusMap[item.alert_send_status];

              return item;
            }) || [];

            this.warningListSource = JSON.parse(JSON.stringify(this.warningList));

            if (this.params.searchType || this.params.searchContent) {
              this.warningList = this.warningListSource.filter(
                item => item.alert_code === this.params.searchType
                  && item[this.fullMessageKey].includes(this.params.searchContent)
              );
            }
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.tableLoading = false;
        });
    },
    /**
     * 前端过滤告警列表
     */
    getFilterResultList() {
      // 存在关键字过滤时需进行关键字过滤，反之则跳过这项过滤
      const contentFilterCondition = item => this.params.searchContent
        ? item[this.fullMessageKey].includes(this.params.searchContent)
        : true;
      // 存在告警类型过滤时需进行类型过滤，反之则跳过这项过滤
      const typeFilterCondition = item => (this.params.searchType
        ? item.alert_type === this.params.searchType
        : true);
      // 存在告警级别过滤时需进行类型过滤，反之则跳过这项过滤
      const leaveFilterCondition = item => this.params.selectedAlertLevel
        ? item.alert_level === this.params.selectedAlertLevel
        : true;
      // 存在告警状态过滤时需进行类型过滤，反之则跳过这项过滤
      const statusFilterCondition = item => this.params.selectedAlertStatus
        ? item.alert_status === this.params.selectedAlertStatus
        : true;

      this.warningList = this.warningListSource.filter(
        item => typeFilterCondition(item)
          && contentFilterCondition(item)
          && leaveFilterCondition(item)
          && statusFilterCondition(item)
      );
    },
    /**
     * 告警策略类型展示
     */
    alertCodeDisplay(alertCode) {
      return alertCodeMappings[alertCode] || '';
    },
  },
};
</script>

<style scoped lang="scss">
.source-data-tooltip {
  width: 500px;
  height: 300px;
}

::v-deep .layout-body {
  overflow: unset !important;
}
.container-with-shadow {
  ::v-deep .layout-content {
    flex-direction: column;
  }
  .filter-box {
    display: flex;
    position: relative;
    z-index: 999;

    > div {
      margin-right: 30px;
    }
    .date-box {
      .bk-date {
        width: 260px;
      }
    }
    .monitor-select {
      width: 180px;
    }
    .identification-input {
      width: 180px;
      .bk-form-input {
        display: block;
      }
    }
  }
  .table-content {
    margin-top: 20px;
    ::v-deep .el-table tr {
      cursor: pointer;
    }
    ::v-deep .el-table td {
      padding: 6px 0;
      .init {
        color: #313238;
      }
      .success {
        color: #2dcb56;
      }
      .partial_error,
      .error {
        color: #ea3636;
      }
    }
    .open-content {
      text-indent: 24px;
      color: #737987;
    }
    .table-pagination {
      display: flex;
      justify-content: flex-end;
      margin-top: 30px;
    }

    .expand-content {
      padding: 5px 0px 5px 60px;
      margin: -6px 0;
      background: #eee;
    }

    .cursor-pointer {
      cursor: pointer;
    }

    .alert-message-title {
      background: #5e5e5e;
      color: #ffffff;
      padding: 2px 5px;
      border-radius: 3px;
    }
    .alert-message-content {
      margin-left: 3px;
    }
  }
}
</style>

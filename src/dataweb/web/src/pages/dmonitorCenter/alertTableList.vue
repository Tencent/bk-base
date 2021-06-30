

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
  <div class="alert-tab">
    <div class="my-alert">
      <div class="my-alert-item">
        <div v-bkloading="{ isLoading: isLoading }"
          class="my-alert-flows">
          <bkdata-table :data="alertList"
            :pagination="pagination"
            :emptyText="$t('暂无数据')"
            @page-change="handlePageChange"
            @page-limit-change="handlePageLimitChange">
            <bkdata-table-column :label="$t('告警对象')"
              :resizable="false"
              minWidth="200">
              <template slot-scope="props">
                <div class="alert-table-option">
                  <a href="javascript:;"
                    :title="getAlertTargetTitle(props.row)"
                    class="operation-button text-overflow"
                    @click="linkAlert(props.row)">
                    {{ getAlertTargetTitle(props.row) }}
                  </a>
                  <i class="bk-icon icon-link-to ml5 cursor-pointer"
                    :title="$t('跳转到告警对象详情页')"
                    @click="linkAlertTarget(props.row)" />
                </div>
              </template>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('告警类型')"
              :resizable="false"
              minWidth="100">
              <template slot-scope="props">
                <div :title="alertTypeDisplay(props.row.alert_type)">
                  {{ alertTypeDisplay(props.row.alert_type) }}
                </div>
              </template>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('告警级别')"
              :resizable="false"
              width="100">
              <template slot-scope="props">
                <div :title="alertLevelDisplay(props.row.alert_level)"
                  :class="props.row.alert_level">
                  {{ alertLevelDisplay(props.row.alert_level) }}
                </div>
              </template>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('项目')"
              :resizable="false"
              minWidth="150">
              <template slot-scope="props">
                <div v-if="props.row.project_id"
                  :title="`[${props.row.project_id}]${props.row.project_alias}`">
                  [{{ props.row.project_id }}]{{ props.row.project_alias }}
                </div>
                <div v-else>
                  -
                </div>
              </template>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('业务')"
              :resizable="false"
              minWidth="150">
              <template slot-scope="props">
                <div v-if="props.row.bk_biz_id"
                  :title="`[${props.row.bk_biz_id}]${props.row.bk_biz_name}`">
                  [{{ props.row.bk_biz_id }}]{{ props.row.bk_biz_name }}
                </div>
                <div v-else>
                  -
                </div>
              </template>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('告警时间')"
              :resizable="false"
              width="150"
              prop="alert_time" />
            <bkdata-table-column :label="$t('告警内容')"
              :resizable="false"
              minWidth="500">
              <template slot-scope="props">
                <div class="alert-table-option">
                  <span v-bk-tooltips.bottom="alertRuleDescription[props.row.alert_code]"
                    class="cursor-pointer alert-message-title">
                    {{ alertCodeDisplay(props.row.alert_code) }}
                  </span>
                  <span class="alert-message-content"
                    :title="`【${alertCodeDisplay(props.row.alert_code)}】${props.row.full_message}`">
                    {{ $i18n.locale === 'en' ? props.row.full_message_en : props.row.full_message }}
                  </span>
                </div>
              </template>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('告警状态')"
              :resizable="false"
              width="100">
              <template slot-scope="props">
                <div v-if="props.row.alert_status === 'alerting'
                       && props.row.alert_send_status === 'init'"
                  class="alert-table-option">
                  {{ alertStatusDisplay(props.row) }}
                  <span v-bk-tooltips.right="$t('为了避免骚扰用户_告警会隔一段时间才发一次')"
                    class="cursor-pointer alert-status-tooltip">
                    <i class="bk-icon icon-info-circle-shape" />
                  </span>
                </div>
                <div v-if="props.row.alert_status === 'shielded'
                       || props.row.alert_status === 'converged'"
                  class="alert-table-option">
                  {{ alertStatusDisplay(props.row) }}
                  <span v-bk-tooltips.right="props.row.description"
                    class="cursor-pointer alert-status-tooltip">
                    <i class="bk-icon icon-info-circle-shape" />
                  </span>
                </div>
                <div v-else>
                  {{ alertStatusDisplay(props.row) }}
                </div>
              </template>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('操作')"
              :resizable="false"
              width="200">
              <template slot-scope="props">
                <div class="alert-table-option">
                  <div class="mr10">
                    <a href="javascript:;"
                      @click="linkAlert(props.row)">
                      {{ $t('详情') }}
                    </a>
                  </div>
                  <div class="mr10">
                    <a href="javascript:;"
                      @click="linkAlertConfig(props.row)">
                      {{ $t('配置') }}
                    </a>
                  </div>
                  <div class="mr10">
                    <a href="javascript:;"
                      @click="openAlertShieldModal(props.row)">
                      {{ $t('屏蔽') }}
                    </a>
                  </div>
                </div>
              </template>
            </bkdata-table-column>
          </bkdata-table>
        </div>
      </div>
    </div>
    <!-- 告警屏蔽 start -->
    <div>
      <alert-shield-modal v-if="isAlertShieldModalShow"
        ref="alertShieldModal"
        @closeAlertShieldModal="isAlertShieldModalShow = false" />
    </div>
    <!-- 告警屏蔽 end -->
  </div>
</template>
<script>
import Bus from '@/common/js/bus.js';
import moment from 'moment';
import { postMethodWarning, showMsg } from '@/common/js/util.js';
import {
  alertTypeMappings,
  alertLevelMappings,
  alertCodeMappings,
  alertStatusMappings,
  alertSendStatusMappings,
  alertRuleDescription
} from '@/common/js/dmonitorCenter.js';
import alertShieldModal from './components/alertShieldModal';

export default {
  components: {
    alertShieldModal,
  },
  data() {
    return {
      isLoading: false,
      alertType: '',
      startTime: moment().subtract(1, 'days'),
      endTime: moment(),
      alertTarget: '',
      alertLevel: '',
      alertStatus: ['alerting'],
      alertList: [],
      currentPage: 1,
      pageCount: 1,
      pageSize: 10,

      isAlertShieldModalShow: false,
      alertShieldForm: {
        alertInfo: {},
      },

      alertRuleDescription: alertRuleDescription,
    };
  },
  computed: {
    pagination() {
      return {
        current: Number(this.currentPage),
        count: this.pageCount,
        limit: this.pageSize,
      };
    },
  },
  mounted() {
    Bus.$on('alertTypeChange', alertType => {
      this.currentPage = 1;
      this.alertType = alertType;
      this.getAlertList();
    });
    Bus.$on('alertTimeChange', alertTime => {
      this.currentPage = 1;
      this.startTime = moment(alertTime[0]);
      this.endTime = moment(alertTime[1]);
      this.getAlertList();
    });
    Bus.$on('alertTargetChange', alertTarget => {
      this.currentPage = 1;
      this.alertTarget = alertTarget;
      this.getAlertList();
    });
    Bus.$on('alertLevelChange', alertLevel => {
      this.currentPage = 1;
      this.alertLevel = alertLevel;
      this.getAlertList();
    });
    Bus.$on('alertStatusChange', alertStatus => {
      this.currentPage = 1;
      this.alertStatus = alertStatus;
      this.getAlertList();
    });
    // 监听message事件
    window.addEventListener(
      'message',
      event => {
        if (event.data.msg === 'closeModel') {
          this.handleCancel();
        }
      },
      false
    );
    this.init();
  },
  methods: {
    init() {
      // 组件加载后默认显示告警
      this.getAlertList();
    },
    /**
     * 跳转到告警对象详情页
     */
    linkAlertTarget(item) {
      if (item.alert_target_type === 'rawdata') {
        this.$router.push(`/data-access/data-detail/${item.alert_target_id}/`);
      } else if (item.alert_target_type === 'dataflow') {
        this.$router.push(`/dataflow/ide/${item.alert_target_id}/`);
      }
    },
    /**
     * 跳转到告警详情
     */
    linkAlert(item) {
      this.$router.push(`/dmonitor-center/alert/${item.id}/`);
    },
    /**
     * 跳转到产生改告警的告警配置
     */
    linkAlertConfig(item) {
      this.$router.push(`/dmonitor-center/alert-config/${item.alert_config_id}/`);
    },
    /**
     * 打开告警屏蔽模态框
     */
    openAlertShieldModal(item) {
      this.isAlertShieldModalShow = true;
      this.$nextTick(() => {
        this.$refs.alertShieldModal.openModal(item);
      });
    },
    /**
     * 分页页码改变
     */
    handlePageChange(page) {
      this.currentPage = page;
      this.getAlertList();
    },
    /**
     * 单页展示数量改变
     */
    handlePageLimitChange(pageSize, preLimit) {
      this.currentPage = 1;
      this.pageSize = pageSize;
      this.getAlertList();
    },
    /**
     * 生成数据AlertTarget的title
     */
    getAlertTargetTitle(alertItem) {
      if (alertItem.alert_target_type === 'dataflow') {
        return `[${$t('数据开发任务')}] ${alertItem.alert_target_alias}`;
      } else if (alertItem.alert_target_type === 'rawdata') {
        return `[${$t('数据源')}] ${alertItem.alert_target_alias}`;
      } else {
        return alertItem.alert_target_type_alias;
      }
    },
    /**
     * 告警分类展示
     */
    getAlertTypeDisplay(alertItem) {
      if (alertItem.alert_type === 'task_monitor') {
        return this.$t('任务监控');
      } else if (alertItem.alert_type === 'data_monitor') {
        return this.$t('数据监控');
      } else {
        return this.$t('未知类型');
      }
    },
    /**
     * 获取告警列表
     */
    getAlertList(alertTarget) {
      this.isLoading = true;
      const options = {
        query: {
          page: this.currentPage,
          page_size: this.pageSize,
          start_time: this.startTime.format('YYYY-MM-DD HH:mm:ss'),
          end_time: this.endTime.format('YYYY-MM-DD HH:mm:ss'),
        },
      };
      if (alertTarget) {
        options.query.alert_target = this.alertTarget;
      }
      if (this.alertTarget) {
        options.query.alert_target = this.alertTarget;
      }
      if (this.alertType) {
        options.query.alert_type = this.alertType;
      }
      if (this.alertLevel) {
        options.query.alert_level = this.alertLevel;
      }
      if (this.alertStatus) {
        options.query.alert_status = this.alertStatus;
      }
      this.bkRequest.httpRequest('dmonitorCenter/getMineDmonitorAlert', options).then(res => {
        if (res.result) {
          this.alertList = res.data.results;
          this.pageCount = res.data.count;
        } else {
          postMethodWarning(res.message, 'error');
        }
        this.isLoading = false;
      });
    },
    alertTypeDisplay(alertType) {
      return alertTypeMappings[alertType] || '';
    },
    alertLevelDisplay(alertLevel) {
      return alertLevelMappings[alertLevel] || '';
    },
    alertStatusDisplay(alertItem) {
      if (alertItem.alert_status === 'alerting') {
        return alertSendStatusMappings[alertItem.alert_send_status] || '';
      } else {
        return alertStatusMappings[alertItem.alert_status] || '';
      }
    },
    alertCodeDisplay(alertCode) {
      return alertCodeMappings[alertCode] || '';
    },
  },
};
</script>
<style lang="scss">
.alert-shield-dialog-button {
  margin: 10px 0 30px;
  display: flex;
  justify-content: space-evenly;
  text-align: center;
  .bk-button {
    margin-top: 20px;
    width: 120px;
    margin-right: 15px;
  }
}
.alert-tab {
  .my-alert {
    .bk-option {
      min-width: 80px;
    }
    padding-top: 10px;
    border-bottom: none;
    .danger {
      color: #ea3636;
    }
    .warning {
      color: #ff9c01;
    }
    &-item {
      .name {
        width: 325px;
        padding: 0 14px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }
    }
    &-flows {
      .alert-table-option {
        position: absolute;
        left: 0;
        top: 0;
        bottom: 0;
        right: 0;
        display: flex;
        align-items: center;
        justify-content: flex-start;
        padding-left: 15px;
        .cursor-pointer {
          cursor: pointer;
        }
        .del-button-wrap {
          flex: 0.5;
          display: flex;
          align-content: center;
        }
        .alert-message-title {
          background: #5e5e5e;
          color: #ffffff;
          padding: 2px 5px;
          border-radius: 3px;
          white-space: nowrap;
          text-overflow: ellipsis;
        }
        .alert-message-content {
          white-space: nowrap;
          overflow-x: hidden;
          margin-left: 3px;
          text-overflow: ellipsis;
        }
        .alert-status-tooltip {
          margin-left: 1px;
        }
        .operation-button {
          display: flex;
          flex-direction: column;
          justify-content: center;
          // flex: .5;
          max-width: 80%;
          white-space: nowrap;
          margin-right: 0;
          margin-left: 12.5px;
        }
        .text-overflow {
          display: block;
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
        }
        .red-point {
          position: absolute;
          left: 10px;
          top: 50%;
          transform: translateY(-50%);
          width: 10px;
          height: 10px;
          background: #ea3636;
          border-radius: 50%;
        }
        .del-button {
          border: none;
          color: #ea3636;
        }
        .disabled {
          color: #babddd !important;
          cursor: not-allowed;
        }
        .copy-to {
          color: #3a84ff !important;
        }
        .table-select {
          display: flex;
          flex-direction: column;
          justify-content: center;
          flex: 0.5;
          .bk-selector {
            .bk-selector-list {
              > ul > .bk-selector-list-item > .bk-selector-node {
                .text {
                  white-space: nowrap;
                  overflow: hidden;
                  text-overflow: ellipsis;
                }
              }
            }
            .bk-selector-wrapper {
              .bk-selector-input {
                color: #666bb4;
                border: none;
              }
            }
          }
        }
      }
      .click-style {
        color: #666bb4;
        cursor: pointer;
      }
    }
  }
}
</style>

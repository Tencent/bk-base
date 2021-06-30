

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
  <div class="access-preview-container">
    <div :style="showTipStyle"
      class="access-preview">
      <div class="access-preview-left">
        <span class="access-runing-state">
          <div v-if="showAlertTips"
            :class="['access-alert', calcStatus.state, $getLocale()]"
            :style="alertTipsStyle">
            {{ calcStatus.message }}
            <span class="tips-alert-close"
              @click="handlCloseTips">
              <i class="bk-icon icon-close-circle" />
            </span>
          </div>
          <div class="running-text">
            {{ $t('当前运行状态') }}：
            <span :class="['running-statu-text', calcStatus.state]">{{ accessSummaryInfo.deploy_status_display }}</span>
          </div>
          <StatusIcons :status="accessSummaryInfo.summary"
            style="background: #eaeaea"
            @refresh="getStatusSummary" />
        </span>
        <span class="operation-list">
          {{ $t('最后操作') }}：{{ recentDate }}
          <span class="user">{{ recentLogInfo.bk_username }}</span>
        </span>
      </div>
      <div class="access-preview-right">
        <template v-if="!taskDisable">
          <span :class="{ 'is-loading': startLoading, disabled: isStartBtnDisabled }"
            @click="startCollectorhub">
            <i
              v-if="startLoading"
              v-tooltip="operationBtnInfo.start.loadingText"
              class="bk-icon icon-refresh loading" />
            <i
              v-else
              v-tooltip="operationBtnInfo.start.text"
              :class="['bk-icon', (calcStatus.state === 'exception' && 'icon-restart') || 'icon-right-shape']" />
          </span>
          <span :class="{ 'is-loading': stopLoading, disabled: isStopBtnDisabled }"
            @click="stopConfirm">
            <i v-if="stopLoading"
              v-tooltip="operationBtnInfo.stop.loadingText"
              class="bk-icon icon-refresh loading" />
            <i v-else
              v-tooltip="operationBtnInfo.stop.text"
              class="bk-icon icon-stop-shape" />
          </span>
          <bkdata-dialog
            v-model="showStopDialog"
            extCls="bkdata-dialog"
            :theme="stopTaskDialogSetting.theme"
            :maskClose="false"
            :width="stopTaskDialogSetting.width"
            :title="stopTaskDialogSetting.title"
            @confirm="stopCollectorhub"
            @cancel="cancelFn">
            <span style="display: inline-block; padding: 15px 30px">{{ stopTaskDialogSetting.content }}</span>
          </bkdata-dialog>
        </template>
        <template v-else>
          <span :class="{ disabled: true }">
            <i v-if="startLoading"
              class="bk-icon icon-refresh loading" />
            <i
              v-else
              :class="['bk-icon', (calcStatus.state === 'exception' && 'icon-restart') || 'icon-right-shape']" />
          </span>
          <span :class="{ disabled: true }">
            <i v-if="stopLoading"
              class="bk-icon icon-refresh loading" />
            <i v-else
              class="bk-icon icon-stop-shape" />
          </span>
        </template>
        <!-- <span class="f16">
                    <i class="bk-icon icon-list"></i>
                </span>-->
        <span class="access-running-statu">
          <template v-if="warningList.length">
            <bkdata-popover placement="bottom-end">
              <span class="f14">
                <i class="bk-icon icon-alert mr5" />
                {{ warningList.length }}
              </span>
              <div v-show="showWarnigTips"
                slot="content">
                <div class="bk-text-primary inner-title"
                  style="color: #fff">
                  {{ $t('近一天警告') }}
                </div>
                <div style="color: #fff">
                  <table class="bk-table access-tips">
                    <tbody>
                      <template v-if="warningList.length">
                        <template v-for="(item, index) in warningList">
                          <tr v-if="index < 5"
                            :key="index">
                            <td>{{ index + 1 }}</td>
                            <td>{{ item.alert_time.split(' ')[1] }}</td>
                            <td>
                              <p class="warning-text">{{ item[fullMessageKey] }}</p>
                            </td>
                          </tr>
                        </template>
                      </template>
                      <template v-else>
                        <tr>
                          <td colspan="3">{{ $t('暂无数据') }}</td>
                        </tr>
                      </template>
                    </tbody>
                  </table>
                </div>
                <div class="preview-all">
                  <a
                    v-if="warningList.length"
                    href="javascript:void(0);"
                    style="color: #288ce2"
                    @click.stop="handlViewWarning">
                    {{ $t('查看所有告警') }}
                  </a>
                </div>
              </div>
            </bkdata-popover>
          </template>
          <template v-else>
            <span class="f14 disabled">
              <i class="bk-icon icon-alert mr5" />
            </span>
          </template>
        </span>
      </div>
    </div>
  </div>
</template>
<script>
import StatusIcons from './StatusIcons';
import { mapState, mapGetters } from 'vuex';
import { postMethodWarning, isChineseChar } from '@/common/js/util.js';

export default {
  components: { StatusIcons },
  props: {
    details: {
      type: Object,
      default: () => {
        return {};
      },
    },
    accessSummaryStatus: {
      type: Object,
      default: () => {
        return {};
      },
    },
  },
  data() {
    return {
      stopTaskDialogSetting: {
        width: '422px',
        title: window.$t('请确认是否停止任务'),
        content: window.$t('任务停止确认详情'),
        theme: 'danger',
      },
      showStopDialog: false,
      showAlertTips: false,
      showWarnigTips: true,
      startLoading: false,
      stopLoading: false,
      accessSummaryInfo: {
        deploy_status: '',
        summary: {},
      },
      // 监控告警列表
      warningList: [],
    };
  },
  computed: {
    fullMessageKey() {
      return `full_message${this.$getLocale() === 'en' ? '_en' : ''}`;
    },
    /** 任务启动停止按钮是否禁用 */
    taskDisable() {
      return this.accessSummaryInfo.task_disable;
    },
    showTipStyle() {
      return {
        'margin-top': `${(this.showAlertTips && 40) || 0}px`,
        transition: 'all .3s ease-in-out',
      };
    },
    ...mapState({
      recentLogInfo: state => state.common.recentLogInfo,
      alertData: state => state.accessDetail.alertData,
    }),
    calcStatus() {
      const sourceStatu = this.accessSummaryInfo.deploy_status;
      const statu =        (/^stopped$/i.test(sourceStatu) && 'stopped')
        || (/^success|running|finish$/i.test(sourceStatu) && 'success')
        || 'exception';

      // 一次性数据类型的状态为finish，需兼容展示
      return {
        state: (/tglog/.test(this.details.data_scenario) && sourceStatu === 'running' && 'success-tglog') || statu,
        message:
          (statu === 'exception' && this.$t('接入任务有异常_重试'))
          || (/tglog/.test(this.details.data_scenario) && this.dataAccessInfo)
          || this.$t('接入停止提示'),
      };
    },
    dataAccessInfo() {
      let info = '';
      if (this.accessSummaryInfo.attribute && this.accessSummaryInfo.attribute.length) {
        this.accessSummaryInfo.attribute.forEach(item => {
          info += item + '、';
        });
        info = info.substr(0, info.length - 1);
        return `${this.$t('数据接入中_TGLOG提示')}${this.$t('您可以联系')}${info}${this.$t('获取详细情况')}`;
      } else {
        return this.$t('数据接入中_TGLOG提示');
      }
    },
    alertTipsStyle() {
      const wordLen = this.calcStatus.message.split('').reduce((pre, curr) => {
        pre += isChineseChar(curr) ? 16.2 : 7.5;
        return pre;
      }, 20);
      // const wordLen = this.calcStatus.message.length * 15.1 + 20
      const showStyle = {
        width: `${wordLen}px`,
        height: 'auto',
      };
      return showStyle;
    },
    rawDataId() {
      return this.$route.params.did;
    },
    recentDate() {
      if (!this.recentLogInfo.created_at) {
        return '---';
      }

      const dateStr = this.recentLogInfo.created_at.substr(0, 10);
      const timeStr = this.recentLogInfo.created_at.substr(11, 8);
      return `${dateStr} ${timeStr}`;
    },
    /**
     * 启动/停止按钮权限控制
     */
    operationBtnInfo() {
      const statusMap = {
        running: {
          start: { text: this.$t('启动任务'), loadingText: this.$t('启动中'), disabled: true },
          stop: { text: this.$t('停止任务'), loadingText: this.$t('停止中'), disabled: false },
        },
        success: {
          start: { text: this.$t('启动任务'), loadingText: this.$t('启动中'), disabled: true },
          stop: { text: this.$t('停止任务'), loadingText: this.$t('停止中'), disabled: false },
        },
        failure: {
          start: { text: this.$t('重试任务'), loadingText: this.$t('重试中'), disabled: false },
          stop: { text: this.$t('停止任务'), loadingText: this.$t('停止中'), disabled: false },
        },
        stopped: {
          start: { text: this.$t('启动任务'), loadingText: this.$t('启动中'), disabled: false },
          stop: { text: this.$t('停止任务'), loadingText: this.$t('停止中'), disabled: true },
        },
      };

      return (
        statusMap[this.accessSummaryInfo.deploy_status] || {
          start: { text: this.$t('启动任务'), loadingText: this.$t('启动中'), disabled: true },
          stop: { text: this.$t('停止任务'), loadingText: this.$t('停止中'), disabled: false },
        }
      );
    },
    isStartBtnDisabled() {
      // 对应状态不可用 或者 正在执行停止操作时不可用 || 一次性数据也不可进行启停操作
      return (
        this.operationBtnInfo.start.disabled || this.stopLoading || this.accessSummaryInfo.deploy_status === 'finish'
      );
    },
    isStopBtnDisabled() {
      // 对应状态不可用 或者 正在执行启动操作时不可用 || 一次性数据也不可进行启停操作
      return (
        this.operationBtnInfo.stop.disabled || this.startLoading || this.accessSummaryInfo.deploy_status === 'finish'
      );
    },
  },
  watch: {
    'details.data_scenario': {
      handler(val) {
        val && this.handleSummaryData(this.accessSummaryStatus);
      },
      immediate: true,
    },
    accessSummaryStatus: {
      immediate: true,
      handler(val) {
        if (this.details.data_scenario) {
          this.handleSummaryData(val);
        }
      },
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
  methods: {
    cancelFn() {
      this.showStopDialog = false;
    },
    stopConfirm() {
      if (!this.isStopBtnDisabled) {
        this.showStopDialog = true;
      }
    },
    handlViewWarning() {
      this.showWarnigTips = !this.showWarnigTips;
      setTimeout(() => {
        this.showWarnigTips = true;
        this.$router.push(`/data-access/data-detail/${this.rawDataId}/5?from=pre`);
      }, 500);
    },
    handlCloseTips() {
      this.showAlertTips = !this.showAlertTips;
    },
    handleSummaryData(data = {}) {
      if (!Object.keys(data).length) return;
      const item = data;

      // 现有两种状态：
      // 一种会有从运行到结束全链路状态的类型
      // 一种是一次性状态的类型，例如文件上传，上传成功后流程结束，只会有一个finish状态
      if (item.summary.hasOwnProperty('finish')) {
        item.summary.success = item.summary.finish;
      }

      this.accessSummaryInfo = item;

      // 更新state中部署状态的存储信息
      this.$store.commit('updateCommonState', {
        accessSummaryInfo: item,
      });

      this.$nextTick(() => {
        this.showAlertTips =          !/^success|running|finish$/i.test(this.accessSummaryInfo.deploy_status)
          || (/tglog/.test(this.details.data_scenario) && /^running$/i.test(this.accessSummaryInfo.deploy_status));

        // this.$forceUpdate()
      });
    },
    /**
     * 获取状态数据统计
     */
    getStatusSummary(closeLoadingFn) {
      this.bkRequest
        .httpRequest('dataAccess/getAccessSummary', {
          params: {
            raw_data_id: this.rawDataId,
          },
        })
        .then(res => {
          if (res.result) {
            this.$store.commit('accessDetail/setAccessSummaryStatus', res.data);
            this.handleSummaryData(res.data);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          closeLoadingFn && closeLoadingFn();
        });
    },
    /**
     * 启动整个源数据采集
     */
    startCollectorhub() {
      if (this.startLoading || this.isStartBtnDisabled) {
        return;
      }
      this.startLoading = true;

      // scope默认为空list, 当前场景不存在scope字段时为空list
      let scopeParam = [];
      const scopeField = this.details.access_conf_info.resource.scope;
      if (scopeField && Array.isArray(scopeField)) {
        scopeParam = this.details.access_conf_info.resource.scope.map(item => Object.assign(
          {},
          {
            deploy_plan_id: item.deploy_plan_id,
            modules: item.module_scope,
          },
          {
            hosts: item.host_scope || [],
          }
        )
        );
      }

      this.bkRequest
        .request(`v3/access/collectorhub/${this.rawDataId}/start/`, {
          method: 'post',
          useSchema: false,
          params: {
            data_scenario: this.details.data_scenario,
            bk_biz_id: this.details.bk_biz_id,
            config_mode: 'full',
            scope: scopeParam,
          },
        })
        .then(res => {
          if (res.result) {
            postMethodWarning(this.$t('任务启动成功'), 'success');
            this.getStatusSummary();
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.startLoading = false;
        });
    },
    /**
     * 停止整个源数据采集
     */
    stopCollectorhub() {
      this.showStopDialog = false;
      if (this.stopLoading || this.isStopBtnDisabled) {
        return;
      }
      this.stopLoading = true;

      // scope默认为空list, 当前场景不存在scope字段时为空list
      let scopeParam = [];
      const scopeField = this.details.access_conf_info.resource.scope;
      if (scopeField && Array.isArray(scopeField)) {
        scopeParam = this.details.access_conf_info.resource.scope.map(item => Object.assign(
          {},
          {
            deploy_plan_id: item.deploy_plan_id,
            modules: item.module_scope,
          },
          {
            hosts: item.host_scope || [],
          }
        )
        );
      }

      this.bkRequest
        .request(`v3/access/collectorhub/${this.rawDataId}/stop/`, {
          method: 'post',
          useSchema: false,
          params: {
            data_scenario: this.details.data_scenario,
            bk_biz_id: this.details.bk_biz_id,
            config_mode: 'full',
            scope: scopeParam,
          },
        })
        .then(res => {
          if (res.result) {
            postMethodWarning(this.$t('任务停止成功'), 'success');
            this.getStatusSummary();
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.stopLoading = false;
        });
    },
    /**
     * 获取告警列表
     */
    getWarningList() {
      this.bkRequest
        .httpRequest('dataAccess/getWarningList', {
          params: {
            raw_data_id: this.rawDataId,
          },
          query: {
            flow_id: `rawdata${this.rawDataId}`,
            alert_config_ids: [this.alertData.id],
            dimensions: JSON.stringify({
              module: 'collector',
            }),
          },
        })
        .then(res => {
          if (res.result) {
            this.warningList = res.data || [];
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
    },
  },
};
</script>

<style lang="scss" scoped>
.tippy-popper {
  .inner-title {
    padding: 5px 0 15px;
  }

  .bk-table {
    border-top: 0;
  }

  .bk-table > thead > tr > th,
  .bk-table > thead > tr > td,
  .bk-table > tbody > tr > th,
  .bk-table > tbody > tr > td {
    line-height: 20px;
    padding: 5px 20px;
    border-top: 1px solid #fff;
    color: #fff;

    &:first-child {
      padding: 5px;
    }
  }

  .preview-all {
    color: #288ce2;
    display: flex;
    justify-content: center;
    padding: 10px;
  }
}
</style>

<style lang="scss" scoped>
.access-preview-container {
  margin-bottom: 20px;
  padding-top: 10px;
  .access-preview {
    background: #fafafa;
    border: solid 1px #d9dfe5;
    display: flex;
    align-items: center;
    justify-content: space-between;
    height: 45px;
    padding-left: 12px;
    .access-preview-left {
      display: flex;
      justify-content: space-between;
      width: 100%;
      padding-right: 30px;
      .access-runing-state {
        display: flex;
        position: relative;
        .access-alert {
          position: absolute;
          width: 350px;
          top: -20px;
          left: -15px;
          transform: translate(0%, -100%);
          color: inherit;
          padding: 10px 30px 10px 10px;
          border-radius: 2px;

          &.en {
            &::after {
              left: 130px;
            }
          }

          &.exception,
          &.success-tglog {
            background: #fbdddd;
            &::after {
              border-color: #fbdddd transparent transparent transparent;
            }
          }

          &.success {
            color: #8dc358;
            &::after {
              border-color: #8dc358 transparent transparent transparent;
            }
          }

          &.stopped {
            color: #fff;
            background: #3a84ff;
            &::after {
              border-color: #3a84ff transparent transparent transparent;
            }
          }

          &::after {
            position: absolute;
            border: 8px solid #eaeaea;
            border-color: #eaeaea transparent transparent transparent;
            border-right-width: 10px;
            left: 112px;
            bottom: -15px;
            // transform: translateX(-50%);
            content: '';
          }

          .tips-alert-close {
            cursor: pointer;
            position: absolute;
            right: 5px;
            top: 50%;
            transform: translateY(-50%);
            //   color:red;
          }
        }

        .running-text {
          position: relative;
          padding-right: 15px;
          display: flex;
          align-items: center;

          .running-statu-text {
            min-width: 30px;
            height: 20px;
            &.exception {
              color: #f54d17;
            }

            &.stopped {
              color: #63656e;
            }

            &.success,
            &.success-tglog {
              color: #8dc358;
            }

            position: relative;
          }
          &::after {
            position: absolute;
            border: 8px solid #eaeaea;
            border-color: transparent #eaeaea transparent transparent;
            border-right-width: 10px;
            top: 50%;
            left: calc(100% - 8px);
            transform: translate(-50%, -50%);
            content: '';
          }
        }
      }

      .operation-list {
        display: flex;
        align-items: center;
        .user {
          background: #efefef;
          border: solid 1px #c0c9d3;
          padding: 2px 5px;
          margin-left: 10px;
          border-radius: 2px;
          height: 25px;
          min-width: 50px;
        }
      }
    }
    .access-preview-right {
      display: flex;
      align-items: center;
      background: #efefef;
      height: 100%;
      span {
        display: flex;
        justify-content: center;
        align-items: center;
        margin: 5px;
        font-size: 18px;
        cursor: pointer;

        .icon-restart {
          font-weight: 900;
          font-size: 12px;
        }

        &.is-loading {
          cursor: not-allowed;
        }

        &.access-running-statu {
          min-width: 60px;
          color: #fb6118;
          span {
            min-width: 50px;
          }
          .table-tooltip {
            &:focus {
              outline: none;
            }
            ::v-deep .bk-tooltip-ref {
              &:focus {
                outline: none;
              }
            }
          }
          .f14 {
            &:focus {
              outline: none;
            }
          }
        }

        &.disabled {
          color: #ccc;
          cursor: not-allowed;
        }

        &:hover {
          background: #d8d9db;
        }
      }

      .access-running-statu {
        .access-warning {
          font-size: 12px;
          i {
            font-size: 14px;
            font-weight: 900;
          }
        }
      }
    }
  }
}

.bk-table {
  &.access-tips {
    width: 100%;
    border-collapse: collapse;

    tbody {
      tr {
        background: transparent;
        border-collapse: collapse;
        td {
          border-collapse: collapse;
          padding: 5px;
          color: #fff;
          // border-top: none;
          border-bottom: none;
        }
      }
    }
  }
}

.icon-refresh.loading {
  -webkit-animation: rotating 2s linear infinite;
  -moz-animation: rotating 2s linear infinite;
  -ms-animation: rotating 2s linear infinite;
  -o-animation: rotating 2s linear infinite;
  animation: rotating 2s linear infinite;
}

.warning-text {
  width: 250px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
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

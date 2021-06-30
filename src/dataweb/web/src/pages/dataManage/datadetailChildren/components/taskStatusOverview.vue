

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
      </div>
      <div class="access-preview-right">
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
import StatusIcons from '@/pages/DataAccess/Details/StatusIcons';
import { mapState } from 'vuex';
import { isChineseChar } from '@/common/js/util.js';

export default {
  components: { StatusIcons },
  props: {
    details: {
      type: Object,
      default: () => ({}),
    },
    accessSummaryStatus: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      showAlertTips: false,
      showWarnigTips: true,
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
    showTipStyle() {
      return {
        'margin-top': `${(this.showAlertTips && 40) || 0}px`,
        transition: 'all .3s ease-in-out',
      };
    },
    ...mapState({
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
        pre += isChineseChar(curr) ? 16.1 : 7.5;
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
  },

  mounted() {
    this.getDefaultData();
  },
  methods: {
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
    getDefaultData() {
      if (Object.keys(this.alertData).length) {
        this.getWarningList();
      } else {
        const params = {
          raw_data_id: this.$route.params.did,
        };
        return this.bkRequest.httpRequest('dataAccess/getAlertConfigIds', { params }).then(res => {
          if (res && res.result) {
            this.$store.commit('accessDetail/setAlertData', res.data);
            this.getWarningList();
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
      }
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
    }
    .access-preview-right {
      display: flex;
      align-items: center;
      background: #efefef;
      height: 100%;
      span {
        width: 26px;
        height: 26px;
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

        &.access-running-statu {
          width: 60px;
          color: #fb6118;
          span {
            min-width: 50px;
            justify-content: flex-start;
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

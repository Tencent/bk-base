

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
  <div v-if="Object.keys(dataInfo).length"
    class="detail-info">
    <div class="detail-info-left fl">
      <p :title="dataInfo.result_table_id"
        class="detail-title fl">
        {{ $t('配置名称') }}: {{ dataInfo.name }}
      </p>
      <span :class="['bk-tag is-fill ml15', cleanClass]">{{ dataInfo.status }}</span>
    </div>
    <div class="detail-info-right fr">
      <div class="list-time clearfix fl">
        <p>{{ $t('最后操作') }}：{{ dataInfo.updatedAt }}</p>
        <span :title="dataInfo.updatedBy || '-'"
          class="label text-overflow">
          {{ dataInfo.updatedBy || '-' }}
        </span>
      </div>
      <div class="list-button clearfix fr">
        <bkdata-button
          :loading="loading.startLoading"
          :disabled="canStart"
          theme="default"
          :title="$t('启动')"
          icon="play-shape"
          @click="$emit('start')" />
        <bkdata-button
          :disabled="canStop"
          :loading="loading.stopLoading"
          theme="default"
          :title="$t('停止')"
          icon="stop-shape"
          @click="$emit('stop')" />
        <bkdata-button theme="default"
          :title="$t('编辑')"
          icon="edit"
          @click="$emit('edit')" />
      </div>
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
</template>

<script>
import { mapState } from 'vuex';

export default {
  props: {
    dataInfo: {
      type: Object,
      default: () => ({}),
    },
    loading: {
      type: Object,
      default: () => {
        return {
          stopLoading: false,
          startLoading: false,
        };
      },
    },
    canStart: {
      type: Boolean,
      default: false,
    },
    canStop: {
      type: Boolean,
      default: false,
    },
    cleanClass: {
      type: String,
      default: '',
    },
    dataType: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      // 监控告警列表
      warningList: [],
      showWarnigTips: true,
    };
  },
  computed: {
    ...mapState({
      alertData: state => state.accessDetail.alertData,
    }),
    fullMessageKey() {
      return `full_message${this.$getLocale() === 'en' ? '_en' : ''}`;
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
        this.$router.push(`/data-access/data-detail/${this.$route.params.did}/5?from=pre`);
      }, 500);
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
    /**
     * 获取告警列表
     */
    getWarningList() {
      this.bkRequest
        .httpRequest('dataAccess/getWarningList', {
          params: {
            raw_data_id: this.$route.params.did,
          },
          query: {
            flow_id: `rawdata${this.$route.params.did}`,
            alert_config_ids: [this.alertData.id],
            dimensions: JSON.stringify({
              module: this.dataType === 'clean' ? 'clean' : 'shipper',
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
.access-running-statu {
  display: flex;
  justify-content: center;
  color: #fb6118;
  background: #efefef;
  padding: 0 5px;
  &:hover {
    background: #d8d9db;
  }
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
.detail-info {
  margin-top: 15px;
  width: 100%;
  height: 48px;
  line-height: 46px;
  padding: 0 0 0 20px;
  border: 1px solid #d9dfe5;
  background: #fafafa;
  .detail-title {
    font-weight: bold;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: calc(100% - 60px);
  }
  .bk-tag {
    line-height: 24px;
    height: 24px;
  }
  .running,
  .started {
    background: #9dcb6b;
    border-color: #9dcb6b;
  }
  .failed {
    background: #ff5555;
    border-color: #ff5555;
  }
  .detail-info-left {
    width: calc(100% - 545px);
    display: flex;
    align-items: center;
  }
  .detail-info-right {
    max-width: 545px;
    display: flex;
    flex-wrap: nowrap;
    justify-content: space-between;
  }
  .list-time {
    flex: 0.5;
    display: flex;
    flex-wrap: nowrap;
    align-items: center;
    p {
      white-space: nowrap;
    }
    .label {
      display: inline-block;
      height: 26px;
      line-height: 24px;
      border: 1px solid #c3cdd7;
      background: #f2f2f2;
      padding: 0 8px;
      border-radius: 2px;
      margin: 0 15px;
      max-width: 130px;
    }
  }
  ::v-deep .list-button {
    display: flex;
    flex-wrap: nowrap;
    flex: 0.5;
    background: #efefef;
    border-left: 1px solid #d9dfe5;
    span {
      display: block;
      width: 46px;
      line-height: 46px;
      text-align: center;
      font-size: 16px;
      float: left;
      cursor: pointer;
      &:hover {
        background: #e1ecff;
      }
    }
    .bk-icon {
      font-size: 14px !important;
    }
    .bk-icon-button,
    .bk-button {
      display: block;
      width: 48px;
      height: 48px;
      line-height: 48px;
      border: none;
      background: none;
      font-size: 16px;
      float: left;
      &:hover {
        background-color: #e1ecff;
      }
      &.is-disabled {
        background: none !important;
      }
    }
    .bk-button {
      padding: 0;
    }
    .bk-icon {
      width: 48px;
      height: 48px;
      line-height: 48px;
    }
    .disable {
      opacity: 0.6;
    }
    .icon-stop-shape {
      font-size: 24px;
    }
  }
}
</style>

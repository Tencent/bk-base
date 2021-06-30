

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
  <div>
    <bkdata-dialog v-model="isModalShow"
      extCls="bkdata-dialog"
      :hasHeader="true"
      :hasFooter="true"
      :loading="createAlertShieldLoading"
      :okText="$t('屏蔽')"
      :cancelText="$t('取消')"
      :width="900"
      :padding="0"
      @confirm="shieldAlert"
      @cancel="closeModal">
      <div class="alert-shield-modal">
        <div class="hearder">
          <div class="text fl">
            <p class="title">
              <i class="bk-icon icon-notification" />{{ $t('屏蔽告警') }}
            </p>
            <p>{{ $t('告警被屏蔽后将在一段时间内不再发送通知_但可以在用户中心') }}</p>
          </div>
        </div>
        <div class="content">
          <div class="bk-form">
            <div class="bk-form-item">
              <label class="bk-label"><span class="required">*</span>{{ $t('告警屏蔽时间') }}：</label>
              <div class="bk-form-content">
                <div class="bk-inline-form">
                  <bkdata-input v-model="alertShieldForm.alertShieldTime"
                    type="number"
                    :min="1"
                    :placeholder="$t('屏蔽时间')"
                    :style="{ width: '70px', 'margin-right': '3px' }" />
                  <bkdata-selector :displayKey="'name'"
                    :list="alertShieldForm.timeUnitList"
                    :placeholder="$t('时间单位')"
                    :selected.sync="alertShieldForm.alertShieldTimeUnit"
                    :settingKey="'key'"
                    :style="{ width: '100px' }" />
                </div>
              </div>
            </div>
            <div v-if="alertConfigList.length > 0"
              class="bk-form-item">
              <label class="bk-label"><span class="required">*</span>{{ $t('告警屏蔽对象') }}：</label>
              <div class="bk-form-content">
                <div v-for="alertConfig in alertConfigList"
                  :key="alertConfig.id">
                  {{ getAlertTargetTitle(alertConfig) }}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </bkdata-dialog>
  </div>
</template>
<script>
import moment from 'moment';
import { postMethodWarning } from '@/common/js/util.js';

export default {
  props: {},
  data() {
    return {
      isModalShow: false,
      createAlertShieldLoading: false,
      alertShieldForm: {
        alertShieldTime: 1,
        alertShieldTimeUnit: 'hours',
        timeUnitList: [
          {
            name: this.$t('小时s'),
            key: 'hours',
          },
          {
            name: this.$t('分钟s'),
            key: 'minutes',
          },
          {
            name: this.$t('天(s)'),
            key: 'days',
          },
        ],
      },
      alertInfo: null,
      alertConfigList: [],
    };
  },
  mounted() {},
  methods: {
    /**
     * 打开模态框
     */
    openModal(alertInfo) {
      this.alertInfo = alertInfo;
      this.isModalShow = true;
    },
    /**
     * 批量屏蔽多个告警对象的告警策略
     */
    openModalByConfig(alertConfigs) {
      this.alertConfigList = alertConfigs;
      this.isModalShow = true;
    },
    /**
     * 关闭模态框，并清空表单
     */
    closeModal() {
      this.alertInfo = null;
      this.alertConfigList = [];
      this.isModalShow = false;
      this.$nextTick(() => {
        this.$emit('closeAlertShieldModal', false);
      });
    },
    /**
     * 按照当前告警的维度屏蔽相同告警
     */
    shieldAlert() {
      const start_time = moment().format('YYYY-MM-DD HH:mm:ss');
      const end_time = moment()
        .add(this.alertShieldForm.alertShieldTime, this.alertShieldForm.alertShieldTimeUnit)
        .format('YYYY-MM-DD HH:mm:ss');

      const options = {
        params: {},
      };
      if (this.alertInfo !== null) {
        options.params = {
          start_time: start_time,
          end_time: end_time,
          alert_code: this.alertInfo.alert_code,
          dimensions: {
            flow_id: this.alertInfo.flow_id,
          },
          reason: this.$t('用户屏蔽告警'),
        };
      } else if (this.alertConfigList.length > 0) {
        options.params = {
          start_time: start_time,
          end_time: end_time,
          alert_config_id: this.alertConfigList[0].id,
          reason: this.$t('用户屏蔽告警'),
        };
      }
      this.bkRequest.httpRequest('dmonitorCenter/dmonitorAlertShield', options).then(res => {
        if (res.result) {
          postMethodWarning(this.$t('屏蔽告警成功'), 'success');
          this.$emit('closeAlertShieldModal', false);
        } else {
          postMethodWarning(res.message, 'error');
        }
      });
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
  },
};
</script>
<style lang="scss" scoped>
::v-deep .bk-dialog-wrapper {
  .bk-dialog-body {
    padding: 0;
  }
}
</style>
<style media="screen" lang="scss">
.alert-shield-modal {
  .hearder {
    height: 70px;
    background: #23243b;
    position: relative;
    .close {
      display: inline-block;
      position: absolute;
      right: 0;
      top: 0;
      width: 40px;
      height: 40px;
      line-height: 40px;
      text-align: center;
      cursor: pointer;
    }

    .text {
      font-size: 12px;
      padding-left: 18px;
      .icon-notification {
        font-size: 18px;
        color: #abacb5;
        margin-left: 6px;
      }
    }
    .title {
      margin: 16px 0 3px;
      color: #fafafa;
      font-size: 18px;
      padding-left: 0;
      display: flex;
      align-items: center;
    }
  }
  .content {
    padding: 30px 0 45px 0;
    .bk-form {
      width: 600px;
      .bk-label {
        padding: 11px 16px 11px 10px;
        width: 180px;
      }
      .bk-form-content {
        margin-left: 180px;

        .bk-inline-form {
          display: flex;
        }
        .bk-tip-text {
          color: #ff5656;
        }
        .sample-data-width {
          width: 100px;
        }
        & > span {
          height: 36px;
          line-height: 36px;
          margin-left: 8px;
        }
      }
    }
    .required {
      color: #f64646;
      display: inline-block;
      vertical-align: -2px;
      margin-right: 5px;
    }
    .bk-button {
      min-width: 120px;
    }
  }
}
</style>



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
  <div />
</template>
<script>
import moment from 'moment';
import { showMsg } from '@/common/js/util.js';
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
            name: '小时',
            key: 'hours',
          },
          {
            name: '分钟',
            key: 'minutes',
          },
          {
            name: '天',
            key: 'days',
          },
        ],
      },
      alertInfo: null,
      alertTargetList: [],
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
    openModalByTarget(alertTargets) {
      this.alertTargetList = alertTargets;
      this.isModalShow = true;
    },
    /**
     * 关闭模态框，并清空表单
     */
    closeModal() {
      this.alertInfo = null;
      this.alertTargetList = [];
      this.isModalShow = false;
      this.$emit('closeAlertShieldModal', false);
    },
    /**
     * 按照当前告警的维度屏蔽相同告警
     */
    shieldAlert() {
      const start_time = moment().format('YYYY-MM-DD HH:mm:ss');
      const end_time = moment()
        .subtract(this.alertShieldForm.alertShieldTime, this.alertShieldForm.alertShieldTimeUnit)
        .format('YYYY-MM-DD HH:mm:ss');
      const options = {
        params: {
          start_time: start_time,
          end_time: end_time,
          alert_code: this.alertInfo.alert_code,
          dimensions: {
            flow_id: this.alertInfo.alert_target_id,
          },
          reason: this.$t('用户屏蔽告警'),
        },
      };
      this.bkRequest.httpRequest('dmonitorCenter/dmonitorAlertShield', options).then(res => {
        if (res.result) {
          postMethodWarning(this.$t('屏蔽告警成功'), 'success');
          this.$emit('closeAlertShieldModal', false);
        } else {
          postMethodWarning(res.message, 'error');
        }
      });
    },
  },
};
</script>
<style media="screen" lang="scss">
.alert-shield-modal {
  .hearder {
    height: 118px;
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
    .icon {
      font-size: 32px;
      color: #abacb5;
      line-height: 118px;
      width: 142px;
      text-align: right;
      margin-right: 16px;
    }
    .text {
      font-size: 12px;
    }
    .title {
      margin: 45px 0 3px 0;
      color: #fafafa;
      font-size: 18px;
    }
  }
  .content {
    padding: 30px 0 45px 0;
    .bk-form {
      width: 480px;
      .bk-label {
        padding: 11px 16px 11px 10px;
        width: 180px;
      }
      .bk-form-content {
        display: block;
        margin-left: 180px;
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

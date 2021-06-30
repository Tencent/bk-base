

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
  <div class="bkdata-dialog">
    <bkdata-dialog
      v-model="isShow"
      extCls="bkdata-dialog-1"
      :loading="isLoading"
      :draggable="false"
      :hasHeader="false"
      :hasFooter="true"
      :closeIcon="true"
      :maskClose="false"
      :okText="$t('确认')"
      :cancelText="$t('取消')"
      :width="800"
      @confirm="saveData"
      @cancel="closeDialog">
      <div>
        <div class="title app-title">
          {{ $t('权限续期') }}
        </div>
        <p class="info-tip">
          <span class="icon-exclamation-circle" />{{
            $t('请在未过期前完成续期_当过期时间超过15天_还未进行续期的DataToken将自动移除')
          }}
        </p>
        <form class="bk-form">
          <div class="bk-form-item is-required mb20">
            <label class="bk-label pr15">{{ $t('有效期') }}</label>
            <div class="bk-form-content">
              <bkdata-selector
                class="normal-width"
                :selected.sync="selectedExpire"
                :list="expireList"
                :searchable="true"
                :settingKey="'value'"
                :displayKey="'name'"
                :searchKey="'name'" />
            </div>
          </div>
        </form>
      </div>
    </bkdata-dialog>
  </div>
</template>

<script>
import { showMsg } from '@/common/js/util.js';

export default {
  props: {
    authId: {
      type: String,
      default: null,
    },
    isOpen: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      selectedExpire: 7, // 必填项，给默认值
      expireList: [
        {
          name: `7 ${this.$t('天s')}`,
          value: 7,
        },
        {
          name: `1 ${this.$t('月s')}`,
          value: 30,
        },
        {
          name: `3 ${this.$t('月s')}`,
          value: 90,
        },
        {
          name: `1 ${this.$t('年')}`,
          value: 365,
        },
      ],
      isShow: this.isOpen,
      isLoading: false,
    };
  },
  watch: {
    isOpen(newVal) {
      this.isShow = newVal;
    },
    isShow(newVal) {
      this.$emit('update:isOpen', newVal);
    },
  },
  methods: {
    saveData() {
      this.isLoading = true;
      this.bkRequest
        .httpRequest('auth/setPermissionRenewal', {
          params: {
            id: this.authId,
            expire: this.selectedExpire,
          },
        })
        .then(res => {
          if (res.result) {
            showMsg(window.$t('权限续期成功'), 'success', { delay: 2000 });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
          this.isLoading = false;
          this.closeDialog(true);
        });
    },
    closeDialog(isFresh = false) {
      this.isShow = false;
      this.$emit('closeDialog', isFresh);
    },
  },
};
</script>
<style lang="scss" scoped>
::v-deep .bk-dialog-wrapper .bk-dialog-body {
  padding: 3px 24px 26px !important;
}
.info-tip {
  margin: 20px 0;
  color: rgb(99, 101, 110);
  font-size: 12px;
  font-family: MicrosoftYaHei;
  display: flex;
  justify-content: flex-start;
  align-items: center;
  padding: 10px 25px 10px 10px;
  background: rgb(255, 244, 226);
  border: 1px solid rgb(255, 184, 72);
  border-radius: 2px;
  .icon-exclamation-circle {
    margin-right: 10px;
  }
}
.bk-form {
  width: 500px;
}
</style>
<style lang="scss">
// @import '../scss/base.scss';
.app-title {
  display: flex;
  align-items: center;
  justify-content: flex-start;
  height: 30px;
  line-height: 30px;

  .per-warning-tips {
    width: calc(100% - 180px);
    margin-left: 56px;
    background: #ffeded;
    border-color: #fd9c9c;
    padding: 5px 10px;
    color: #ff5656;

    i {
      width: 18px;
      height: 18px;
      line-height: 18px;
      font-size: 14px;
      text-align: center;
      color: #ffffff;
      border-radius: 50%;
      background: #ea3636;
      margin-right: 5px;
    }
  }
}

.bkdata-form-horizontal {
  .form-item {
    margin-bottom: 10px;

    label.label-title {
      float: left;
      margin-top: 5px;
      width: 80px;
    }

    .bk-form-content {
      margin-left: 80px;
      .apply-rule-select {
        width: 100%;
        .auth-role-wrap-content {
          padding: 0 !important;
        }
      }
    }
  }
  .item-inline-wrap {
    display: flex;
    justify-content: space-between;
    .form-item {
      margin-bottom: 10px;

      label.label-title {
        float: left;
        margin-top: 5px;
        width: 80px;
      }

      .bk-form-content {
        margin-left: 80px;
      }
    }
    .item-inline {
      display: inline-block;
      flex: 0.48;
    }
    .flex100 {
      flex: 1;
    }
  }
}

.apply-form {
  padding-left: 60px;
  padding-right: 60px;
  padding-top: 30px;
}
</style>

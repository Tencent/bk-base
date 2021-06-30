

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
    class="data-detail-container"
    :crumbName="[{ name: $t('数据源列表'), to: '/data-access/' }, { name: `${$t('数据详情')}(${$route.params.did})` }]"
    :withMargin="isSmallScreen">
    <div v-bkloading="{ isLoading: loading }"
      class="data-detail-new">
      <section v-show="!loading"
        class="content clearfix">
        <div class="left-content">
          <left-content :details="details" />
        </div>
        <div class="right-contents">
          <right-content class="no-border"
            :details="details"
            :delpoyedList="delpoyedList" />
        </div>
      </section>
      <bkdata-dialog
        v-model="billDetailData.isShow"
        :closeIcon="false"
        :extCls="'tdm-ticket-dialog'"
        :loading="billDetailData.loading"
        :cancelText="$t('关闭')"
        :title="billDetailData.title">
        <div>
          {{ $t('当前接入流程正在审核中') }}，{{ $t('请')
          }}<a v-if="ticketDetailId"
            href="jacascript:;"
            @click="showTicketDetail">
            {{ $t('查看详情') }}
          </a><span v-else>{{ $t('联系管理员') }}</span>
        </div>
      </bkdata-dialog>
      <bkdata-sideslider :quickClose="true"
        :isShow.sync="isShowTicketDetail"
        :width="560"
        :title="$t('单据详情')">
        <template slot="content">
          <TicketDetail :stateId="ticketDetailId"
            :statusInfos="statusInfos" />
        </template>
      </bkdata-sideslider>
    </div>

    <!-- 无权限申请 -->
    <PermissionApplyWindow
      ref="permissionApply"
      :isOpen.sync="permissionShow"
      :defaultSelectValue="applySelected"
      @closeDialog="linkToSourceList" />
  </Layout>
</template>

<script>
import Layout from '../../components/global/layout';
import PermissionApplyWindow from '@/pages/authCenter/permissions/PermissionApplyWindow';
import { mapState } from 'vuex';

export default {
  components: {
    leftContent: () => import('./datadetailChildren/dataAccessLeftContent'),
    rightContent: () => import('./datadetailChildren/dataAccessRightContent'),
    TicketDetail: () => import('@/pages/authCenter/ticket/Detail'),
    Layout,
    PermissionApplyWindow,
  },
  data() {
    return {
      permissionShow: false,
      applySelected: {
        objectClass: 'raw_data',
      },
      bizId: '',
      details: {},
      loading: true,
      delpoyedList: {},
      billDetailData: {
        title: this.$t('流程审批提示'),
        isShow: false,
        loading: false,
      },
      statusInfos: {
        pending: {
          id: 'pending',
          name: '待处理',
          location: 'head',
          class: 'label-primary',
          actions: [],
        },
        processing: {
          id: 'processing',
          name: '处理中',
          location: 'middle',
          class: 'label-primary',
          actions: ['withdraw'],
        },
        succeeded: {
          id: 'succeeded',
          name: '已同意',
          location: 'tail',
          class: 'label-success',
          actions: [],
        },
        failed: {
          id: 'failed',
          name: '已驳回',
          location: 'tail',
          class: 'label-danger',
          actions: ['resubmit'],
        },
        stopped: {
          id: 'stopped',
          name: '已终止',
          location: 'stopped',
          class: 'label-danger',
          actions: [],
        },
      },
      isShowTicketDetail: false,
      ticketDetailId: null,
    };
  },

  computed: {
    isSmallScreen() {
      return document.body.clientWidth < 1441;
    },
    ...mapState({
      alertData: state => state.accessDetail.alertData,
    }),
    sourceId() {
      return parseInt(this.$route.params.did);
    },
  },
  watch: {
    permissionShow(val, old) {
      if (old && !val) {
        setTimeout(() => {
          this.getDetail(true);
        }, 500);
      }
    },
  },
  async mounted() {
    this.getBizIdBySourceId();
    const result = await this.getDetail(true);
    result && this.getDefaultData();
  },
  beforeDestroy() {
    this.$store.dispatch('updateBizId', 0);
  },
  methods: {
    linkToSourceList() {
      window.history.back();
    },
    getDefaultData() {
      if (!Object.keys(this.alertData).length) {
        const params = {
          raw_data_id: this.sourceId,
        };
        return this.bkRequest.httpRequest('dataAccess/getAlertConfigIds', { params }).then(res => {
          if (res && res.result) {
            this.$store.commit('accessDetail/setAlertData', res.data);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
      }
    },
    showTicketDetail() {
      this.isShowTicketDetail = true;
      this.billDetailData.isShow = false;
    },
    getDetail(statue) {
      if (!this.sourceId) return;
      if (statue) {
        this.loading = true;
      }
      return this.$store
        .dispatch('getDelpoyedList', {
          params: { raw_data_id: this.sourceId },
          query: { show_display: 1 },
        })
        .then(res => {
          if (res.result) {
            const items = res.data;
            this.details = items;
            // 接入类型为tdw并且没有权限时，查询是否审批完成
            if (this.details.data_scenario === 'tdm' && this.details.access_raw_data.permission === 'access_only') {
              this.checkApproval();
            }
            this.$set(this.details, 'data_id', this.sourceId);
            this.delpoyedList = (items.access_conf_info && items.access_conf_info.resource) || {};
            if (this.$store.getters.getBizId !== items.bk_biz_id) {
              this.$store.dispatch('updateBizId', items.bk_biz_id);
            }
            return true;
          } else {
            this.getMethodWarning(res.message, res.code);
            this.permissionShow = true;
            return false;
          }
        })
        ['finally'](() => {
          this.loading = false;
        });
    },
    checkApproval() {
      this.billDetailData.loading = true;
      const options = {
        query: {
          ticket_type: 'verify_tdm_data',
          status: 'processing',
          process_id: this.sourceId,
        },
      };
      this.bkRequest.httpRequest('auth/getMethodTicketsList', options).then(res => {
        if (res.result) {
          this.billDetailData.isShow = true;
          if (res.data[0]) {
            this.ticketDetailId = res.data[0].state_id;
          }
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      });
      this.billDetailData.loading = false;
    },
    getBizIdBySourceId() {
      this.bkRequest
        .httpRequest('dataAccess/getBizIdBySourceId', {
          params: {
            sourceId: this.sourceId,
          },
        })
        .then(res => {
          if (res.result) {
            this.bizId = res.data.bk_biz_id.toString();
            console.log(res.data.bk_biz_id);
            this.$refs.permissionApply.selectedValue.bizId = this.bizId;
            this.$nextTick(() => {
              this.$refs.permissionApply.selectedValue.scopeId = this.sourceId;
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
::v-deep .tdm-ticket-dialog {
  .bk-dialog-footer {
    .footer-wrapper {
      button[name='confirm'] {
        display: none;
      }
    }
  }
}
</style>
<style lang="scss">
.layout-content {
  &.with-margin {
    margin: 0 50px;
  }
}
.data-detail-container {
  .layout-header {
    padding: 0 30px !important;
  }
  @media (min-width: 1400px) {
    .layout-header {
      padding: 0 70px !important;
    }
  }

  .layout-body {
    display: flex;

    .layout-content {
      width: 100%;
      display: flex;
    }
  }
}
.data-detail-new {
  display: flex;
  min-height: 300px;
  width: 100%;
  .el-checkbox__input.is-disabled.is-checked .el-checkbox__inner {
    background: #3a84ff;
  }
  > .content {
    margin: 0 auto;
    width: 1600px;
    justify-content: center;
    display: flex;
    .left-content {
      margin-right: 5px;
      min-width: 410px;
      width: 25%;
    }
    .right-contents {
      width: 75%;
      min-width: 1250px;
      .no-border {
        border: 0;
      }
    }
    .shadows {
      box-shadow: 2px 3px 5px 0px rgba(33, 34, 50, 0.15);
      border-radius: 2px;
      border: solid 1px rgba(195, 205, 215, 0.6);
    }
  }
  .type {
    height: 55px;
    line-height: 55px;
    padding: 0px 15px;
    font-weight: bold;
    &::before {
      content: '';
      width: 2px;
      height: 19px;
      background: #3a84ff;
      display: inline-block;
      margin-right: 15px;
      position: relative;
      top: 4px;
    }
    .name {
      display: inline-block;
      min-width: 58px;
      height: 24px;
      color: white;
      line-height: 24px;
      background-color: #737987;
      border-radius: 2px;
      text-align: center;
      margin-top: 16px;
      padding: 0 5px;
    }
    .bk-icon {
      color: #3a84ff;
      cursor: pointer;
    }
  }
}
</style>

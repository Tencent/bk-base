

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
  <Layout :crumbName="[{ name: $t('申请列表'), to: '/auth-manage/' }, { name: $t('申请详情') }]">
    <div class="apply-auth">
      <div v-bkloading="{ isLoading: loading }"
        class="data-detail clearfix">
        <div v-show="openPage"
          class="auth-content fl pr30">
          <div class="auth-detail">
            <div class="auth-data-detail">
              <div class="title">
                <span class="before-mark mr15" />
                <span class="text">{{ $t('单据详情') }} ({{ ticketInfo.ticket_type_display }})</span>
              </div>
              <div class="auth-content-table">
                <table>
                  <template v-if="ticketInfo.ticket_type === 'project_biz'">
                    <tr>
                      <td class="key pr20">
                        {{ $t('申请人') }}
                      </td>
                      <td class="value">
                        {{ ticketInfo.created_by }}
                      </td>
                      <td class="key pr20">
                        {{ $t('申请时间') }}
                      </td>
                      <td class="value">
                        {{ ticketInfo.created_at }}
                      </td>
                    </tr>
                    <tr>
                      <td class="key pr20">
                        {{ $t('申请理由') }}
                      </td>
                      <td class="pl20 refuse-reson"
                        colspan="3">
                        {{ ticketInfo.reason }}
                      </td>
                    </tr>
                    <tr>
                      <td class="key"
                        colspan="4"
                        style="text-align: center">
                        {{ $t('申请内容') }}
                      </td>
                    </tr>
                    <tr v-for="permission in ticketInfo.permissions"
                      :key="permission.id">
                      <td class="key pr20">
                        {{ $t('项目名称') }}
                      </td>
                      <td class="value result-td">
                        {{ permission.subject_name }}
                      </td>
                      <td class="key pr20">
                        {{ permission.object_class === 'biz' ? $t('业务') : $t('结果表') }}
                      </td>
                      <td class="value">
                        {{ permission.object_class === 'biz'
                          ? permission.scope.bk_biz_info.name
                          : permission.scope.result_table_info.name }}
                      </td>
                    </tr>
                  </template>
                  <template v-if="ticketInfo.ticket_type === 'app_biz'">
                    <tr>
                      <td class="key pr20">
                        {{ $t('申请人') }}
                      </td>
                      <td class="value">
                        {{ ticketInfo.created_by }}
                      </td>
                      <td class="key pr20">
                        {{ $t('申请时间') }}
                      </td>
                      <td class="value">
                        {{ ticketInfo.created_at }}
                      </td>
                    </tr>
                    <tr>
                      <td class="key pr20">
                        {{ $t('申请理由') }}
                      </td>
                      <td class="pl20 refuse-reson"
                        colspan="3">
                        {{ ticketInfo.reason }}
                      </td>
                    </tr>
                    <tr>
                      <td class="key"
                        colspan="4"
                        style="text-align: center">
                        {{ $t('申请内容') }}
                      </td>
                    </tr>
                    <tr v-for="permission in ticketInfo.permissions"
                      :key="permission.id">
                      <td class="key pr20">
                        {{ $t('APP名称') }}
                      </td>
                      <td class="value result-td">
                        {{ permission.subject_name }}
                      </td>
                      <td class="key pr20">
                        {{ permission.object_class === 'biz' ? $t('业务') : $t('结果表') }}
                      </td>
                      <td class="value">
                        {{ permission.object_class === 'biz'
                          ? permission.scope.bk_biz_info.name
                          : permission.scope.result_table_info.name }}
                      </td>
                    </tr>
                  </template>
                </table>
              </div>
            </div>
          </div>
          <div v-if="ticketInfo.ticket_type === 'app_biz'"
            class="auth-detail-example">
            <div class="example pt40">
              <div class="title">
                <span class="before-mark mr15" />
                <span class="text">{{ $t('调用代码示例') }}</span>
              </div>
              <div class="editor">
                <call-example ref="vCallExample" />
              </div>
            </div>
          </div>
        </div>
        <div v-show="openPage"
          class="auth-content-right fr clearfix">
          <div class="approve-flow clearfix">
            <div class="title">
              <span class="before-mark mr15" />
              <span class="text">{{ $t('审批流程') }}</span>
            </div>
            <div class="approve-flow-table">
              <table>
                <tr>
                  <td class="status">
                    <i class="bk-icon icon-check-circle" />
                  </td>
                  <td class="message">
                    <p class="apply mb5">
                      <span>{{ ticketInfo.created_by }}</span> {{ $t('提交申请') }}
                    </p>
                    <span class="time">{{ ticketInfo.created_at }}</span>
                  </td>
                </tr>
                <tr v-for="item of ticketStates"
                  :key="item.id">
                  <td class="status">
                    <i class="bk-icon"
                      :class="parseStatusIcon(item.status)" />
                  </td>
                  <td class="message">
                    <p class="apply processor mb5"
                      :class="{ warning: item.status === 'failed' }">
                      <template v-if="item.status === 'failed'">
                        {{ item.processed_by }} {{ $t('审核不通过') }}
                      </template>
                      <template v-else-if="item.status === 'succeeded'">
                        {{ item.processed_by }} {{ $t('审核通过') }}
                      </template>
                      <template v-else-if="item.status === 'processing'">
                        {{ joinArray(item.processors) }} {{ $t('审核中') }}
                      </template>
                      <template v-if="item.status !== 'pending'">
                        <p class="time mb5">
                          {{ item.process_message }}
                        </p>
                        <span class="time">{{ item.processed_at }}</span>
                      </template>
                    </p>
                  </td>
                </tr>
                <tr v-if="ticketInfo.status !== 'processing'">
                  <td class="status">
                    <i class="bk-icon icon-end" />
                  </td>
                  <td class="message">
                    <p class="apply">
                      {{ $t('流程结束') }}
                    </p>
                  </td>
                </tr>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>
  </Layout>
</template>

<script>
import $ from 'jquery';
import { copyObj } from '@/common/js/util';
import CallExample from './children/apiCallExample';
import Vue from 'vue';
import VueClipboards from 'vue-clipboards';
import { postMethodWarning } from '@/common/js/util.js';
import Layout from '../../components/global/layout';
Vue.use(VueClipboards);

export default {
  components: {
    Layout,
    CallExample,
  },
  data() {
    return {
      loading: true,
      openPage: false,
      ticketDetail: {},
      ticketTypes: [],
      ticketStates: [],
      processParams: {
        process_message: '',
        status: '',
      },
      buttonLoading: false,
    };
  },
  computed: {
    ticketInfo() {
      return this.ticketDetail;
    },
  },
  watch: {
    '$route.params'() {
      this.init();
    },
  },
  mounted() {
    this.init();
  },
  methods: {
    joinArray(arr) {
      if (arr) {
        return arr.join();
      } else {
        return '';
      }
    },
    parseStatusIcon(status) {
      if (status === 'succeeded') {
        return 'icon-check-circle';
      } else if (status === 'pending' || status === 'processing') {
        return 'icon-ellipsis';
      } else {
        return 'icon-close-circle';
      }
    },
    check(result) {
      if (result === 'failed' && this.processParams.process_message.trim() === '') {
        this.$bkMessage({
          message: this.$t('驳回需填写理由'),
          theme: 'error',
        });
        return false;
      }
      return true;
    },
    async getApplyDetail() {
      this.loading = true;
      await this.axios
        .get(`v3/auth/ticket/${this.$route.params.applyid}/`)
        .then(resp => {
          if (resp.result) {
            this.ticketDetail = resp.data;
          } else {
            this.$router.push({ name: '404' });
          }
        })
        ['finally'](() => {
          this.loading = false;
        });
    },
    async init() {
      this.getApplyDetail().then(() => {
        this.openPage = true;
      });
      this.getTicketStates();
    },
    getTicketStates() {
      this.axios.get(`v3/auth/ticket_state/?ticket_id=${this.$route.params.applyid}`).then(resp => {
        if (resp.result) {
          this.ticketStates = resp.data;
        }
      });
    },
    approveTicket(status) {
      if (!this.check(status)) {
        return false;
      }
      this.processParams.status = status;
      const url = `v3/auth/ticket_state/${this.ticketInfo.is_in_process.state_id}/approve/`;
      this.axios.post(url, this.processParams)
        .then(resp => {
          if (resp.result) {
            this.$bkMessage({
              message: this.$t('提交成功'),
              theme: 'success',
            });
            this.$router.go(0);
          } else {
            postMethodWarning(resp.message, 'error');
          }
        });
    },
  },
};
</script>

<style lang="scss">
.apply-auth {
  min-height: calc(110%);
  background: #f2f4f9;
  min-width: 1200px;
  overflow: auto;
  .apply-breadcrumb {
    border-bottom: 1px solid #dfe2ef;
    background: none;
  }
  .data-detail {
    height: calc(100% - 61px);
    padding: 20px;
    overflow: hidden;
    .auth-content {
      width: 69%;
      height: 100%;
    }
    .auth-content-right {
      width: 31%;
    }
    .auth-detail,
    .approve-flow,
    .auth-detail-example {
      .title {
        height: 20px;
        line-height: 20px;
        font-size: 15px;
        color: #212232;
        .before-mark {
          display: inline-block;
          width: 4px;
          height: 19px;
          background: #3a84ff;
        }
        span.text {
          position: relative;
          top: -3px;
        }
      }
      .auth-content-table {
        padding-top: 27px;
        width: 100%;
        table {
          width: 100%;
        }
        tr {
          background-color: #fff;
          width: 100%;
          border-bottom: 1px solid #f2f4f9;
          td {
            height: 50px;
            word-break: break-all;
            .rt-label {
              padding: 0px 8px;
              border: 1px solid #ddd;
              border-radius: 3px;
              display: inline-block;
              background: #fafafa;
              margin: 5px;
            }
            &.result-td {
              word-break: break-all;
              overflow: hidden;
              text-overflow: ellipsis;
              padding-right: 10px;
            }
            &.key {
              text-align: right;
              padding-right: 10px;
              width: 125px;
              background: #fafafa;
            }
            &.value {
              border-left: 1px solid #f2f4f9;
              border-right: 1px solid #f2f4f9;
              width: calc(100% - 118px);
              padding: 0px 20px;
              color: #212232;
            }
            &.refuse-reson {
              color: #212232;
              border-left: 1px solid #f2f4f9;
            }
          }
        }
      }

      .approve-flow-table,
      .approve-flow-table2 {
        margin-top: 27px;
        width: 100%;
        background: #fff;
        .bk-form-content {
          padding: 25px;
        }
        .tip {
          padding: 20px;
          background-color: #fafafa;
          min-height: 130px;
          p {
            color: #b2bac0;
            line-height: 20px;
          }
        }
        tr {
          border-bottom: 1px solid #f2f4f9;
        }
        .status {
          width: 100px;
          background-color: #fafafa;
          text-align: center;
          .icon-check-circle {
            color: #9dcb6b;
            font-size: 36px;
          }
          .icon-ellipsis {
            color: #3a84ff;
            font-size: 32px;
            border: 2px solid #3a84ff;
            border-radius: 50%;
          }
          .icon-end {
            color: #9dcb6b;
            font-size: 36px;
          }
          .icon-close-circle {
            font-size: 36px;
            color: #ff6600;
          }
        }
        .message {
          width: calc(100% - 100px);
          padding: 18px 15px 18px 38px;
          .apply {
            color: #212232;
            font-size: 16px;
            &.warning {
              color: #ff6600;
            }
          }
          .time {
            font-size: 14px;
            color: #737987;
          }
          .processor {
            word-break: break-all;
          }
        }
      }
      .editor {
        margin-top: 27px;
      }
    }
    .auth-detail-example {
      height: 600px;
      .example {
        height: 100%;
        .editor {
          height: 100%;
        }
      }
    }
  }
}
.bk-loading {
  background-color: rgba(255, 255, 255, 1) !important;
}
.apply-auth {
  .ace_identifier,
  .ace_editor {
    &,
    & * {
      font: 16px / normal 'Monaco', 'Menlo', 'Ubuntu Mono', 'Consolas', 'source-code-pro', monospace !important;
    }
  }
}
</style>

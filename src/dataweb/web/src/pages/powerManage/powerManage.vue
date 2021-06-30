

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
  <Layout :crumbName="$t('申请列表')">
    <div class="data-powerManage">
      <ticket-list ref="ticketList"
        :showNewBtn="true"
        @applyAuth="applyAuth" />
      <div class="dialog">
        <bkdata-dialog v-model="applyTicketDialog.isShow"
          extCls="bkdata-dialog"
          :hasHeader="false"
          :closeIcon="false"
          :hasFooter="false"
          :maskClose="false"
          :okText="$t('提交')"
          :cancelText="$t('取消')"
          :width="680"
          @cancel="cancelDialog"
          @confirm="confirm">
          <div>
            <a href="javascript:;"
              class="close-button"
              @click="cancelDialog()">
              <i :title="$t('关闭')"
                class="bk-icon icon-close" />
            </a>
            <div class="title">
              <span />
              {{ $t('数据权限申请') }}
            </div>
            <div class="content mt25">
              <div class="bk-form-item">
                <span class="label-title">
                  {{ $t('业务') }}
                  <span class="required">*</span>
                </span>
                <div class="bk-form-content">
                  <bkdata-selector
                    :disabled="bizInfo.isDisabled"
                    :selected.sync="params.scope.bk_biz_id"
                    :class="{ error: applyTicketDialog.isBizError }"
                    :list="bizInfo.data"
                    :searchable="true"
                    :settingKey="'bk_biz_id'"
                    :displayKey="'bk_biz_name'"
                    :searchKey="'bk_biz_name'"
                    @item-selected="bizChange" />
                </div>
                <div v-if="applyTicketDialog.isBizError"
                  class="pt10 errot-tip">
                  {{ validator.message.biz }}
                </div>
              </div>
            </div>
            <div id="result-tables"
              class="content mt25">
              <div class="bk-form-item">
                <span class="label-title">
                  {{ $t('结果数据表') }}
                  <span class="required">*</span>
                </span>
                <div class="bk-form-content">
                  <bkdata-selector
                    :placeholder="resultInfo.placeholder"
                    :class="{
                      error: applyTicketDialog.isResultError,
                      'is-all': resultInfo.placeholder === '全部结果数据表',
                    }"
                    :selected.sync="params.scope.result_table_id"
                    :multiple="true"
                    :searchable="true"
                    :disabled="resultInfo.isDisabled"
                    :list="resultInfo.data"
                    :settingKey="'result_table_id'"
                    :displayKey="'result_table_id'"
                    @item-selected="resultChange" />
                </div>
                <div v-if="applyTicketDialog.isResultError"
                  class="pt10 errot-tip">
                  {{ validator.message.results }}
                </div>
              </div>
            </div>
            <div class="content mt25 app-id">
              <div class="bk-form-item">
                <span class="label-title">
                  {{ $t('蓝鲸APP') }}
                  <span class="required">*</span>
                </span>
                <div class="bk-form-content">
                  <bkdata-selector :selected.sync="params.subject_id"
                    :class="{ error: applyTicketDialog.isAppError }"
                    :list="applyTicketInfo.blueKingAppList"
                    :searchable="true"
                    :settingKey="'app_code'"
                    :displayKey="'app_name'"
                    :searchKey="'app_name'"
                    @item-selected="appChange" />
                </div>
                <div v-if="applyTicketDialog.isAppError"
                  class="pt10 errot-tip">
                  {{ validator.message.bluekingApp }}
                </div>
              </div>
            </div>
            <div class="content mt25">
              <div class="bk-form-item">
                <span class="label-title">
                  {{ $t('用途描述') }}
                  <span class="required">*</span>
                </span>
                <div class="bk-form-content">
                  <textarea id="desc"
                    v-model="params.reason"
                    name=""
                    class="bk-form-textarea"
                    :class="{ error: applyTicketDialog.isDescriptionError }"
                    :placeholder="$t('将作为审批依据_请认真填写')"
                    @input="descheck" />
                </div>
                <div v-if="applyTicketDialog.descriptionError"
                  class="pt10 errot-tip">
                  {{ validator.message.desRequired }}
                </div>
              </div>
            </div>
          </div>
        </bkdata-dialog>
      </div>
    </div>
  </Layout>
</template>

<script>
import { placeholder, postMethodWarning } from '@/common/js/util.js';
import TicketList from '@/pages/powerManage/ticketList';
import Layout from '../../components/global/layout';
export default {
  components: {
    TicketList,
    Layout,
  },
  data() {
    return {
      // 单据申请弹框状态
      applyTicketDialog: {
        isShow: false,
        isProjectError: false,
        isBizError: false,
        isResultError: false,
        isDescriptionError: false,
        isAppError: false,
      },
      // 单据申请信息
      applyTicketInfo: {
        blueKingAppList: [],
      },
      bizInfo: {
        isDisabled: true,
        data: [],
      },
      resultInfo: {
        isDisabled: false,
        data: [],
        placeholder: this.$t('请选择'),
      },
      params: {
        reason: '',
        action: 'result_table.query_data',
        subject_class: 'app',
        subject_id: '',
        subject_name: '',
        object_class: 'result_table',
        scope: {
          project_id: '',
          bk_biz_id: '',
          is_all: false,
          result_table_id: [],
        },
      },
      loadingButton: false,
    };
  },
  mounted() {
    this.init();
  },
  methods: {
    applyAuth() {
      this.applyTicketDialog.isShow = true;
    },
    /**
     * 取消后清空数据
     */
    cancelDialog() {
      this.resultInfo.placeholder = this.$t('请选择');
      this.applyTicketDialog.isShow = false;
      this.applyTicketDialog.isProjectError = false;
      this.applyTicketDialog.isBizError = false;
      this.applyTicketDialog.isResultError = false;
      this.applyTicketDialog.isAppError = false;
      this.applyTicketDialog.isDescriptionError = false;
      this.bizInfo.isDisabled = true;
      this.resultInfo.isDisabled = true;
      this.resultInfo.data.splice(0);
      this.resetParams();
    },
    init() {
      this.getBizList();
      this.$refs.ticketList.init();
      if (this.$route.hash === '#apply') {
        this.applyAuth();
      }
      this.getBlueKingAppList();
    },
    getBizList(pid) {
      this.bizInfo.isDisabled = true;
      this.axios
        .get('bizs/')
        .then(resp => {
          if (resp.result) {
            this.bizInfo.data.splice(0, this.bizInfo.data.length);
            resp.data.map(v => {
              this.bizInfo.data.push(v);
            });
          } else {
            if (resp.data) {
              this.getMethodWarning(resp.message, resp.code);
            }
          }
        })
        ['finally'](() => {
          this.bizInfo.isDisabled = false;
        });
    },
    bizChange(index, item) {
      this.bizCheck();
      this.getResultList(item.bk_biz_id);
      this.params.scope.result_table_id = [];
    },
    bizCheck() {
      this.applyTicketDialog.isBizError = this.params.scope.bk_biz_id === '';
    },
    /**
     * 获取结果数据表
     */
    getResultList(bizId) {
      this.resultInfo.isDisabled = true;
      this.resultInfo.data.splice(0);
      // TODO: 这里是否需要统一换成bk_biz_id
      this.axios
        .get(`result_tables/?bk_biz_id=${bizId}`)
        .then(resp => {
          if (resp.result) {
            this.resultInfo.isDisabled = false;
            let temp = [];
            for (let item of resp.data) {
              item.disable = false;
              temp.push(item);
            }
            // 去除重复的result_table_id
            temp.map(item => {
              let hasCommon = false;
              for (let result of this.resultInfo.data) {
                if (item.result_table_id === result.result_table_id) {
                  hasCommon = true;
                }
              }
              if (!hasCommon) {
                this.resultInfo.data.push(item);
              }
            });
          } else {
            this.getMethodWarning(resp.message, resp.code);
          }
        })
        ['finally'](() => {
          this.resultInfo.isDisabled = false;
        });
    },
    resultChange() {
      this.applyTicketDialog.isResultError = this.params.scope.is_all === false
             && this.params.scope.result_table_id.length === 0;
    },
    /**
     * 结果数据全选
     */
    selectAll() {
      this.params.scope.result_table_id = [];
      this.applyTicketDialog.isResultError = false;
      this.params.scope.is_all = !this.params.scope.is_all;
      let isSelect = this.params.scope.is_all;
      for (let item of this.resultInfo.data) {
        item.disable = isSelect;
      }
      this.params.scope.is_all === true
        ? (this.resultInfo.placeholder = this.$t('全部结果数据表'))
        : (this.resultInfo.placeholder = this.$t('请选择'));
    },
    /**
     * 申请权限提交
     */
    confirm() {
      let desc = document.getElementById('desc');
      // 检验表单是否都不为空
      if (this.loadingButton) return;
      let result = desc.value.length > 0
                && this.params.app_code !== ''
                && (this.params.scope.result_table_id.length > 0
                || this.params.scope.is_all === true)
                && this.params.scope.bk_biz_id !== '';

      if (result) {
        this.loadingButton = true;
        for (let item of this.applyTicketInfo.blueKingAppList) {
          if (item.app_code === this.params.subject_id) {
            this.params.subject_name = item.app_name;
          }
        }
        let permissions = [];
        if (typeof this.params.scope.result_table_id === 'string') {
          this.params.scope.result_table_id = this.params.scope.result_table_id.split(',');
          for (let resultTableId of this.params.scope.result_table_id) {
            permissions.push({
              subject_id: this.params.subject_id,
              subject_name: this.params.subject_name,
              subject_class: 'app',
              object_class: 'result_table',
              action: 'result_table.query_data',
              scope: {
                result_table_id: resultTableId,
              },
            });
          }
        } else {
          this.params.scope.result_table_id = this.params.scope.result_table_id;
          permissions.push({
            subject_id: this.params.subject_id,
            subject_name: this.params.subject_name,
            subject_class: 'app',
            object_class: 'result_table',
            action: 'result_table.query_data',
            scope: {
              result_table_id: this.params.scope.result_table_id,
            },
          });
        }
        let createParams = {
          reason: this.params.reason,
          ticket_type: 'app_biz',
          permissions: permissions,
        };
        this.axios.post('v3/auth/ticket/', createParams).then(resp => {
          if (resp.result) {
            this.applyTicketDialog.isShow = false;
            this.$refs.ticketList.getProjectApplyList();
            this.resetParams();
            this.resultInfo.placeholder = this.$t('请选择');
          } else {
            let h = this.$createElement;
            let self = this;
            if (resp.code === '00007') {
              this.$bkMessage({
                message: h(
                  'a',
                  {
                    style: {
                      color: '#f00',
                      cursor: 'pointer',
                    },
                    on: {
                      click: function () {
                        self.detail(resp.data);
                      },
                    },
                  },
                  `${resp.message} ${self.$t('点击查看详情')}`
                ),
                theme: 'error',
                delay: 3000,
              });
            } else {
              postMethodWarning(resp.message, 'error');
            }
          }
          this.loadingButton = false;
        });
      } else {
        this.bizCheck();
        this.resultChange();
        this.appChange();
        this.descheck();
      }
    },
    resetParams() {
      this.params = {
        reason: '',
        action: 'result_table.query_data',
        subject_class: 'app',
        subject_id: '',
        subject_name: '',
        object_class: 'result_table',
        scope: {
          project_id: '',
          bk_biz_id: '',
          is_all: false,
          result_table_id: [],
        },
      };
    },
    appChange() {
      this.applyTicketDialog.isAppError = this.params.subject_id === '';
    },
    descheck() {
      let descInput = document.getElementById('desc');
      this.applyTicketDialog.isDescriptionError = descInput.value.trim().length === 0;
    },
    getBlueKingAppList() {
      this.bkRequest.httpRequest('common/listBKApp').then(resp => {
        if (resp.result) {
          this.applyTicketInfo.blueKingAppList.splice(0);
          for (let item of resp.data) {
            this.applyTicketInfo.blueKingAppList.push({
              app_code: item.app_code,
              app_name: item.app_name + '(' + item.app_code + ')',
            });
          }
        } else {
          this.getMethodWarning(resp.message, resp.code);
        }
      });
    },
  },
};
</script>

<style lang="scss">
.data-powerManage {
  .inquire-content {
    box-shadow: none;
  }
}
</style>

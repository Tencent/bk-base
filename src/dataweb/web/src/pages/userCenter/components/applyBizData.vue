

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
    <bkdata-dialog v-model="biz.isShow"
      :showFooter="false"
      width="802"
      extCls="bkdata-dialog none-padding"
      :theme="'primary'"
      :maskClose="false"
      @cancel="closeDialog">
      <div class="apply-project">
        <apply-header :title="headerTitle" />
        <div class="bk-form biz-style">
          <div class="bk-form-item">
            <label class="bk-label">{{ $t('数据类型') }}</label>
            <div class="bk-form-content">
              <div class="bk-button-group"
                style="width: 247px">
                <bkdata-button v-bk-tooltips="$t('适用于数据开发等场景')"
                  style="width: 50%"
                  :class="dataTypeSelected === 'rt' ? 'is-selected' : ''"
                  @click="dataTypeClickHandle('rt')">
                  {{ $t('结果数据表') }}
                </bkdata-button>
                <bkdata-button v-bk-tooltips="$t('适用于在笔记中使用Python探索数据')"
                  style="width: 50%"
                  :class="dataTypeSelected === 'st' ? 'is-selected' : ''"
                  @click="dataTypeClickHandle('st')">
                  {{ $t('原始数据源') }}
                </bkdata-button>
              </div>
            </div>
          </div>
          <div class="bk-form-item">
            <label class="bk-label">{{ $t('业务列表') }}</label>
            <div class="bk-form-content">
              <bkdata-selector v-tooltip.notrigger.right="validate['selectedBizId']"
                :isLoading="biz.isSelectorLoading"
                :placeholder="$t('请选择')"
                :searchable="true"
                :list="biz.list"
                :displayKey="'showName'"
                :selected.sync="selectedBizId"
                :settingKey="'bk_biz_id'"
                :searchKey="'bk_biz_name'" />
            </div>
          </div>
        </div>
        <div class="content">
          <div class="content-left">
            <div class="bk-form">
              <div v-if="dataTypeSelected === 'rt'"
                class="bk-form-item">
                <label v-bk-tooltips="$t('数据开发中的数据源类型')"
                  class="bk-label">
                  <span class="has-desc">{{ $t('数据源类型') }}</span>
                </label>
                <div class="bk-form-content">
                  <bkdata-selector :list="dataType"
                    :allowClear="true"
                    :placeholder="$t('全部')"
                    :displayKey="'name'"
                    :selected.sync="selectedDataType"
                    :settingKey="'setKey'" />
                </div>
              </div>
              <div v-else
                class="bk-form-item">
                <label class="bk-label">{{ $t('接入类型') }}</label>
                <div class="bk-form-content">
                  <bkdata-input v-model="fileLabel"
                    disabled />
                </div>
              </div>
              <div class="bk-form-item">
                <label class="bk-label">{{ $t('业务数据') }}</label>
                <div class="bk-form-content">
                  <bkdata-selector
                    v-tooltip.notrigger.right="validate['selectedBizTableId']"
                    :isLoading="isBizResultLoading || biz.isSelectorLoading"
                    :placeholder="$t('请选择')"
                    :multiSelect="true"
                    :searchable="true"
                    :list="bizResultTableList"
                    :displayKey="'showName'"
                    :selected.sync="selectedBizTableId"
                    :settingKey="bizSettingKey"
                    :searchKey="'showName'" />
                </div>
              </div>
              <div class="bk-form-item">
                <label class="bk-label">{{ $t('申请理由') }}</label>
                <div class="bk-form-content">
                  <bkdata-input v-model="biz.generalParams.reason"
                    v-tooltip.notrigger.right="validate['reason']"
                    :type="'textarea'"
                    :rows="3"
                    @change="calcelValidate" />
                </div>
              </div>
              <div class="bk-form-item">
                <div class="bk-form-content">
                  <bkdata-button :theme="'primary'"
                    :loading="biz.isLoading"
                    class="mr10"
                    @click="applyGneralData">
                    {{ $t('提交') }}
                  </bkdata-button>
                </div>
                <div class="fl data-checkbox">
                  <div v-show="validata.status"
                    class="tip">
                    {{ validata.errorInfo }}
                  </div>
                </div>
              </div>
            </div>
          </div>
          <div v-bkloading="{ isLoading: isApplyListLoading }"
            class="content-right bk-scroll-y">
            <div class="apply-status">
              <div class="status-item">
                <span />{{ $t('已申请') }}({{ bkBizTicketsCountObj.succeeded }})
              </div>
              <div class="status-item">
                <span />{{ $t('申请中') }}({{ bkBizTicketsCountObj.processing }})
              </div>
              <div class="status-item">
                <span />{{ $t('被驳回') }}({{ bkBizTicketsCountObj.failed }})
              </div>
            </div>
            <div
              v-for="(item, index) in bizNameList"
              :key="index"
              v-bk-tooltips="item.name"
              class="apply-process"
              :class="{
                'apply-processing': item.status === 'processing',
                'apply-succeeded': item.status === 'succeeded',
                'apply-failed': item.status === 'failed',
              }"
              @click="sliderOpen(item.state_id)">
              {{ item.name }}
            </div>
          </div>
        </div>
      </div>
    </bkdata-dialog>
    <bkdata-sideslider :quickClose="true"
      class="zIndex2501"
      :isShow.sync="sideslider.isShow"
      :width="560"
      :title="$t('单据详情')">
      <template slot="content">
        <template v-if="sideslider.isShow">
          <TicketDetail :stateId="sideslider.stateId"
            :statusInfos="statusInfos" />
        </template>
      </template>
    </bkdata-sideslider>
  </div>
</template>
<script>
import { postMethodWarning } from '@/common/js/util.js';
import TicketDetail from '../../authCenter/ticket/Detail';
import applyHeader from './applyProject/applyHeader';

export default {
  components: {
    TicketDetail,
    applyHeader,
  },
  props: {
    bkBizId: {
      type: Number,
    },
  },
  data() {
    return {
      fileLabel: '离线文件上传',
      dataTypeSelected: 'rt',
      sourceType: {
        batch_source: 'hdfs',
        stream_source: 'kafka',
        kv_source: 'tredis',
        tdw_source: 'tdw',
      },
      isListUnfold: false,
      isBizNameListShow: false,
      isBizResultLoading: false,
      isApplyListLoading: false,
      selectedBizId: '',
      bizNameList: [],
      selectedBizTableId: [],
      bizResultTableList: [],
      accessResultTablesData: '',
      accessResultTablesArr: [],
      bkBizLoading: false,
      bkBizTicketsCount: '',
      bkBizTicketsCountData: {},
      bkBizTicketsCountObj: {
        total: 0,
        succeeded: 0,
        failed: 0,
        processing: 0,
      }, // 返回的带有结果表数量的对象
      project: {},
      validata: {
        status: false,
        errorInfo: '',
      },
      biz: {
        isShow: false,
        isLoading: false,
        isSelectorLoading: true,
        list: [],
        generalParams: {
          reason: '',
          ticket_type: 'project_biz',
          permissions: [],
        },
      },
      sideslider: {
        isShow: false,
        stateId: null,
      },
      statusInfos: {
        pending: {
          id: 'pending',
          name: this.$t('待处理'),
          location: 'head',
          class: 'label-primary',
          actions: [],
        },
        processing: {
          id: 'processing',
          name: this.$t('处理中'),
          location: 'middle',
          class: 'label-primary',
          actions: ['withdraw'],
        },
        succeeded: {
          id: 'succeeded',
          name: this.$t('已同意'),
          location: 'tail',
          class: 'label-success',
          actions: [],
        },
        failed: {
          id: 'failed',
          name: this.$t('已驳回'),
          location: 'tail',
          class: 'label-danger',
          actions: ['resubmit'],
        },
        stopped: {
          id: 'stopped',
          name: this.$t('已终止'),
          location: 'stopped',
          class: 'label-danger',
          actions: [],
        },
      },
      dataType: [
        {
          name: this.$t('全部'),
          setKey: 'all',
        },
        {
          name: this.$t('实时数据源'),
          setKey: 'kafka',
          enName: 'stream_source',
        },
        {
          name: this.$t('离线数据源'),
          setKey: 'hdfs',
          enName: 'batch_source',
        },
        {
          name: this.$t('TDW数据源'),
          setKey: 'tdw',
          enName: 'tdw_source',
        },
        {
          name: this.$t('关联数据源'),
          setKey: 'ignite_new',
          enName: 'unified_kv_source',
        },
        {
          name: this.$t('离线关联数据源'),
          setKey: 'ignite',
          enName: 'batch_kv_source',
        },
        {
          name: 'TRedis' + this.$t('关联数据源'),
          setKey: 'tredis',
          enName: 'kv_source',
        },
      ],
      selectedDataType: '',
      validate: {
        selectedBizId: {
          regs: { required: true, type: 'string', error: window.$t('不能为空') },
          content: window.$t('不能为空'),
          visible: false,
          class: 'error-red',
        },
        selectedBizTableId: {
          regs: { required: true, type: 'string', error: window.$t('不能为空') },
          content: window.$t('不能为空'),
          visible: false,
          class: 'error-red',
        },
        reason: {
          regs: [
            { required: true, error: window.$t('不能为空') },
            { length: 50, error: window.$t('长度不能大于50') },
          ],
          content: window.$t('不能为空'),
          visible: false,
          class: 'error-red',
        },
      },
      isFromSourceNode: false,
      tagCode: null,
      isGetTagCode: false,
    };
  },
  computed: {
    bizSettingKey() {
      return this.dataTypeSelected === 'rt' ? 'result_table_id' : 'id';
    },
    headerTitle() {
      return this.project.project_name ? this.project.project_name : this.project.project_id;
    },
  },
  watch: {
    selectedBizTableId(val) {
      val && this.calcelValidate();
    },
    selectedBizId(newVal) {
      if (newVal) {
        if (this.isGetTagCode && !this.tagCode) {
          postMethodWarning(this.$t('请申请您所在区域的业务权限'), 'warning');
          return;
        }
        this.calcelValidate();
        this.isApplyListLoading = true;
        if (!this.isFromSourceNode) {
          this.selectedDataType = 'all';
        }
        this.selectedBizTableId = []; // 重置多选结果数据列表
        this.bizNameList = [];
        this.getAccessResultTable(); // 获取之前申请过的结果数据列表
        if (this.isFromSourceNode) {
          this.handleDataTypeChange(this.selectedDataType);
          this.isFromSourceNode = false;
        } else {
          this.getBizResultList(newVal);
        }
        this.showProjectCount(newVal);
      }
    },
    async project(newVal) {
      if (newVal) {
        // project_id存在时
        // if (this.biz.list.length && this.biz.list[0].showName) return // 存在showName字段，则不进行数据处理
        this.getBizTicketsCount(); // 获取业务信息
        this.getAccessResultTable(); // 获取已经申请过的业务结果数据表
        await this.modifyProjectInfo();
      }
    },
    selectedDataType: {
      handler(newVal) {
        if (this.isGetTagCode && !this.tagCode) {
          postMethodWarning(this.$t('请申请您所在区域的业务权限'), 'warning');
          return;
        }
        if (this.selectedBizId && typeof newVal === 'string') {
          // 数据类型改变之后需要重新拉取之前申请过的业务结果数据表
          this.handleDataTypeChange(newVal);
        }
      },
    },
  },
  mounted() {
    this.dataType = this.dataType.filter(item => {
      return this.$modules.isActive(item.enName) || item.setKey === 'all';
    });
  },
  methods: {
    dataTypeClickHandle(type) {
      this.dataTypeSelected = type;
      this.selectedBizId = '';
      this.selectedBizTableId = [];
      this.biz.list = [];
      this.bizNameList = [];
      this.bizResultTableList = [];

      this.getBizTicketsCount(); // 获取业务信息
      this.getAccessResultTable(); // 获取已经申请过的业务结果数据表
      this.modifyProjectInfo();
    },
    modifyProjectInfo() {
      const options = {
        params: {
          project_id: this.project.project_id,
        },
      };
      return this.bkRequest.httpRequest('meta/modifyProjectInfo', options).then(resp => {
        if (resp.result) {
          this.isGetTagCode = true;
          this.tagCode = resp.data.tags.manage.geog_area[0].code;
        } else {
          postMethodWarning(resp.message, 'error');
        }
      });
    },
    showProjectCount(newVal) {
      this.bkBizTicketsCountObj.processing = this.bkBizTicketsCountData[newVal].processing === undefined
        ? 0 : this.bkBizTicketsCountData[newVal].processing;
      this.bkBizTicketsCountObj.succeeded = this.bkBizTicketsCountData[newVal].succeeded === undefined
        ? 0 : this.bkBizTicketsCountData[newVal].succeeded;
      this.bkBizTicketsCountObj.failed = this.bkBizTicketsCountData[newVal].failed === undefined
        ? 0 : this.bkBizTicketsCountData[newVal].failed;
    },
    async handleDataTypeChange(type) {
      this.isApplyListLoading = true;
      this.selectedBizTableId = []; // 重置多选结果数据列表
      this.bizNameList = [];
      await this.getAccessResultTable(); // 获取已经申请过的业务结果数据表
      this.getBizResultList(this.selectedBizId, type);
    },
    calcelValidate() {
      Object.keys(this.validate).forEach(attr => {
        this.$set(this.validate[attr], 'visible', false);
      });
    },
    validateForm() {
      let flag = true;
      Object.keys(this.validate).forEach(attr => {
        this.validate[attr].visible = false;
      });
      if (!this.selectedBizId) {
        flag = false;
        this.$set(this.validate, 'selectedBizId', {
          regs: { required: true, type: 'string', error: window.$t('不能为空') },
          content: window.$t('不能为空'),
          visible: true,
          class: 'error-red',
        });
      }
      if (!this.selectedBizTableId[0]) {
        flag = false;
        this.$set(this.validate, 'selectedBizTableId', {
          regs: { required: true, type: 'string', error: window.$t('不能为空') },
          content: window.$t('不能为空'),
          visible: true,
          class: 'error-red',
        });
      }
      if (!this.biz.generalParams.reason) {
        flag = false;
        this.$set(this.validate, 'reason', {
          regs: [
            { required: true, error: window.$t('不能为空') },
            { length: 50, error: window.$t('长度不能大于50') },
          ],
          content: window.$t('不能为空'),
          visible: true,
          class: 'error-red',
        });
      }
      return flag;
    },
    getTableDiff(resultTable) {
      // 对比之前申请的结果数据表和现在的结果数据表的差异，过滤掉重复数据
      return resultTable.filter(child => {
        return !this.accessResultTablesArr.includes(child.result_table_id);
      });
    },
    checkApplyReason() {
      // 校验申请理由
      if (this.biz.generalParams.reason.length) {
        this.validata.status = false;
      } else {
        this.validata.status = true;
        this.validata.errorInfo = this.validator.message.required;
        return false;
      }
      return true;
    },
    show(data, type, isSourceData = false) {
      this.dataTypeSelected = isSourceData ? 'st' : 'rt';
      if (type) {
        this.isFromSourceNode = true;
      }
      this.biz.isShow = true;
      this.project = data;
      this.$nextTick(() => {
        this.selectedDataType = this.sourceType[type];
      });
      // this.biz.isLoading = true
    },
    closeDialog() {
      this.$nextTick(() => {
        this.$emit('closeApplyBiz');
      });
      // this.biz.isLoading = true
    },
    getSourceBizList(bizId) {
      this.isBizResultLoading = true;
      this.bkRequest
        .httpRequest('dataAccess/getSourceBizList', {
          query: {
            bk_biz_id: bizId,
            data_scenario: 'offlinefile',
          },
        })
        .then(res => {
          if (res.result) {
            this.bizResultTableList = res.data
              .filter(rowData => !this.accessResultTablesArr.includes(rowData.id.toString()))
              .map(item => {
                return {
                  showName: `[${item.id}]${item.raw_data_alias}`,
                  ...item,
                };
              });

            let resultTableObj = {};
            res.data.forEach(item => {
              resultTableObj[item.id] = item.raw_data_alias;
            });
            this.accessResultTablesArr.forEach(attr => {
              // accessResultTablesData对象添加别名属性
              this.accessResultTablesData[attr].raw_data_alias = resultTableObj[attr];
            });

            let keyArr = Object.keys(this.accessResultTablesData);
            this.bizNameList = [];
            Object.keys(this.accessResultTablesData).forEach(child => {
              if (keyArr.includes(child)) {
                let obj = {
                  status: this.accessResultTablesData[child].status,
                  name: `[${child}]${this.accessResultTablesData[child].raw_data_alias}`,
                  state_id: this.accessResultTablesData[child].state_id,
                };
                console.log('push');
                this.bizNameList.push(obj);
              }
            });
            this.isApplyListLoading = false;
          } else {
            this.getMethodsWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isBizResultLoading = false;
        });
    },
    getBizResultList(bizId, selectedDataType) {
      // 获取某个业务下的结果数据表
      /** 四种数据类型
       * 实时数据源： kafka
       * 离线数据源： hdfs
       * 关联数据源： tredis
       * TDW数据源： tdw
       */

      console.log('getBizResultList');

      if (this.dataTypeSelected === 'st') {
        this.getSourceBizList(bizId);
        return;
      }
      this.isBizNameListShow = true;
      this.isBizResultLoading = true;
      let options = {
        query: {
          bk_biz_id: bizId,
        },
      };
      let selectTypeToQuery;
      if (selectedDataType === 'tredis') {
        selectTypeToQuery = ['tredis', 'ipredis'];
      } else if (selectedDataType === 'ignite_new') {
        selectTypeToQuery = ['ignite'];
      } else {
        selectTypeToQuery = [selectedDataType];
      }
      // const selectTypeToQuery = selectedDataType === 'tredis' ? ['tredis', 'ipredis'] : [selectedDataType]
      if (this.tagCode) options.query.tags = this.tagCode;
      if (this.selectedDataType && this.selectedDataType !== 'all') {
        options.query.related_filter = JSON.stringify({
          attr_name: 'common_cluster.cluster_type',
          type: 'storages',
          attr_value: selectTypeToQuery,
        });
      }
      this.bkRequest.httpRequest('meta/getBizResultTableList', options).then(resp => {
        if (resp.result) {
          if (this.selectedDataType && this.selectedDataType !== 'all') {
            // 取得某一数据类型下的之前申请过的结果数据表
            resp.data = resp.data.filter(item => {
              for (let i = 0; i < selectTypeToQuery.length; i++) {
                if (Object.keys(item.storages).includes(selectTypeToQuery[i])) {
                  return true;
                }
              }
              return false;
            });
          }
          let resultTableObj = {};
          resp.data.forEach(item => {
            resultTableObj[item.result_table_id] = item.result_table_name_alias;
          });
          this.accessResultTablesArr.forEach(attr => {
            // accessResultTablesData对象添加别名属性
            this.accessResultTablesData[attr].result_table_name_alias = resultTableObj[attr];
          });
          this.showApplyResultList(this.selectedBizId); // 展示之前申请过的某一业务下的结果数据表，拼接字段（id+别名)
          resp.data.forEach(child => {
            // 拼接showName属性(id+别名)
            child.showName = `${child.result_table_id}(${child.result_table_name_alias})`;
          });
          this.bizResultTableList = resp.data.filter(child => {
            // 对比之前申请的结果数据表和现在的结果数据表的差异，过滤掉重复数据
            return !this.accessResultTablesArr.includes(child.result_table_id);
          });
          this.isBizResultLoading = false;
          this.isBizNameListShow = false;
        } else {
          postMethodWarning(resp.message, 'error');
        }
      });
    },
    applyGneralData(item) {
      // 发起申请请求
      // if (!this.checkApplyReason()) {
      //     return
      // }
      if (!this.validateForm()) {
        return;
      }
      this.biz.generalParams.permissions = [];
      this.selectedBizTableId.forEach(item => {
        let obj = {
          subject_id: this.project.project_id,
          subject_class: 'project',
          subject_name: this.project.project_name,
          action: this.dataTypeSelected === 'rt' ? 'result_table.query_data' : 'raw_data.query_data',
          object_class: this.dataTypeSelected === 'rt' ? 'result_table' : 'raw_data',
          scope:
            this.dataTypeSelected === 'rt'
              ? {
                result_table_id: item,
              }
              : {
                raw_data_id: item,
              },
        };
        this.biz.generalParams.permissions.push(obj);
      });
      this.biz.isLoading = true;
      let options = {
        params: this.biz.generalParams,
      };
      this.bkRequest
        .httpRequest('auth/getTicketsList', options)
        .then(res => {
          if (res.result) {
            this.$bkMessage({
              message: this.$t('已成功提交申请_等待管理员审批'),
              theme: 'success',
              hasCloseIcon: true,
            });
            this.selectedBizTableId = []; // 重置多选结果数据列表
            res.data.forEach(item => {
              let resultIdList = [];
              let isFailedStatus = false;
              // 请求完成后，更新申请中结果数据表的数量
              // this.bkBizTicketsCountObj.total += item.permissions.length
              if (item.status === 'processing') {
                this.bkBizTicketsCountObj.processing += item.permissions.length;
              } else if (res.data[0].status === 'succeeded') {
                this.bkBizTicketsCountObj.succeeded += item.permissions.length;
              } else if (item.status === 'failed') {
                isFailedStatus = true;
              }

              // 把刚刚申请的结果数据列表添加到结果数据展示的列表
              item.permissions.forEach(child => {
                let obj = {
                  status: item.status,
                  name: `${child.scope.scope_id}(${child.scope.description})`,
                  state_id: item.state_id,
                };
                this.bizNameList.push(obj);
                let arr = this.biz.list.filter(item => {
                  // 找到刚刚添加的结果表，给其中的count.processing加1
                  return item.bk_biz_id === child.scope.scope_id.split('_')[0];
                });
                arr[0].count[item.status] = (arr[0].count[item.status]
                  ? arr[0].count[item.status] : 0) + 1;
                arr[0].showName = arr[0].bk_biz_name
                                + ` 【${this.$t('已申请')}(${arr[0].count.succeeded
                                  ? arr[0].count.succeeded : 0}) / ${this.$t('申请中')}(${arr[0].count.processing
                                    ? arr[0].count.processing : 0})】`;
                // 拿到返回的处理中状态的数据id,过滤掉刚刚申请的结果数据
                if (!isFailedStatus) {
                  resultIdList.push(child.scope.scope_id);
                }
              });
              this.bizResultTableList = this.bizResultTableList.filter(child => {
                return !resultIdList.includes(child.result_table_id);
              });
            });
            this.$emit('applySuccess');
            this.closeDialog();
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.biz.isLoading = false;
        });
    },
    showApplyResultList(id) {
      // 展示之前申请的结果表
      this.bizNameList = [];
      let keyArr = Object.keys(this.accessResultTablesData).filter(child => {
        return child.split('_')[0] === id;
      });
      Object.keys(this.accessResultTablesData).forEach(child => {
        if (keyArr.includes(child)) {
          if (this.accessResultTablesData[child].result_table_name_alias) {
            let obj = {
              status: this.accessResultTablesData[child].status,
              name: `${child}(${this.accessResultTablesData[child].result_table_name_alias})`,
              state_id: this.accessResultTablesData[child].state_id,
            };
            this.bizNameList.push(obj);
          }
        }
      });
      // 如果数据类型不为空，那就自己计算各种状态的数据量
      if (this.selectedDataType && this.selectedDataType !== 'all') {
        this.bkBizTicketsCountObj.processing = 0;
        this.bkBizTicketsCountObj.succeeded = 0;
        this.bkBizTicketsCountObj.failed = 0;
        this.bizNameList.forEach(item => {
          if (item.status === 'processing') {
            this.bkBizTicketsCountObj.processing++;
          } else if (item.status === 'failed') {
            this.bkBizTicketsCountObj.failed++;
          } else {
            this.bkBizTicketsCountObj.succeeded++;
          }
        });
      } else {
        this.showProjectCount(this.selectedBizId);
      }
      this.isApplyListLoading = false;
    },
    getAccessResultTable() {
      // 获取之前申请过的结果数据列表
      let options = Object.assign(
        {
          params: {
            project_id: this.project.project_id,
          },
        },
        this.dataTypeSelected === 'rt'
          ? {}
          : {
            query: {
              action_ids: 'raw_data.query_data',
            },
          }
      );
      return this.bkRequest.httpRequest('auth/getAccessResultTable', options).then(res => {
        if (res.result) {
          this.accessResultTablesData = res.data;
          this.accessResultTablesArr = Object.keys(res.data);
          // this.accessResultTablesArr = Object.keys(res.data).filter(child => { // 过滤掉之前申请失败的项目
          //     return res.data[child].status !== 'failed'
          // })
          this.isBizNameListShow = false;
        } else {
          postMethodWarning(res.message, 'error');
        }
      });
    },
    getBizTicketsCount() {
      let options = {
        params: {
          project_id: this.project.project_id,
        },
        query: {
          action_ids: this.dataTypeSelected === 'rt' ? 'result_table.query_data' : 'raw_data.query_data',
        },
      };
      this.bkRequest.httpRequest('auth/getBizTicketsCount', options).then(res => {
        if (res.result) {
          this.biz.list = res.data;
          this.biz.isSelectorLoading = false;
          this.handleResultTableField(); // 处理业务信息，把已申请和申请中的业务结果表加进去
          if (this.bkBizId) {
            this.selectedBizId = String(this.bkBizId);
          }
        } else {
          postMethodWarning(res.message, 'error');
        }
      });
    },
    handleResultTableField() {
      this.biz.list.forEach(item => {
        this.bkBizTicketsCountData[item.bk_biz_id] = item.count;
        if (Object.keys(item.count).length) {
          this.$set(item, 'showName', item.bk_biz_name
                    + ` 【${this.$t('已申请')}(${item.count.succeeded}) / ${this.$t('申请中')}(${item.count.processing})】`);
        } else {
          this.$set(item, 'showName', item.bk_biz_name);
        }
      });
    },
    sliderOpen(data) {
      this.sideslider.isShow = true;
      this.sideslider.stateId = data;
    },
  },
};
</script>
<style lang="scss" scoped>
.apply-project {
  .biz-style {
    clear: both;
    padding-top: 20px;
    .bk-form-item {
      padding-right: 50px;
      .bk-label {
        width: 126px;
        color: #313239;
      }
      .bk-form-content {
        margin-left: 126px;
      }
    }
  }
  .content {
    padding: 20px 50px 50px 0;
    display: flex;
    flex-wrap: nowrap;
    justify-content: space-between;
    .content-left {
      .bk-form {
        .bk-form-item {
          font-size: 14px;
          .bk-label {
            width: 126px;
            color: #313239;
          }
          .bk-form-content {
            margin-left: 126px;
            width: 247px;
            .bk-select.is-unselected:before {
              color: #63656e;
            }
            .bk-button {
              padding: 0 44px;
            }
            textarea {
              height: 120px;
            }
          }
        }
      }
    }
    .content-right {
      width: 360px;
      height: 328px;
      padding: 20px 3px 20px 20px;
      background: rgba(250, 251, 253, 1);
      .apply-status {
        display: flex;
        flex-wrap: nowrap;
        justify-content: space-between;
        margin-bottom: 10px;
        .status-item {
          height: 20px;
          line-height: 20px;
          font-size: 14px;
          color: #63656f;
          span {
            display: inline-block;
            width: 12px;
            height: 12px;
            margin-right: 6px;
          }
          &:first-child {
            span {
              background: #00d042;
            }
          }
          &:nth-child(2) {
            span {
              background: #0484ff;
            }
          }
          &:nth-child(3) {
            span {
              background: #ff0b28;
            }
          }
        }
      }
      .apply-process {
        float: left;
        width: 155px;
        margin-right: 10px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        background-color: rgba(222, 236, 255, 1);
        padding: 3px 8px 5px 8px;
        font-size: 12px;
        color: #3a84ff;
        margin-bottom: 6px;
        cursor: pointer;
      }
      .apply-process:nth-child(2n) {
        margin-right: 0;
      }
      .apply-processing {
        background: #3a84ff;
        color: white;
      }
      .apply-succeeded {
        background: #30d878;
        color: white;
      }
      .apply-failed {
        background: #ff5656;
        color: white;
      }
    }
  }
}
</style>

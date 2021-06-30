

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
  <div class="flow-add-tdw">
    <bkdata-dialog
      v-model="isShow"
      extCls="bkdata-dialog zIndex2501"
      width="700"
      :loading="dialogLoading"
      :hasFooter="false"
      :okText="$t('提交')"
      :cancelText="$t('删除')"
      :hasHeader="false"
      :closeIcon="false"
      :maskClose="false"
      @cancel="closeDialog"
      @confirm="confirm">
      <div class="flow-add-tdwWrap">
        <div class="apply-biz-header">
          {{ (isEdit ? $t('编辑') : $t('新增')) + $t('TDW表') }} ({{
            project.project_name ? project.project_name : project.project_id
          }})
          <i :title="$t('关闭')"
            class="bk-icon icon-close"
            @click="closeDialog" />
        </div>
        <div v-bkloading="{ isLoading: false }"
          class="add-tdw-content">
          <div class="bk-form add-tdw-form">
            <div class="bk-form-item">
              <label class="bk-label">{{ $t('集群') }}</label>
              <div class="bk-form-content">
                <bkdata-selector
                  v-tooltip.notrigger="{
                    content: validata.cluster_id.errorInfo,
                    visible: validata.cluster_id.status,
                    class: 'error-red',
                  }"
                  :isLoading="newChart.clusterLoading"
                  :list="newChart.clusterList"
                  :disabled="isEdit"
                  :displayKey="'name'"
                  :searchKey="'id'"
                  :settingKey="'id'"
                  :searchable="false"
                  :selected.sync="params.cluster_id"
                  @item-selected="clusterChange" />
              </div>
            </div>
            <div class="bk-form-item">
              <label class="bk-label">{{ $t('DB') }}</label>
              <div class="bk-form-content">
                <bkdata-selector
                  v-tooltip.notrigger="{
                    content: validata.db_name.errorInfo,
                    visible: validata.db_name.status,
                    class: 'error-red',
                  }"
                  :isLoading="newChart.dbLoading"
                  :list="newChart.dbList"
                  :disabled="isEdit"
                  :displayKey="'name'"
                  :searchKey="'name'"
                  :settingKey="'id'"
                  :searchable="true"
                  :selected.sync="params.db_name"
                  @item-selected="dbChange" />
              </div>
            </div>
            <div class="bk-form-item">
              <label class="bk-label">{{ $t('TDW表') }}</label>
              <div class="bk-form-content">
                <bkdata-selector
                  v-tooltip.notrigger="{
                    content: validata.table_name.errorInfo,
                    visible: validata.table_name.status,
                    class: 'error-red',
                  }"
                  :isLoading="newChart.tdwLoading"
                  :list="newChart.tdwList"
                  :disabled="isEdit"
                  :displayKey="'name'"
                  :searchKey="'id'"
                  :settingKey="'id'"
                  :searchable="true"
                  :selected.sync="params.table_name"
                  @item-selected="checkTableName" />
              </div>
            </div>
            <div class="bk-form-item">
              <label class="bk-label">{{ $t('结果表') }}</label>
              <div class="bk-form-content rt-content">
                <div class="biz_id">
                  <bkdata-selector
                    :disabled="isEdit"
                    :list="newChart.bizList"
                    :selected.sync="bk_biz_id"
                    :settingKey="'bk_biz_id'"
                    :displayKey="'bk_biz_name'" />
                </div>
                <div class="bk-form-content output">
                  <bkdata-input
                    v-model="result_table_name"
                    :class="[{ 'bk-form-input-error': validata.result_table_name.status }]"
                    :placeholder="$t('由英文字母_下划线和数字组成_且字母开头')"
                    :title="result_table_name"
                    :disabled="isEdit"
                    :maxlength="50"
                    name="validation_name"
                    type="text"
                    @keyup="checkDataFormat" />
                </div>
              </div>
            </div>
            <div class="bk-form-item"
              style="margin-top: 0">
              <label class="bk-label" />
              <div
                v-tooltip.notrigger="{
                  content: validata.result_table_name.errorInfo,
                  visible: validata.result_table_name.status,
                  class: 'error-red',
                }"
                class="bk-form-content table-name clearfix"
                :title="bk_biz_id + '_ ' + result_table_name">
                {{ bk_biz_id }}_{{ result_table_name }}
              </div>
            </div>
            <!-- <div class="bk-form-item" v-if="tdw.showLzId"> -->
            <div class="bk-form-item">
              <label class="bk-label">{{ $t('洛子ID') }}</label>
              <div class="bk-form-content"
                style="display: block">
                <bkdata-input
                  v-model="tdw.associated_lz_id"
                  :disabled="!tdw.checkResult"
                  :placeholder="$t('输入洛子ID')"
                  type="text"
                  @keyup="checkLzId" />
                <div style="margin-top: 10px; font-size: 0">
                  <label class="bk-label"
                    style="width: 90px">
                    {{ $t('统计频率') }}
                  </label>
                  <div class="bk-form-content"
                    style="margin-left: 90px; width: 310px">
                    <bkdata-input
                      v-model="tdw.count_freq"
                      style="margin-right: 10px; width: 150px; vertical-align: top"
                      disabled
                      :min="1"
                      type="number" />
                    <bkdata-selector
                      style="width: 150px; display: inline-block"
                      disabled
                      :list="countFreqList"
                      :selected.sync="tdw.count_freq_unit"
                      :settingKey="'value'"
                      :displayKey="'name'" />
                  </div>
                </div>
                <div v-show="validata.lzId.status"
                  class="error-tip">
                  {{ validata.lzId.errorInfo }}
                </div>
              </div>
            </div>
            <div class="bk-form-item mt20">
              <label class="bk-label">
                {{ $t('输出中文名') }}
              </label>
              <div class="bk-form-content">
                <bkdata-input
                  v-model="result_table_name_alias"
                  v-tooltip.notrigger="{
                    content: validata.result_table_name_alias.errorInfo,
                    visible: validata.result_table_name_alias.status,
                    class: 'error-red',
                  }"
                  :class="[{ 'bk-form-input-error': validata.result_table_name_alias.status }]"
                  :placeholder="$t('输出中文名')"
                  name="validation_name"
                  type="text"
                  @keyup="checkOutputName" />
              </div>
            </div>
            <div class="bk-form-item mt20">
              <label class="bk-label">
                {{ $t('描述') }}
              </label>
              <div class="bk-form-content">
                <bkdata-input v-model="description"
                  placeholder=""
                  name="validation_name"
                  type="text" />
              </div>
            </div>
          </div>
        </div>
      </div>
    </bkdata-dialog>
  </div>
</template>

<script>
import { compare, postMethodWarning } from '@/common/js/util.js';

export default {
  props: {
    bkBizId: {
      type: Number,
    },
    resultTableId: {
      type: String,
    },
    project: {
      type: Object,
      default: () => ({}),
    },
    commitType: {
      type: String,
      default: 'add',
    },
  },
  data() {
    return {
      dialogLoading: false,
      isShow: false,
      bk_biz_id: '',
      result_table_name: '', //  RT表名
      result_table_id: '', // 业务id_RT表名 需拼接
      result_table_name_alias: '', // 中文名
      description: '', // 描述
      params: {
        cluster_id: '', //  集群名
        db_name: '', // 库名
        table_name: '', // TDW表名
        table_comment: '',
        usability: 'Init', // 默认
        cols_info: [], // 默认
      },
      validata: {
        cluster_id: {
          status: false,
          errorInfo: this.validator.message.required,
        },
        db_name: {
          status: false,
          errorInfo: this.validator.message.required,
        },
        table_name: {
          status: false,
          errorInfo: this.validator.message.required,
        },
        bk_biz_id: {
          status: false,
          errorInfo: this.validator.message.required,
        },
        result_table_name: {
          status: false,
          errorInfo: this.validator.message.required,
        },
        result_table_name_alias: {
          status: false,
          errorInfo: this.validator.message.required,
        },
        lzId: {
          status: false,
          errorInfo: '',
        },
      },
      newChart: {
        btnLoading: false,
        bizList: [],
        bizLoading: false,
        clusterList: [],
        clusterLoading: false,
        dbList: [],
        dbLoading: false,
        tdwList: [],
        tdwLoading: false,
        rtLoading: false,
      },
      tdw: {
        // 洛子ID 相关
        associated_lz_id: '',
        count_freq: 0,
        count_freq_unit: '',
        showLzId: false,
        checkResult: false, // 检测结果的状态
      },
      countFreqList: [
        {
          value: 'H',
          name: this.$t('H-小时任务'),
        },
        {
          value: 'D',
          name: this.$t('D-天任务'),
        },
        {
          value: 'W',
          name: this.$t('W-周任务'),
        },
        {
          value: 'M',
          name: this.$t('M-月任务'),
        },
        {
          value: 'I',
          name: this.$t('I-分钟任务（最小粒度10分钟）'),
        },
        {
          value: 'S',
          name: this.$t('S-秒任务'),
        },
        {
          value: 'O',
          name: this.$t('O-一次性任务'),
        },
        {
          value: 'R',
          name: this.$t('R-非周期任务'),
        },
      ],
    };
  },
  computed: {
    isEdit() {
      return this.commitType === 'edit';
    },
  },
  watch: {
    'params.table_name'(newVal, oldVal) {
      if (newVal && oldVal) {
        this.resetLzId();
      }
    },
    isShow(newVal) {
      if (newVal) {
        this.getBizsByProjectId(this.bkBizId);
        this.getClusterList();
        if (this.isEdit) {
          this.getTdwInfoRequest();
        }
      } else {
        this.reset();
      }
    },
  },
  methods: {
    show(data) {
      this.isShow = true;
    },
    closeDialog() {
      this.isShow = false;
      this.$emit('closeApplyBiz');
    },
    /**
     * 获取集群列表
     */
    getClusterList() {
      this.newChart.clusterLoading = true;
      this.bkRequest
        .httpRequest('tdw/getClusterListForTdw')
        .then(res => {
          if (res.result) {
            const arr = [];
            for (const key in res.data) {
              arr.push({
                id: key,
                name: res.data[key],
              });
            }
            this.newChart.clusterList = [...arr];
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.newChart.clusterLoading = false;
        });
    },
    /**
     * 获取数据库列表
     */
    getDbListForTdw() {
      this.newChart.dbLoading = true;
      this.bkRequest
        .httpRequest('tdw/getDbListForTdw', {
          query: {
            cluster_id: this.params.cluster_id,
          },
        })
        .then(res => {
          if (res.result) {
            const arr = [];
            res.data.map(item => {
              arr.push({
                id: item,
                name: item,
              });
            });
            this.newChart.dbList = [...arr];
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.newChart.dbLoading = false;
        });
    },
    /**
     * 获取tdw表
     */
    getTdwTableList() {
      this.newChart.tdwLoading = true;
      this.bkRequest
        .httpRequest('tdw/getTdwTableList', {
          query: {
            cluster_id: this.params.cluster_id,
            db_name: this.params.db_name,
          },
        })
        .then(res => {
          if (res.result) {
            const arr = [];
            res.data.map(item => {
              arr.push({
                id: item.table_name,
                name: item.table_name_alias,
              });
            });
            this.newChart.tdwList = [...arr];
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.newChart.tdwLoading = false;
        });
    },
    /**
     * 获取应用列表
     */
    getBizsByProjectId(id) {
      this.newChart.bizList = [
        {
          bk_biz_name: '腾讯数据仓库TDW',
          bk_biz_id: 100616,
        },
      ];
      // this.newChart.bizLoading = true
      // this.bkRequest.httpRequest('dataFlow/getBizListByProjectId', { params: { pid: id } }).then(res => {
      //     if (res.result) {
      //         this.newChart.bizList = res.data
      //         if (res.data.length) {
      //             this.bk_biz_id = res.data[0].bk_biz_id
      //         }
      //     } else {
      //         postMethodWarning(res.message, 'error')
      //     }
      // }).finally(_ => {
      //     this.newChart.bizLoading = false
      // })
    },
    /**
     * 触发编辑时，获取标准化之后的信息并回填
     */
    getTdwInfoRequest() {
      this.bkRequest
        .httpRequest('tdw/getInfoTdw', {
          params: {
            rtid: this.resultTableId,
          },
          query: {
            extra: 'True',
          },
        })
        .then(res => {
          if (res.result) {
            const data = res.data;
            const tdw = data.extra.tdw ? data.extra.tdw : {};

            this.bk_biz_id = data.bk_biz_id;
            this.result_table_id = data.result_table_id;
            this.result_table_name = data.result_table_name;
            this.result_table_name_alias = data.result_table_name_alias;
            this.description = data.description;

            this.params.cluster_id = tdw.cluster_id;
            this.params.db_name = tdw.db_name;
            this.params.table_name = tdw.table_name;
            this.params.table_comment = tdw.table_comment;

            this.tdw.checkResult = false;
            this.tdw.associated_lz_id = tdw.associated_lz_id ? tdw.associated_lz_id['import'] : '';
            this.tdw.count_freq = tdw.count_freq;
            this.tdw.count_freq_unit = tdw.count_freq_unit;
            // 无法获取到TDW表 匹配的值，手动回填
            this.newChart.tdwList = [
              {
                id: tdw.table_name,
                name: tdw.table_name,
              },
            ];
            this.requestCheckLzId();
            this.getDbListForTdw();
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        ['finally'](_ => {
          this.newChart.bizLoading = false;
        });
    },
    /**
     * 回填之后需要检查洛子ID能否进行修改
     */
    requestCheckLzId() {
      this.bkRequest
        .httpRequest('tdw/getCheckLzId', {
          query: {
            db: this.params.db_name,
            table_name: this.params.table_name,
          },
        })
        .then(res => {
          if (res.result && res.data === null) {
            this.tdw.checkResult = true;
          }
        });
    },
    clusterChange() {
      this.getDbListForTdw();
      this.checkCluster();
      this.params.db_name = '';
    },
    dbChange() {
      // 校验
      this.checkDbName();
      this.params.table_name = '';
      this.getTdwTableList();
    },
    checkCluster() {
      this.validata.cluster_id.status = !(this.params.cluster_id || this.params.cluster_id === 0);
    },
    checkDbName() {
      this.validata.db_name.status = !(this.params.db_name || this.params.db_name === 0);
    },
    checkTableName() {
      this.validata.table_name.status = !(this.params.table_name || this.params.table_name === 0);
    },
    checkBkBizId() {
      this.validata.bk_biz_id.status = !(this.bk_biz_id || this.bk_biz_id === 0);
    },
    checkRt() {
      this.validata.result_table_name.status = !(this.result_table_name || this.result_table_name === 0);
    },
    checkOutputName() {
      this.validata.result_table_name_alias.status = !(
        this.result_table_name_alias || this.result_table_name_alias === 0
      );
    },
    checkLzId() {
      const value = this.tdw.associated_lz_id.trim();
      if (value) {
        this.validata.lzId.status = false;
      } else {
        this.validata.lzId.status = true;
        this.validata.lzId.errorInfo = this.validator.message.required;
      }
    },
    /*
     *   检验计算名称
     */
    checkDataFormat() {
      let val = this.result_table_name.trim();
      if (this.validator.validWordFormat(val)) {
        this.validata.result_table_name.status = false;
      } else {
        this.validata.result_table_name.status = true;
        if (val.length === 0) {
          this.validata.result_table_name.errorInfo = this.validator.message.required;
        } else {
          this.validata.result_table_name.errerrorInfoorMsg = this.validator.message.wordFormat;
        }
      }
    },
    confirm(item) {
      // 发起添加请求
      this.dialogLoading = true;
      setTimeout(() => {
        this.dialogLoading = false;
      }, 0);
      this.checkCluster();
      this.checkDbName();
      this.checkTableName();
      this.checkBkBizId();
      this.checkRt();
      this.checkOutputName();
      if (this.tdw.checkResult) {
        // 如果需要手动输入洛子ID
        this.checkLzId();
      }
      // this.checkRt()
      for (const key in this.validata) {
        if (this.validata[key].status) {
          return false;
        }
      }
      const data = {
        fid: this.$route.params.fid,
        bk_biz_id: this.bk_biz_id,
        table_name: this.result_table_name,
        output_name: this.result_table_name_alias,
        description: this.description,
      };
      if (this.tdw.checkResult) {
        data.associated_lz_id = this.tdw.associated_lz_id;
      }
      if (this.commitType === 'add') {
        this.addTdwRt(
          Object.assign(data, {
            cluster_id: this.params.cluster_id,
            db_name: this.params.db_name,
            tdw_table_name: this.params.table_name,
          })
        );
      } else {
        this.reverseTdwRt(data);
      }
    },
    addTdwRt(params) {
      this.newChart.btnLoading = true;
      this.bkRequest
        .httpRequest('tdw/createTdwRt', { params })
        .then(res => {
          if (res.result) {
            this.$parent.addTdwAfter(this.bkBizId);
            this.$bkMessage({
              message: this.$t('成功'),
              theme: 'success',
            });
            this.$emit('addTdwSuccess', { biz_id: params.bk_biz_id, rtId: res.data.result_table_id });
            this.closeDialog();
          } else {
            if (res.code === '1521284') {
              // this.tdw.showLzId = true
              this.tdw.checkResult = true;
              this.validata.lzId.status = true;
              this.validata.lzId.errorInfo = this.$t('未采集到统计频率，请输入洛子ID后重试');
            }
            postMethodWarning(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.newChart.btnLoading = false;
        });
    },
    reverseTdwRt(params) {
      this.newChart.btnLoading = true;
      this.bkRequest
        .httpRequest('tdw/updateTdwRt', { params })
        .then(res => {
          if (res.result) {
            this.$parent.addTdwAfter(this.bkBizId);
            this.$bkMessage({
              message: this.$t('成功'),
              theme: 'success',
            });
            this.closeDialog();
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.newChart.btnLoading = false;
        });
    },
    deleteTdwRt(done) {
      this.newChart.btnLoading = true;
      this.bkRequest
        .httpRequest('tdw/deleteTdwRt', {
          params: {
            fid: this.$route.params.fid,
            bk_biz_id: this.bkBizId,
            table_name: this.result_table_name,
          },
        })
        .then(res => {
          if (res.result) {
            this.$parent.addTdwAfter(this.bkBizId);
            this.closeDialog();
            this.$bkMessage({
              message: this.$t('成功'),
              theme: 'success',
            });
            done();
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.newChart.btnLoading = false;
        });
    },
    deleteRt() {
      this.$bkInfo({
        theme: 'warning',
        title: '此操作存在风险',
        content: '删除后会影响与此表相关联的任务，且该表无法在数据查询中使用，请确认是否删除？',
        confirmFn: done => {
          this.deleteTdwRt(done);
        },
      });
    },
    resetLzId() {
      this.tdw.checkResult = false;
      this.tdw.associated_lz_id = '';
      this.validata.lzId.status = false;
    },
    reset() {
      this.bk_biz_id = '';
      this.result_table_name = ''; //  RT表名
      this.result_table_id = ''; // 业务id_RT表名 需拼接
      this.result_table_name_alias = ''; // 中文名
      this.description = ''; // 描述
      this.params = {
        cluster_id: '', //  集群名
        db_name: '', // 库名
        table_name: '', // TDW表名
        table_comment: '',
        usability: 'Init', // 默认
        cols_info: [], // 默认
      };
      this.tdw = {
        // 洛子ID 相关
        associated_lz_id: '',
        count_freq: 0,
        count_freq_unit: '',
        showLzId: false,
        checkResult: false, // 检测结果的状态
      };
      this.newChart.bizList = [];
      this.newChart.clusterList = [];
      this.newChart.dbList = [];
      this.newChart.tdwList = [];
      for (const key in this.validata) {
        this.validata[key].status = false;
      }
    },
  },
};
</script>

<style lang="scss">
.error-red {
  z-index: 9999;
}
.flow-add-tdwWrap {
  padding: 20px;
  .add-tdw-content {
    margin: 20px 0 0;
  }
  .add-tdw-form {
    padding: 30px 0;
    border: 1px solid #e6e9f0;
  }
  .bk-form-content {
    width: 400px;
  }
  .rt-content {
    font-size: 0;
  }
  .biz_id {
    display: inline-block;
    width: 160px;
  }
  .output {
    margin-left: 20px;
    display: inline-block;
    width: 220px;
    vertical-align: top;
  }
  .add-button {
    text-align: center;
    button {
      min-width: 120px;
    }
  }
  .error-tip {
    color: red;
  }
  .bk-form-content-half {
    display: flex;
  }
  .bk-form-half {
    flex: 1;
    padding: 0 10px 0 0;
    & + .bk-form-half {
      padding: 0 0 0 10px;
    }
  }
  .table-name {
    margin-top: 5px;
    line-height: 28px;
    height: 28px;
    background: #3a84ff;
    border-radius: 2px;
    color: #fff;
    margin-left: 125px;
    padding-left: 10px;
    overflow: hidden;
  }
}
.apply-biz {
  &-header {
    padding-left: 10px;
    position: relative;
    color: #212232;
    font-size: 16px;
    font-weight: bold;
    &::after {
      position: absolute;
      height: 100%;
      left: 0px;
      content: '';
      border-left: 4px solid #3a84ff;
    }
    .bk-icon {
      float: right;
      cursor: pointer;
    }
  }

  &-content {
    margin: 20px 0 0 0px;
    &-header {
      margin-bottom: 20px;
      background: #e1f3ff;
      min-height: 42px;
      line-height: 42px;
      color: #3c96ff;
      padding: 0px 10px;
    }
  }
}
</style>

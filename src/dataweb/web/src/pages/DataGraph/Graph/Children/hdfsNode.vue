

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
  <bkdata-tab :active="'config'"
    :class="'node-editing'">
    <bkdata-tab-panel :label="$t('节点配置')"
      name="config">
      <div id="validate_form3"
        class="bk-form">
        <!--储存节点编辑-->
        <div v-bkloading="{ isLoading: loading }"
          class="save-edit node">
          <div class="content-up">
            <StorageHeader
              v-model="params"
              :class="[{ 'tdw-layout': nodeType === 'tdw_storage' }]"
              :parentConfig="parentConfig"
              :isTdwStorage="nodeType === 'tdw_storage'"
              :tdwDisplay="outputDisplay"
              :isNewForm="!selfConfig.hasOwnProperty('node_id')">
              <template v-if="nodeType === 'tdw_storage'"
                #resultTable>
                <bkdata-input
                  v-model="editName"
                  v-tooltip.notrigger="{
                    content: validata.editName.errorMsg,
                    visible: validata.editName.status,
                    class: 'error-red',
                  }"
                  :title="outputDisplay"
                  class="edit-content"
                  type="text"
                  :disabled="selfConfig.hasOwnProperty('node_id')"
                  @keyup="checkEditName" />
              </template>
            </StorageHeader>

            <div v-if="nodeType === 'tdw_storage' && isEnableTdw"
              class="bk-form-item mt20">
              <label class="bk-label">
                {{ $t('输出中文名') }}
                <span class="required">*</span>
              </label>
              <div :class="['bk-form-content', { 'is-danger': validata.output_name.status }]">
                <bkdata-input
                  v-model="params.config.output_name"
                  v-tooltip.notrigger="{
                    content: validata.output_name.errorMsg,
                    visible: validata.output_name.status,
                    class: 'error-red',
                  }"
                  :placeholder="$t('输出中文名')"
                  :maxlength="50"
                  name="validation_name"
                  type="text"
                  @keyup="checkOutputChineseName" />
              </div>
            </div>
          </div>
          <div class="content-down">
            <template v-if="nodeType === 'tdw_storage' && isEnableTdw">
              <div class="bk-form-item">
                <label class="bk-label">{{ $t('应用组名称') }}：</label>
                <div class="bk-form-content">
                  <bkdata-input v-model="applicationGroupValue"
                    :disabled="true" />
                </div>
              </div>
              <div class="bk-form-item">
                <label class="bk-label"
                  style="text-align: left">
                  {{ $t('td-bank接入') }}
                </label>
              </div>
              <div class="bk-form-item">
                <label class="bk-label"><span class="required">*</span>BID：</label>
                <div :class="['bk-form-content', { 'is-danger': validata.bid.status }]">
                  <bkdata-selector
                    v-tooltip.notrigger="{
                      content: validata.bid.errorMsg,
                      visible: validata.bid.status,
                      class: 'error-red',
                    }"
                    :list="dataByAppliedGroup"
                    :placeholder="$t('请选择')"
                    :settingKey="'cluster_name'"
                    :displayKey="'cluster_name'"
                    :selected.sync="params.config.bid"
                    @item-selected="bidSelected" />
                </div>
              </div>
              <div class="bk-form-item">
                <label class="bk-label"><span class="required">*</span>{{ $t('TdBank地址') }}：</label>
                <div :class="['bk-form-content', { 'is-danger': validata.tdbank_address.status }]">
                  <bkdata-input
                    v-model.trim="params.config.tdbank_address"
                    v-tooltip.notrigger="{
                      content: validata.tdbank_address.errorMsg,
                      visible: validata.tdbank_address.status,
                      class: 'error-red',
                    }"
                    :disabled="true"
                    :placeholder="$t('请输入TdBank地址')"
                    @keyup="checkTdbankAddress" />
                </div>
              </div>
              <div class="bk-form-item">
                <label class="bk-label"><span class="required">*</span>{{ $t('KV分隔符') }}：</label>
                <div :class="['bk-form-content', { 'is-danger': validata.kv_separator.status }]">
                  <bkdata-input
                    v-model.trim="params.config.kv_separator"
                    v-tooltip.notrigger="{
                      content: validata.kv_separator.errorMsg,
                      visible: validata.kv_separator.status,
                      class: 'error-red',
                    }"
                    :disabled="true"
                    :placeholder="$t('请输入KV分隔符')"
                    @keyup="checkKvSeparator" />
                </div>
              </div>
              <div class="bk-form-item">
                <label class="bk-label"><span class="required">*</span>{{ $t('记录分隔符') }}：</label>
                <div :class="['bk-form-content', { 'is-danger': validata.record_separator.status }]">
                  <bkdata-input
                    v-model.trim="params.config.record_separator"
                    v-tooltip.notrigger="{
                      content: validata.record_separator.errorMsg,
                      visible: validata.record_separator.status,
                      class: 'error-red',
                    }"
                    :placeholder="$t('请输入记录分隔符')"
                    :disabled="true"
                    @keyup="checkRecordSeparator" />
                </div>
              </div>
              <div class="bk-form-item">
                <label class="bk-label"
                  style="text-align: left">
                  {{ $t('tdw配置') }}
                </label>
              </div>
            </template>
            <template v-if="nodeType === 'tdw_storage' && !isEnableTdw">
              <div class="bk-form-item">
                <label class="bk-label" />
                <div class="bk-form-content">
                  <a
                    v-tooltip.notrigger="{
                      content: $t('请配置TDW'),
                      visible: validata.bid.status,
                      class: 'error-red zIndex9999',
                    }"
                    href="javascript:;"
                    @click="toggleTaskDetail">
                    {{ $t('启用TDW并配置应用组') }}
                  </a>
                </div>
              </div>
            </template>
            <div class="bk-form-item">
              <label class="bk-label">
                {{ $t('存储集群') }}
                <span class="required">*</span>
              </label>
              <div class="bk-form-content">
                <cluster-selector
                  v-model="cluster"
                  :clusterLoading.sync="clusterLoading"
                  :colorLevel="true"
                  :selfConfig="selfConfig"
                  :clusterType="nodeTypeLower" />
              </div>
            </div>
            <div v-if="nodeType === 'tdw_storage' && isEnableTdw"
              class="bk-form-item">
              <label class="bk-label">
                {{ $t('DB') }}
                <span class="required">*</span>
              </label>
              <div :class="['bk-form-content', { 'is-danger': validata.db_name.status }]">
                <bkdata-selector
                  v-tooltip.notrigger="{
                    content: validata.db_name.errorMsg,
                    visible: validata.db_name.status,
                    class: 'error-red',
                  }"
                  :list="dbList"
                  :selected.sync="params.config.db_name"
                  :settingKey="'id'"
                  :displayKey="'name'"
                  @change="checkDbName" />
              </div>
            </div>
            <div class="bk-form-item">
              <label class="bk-label">
                {{ $t('过期时间') }}
                <span class="required">*</span>
              </label>
              <div class="bk-form-content">
                <bkdata-selector
                  v-tooltip.notrigger="{
                    content: validata.lastTime.errorMsg,
                    visible: validata.lastTime.status,
                    class: 'error-red',
                  }"
                  :class="{ error: validata.lastTime.status }"
                  :list="expiresList"
                  :disabled="!cluster"
                  :selected.sync="params.config.expires"
                  :settingKey="'id'"
                  :displayKey="'text'"
                  @visible-toggle="divScrollBottom" />
              </div>
            </div>
          </div>
          <div v-if="selfConfig.hasOwnProperty('node_id') || nodeType !== 'tdw_storage'"
            class="content-down">
            <StorageTypeFieldsConfig
              v-model="fieldConfig"
              :clusterType="nodeTypeLower"
              :forceAll="!storageConfig.map(item => `${item}_storage`).includes(nodeType)"
              :isNewform="!selfConfig.hasOwnProperty('node_id')"
              :rtId="rtId"
              :withConfiguration="false"
              @onSchemaAndSqlComplete="onSchemaAndSqlComplete" />
          </div>
        </div>
      </div>
    </bkdata-tab-panel>
    <bkdata-tab-panel
      v-if="selfConfig.hasOwnProperty('node_id')"
      :class="'result-tab'"
      :label="$t('结果查询')"
      name="sql">
      <template v-if="selfConfig.hasOwnProperty('node_id')">
        <SqlQuery
          :nodeSearch="true"
          :formData="sqlQueryData"
          :showTitle="false"
          :resultTable="params.config.result_table_id"
          :activeType="clusterType" />
      </template>
    </bkdata-tab-panel>
  </bkdata-tab>
</template>

<script>
import Bus from '@/common/js/bus';
import SqlQuery from '@/pages/DataQuery/Components/SqlQuery';
import clusterSelector from './clusterSelect/cluster_type_selector';
import StorageTypeFieldsConfig from './components/StorageTypeFieldsConfig';
import { postMethodWarning } from '@/common/js/util.js';
import StorageHeader from './components/Storage.header';
import childMixin from './config/child.global.mixin.js';
export default {
  components: {
    clusterSelector,
    StorageTypeFieldsConfig,
    StorageHeader,
    SqlQuery,
  },
  mixins: [childMixin],
  props: {
    nodeType: {
      type: String,
      default: '',
    },
    nodeDisname: String,
    project: {
      type: Object,
      default() {
        return {};
      },
    },
  },
  data() {
    return {
      fieldConfig: {},
      editName: '',
      dataByAppliedGroup: [],
      // bidSelected: '',
      clusterLoading: true,
      cluster: '',
      buttonStatus: {
        isDisabled: false,
      },
      applicationGroupValue: '',
      loading: true,
      mySqlData: [], // 字段列表
      parentConfig: {}, // 父节点配置
      selfConfig: [], // 自身节点配置
      expiresList: [],
      clusterList: [
        {
          id: 'default',
          text: this.$t('默认集群'),
        },
      ],
      dbList: [],
      // 请求的参数
      params: {
        node_type: this.nodeType,
        config: {
          bk_biz_id: '',
          table_name: '',
          name: '',
          biz_id: 0,
          cluster: 'default',
          expires: 3,
          bid: '',
          tdbank_address: '',
          kv_separator: '',
          record_separator: '',
          result_table_id: '',
          output_name: '',
          cluster_id: '',
          db_name: '',
        },
        from_links: [],
        frontend_info: {},
      },
      validata: {},
      parentResultList: {},
    };
  },
  computed: {
    sqlQueryData() {
      return {
        storage: {
          [this.clusterType]: this.fieldConfig,
        },
      };
    },
    clusterType() {
      return this.currentDataType.clusterType;
    },
    currentDataType() {
      let index = this.nodeType.indexOf('_');
      let name = (index && this.nodeType.substring(0, index)) || '';
      return (
        (index && { displayName: this.nodeDisname, clusterType: name }) || {
          displayName: 'MySQL',
          clusterType: 'mysql',
        }
      );
    },
    nodeName: function () {
      let nodeName;
      if (this.nodeType === 'tdw_storage') {
        nodeName = `${this.outputDisplay}${this.editName}(TDW)`;
      } else {
        nodeName = `${this.rtId}(${this.nodeDisname})`;
      }
      // eslint-disable-next-line vue/no-side-effects-in-computed-properties
      this.params.config.name = nodeName;
      return nodeName;
    },
    rtId() {
      if (Object.keys(this.parentConfig).length) {
        return this.nodeType === 'tdw_storage'
          ? `${this.outputDisplay}${this.editName}`
          : `${this.parentConfig.result_table_ids[0]}`;
      }
      return '';
    },
    isEnableTdw() {
      return this.project.tdw_conf && this.project.tdw_conf !== '{}';
    },
    storageConfig() {
      return this.$store.state.ide.storageConfig;
    },
    nodeTypeLower() {
      return this.nodeType === 'tdw_storage' ? 'tdw' : 'hdfs';
    },
    outputDisplay() {
      return this.nodeType === 'tdw_storage'
        ? `${this.parentConfig.bk_biz_id}_tdw_`
        : this.rtId
            + `(${this.getResultIdAlias(
              this.parentConfig.result_table_ids ? this.parentConfig.result_table_ids[0] : ''
            )})`;
    },
  },
  watch: {
    dataByAppliedGroup(val) {
      let obj = val.find(item => item.cluster_name === this.params.config.bid);
      if (obj && obj.connection) {
        this.params.config.tdbank_address = obj.connection.tdbank_address;
        this.params.config.kv_separator = obj.connection.kv_separator;
        this.params.config.record_separator = obj.connection.record_separator;
      }
    },
    rtId(val) {
      if (val) {
        this.params.config.result_table_id = val;
      }
    },
    cluster(val) {
      this.params.config.cluster = val;
      this.getStorageClusterExpiresConfigs();
      if (this.nodeType === 'tdw_storage' && this.isEnableTdw) {
        this.getDbListForTdw();
      }
    },
    isEnableTdw(newVal) {
      if (newVal) {
        const flow = JSON.parse(this.project.tdw_conf);
        this.applicationGroupValue = flow.tdwAppGroup;
      }
    },
  },
  created() {
    this.initValid();
  },
  methods: {
    getBidList() {
      let query = {
        cluster_group: this.applicationGroupValue,
      };
      this.bkRequest.httpRequest('tdw/getBIDTdw', { query }).then(res => {
        if (res.result) {
          this.dataByAppliedGroup = res.data;
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      });
    },
    getResultIdAlias(rtid) {
      const result = this.parentResultList[rtid] || {};
      const alias = (result.resultTable || {}).alias;
      return alias || rtid;
    },
    getDbListForTdw() {
      this.bkRequest
        .httpRequest('tdw/getDbListWithCreate', {
          // 2019/5/22 更新新接口  原接口getDbListForTdw保留
          query: {
            cluster_id: this.cluster,
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
            this.dbList = [...arr];
          } else {
            postMethodWarning(res.message, 'error');
          }
        });
    },
    selectedType(val) {
      this.params.config.cluster = val;
    },
    /*
     * @改变按钮的状态
     * */
    changeButtonStatus() {
      this.buttonStatus.isDisabled = false;
    },
    /**
     * bid选中
     */
    bidSelected(id, item) {
      this.params.config.tdbank_address = item.connection.tdbank_address;
      this.params.config.kv_separator = item.connection.kv_separator;
      this.params.config.record_separator = item.connection.record_separator;
      if (this.fieldConfig.config) {
        // 首次建立时，TDM没有fieldConfig，编辑时才有
        this.fieldConfig.config.bid = item.cluster_name;
      }
    },
    /*
     *   下拉滚动
     * */
    divScrollBottom() {
      var div = document.getElementById('validate_form3');
      setTimeout(function () {
        div.scrollTop = 150;
      }, 200);
    },
    toggleTaskDetail() {
      Bus.$emit('toggleTaskDetail', true);
    },
    /*
     *   校验存储名称
     */
    checkDataFormat() {
      let val = this.params.config.table_name.trim();
      if (this.validator.validWordFormat(val)) {
        this.validata.table_name.status = false;
      } else if (val.length === 0) {
        this.validata.table_name.status = true;
        this.validata.table_name.errorMsg = this.validator.message.required;
      } else {
        this.validata.table_name.status = true;
        this.validata.table_name.errorMsg = this.validator.message.wordFormat;
      }
    },
    /*
     *   校验存储描述
     */
    checkDataDes() {
      let val = this.params.config.name.trim();
      if (val.length < 51 && val.length !== 0) {
        this.validata.name.status = false;
      } else {
        this.validata.name.status = true;
        if (val.length === 0) {
          this.validata.name.errorMsg = this.validator.message.required;
        } else {
          this.validata.name.errorMsg = this.validator.message.max50;
        }
      }
    },
    /**
     * 当为tdw存储时需进行以下验证
     */
    checkTdwBid() {
      let val = this.params.config.bid.trim();
      let reg = new RegExp('^[0-9a-zA-Z_]{1,}$');
      if (this.validator.validWordFormat(val)) {
        this.validata.bid.status = false;
      } else {
        this.validata.bid.status = true;
        if (val.length === 0) {
          this.validata.bid.errorMsg = this.validator.message.required;
        } else {
          this.validata.bid.errorMsg = this.validator.message.wordFormat;
        }
      }
    },
    checkTdbankAddress() {
      if (this.params.config.tdbank_address.trim()) {
        this.validata.tdbank_address.status = false;
      } else {
        this.validata.tdbank_address.status = true;
        this.validata.tdbank_address.errorMsg = this.validator.message.required;
      }
    },
    checkEditName() {
      if (this.editName) {
        this.validata.editName.status = false;
      } else {
        this.validata.editName.status = true;
        this.validata.editName.errorMsg = this.validator.message.required;
      }
    },
    checkKvSeparator() {
      if (this.params.config.kv_separator.trim()) {
        this.validata.kv_separator.status = false;
      } else {
        this.validata.kv_separator.status = true;
        this.validata.kv_separator.errorMsg = this.validator.message.required;
      }
    },
    checkRecordSeparator() {
      if (this.params.config.record_separator.trim()) {
        this.validata.record_separator.status = false;
      } else {
        this.validata.record_separator.status = true;
        this.validata.record_separator.errorMsg = this.validator.message.required;
      }
    },
    checkDbName() {
      this.$nextTick(() => {
        if (this.params.config.db_name) {
          this.validata.db_name.status = false;
        } else {
          this.validata.db_name.status = true;
          this.validata.db_name.errorMsg = this.validator.message.required;
        }
      });
    },
    checkOutputChineseName(e) {
      if (this.params.config.output_name) {
        let val = this.params.config.output_name.trim();
        if (val.length < 51 && val.length !== 0) {
          this.validata.output_name.status = false;
        } else {
          this.validata.output_name.status = true;
          if (val.length === 0) {
            this.validata.output_name.errorMsg = this.validator.message.required;
          } else {
            this.validata.output_name.errorMsg = this.validator.message.max50;
          }
          return true;
        }
      } else {
        this.validata.output_name.status = true;
        this.validata.output_name.errorMsg = this.validator.message.required;
        return true;
      }
    },
    /*
     *   配置回填
     */
    setConfigBack: function (self, source, fl) {
      this.selfConfig = self;
      this.parentConfig = source[0];
      this.params = this.resetParams(); // tdw存储存存在超出的参数，需要重置一下
      this.editName =        (this.selfConfig.hasOwnProperty('node_id')
          && self.node_config.result_table_id.replace(/\d*_tdw_(\.*)/, '$1'))
        || this.parentConfig.result_table_ids[0].replace(/\d*_(\.*)/, '$1');
      this.params.from_links = fl;
      this.params.frontend_info = self.frontend_info;
      this.params.config.bk_biz_id = this.parentConfig.bk_biz_id;
      this.params.config.bk_biz_name = this.getBizNameWithoutBizIdByBizId(this.parentConfig.bk_biz_id);
      this.initValid();
      this.getResultList(this.parentConfig.result_table_ids);

      // 对于没有 config 节点，初始化 config
      if (self.node_config === undefined) {
        self.node_config = this.initConfig();
      }
      if (this.selfConfig.hasOwnProperty('node_id') || this.selfConfig.isCopy) {
        this.cluster = this.selfConfig.node_config.cluster;
        for (let key in self.node_config) {
          if (self.node_config[key] || self.node_config[key] === 0) {
            this.params.config[key] = self.node_config[key];
          } else {
            this.params.config[key] = '';
          }
        }
      } else {
        if (this.nodeType === 'tdw_storage') {
          // 首次进入且为tdw存储， 不加载scehma， loding置为false
          this.loading = false;
        }
      }
      if (this.isEnableTdw) {
        const flow = JSON.parse(this.project.tdw_conf);
        this.applicationGroupValue = flow.tdwAppGroup;
        this.getBidList();
      }
    },

    getResultList(resultTableIds = []) {
      this.loading = true;
      this.getAllResultList(resultTableIds)
        .then(res => {
          this.parentResultList = res;
        })
        ['catch'](err => {
          this.getMethodWarning(err.message, 'error');
        });
    },
    initConfig() {
      const data = {
        result_table_id: '',
        bk_biz_id: this.parentConfig.bk_biz_id,
        table_name: '',
        name: '',
        cluster: this.cluster,
        expires: 3,
      };
      if (this.nodeType === 'tdw_storage') {
        Object.assign(data, {
          // tdw存储才有的参数
          bid: '',
          tdbank_address: '',
          kv_separator: '',
          record_separator: '',
          expires_unit: '',
        });
      }
      return data;
    },
    /*
     * @初始化错误提示状态
     */
    initValid() {
      this.validata = {
        editName: {
          status: false,
          errorMsg: '',
        },
        table_name: {
          status: false,
          errorMsg: '',
        },
        name: {
          status: false,
          errorMsg: '',
        },
        lastTime: {
          status: false,
          errorMsg: '',
        },
        bid: {
          status: false,
          errorMsg: '',
        },
        tdbank_address: {
          status: false,
          errorMsg: '',
        },
        kv_separator: {
          status: false,
          errorMsg: '',
        },
        record_separator: {
          status: false,
          errorMsg: '',
        },
        output_name: {
          status: false,
          errorMsg: '',
        },
        cluster_id: {
          status: false,
          errorMsg: '',
        },
        db_name: {
          status: false,
          errorMsg: '',
        },
      };
    },
    /*
     *   重置参数
     */
    resetParams() {
      const params = {
        node_type: this.nodeType,
        config: {
          bk_biz_id: '',
          table_name: '',
          name: '',
          biz_id: 0,
          cluster: 'default',
          expires: 3,
        },
        from_links: [],
        frontend_info: {},
      };
      if (this.nodeType === 'tdw_storage') {
        Object.assign(params.config, {
          // tdw存储才有的参数
          bid: '',
          tdbank_address: '',
          kv_separator: '',
          record_separator: '',
          result_table_id: '',
          output_name: '',
          cluster_id: '',
          db_name: '',
        });
      }
      return params;
    },
    /*
     *   保存触发创建还是更新
     */
    validateFormData: function () {
      if (this.nodeType === 'tdw_storage') {
        // 需要进行以下验证
        this.checkEditName();
        this.checkTdwBid();
        this.checkTdbankAddress();
        this.checkKvSeparator();
        this.checkRecordSeparator();
        this.checkDbName();
        this.checkOutputChineseName();
      }
      let result = [];
      Object.assign(this.params.config, this.fieldConfig.config);
      for (var key in this.validata) {
        result.push(this.validata[key].status);
        result.forEach(function (item, index) {
          if (!item) {
            result.splice(index, 1);
          }
        });
      }
      if (result.length === 0) {
        this.params.config.from_result_table_ids = this.parentConfig.result_table_ids;
        if (this.nodeType === 'tdw_storage') {
          this.params.config.cluster_id = this.cluster;
          this.params.config.result_table_id = `${this.outputDisplay}${this.editName}`;
        }
        if (this.selfConfig.hasOwnProperty('node_id')) {
          this.params.flow_id = this.$route.params.fid;
        }
      } else {
        return false;
      }
      return true;
    },
    /*
     *   关闭右侧弹窗
     */
    closeSider: function () {
      this.buttonStatus.isDisabled = false;
      this.loading = true;
      this.$emit('closeSider');
    },
    onSchemaAndSqlComplete(res) {
      this.loading = false;
    },

    getStorageClusterExpiresConfigs() {
      this.expiresList = [];
      this.$store
        .dispatch('api/flows/getStorageClusterExpiresConfigs', { type: this.nodeTypeLower, name: this.cluster })
        .then(res => {
          if (res.result) {
            res.data.expires = JSON.parse(res.data.expires);
            this.expiresConfig = res.data;
            this.getClusterExpiresConfig();
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
    },
    getClusterExpiresConfig() {
      this.getExpiresListCommon();
    },
  },
};
</script>
<style lang="scss" scoped>
.edit-content {
  margin-left: 2px;
}
</style>

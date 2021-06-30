

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
    :class="['esNode node-editing', { esSearch: isEs }]"
    @tab-change="tabChanged">
    <bkdata-tab-panel :label="$t('节点配置')"
      name="config">
      <div class="bk-form">
        <!--储存节点编辑-->
        <div v-bkloading="{ isLoading: loading }"
          class="save-edit node">
          <div class="content-up">
            <!-- <h3 class="title">{{$t('Elasticsearch节点')}}</h3> -->
            <div class="result-table clearfix">
              <StorageHeader
                :parentConfig="parentConfig"
                :nodeParams="params"
                :isNewForm="!selfConfig.hasOwnProperty('node_id')"
                :jumpResult="jumpResult" />
            </div>
          </div>
          <div class="content-down">
            <div class="bk-form-item">
              <label class="bk-label">
                {{ $t('存储集群') }}
                <span class="required">*</span>
              </label>
              <div class="bk-form-content">
                <cluster-selector
                  v-model="cluster"
                  v-tooltip.notrigger.top="validateCluster['cluster']"
                  :colorLevel="true"
                  :selfConfig="selfConfig"
                  :clusterLoading.sync="clusterLoading"
                  :clusterType="clusterType"
                  @change="reflesh('cluster')"
                  @handlerCopyStorage="handlerCopyStorage" />
              </div>
            </div>
            <div class="bk-form-item">
              <label class="bk-label">
                {{ $t('过期时间') }}
                <span class="required">*</span>
              </label>
              <div class="bk-form-content">
                <bkdata-selector
                  :list="expiresList"
                  :disabled="!cluster"
                  :selected.sync="params.config.expires"
                  :settingKey="'id'"
                  :displayKey="'text'" />
              </div>
            </div>
            <!-- 注意！！！  打开此选项，请务必打开 validateCluste 下的has_replica 校验-->
            <div class="bk-form-item">
              <label class="bk-label">
                {{ $t('副本存储') }}
                <span class="required">*</span>
              </label>
              <div v-tooltip.notrigger.top="validateCluster['has_replica']"
                class="bk-form-content">
                <bkdata-radio-group v-model="params.config.has_replica"
                  @change="reflesh('has_replica')">
                  <bkdata-radio v-bk-tooltips="$t('数据存一个副本')"
                    :disabled="isCopyStorageDisabled"
                    :value="true">
                    {{ $t('是') }}
                  </bkdata-radio>
                  <bkdata-radio v-bk-tooltips="$t('数据不存副本')"
                    :value="false">
                    {{ $t('否') }}
                  </bkdata-radio>
                </bkdata-radio-group>
              </div>
            </div>

            <StorageDudeplication v-model="params.config" />
          </div>
          <div class="content-down">
            <StorageTypeFieldsConfig
              ref="fieldConfig"
              v-model="fieldConfig"
              :clusterType="clusterType"
              :forceAll="forceAllCluster"
              :isNewform="isNewForm"
              :rtId="rtId"
              :withConfiguration="true"
              :openDedeplication="params.config.has_unique_key"
              :shouldProcessFields="shouldProcessFields"
              :nodeType="nodeType"
              @onSchemaAndSqlComplete="onSchemaAndSqlComplete" />
          </div>
        </div>
      </div>
    </bkdata-tab-panel>
    <bkdata-tab-panel v-if="selfConfig.hasOwnProperty('node_id')"
      :label="$t('结果查询')"
      name="es">
      <template v-if="selfConfig.hasOwnProperty('node_id')">
        <es-query
          ref="esQuery"
          :chineseData="results.chineseData"
          :nodeSearch="true"
          :resultTable="params.config.result_table_id"
          :showTitle="false" />
      </template>
    </bkdata-tab-panel>
  </bkdata-tab>
</template>

<script>
import esQuery from '@/pages/dataManage/esQuery';
import clusterSelector from './clusterSelect/cluster_type_selector';
import StorageTypeFieldsConfig from './components/StorageTypeFieldsConfig';
import StorageDudeplication from './components/Storage.fieldDudeplication';
import { validateRules } from '@/common/js/validate.js';
import StorageHeader from './components/Storage.header';
import childMixin from './config/child.global.mixin.js';
import uniqueKeyMixin from './config/storage.uniqueKey.mixin.js';
export default {
  components: {
    esQuery,
    clusterSelector,
    StorageTypeFieldsConfig,
    StorageDudeplication,
    StorageHeader,
  },
  mixins: [childMixin, uniqueKeyMixin],
  props: {
    nodeType: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      clusterType: 'es',
      fieldConfig: {},
      clusterLoading: true,
      cluster: '',
      outputDisplay: '',
      jumpResult: false, // 跳转结果数据表
      buttonStatus: {
        isDisabled: false,
      },
      loading: true,
      mySqlData: [], // 表格数据
      parentConfig: {}, // 父节点配置
      selfConfig: '', // 自身节点参数
      params: {
        // 请求的参数
        node_type: 'elastic_storage',
        config: {
          result_table_id: '',
          name: '',
          bk_biz_id: 1,
          cluster: '', // 存储集群
          analyzed_fields: [], // 分词字段列表
          json_fields: [], // json字段
          doc_values_fields: [], // 分组聚合字段
          date_fields: [], // 时间字段列表,
          storage_keys: [],
          expires: 30, // 过期时间
          has_replica: '',
          has_unique_key: false,
        },
        from_links: [],
        frontend_info: {},
      },
      validata: {
        // 校验状态和错误信息
        table_name: {
          status: false,
          errorMsg: '',
        },
        name: {
          status: false,
          errorMsg: '',
        },
      },
      validateCluster: {
        cluster: {
          regs: { required: true },
          content: '',
          visible: false,
          class: 'error-red',
        },
        has_replica: {
          regs: { required: true },
          content: '',
          visible: false,
          class: 'error-red',
        },
      },
      expiresList: [],
      results: {
        name: '2_output',
        chineseData: '',
      },
      isEs: false,
      isCopyStorageDisabled: false,
    };
  },
  computed: {
    forceAllCluster() {
      return !this.storageConfig.map(item => `${item}_storage`).includes(this.nodeType);
    },
    isNewForm() {
      return !this.selfConfig.hasOwnProperty('node_id');
    },
    nodeName() {
      // eslint-disable-next-line vue/no-side-effects-in-computed-properties
      this.params.config.name = `${this.rtId}(Elasticsearch)`;
      return `${this.rtId}(Elasticsearch)`;
    },
    rtId() {
      if (Object.keys(this.parentConfig).length) {
        return `${this.parentConfig.result_table_ids[0]}`;
      }
      return '';
    },
    storageConfig() {
      return this.$store.state.ide.storageConfig;
    },
  },
  watch: {
    rtId(val) {
      if (val) {
        this.params.config.result_table_id = val;
      }
    },
    cluster(val) {
      this.params.config.cluster = val;
      this.getStorageClusterExpiresConfigs();
    },
  },
  mounted() {
    // this.getStorageClusterExpiresConfigs()
  },
  methods: {
    reflesh(param) {
      this.$nextTick(() => {
        this.validateForms(param);
      });
    },
    handlerCopyStorage(status) {
      if (status) {
        this.params.config.has_replica = '';
        this.isCopyStorageDisabled = false;
      } else {
        this.params.config.has_replica = false;
        this.isCopyStorageDisabled = true;
      }
    },
    validateForms(param) {
      const that = this;
      let isValidate = true;
      if (param) {
        Object.keys(this.validateCluster).forEach(key => {
          if (param === key) {
            changeStatus(key);
          }
        });
      } else {
        Object.keys(this.validateCluster).forEach(key => {
          changeStatus(key);
        });
      }

      return isValidate;
      function changeStatus(key) {
        that.$set(that.validateCluster, key, {
          regs: { required: true },
          content: window.$t('不能为空'),
          visible: false,
          class: 'error-red',
        });
        if (!validateRules(that.validateCluster[key].regs, that.params.config[key], that.validateCluster[key])) {
          that.$set(that.validateCluster, key, {
            regs: { required: true },
            content: window.$t('不能为空'),
            visible: true,
            class: 'error-red',
          });
          isValidate = false;
        }
      }
    },
    tabChanged(name) {
      this.isEs = name === this.clusterType;
    },
    change(item, add, delect) {
      let list = this.params.config;
      if (list[add].indexOf(item) > -1 && list[delect].indexOf(item) > -1) {
        list[delect].splice(
          list[delect].findIndex(field => field === item),
          1
        );
      }
    },
    /*
     * @改变按钮的状态
     * */
    changeButtonStatus() {
      this.buttonStatus.isDisabled = false;
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
    /*
                设置回填
            */
    setConfigBack(self, source, fl) {
      this.parentConfig = source[0];
      this.selfConfig = self;
      this.results.name = this.parentConfig.output;
      this.results.chineseData = this.rtId;
      this.outputDisplay = this.rtId + `(${this.rtId})`;
      this.params.frontend_info = self.frontend_info;
      this.params.from_links = fl;
      this.initValid();
      this.checkExists();
      // 对于没有 config 节点，初始化 config
      if (self.node_config === undefined) {
        self.node_config = this.initConfig();
      }
      if (this.selfConfig.hasOwnProperty('node_id') || this.selfConfig.isCopy) {
        this.cluster = this.selfConfig.node_config.cluster;
        for (let key in self.node_config) {
          if (self.node_config[key] || self.node_config[key] === 0 || self.node_config[key] === false) {
            this.params.config[key] = self.node_config[key];
          } else {
            this.params.config[key] = '';
          }
        }
        this.params.config.expires = self.node_config.expires;
        if (this.$refs.sqlQuery) {
          this.$refs.sqlQuery.analogScroll();
          this.$refs.sqlQuery.init();
        }
      } else {
        this.params.config = this.initConfig();
      }
    },
    /*
                检查结果数据表是否存在
            */
    checkExists() {
      if (!this.selfConfig.hasOwnProperty('node_id')) {
        this.jumpResult = false;
      } else {
        this.jumpResult = true;
      }
    },
    checkResult(data) {
      this.$router.push(
        `/data-access/query/${this.params.config.bk_biz_id}/${this.parentConfig.output}/${this.clusterType}/`
      );
    },
    /*
     * 获取初始化配置信息
     */
    initConfig() {
      return {
        result_table_id: this.rtId,
        name: '',
        bk_biz_id: this.parentConfig.bk_biz_id,
        cluster: this.cluster,
        expires: 30,
        has_replica: '',
        storage_keys: [],
        has_unique_key: false,
      };
    },
    initValid() {
      this.validata = {
        table_name: {
          status: false,
          errorMsg: '',
        },
        name: {
          status: false,
          errorMsg: '',
        },
      };
    },
    /*
                关闭侧边弹框
            */
    closeSider() {
      this.buttonStatus.isDisabled = false;
      this.loading = true;
      this.$emit('closeSider');
    },
    /*
     *  获取参数
     */
    validateFormData() {
      if (!this.validateForms()) return false;
      let result = [];
      let hasReplica = this.params.config.has_replica;
      Object.assign(this.params.config, this.fieldConfig.config);
      this.params.config.has_replica = hasReplica;
      for (var key in this.validata) {
        result.push(this.validata[key].status);
        result.forEach(function (item, index) {
          if (!item) {
            result.splice(index, 1);
          }
        });
      }
      if (result.length === 0) {
        this.buttonStatus.isDisabled = true;
        this.params.config.from_result_table_ids = this.parentConfig.result_table_ids;

        if (this.selfConfig.hasOwnProperty('node_id')) {
          this.params.flow_id = this.$route.params.fid;
        }
      } else {
        return false;
      }
      return true;
    },
    onSchemaAndSqlComplete(res) {
      if (!this.isNewForm) {
        Object.assign(this.params.config, res.config || {});
      }
      this.$refs.esQuery && this.$refs.esQuery.init();
      this.loading = false;
    },

    getStorageClusterExpiresConfigs() {
      this.expiresList = [];
      this.$store
        .dispatch('api/flows/getStorageClusterExpiresConfigs', { type: this.clusterType, name: this.cluster })
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

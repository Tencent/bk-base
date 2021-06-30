

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
  <bkdata-tab :class="['mySqlSaveNode node-editing', { sqlSearch: isSql }]"
    active="config"
    @tab-change="tabChanged">
    <bkdata-tab-panel :class="'config-tab'"
      :label="$t('节点配置')"
      name="config">
      <div class="bk-form">
        <!--储存节点编辑-->
        <div class="save-edit node">
          <div class="content-up">
            <StorageHeader
              v-model="params"
              :parentConfig="parentConfig"
              :isNewForm="!selfConfig.hasOwnProperty('node_id')"
              :jumpResult="jumpResult" />
          </div>
          <div class="content-down">
            <StorageCluster
              ref="storageCluster"
              v-model="params"
              :clusterColorLevel="true"
              :selfConfig="selfConfig"
              :validate="fieldValidate"
              :clusterType="clusterType" />
            <StorageDudeplication v-if="showDudeplication"
              v-model="params.config" />
            <template v-if="nodeType === 'tcaplus_storage'">
              <div class="bk-form-item mt10">
                <label class="bk-label">
                  {{ $t('游戏区') }}
                  <span class="required">*</span>
                </label>
                <div class="bk-form-content">
                  <GameZoneSelect
                    ref="gameZone"
                    :clusterName="params.config.cluster"
                    :zoneID.sync="params.config.zone_id"
                    @zoneselect="setZone" />
                </div>
              </div>
              <Info v-if="showFiledTips"
                :text="$t('主键配置_字段名称提示')"
                size="small" />
            </template>
            <template v-if="!showIndexFields || !flexIndexConfig">
              <div v-if="isMounted"
                class="content-down edit-table">
                <StorageTypeFieldsConfig
                  ref="fieldConfig"
                  v-model="fieldConfig"
                  v-tooltip.notrigger="fieldValidate['storageType']"
                  :clusterType="clusterType"
                  :readonly="configReadonly"
                  :forceAll="forceAllCluster"
                  :isNewform="isNewForm"
                  :rtId="rtId"
                  :openDedeplication="params.config.has_unique_key"
                  :withConfiguration="withConfiguration"
                  :shouldProcessFields="shouldProcessFields"
                  :height="fieldTableHeight"
                  @onSchemaAndSqlComplete="onSchemaAndSqlComplete"
                  @uniqueKeyChange="changeUniqueKey"
                  @change="handleFieldCheckChanged" />
              </div>
              <div v-if="showIndexFields && !flexIndexConfig"
                class="content-down">
                <IndexFields
                  v-bkloading="{ isLoading: loading }"
                  :dragTableWidth="559"
                  :indexTableData="indexTableData"
                  :readonly="!isNewForm"
                  :bodyHeight="`150px`"
                  :showTip="true"
                  @drag-end="handleDragEnd"
                  @remove-index-field="handleRemoveIndexField" />
              </div>
            </template>
          </div>
        </div>
      </div>
    </bkdata-tab-panel>
    <template v-if="showIndexFields && flexIndexConfig">
      <bkdata-tab-panel :class="'config-tab'"
        :label="$t('字段配置')"
        name="fieldConfig">
        <Info v-if="showFiledTips"
          :text="$t('主键配置_字段名称提示')"
          size="small" />
        <div v-if="isMounted"
          class="content-down edit-table">
          <StorageTypeFieldsConfig
            ref="fieldConfig"
            v-model="fieldConfig"
            v-tooltip.notrigger="fieldValidate['storageType']"
            :clusterType="clusterType"
            :readonly="configReadonly"
            :forceAll="forceAllCluster"
            :isNewform="isNewForm"
            :rtId="rtId"
            :openDedeplication="params.config.has_unique_key"
            :withConfiguration="withConfiguration"
            :shouldProcessFields="shouldProcessFields"
            :height="fieldTableHeight"
            @onSchemaAndSqlComplete="onSchemaAndSqlComplete"
            @uniqueKeyChange="changeUniqueKey"
            @change="handleFieldCheckChanged" />
        </div>
        <div v-if="showIndexFields"
          class="content-down">
          <IndexFields
            v-bkloading="{ isLoading: loading }"
            :dragTableWidth="559"
            :indexTableData="indexTableData"
            :readonly="!isNewForm"
            :bodyHeight="`150px`"
            :showTip="true"
            @drag-end="handleDragEnd"
            @remove-index-field="handleRemoveIndexField" />
        </div>
      </bkdata-tab-panel>
    </template>
    <bkdata-tab-panel
      v-if="selfConfig.hasOwnProperty('node_id') && storageConfig.includes(currentDataType.clusterType)"
      :class="'result-tab'"
      :label="$t('结果查询')"
      name="sql">
      <template v-if="selfConfig.hasOwnProperty('node_id') && storageConfig.includes(currentDataType.clusterType)">
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
import SqlQuery from '@/pages/DataQuery/Components/SqlQuery';
import StorageTypeFieldsConfig from './components/StorageTypeFieldsConfig';
import StorageHeader from './components/Storage.header';
import StorageCluster from './components/Storage.cluster';
import StorageDudeplication from './components/Storage.fieldDudeplication';
import childMixin from './config/child.global.mixin.js';
import uniqueKey from './config/storage.uniqueKey.mixin.js';
import Info from '@/components/TipsInfo/TipsInfo.vue';
import GameZoneSelect from '@/pages/DataAccess/Details/DataStorage/GameZoneSelect.vue';
import IndexFields from '@/components/indexFields/index';

export default {
  components: {
    SqlQuery,
    StorageTypeFieldsConfig,
    StorageDudeplication,
    StorageHeader,
    StorageCluster,
    GameZoneSelect,
    Info,
    IndexFields,
  },
  mixins: [childMixin, uniqueKey],
  props: {
    nodeType: {
      type: String,
      default: '',
    },
    nodeDisname: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      resultTableData: {},
      nodeConfig: {},
      fieldConfig: {},
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
        node_type: this.nodeType,
        config: {
          result_table_id: '',
          bk_biz_id: '',
          name: '',
          cluster: '',
          expires: 0,
          storage_keys: [],
          dim_fields: [],
          has_unique_key: false,
        },
        from_links: [],
        frontend_info: {},
      },
      expiresConfig: null,
      expiresList: [],
      fieldValidate: {
        storageType: this.generateEmpyValidate(),
        expires: this.generateEmpyValidate(),
        zone_id: this.generateEmpyValidate(),
        set_id: this.generateEmpyValidate(),
      },
      fieldValidateMap: {},
      validata: {
        table_name: {
          status: false,
          errorMsg: '',
        },
        name: {
          status: false,
          errorMsg: '',
        },
      },
      isSql: false,
      isSchemaSqlComplete: false,
      fieldTableHeight: 300,
      flexIndexConfig: false,
      isMounted: false,
      index_fields: [],
    };
  },
  computed: {
    localIndexFields() {
      return (this.index_fields || []).map(field => Object.assign(field, { isDrag: this.isNewForm }));
    },
    indexTableData() {
      return {
        columns: [$t('字段'), ''].map(field => ({ label: field })),
        list: this.localIndexFields.map(field => ({ isDrag: true, ...field })),
      };
    },
    indexFieldsLength() {
      return this.localIndexFields.length;
    },
    showIndexFields() {
      return this.nodeType === 'clickhouse_storage';
    },
    showFiledTips() {
      return this.nodeType === 'tcaplus_storage' || this.nodeType === 'clickhouse_storage';
    },
    showDudeplication() {
      return this.nodeType === 'tspider_storage' || this.nodeType === 'mysql_storage';
    },
    storageHeaderConfig() {
      return {
        nodeName: '',
        resultTable: '',
      };
    },
    sqlQueryData() {
      return {
        storage: {
          [this.clusterType]: this.fieldConfig,
        },
      };
    },
    configReadonly() {
      return (!this.isNewForm && /^mysql|tspider/i.test(this.clusterType)) || this.nodeTypeReadOnly;
    },
    nodeTypeReadOnly() {
      return !this.isNewForm && /^tcaplus_storage|clickhouse_storage/i.test(this.nodeType);
    },
    withConfiguration() {
      return ['mysql_storage', 'tspider_storage', 'tsdb_storage', 'tcaplus_storage', 'clickhouse_storage'].includes(
        this.nodeType
      );
    },
    forceAllCluster() {
      return !this.storageConfig.map(item => `${item}_storage`).includes(this.nodeType);
    },
    isNewForm() {
      return !this.selfConfig.hasOwnProperty('node_id');
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
    nodeName() {
      let name = `${this.rtId}(${this.currentDataType.displayName})`;
      // eslint-disable-next-line vue/no-side-effects-in-computed-properties
      this.params.config.name = name;
      return name;
    },
    rtId() {
      if (Object.keys(this.parentConfig).length) {
        return `${this.parentConfig.result_table_ids[0]}`;
      }

      return '';
    },
    clusterType() {
      return this.currentDataType.clusterType;
    },
    storageConfig() {
      return this.$store.state.ide.storageConfig || [];
    },
    isTsdb() {
      return /^tsdb/.test(this.nodeType);
    },
  },
  watch: {
    rtId(val) {
      if (val) {
        this.params.config.result_table_id = val;
      }
    },
  },
  mounted() {
    const height = this.$el.offsetHeight;
    this.flexIndexConfig = height <= 850;
    this.fieldTableHeight = height - (this.flexIndexConfig ? 0 : 200) - 85 - (this.showIndexFields ? 265 : 0);
    this.isMounted = true;
  },
  methods: {
    handleDragEnd(endList) {
      const copyList = (endList[0] || []).map((field, index) => ({
        index,
        physical_field: field.physical_field,
      }));
      this.$set(this, 'index_fields', copyList);
    },
    handleRemoveIndexField(field) {
      const originField = this.fieldConfig.fields.find(f => f.physical_field === field.physical_field);
      if (originField) {
        const cfg = originField.configs.find(cf => cf.field === 'is_pri_key');

        if (cfg) {
          this.$set(cfg, 'checked', false);
          this.handleFieldCheckChanged(cfg);
        }
      }
    },
    handleFieldCheckChanged(cfg) {
      if (this.isSchemaSqlComplete) {
        const indexFields = (
          (this.nodeType === 'clickhouse_storage'
            && (this.fieldConfig.fields || [])
              .filter(field => field.configs.some(cfg => cfg.field === 'is_pri_key' && cfg.checked)))
          || []
        ).map((field, index) => ({ index, physical_field: field.physical_field }));
        this.$set(this, 'index_fields', indexFields);
      }
    },
    setZone(zone) {
      this.params.config.zone_id = zone.zone_id;
      this.params.config.set_id = zone.set_id;
    },
    generateEmpyValidate() {
      return {
        content: window.$t('不能为空'),
        visible: false,
        class: 'error-red ide-node-edit',
      };
    },
    tabChanged(name) {
      this.isSql = name === 'sql';
    },
    /*
     * @改变按钮的状态
     * */
    changeButtonStatus() {
      this.buttonStatus.isDisabled = false;
    },
    /*
                设置回填
            */
    setConfigBack(self, source, fl) {
      this.parentConfig = source[0];
      this.selfConfig = self;
      this.params.frontend_info = self.frontend_info;
      this.params.from_links = fl;
      this.checkExists();

      this.$set(this.params, 'config', this.initConfig());
      if (self.hasOwnProperty('node_id') || self.isCopy) {
        if (self.node_config) {
          for (let key in self.node_config) {
            if (self.node_config[key] || self.node_config[key] === 0 || self.node_config[key] === false) {
              this.params.config[key] = self.node_config[key];
            } else {
              this.params.config[key] = '';
            }
          }
          this.params.config.expires = self.node_config.expires;
        }
      }

      this.$nextTick(() => {
        document.querySelectorAll('.mySqlSaveNode .bk-tab-label-item')[0].click();
      });
    },
    /*
                检查结果数据表是否存在
            */
    checkExists() {
      this.jumpResult = this.selfConfig.hasOwnProperty('node_id');
    },
    /*
     * 获取初始化配置信息
     */
    initConfig() {
      const baseConfig = {
        result_table_id: '',
        bk_biz_id: this.parentConfig.bk_biz_id,
        name: '',
        cluster: this.params.config.cluster,
        expires: null,
        // dtEventTimeStamp和thedate初始化时默认勾上为索引字段
        storage_keys: [],
        dim_fields: [],
        has_unique_key: false,
      };

      this.fieldValidateMap =        this.nodeType === 'tcaplus_storage'
        ? {
          storageType: 'cluster',
          zone_id: 'zone_id',
          set_id: 'set_id',
        }
        : {
          storageType: 'cluster',
          expires: 'expires',
        };
      return Object.assign(baseConfig, this.nodeType === 'tcaplus_storage' ? { zone_id: '', set_id: '' } : {});
    },
    /*
                关闭侧边弹框
            */
    closeSider() {
      this.buttonStatus.isDisabled = false;
      this.$emit('closeSider');
    },

    validateParams() {
      this.params.config.order_by = this.index_fields.map(field => field.physical_field);
      for (const [key, value] of Object.entries(this.fieldValidateMap)) {
        this.$set(this.fieldValidate[key], 'visible', !this.params.config[value]);
      }

      const result = Object.keys(this.fieldValidate).every(key => {
        return this.fieldValidate[key].visible === false;
      });

      let zoneValidate = true;

      if (this.nodeType === 'tcaplus_storage') {
        zoneValidate = this.$refs.gameZone.validator();
      }

      return result && zoneValidate;
    },

    /*
     *  获取参数
     */
    validateFormData() {
      if (!this.validateParams()) {
        return false;
      }
      let result = [];
      const fieldConfig = JSON.parse(
        JSON.stringify(this.fieldConfig.config, (key, value) => {
          if (key === 'has_unique_key' || key === 'zone_id' || key === 'set_id') {
            return undefined;
          } else {
            return value;
          }
        })
      );
      Object.assign(this.params.config, fieldConfig);
      for (const key in this.validata) {
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
    onSchemaAndSqlComplete(res, resData) {
      this.loading = false;
      setTimeout(() => {
        this.$set(
          this,
          'index_fields',
          (res.config.order_by || []).map((field, index) => ({ index, physical_field: field }))
        );
        this.isSchemaSqlComplete = true;
      }, 300);
    },
    handleCheckboxChange(checked, key, value) {
      if (checked) {
        this.params.config[key].push(value);
      } else {
        const index = this.params.config[key].findIndex(item => item === value);
        if (index !== -1) {
          this.params.config[key].splice(index, 1);
        }
      }
    },
  },
};
</script>
<style lang="scss" scoped>
.icon-bar-chart {
  width: 32px;
  height: 32px;
  line-height: 32px;
  background-color: rgb(250, 250, 250);
  text-align: center;
  margin-left: 10px;
  cursor: pointer;
  border-width: 1px;
  border-style: solid;
  border-color: rgb(234, 234, 234);
  border-image: initial;
  border-radius: 2px;
}

.config-tab {
  height: 100%;

  .bk-form {
    height: 100%;

    .save-edit {
      height: 100%;
    }
  }
}

.result-tab {
  ::v-deep .resultsData {
    padding-top: 0;
  }
}

::v-deep .bk-table-scrollable-y {
  .bk-table-body-wrapper {
    &::-webkit-scrollbar {
      width: 6px;
      background-color: transparent;
    }
    &::-webkit-scrollbar-thumb {
      border-radius: 6px;
      background-color: #a0a0a0;
    }
  }
}
</style>

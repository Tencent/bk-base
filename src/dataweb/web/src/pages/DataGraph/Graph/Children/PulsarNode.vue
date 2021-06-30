

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
  <bkdata-tab class="pulsar-node"
    active="config">
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
              :isNewForm="isNewForm"
              :jumpResult="jumpResult" />
          </div>
          <div class="content-down">
            <StorageCluster
              ref="storageCluster"
              v-model="params"
              :clusterColorLevel="true"
              :validate="fieldValidate"
              :clusterType="clusterType" />
          </div>
          <div class="content-down">
            <StorageTypeFieldsConfig
              v-model="fieldConfig"
              v-tooltip.notrigger="fieldValidate['storageType']"
              :clusterType="clusterType"
              :forceAll="forceAllCluster"
              :isNewform="isNewForm"
              :rtId="rtId" />
          </div>
        </div>
      </div>
    </bkdata-tab-panel>
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
export default {
  components: {
    SqlQuery,
    StorageTypeFieldsConfig,
    StorageHeader,
    StorageCluster,
  },
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
      fieldConfig: {},
      jumpResult: false, // 跳转结果数据表
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
          indexed_fields: [],
          dim_fields: [],
        },
        from_links: [],
        frontend_info: {},
      },
      fieldValidate: {
        storageType: {
          content: window.$t('不能为空'),
          visible: false,
          class: 'error-red ide-node-edit',
        },
        expires: {
          content: window.$t('不能为空'),
          visible: false,
          class: 'error-red ide-node-edit',
        },
      },
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
    forceAllCluster() {
      return !this.storageConfig.map(item => `${item}_storage`).includes(this.nodeType);
    },
    isNewForm() {
      return !this.selfConfig.hasOwnProperty('node_id');
    },
    storageConfig() {
      return this.$store.state.ide.storageConfig || [];
    },
    currentDataType() {
      return {
        displayName: this.nodeDisname,
        clusterType: 'queue_pulsar',
      };
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
  },
  watch: {
    rtId(val) {
      if (val) {
        this.params.config.result_table_id = val;
      }
    },
  },
  methods: {
    /*
                设置回填
            */
    setConfigBack(self, source, fl) {
      this.parentConfig = source[0];
      this.selfConfig = self;
      this.params.frontend_info = self.frontend_info;
      this.params.from_links = fl;
      this.params.config.bk_biz_id = this.parentConfig.bk_biz_id;
      this.checkExists();
      // 对于没有 config 节点，初始化 config
      if (self.node_config === undefined) {
        self.node_config = this.initConfig();
      }
      if (self.hasOwnProperty('node_id') || self.isCopy) {
        for (let key in self.node_config) {
          if (self.node_config[key] || self.node_config[key] === 0) {
            this.params.config[key] = self.node_config[key];
          } else {
            this.params.config[key] = '';
          }
        }
        this.params.config.expires = self.node_config.expires;
      } else {
        this.params.config = this.initConfig();
      }
      this.$nextTick(() => {
        document.querySelectorAll('.pulsar-node')[0].click();
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
      return {
        result_table_id: '',
        bk_biz_id: this.parentConfig.bk_biz_id,
        name: '',
        cluster: this.params.config.cluster,
        expires: null,
        // dtEventTimeStamp和thedate初始化时默认勾上为索引字段
        indexed_fields: [], // ['dtEventTimeStamp', 'thedate'],
        dim_fields: [],
      };
    },
    validateParams() {
      this.$set(this.fieldValidate.storageType, 'visible', !this.params.config.expires);
      this.$set(this.fieldValidate.expires, 'visible', !this.params.config.expires);
      return !this.fieldValidate.storageType.visible && !this.fieldValidate.expires.visible;
    },
    /*
     *  获取参数
     */
    validateFormData() {
      if (!this.validateParams()) {
        return false;
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
        // 节点params.config.from_result_table_ids结构改造
        this.params.config.from_nodes = [
          {
            id: this.parentConfig.node_id,
            from_result_table_ids: this.parentConfig.result_table_ids,
          },
        ];
        if (this.selfConfig.hasOwnProperty('node_id')) {
          this.params.flow_id = this.$route.params.fid;
        }
      } else {
        return false;
      }
      return true;
    },
  },
};
</script>

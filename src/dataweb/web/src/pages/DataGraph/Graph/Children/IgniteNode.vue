

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
      <div class="bk-form">
        <div v-bkloading="{ isLoading: loading }"
          class="save-edit node">
          <div class="content-up">
            <StorageHeader
              v-model="params"
              :parentConfig="parentConfig"
              :isNewForm="!selfConfig.hasOwnProperty('node_id')" />
          </div>
          <div class="content-down">
            <bkdata-form ref="formData"
              :labelWidth="125"
              :model="params.config"
              :rules="rules"
              extCls="ignite-form">
              <bkdata-form-item :label="$t('存储集群')"
                :required="true"
                property="cluster">
                <cluster-selector
                  v-model="cluster"
                  :clusterLoading.sync="clusterLoading"
                  :colorLevel="true"
                  :selfConfig="selfConfig"
                  clusterType="ignite" />
              </bkdata-form-item>
              <bkdata-form-item :label="$t('数据类型')"
                :required="true"
                :property="'storage_type'">
                <bkdata-radio-group v-model="params.config.storage_type">
                  <bkdata-radio value="join">
                    {{ $t('静态关联类型') }}
                  </bkdata-radio>
                </bkdata-radio-group>
              </bkdata-form-item>
              <bkdata-form-item :label="$t('过期时间')"
                :required="true"
                property="expires">
                <bkdata-selector
                  :list="expiresList"
                  :placeholder="$t('请选择')"
                  :selected.sync="expiresSelected"
                  displayKey="text"
                  settingKey="id" />
              </bkdata-form-item>
              <bkdata-form-item :label="$t('最大数据量')"
                :required="true"
                property="max_records">
                <bkdata-input v-model="params.config.max_records"
                  type="number"
                  :max="maxRecords" />
              </bkdata-form-item>
              <div class="tip">
                <TipsInfo :text="$t('单表不能超过xx条_超出数据无法导入', { count: calculateCount })"
                  size="small" />
              </div>
            </bkdata-form>
          </div>
          <div class="content-down">
            <StorageTypeFieldsConfig
              v-model="fieldConfig"
              :clusterType="'ignite'"
              :forceAll="true"
              :isNewform="!selfConfig.hasOwnProperty('node_id')"
              :rtId="rtId"
              :withConfiguration="true"
              @onSchemaAndSqlComplete="onSchemaAndSqlComplete" />
          </div>
        </div>
      </div>
    </bkdata-tab-panel>
  </bkdata-tab>
</template>

<script>
import StorageHeader from './components/Storage.header';
import StorageTypeFieldsConfig from './components/StorageTypeFieldsConfig';
import clusterSelector from './clusterSelect/cluster_type_selector';
import childMixin from './config/child.global.mixin.js';
import TipsInfo from '@/components/TipsInfo/TipsInfo.vue';
import { numberFormat } from '@/common/js/util.js';
import Cookies from 'js-cookie';
export default {
  components: {
    StorageHeader,
    StorageTypeFieldsConfig,
    clusterSelector,
    TipsInfo,
  },
  mixins: [childMixin],
  props: {
    nodeType: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      expiresConfig: null,
      expiresSelected: '',
      maxRecords: 2000000,
      cluster: '',
      loading: false,
      clusterLoading: false,
      expiresList: [],
      fieldConfig: {},
      rules: {
        cluster: [
          {
            required: true,
            message: this.$t('必填项不可为空'),
            trigger: 'change',
          },
        ],
        expires: [
          {
            required: true,
            message: this.$t('必填项不可为空'),
            trigger: 'change',
          },
        ],
        max_records: [
          {
            required: true,
            message: this.$t('必填项不可为空'),
            trigger: 'blur',
          },
          {
            validator: val => {
              return val < this.maxRecords;
            },
            message: this.$t('超过最大值'),
            trigger: 'change',
          },
        ],
      },
      params: {
        node_type: 'ignite',
        config: {
          storage_type: 'join',
          result_table_id: '',
          name: '',
          bk_biz_id: 1,
          cluster: 'default',
          expires: null,
          expires_unit: '',
          indexed_fields: [],
          storage_keys: [],
          from_nodes: [],
          max_records: 1000000,
        },
        from_links: [],
        frontend_info: {},
      },
      parentConfig: {},
      selfConfig: {},
    };
  },
  computed: {
    rtId() {
      if (Object.keys(this.parentConfig).length) {
        return `${this.parentConfig.result_table_ids[0]}`;
      }

      return '';
    },
    calculateCount() {
      const curLang = Cookies.get('blueking_language') || 'zh-cn';
      const formatter = curLang === 'zh-cn' ? numberFormat.cnFormatter : numberFormat.kFormatter;
      const num = this.maxRecords;
      return formatter(num);
    },
  },
  watch: {
    cluster(val) {
      this.params.config.cluster = val;
      this.getExpiresConfigByCluster('ignite');
    },
    expiresSelected(val) {
      this.formatExpiresConfig();
    },
    'params.config.max_records'(val) {
      if (this.fieldConfig.config) {
        this.fieldConfig.config.max_records = val;
      }
    },
    expiresConfig(val) {
      this.maxRecords = val.connection.cache_records_limit ? val.connection.cache_records_limit : 2000000;
    },
  },
  methods: {
    onSchemaAndSqlComplete() {
      this.loading = false;
    },
    initConfig() {
      return Object.assign(this.params.config, {
        result_table_id: this.rtId,
        name: '',
        bk_biz_id: this.parentConfig.bk_biz_id,
        cluster: this.cluster,
        expires: null,
        storage_type: 'join',
      });
    },
    setConfigBack(self, source, fl) {
      this.parentConfig = source[0];
      this.selfConfig = self;
      this.params.frontend_info = self.frontend_info;
      this.params.from_links = fl;

      // 对于没有 config 节点，初始化 config
      if (self.node_config === undefined) {
        self.node_config = this.initConfig();
      }
      if (self.hasOwnProperty('node_id') || self.isCopy) {
        this.cluster = this.selfConfig.node_config.cluster;
        for (let key in self.node_config) {
          if (self.node_config[key] || self.node_config[key] === 0) {
            this.params.config[key] = self.node_config[key];
          } else {
            this.params.config[key] = '';
          }
        }
        this.params.config.expires = self.node_config.expires;
      }
    },
    async validateFormData() {
      let result = true;
      this.params.config.from_nodes = [
        {
          id: this.parentConfig.node_id,
          from_result_table_ids: this.parentConfig.result_table_ids,
        },
      ];
      Object.assign(this.params.config, this.fieldConfig.config);
      await this.$refs.formData.validate().then(
        validator => {
          if (this.selfConfig.hasOwnProperty('node_id')) {
            this.params.flow_id = this.$route.params.fid;
          }
          result = true;
        },
        validator => {
          result = false;
        }
      );
      console.log(this.params);
      return result;
    },
  },
};
</script>

<style lang="scss" scoped>
.ignite-form .tip {
  margin-left: 125px;
  width: 341px;
}
</style>

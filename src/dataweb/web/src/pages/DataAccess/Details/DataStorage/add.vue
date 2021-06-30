

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
  <div class="data-edit-container">
    <bkdata-button size="small"
      theme="primary"
      class="mb10"
      @click="$emit('prev')">
      {{ $t('返回列表') }}
    </bkdata-button>
    <Layout
      class="container-with-shadow scroll-show"
      :crumbName="$t('存储配置')"
      :withMargin="false"
      :headerMargin="false"
      :headerBackground="'inherit'"
      height="auto">
      <StorageConfigModule
        :bizId="details.bk_biz_id"
        :loading="isStorageConfigLoading"
        :mode="'add'"
        :validate="validate"
        :storageConfigModuleData="StorageConfigModuleData"
        @changeDuplication="changeDuplication"
        @changeType="changeType"
        @changeCluster="changeCluster"
        @dataSourceSelect="onDataSourceSelect" />
    </Layout>

    <Layout
      class="container-with-shadow"
      :crumbName="$t('字段设置')"
      :withMargin="false"
      :headerMargin="false"
      :headerBackground="'inherit'"
      height="auto">
      <template v-if="isTredis">
        <span class="tredis-keylen-warn">{{ $t('tredis入库Key长度提示') }}</span>
      </template>
      <FieldConfigModule
        :fieldConfigModuleData="FieldConfigModuleData"
        :backupField.sync="backupField"
        :type="storageType"
        mode="add" />
    </Layout>

    <div class="submit-btn">
      <bkdata-button theme="primary"
        :loading="submitLoading"
        @click="handlePreSubmitValidate">
        {{ $t('提交') }}
      </bkdata-button>
    </div>
  </div>
</template>

<script>
import Layout from '@/components/global/layout';
import StorageConfigModule from './storage-config-module';
import FieldConfigModule from './field-config-module';
import { setTimeout } from 'timers';
import optionsMixin from './mixin/options';

export default {
  components: {
    Layout,
    StorageConfigModule,
    FieldConfigModule,
  },
  mixins: [optionsMixin],
  props: {
    details: {
      type: Object,
      default: () => {},
    },
    name: {
      type: String,
    },
  },
  data() {
    return {
      submitLoading: false,
      isFirstValidate: true,
      validate: {
        rawData: {
          regs: { required: true, error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
          objKey: 'id',
        },
        dataName: {
          regs: [
            { required: true, error: window.$t('不能为空') },
            { regExp: /^[a-zA-Z][a-zA-Z0-9_]*$/, error: '只能是字母加下划线格式' },
            { length: 50, error: window.$t('长度不能大于50') },
          ],
          content: '',
          visible: false,
          class: 'error-red',
        },
        dataAlias: {
          regs: [
            { required: true, error: window.$t('不能为空') },
            { length: 50, error: window.$t('长度不能大于50') },
          ],
          content: '',
          visible: false,
          class: 'error-red',
        },
        deadline: {
          regs: { required: true, error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
        },
        storageType: {
          regs: { required: true, error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
          objKey: 'id',
        },
        storageCluster: {
          regs: { required: true, error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
          objKey: 'id',
        },
        copyStorage: {
          regs: { required: true, error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
        },
        maxRecords: {
          regs: { required: true, error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
        },
      },
      isStorageConfigLoading: false,
    };
  },
  computed: {
    // 数据信息添加区域
    region() {
      try {
        return this.details.access_raw_data.tags.manage.geog_area[0].code;
      } catch (error) {
        return '-';
      }
    },

    isTredis() {
      return /TRedis/i.test(this.changeOptions.type);
    },
  },
  watch: {
    StorageConfigModuleData: {
      deep: true,
      handler(val) {
        this.validateForms();
      },
    },
  },
  async mounted() {
    /** 查询数据源信息列表 */
    this.getDataSourceList();
  },
  methods: {
    /**
     * 提交之前操作
     */
    handlePreSubmitValidate() {
      this.isFirstValidate = false;
      if (this.validateForms()) {
        if (['clickhouse'].includes(this.storageType)) {
          this.$bkInfo({
            extCls: 'bk-clickhouse-confirm',
            title: $t('请确认是否提交'),
            subTitle: $t('主键的配置提交确认'),
            confirmFn: () => this.submitInfo(),
          });
        } else {
          this.submitInfo();
        }
      }
    },
    submitInfo() {
      this.submitLoading = true;
      const params = {
        raw_data_id: this.$route.params.did,
        data_type: this.StorageConfigModuleData.rawData.data_type,
        result_table_name: this.StorageConfigModuleData.dataName,
        result_table_name_alias: this.StorageConfigModuleData.dataAlias,
        storage_type: this.StorageConfigModuleData.storageType.id,
        storage_cluster: this.StorageConfigModuleData.storageCluster.id,
        expires: this.StorageConfigModuleData.deadline,
        fields: this.FieldConfigModuleData.fields,
        index_fields: this.FieldConfigModuleData.index_fields,
        // esnode 海外版暂时注释，恢复时，加回校验
        config: this.generateConfigParams(),
      };
      this.bkRequest
        .httpRequest('dataStorage/createStorageConfig', { params: params })
        .then(res => {
          if (res.result) {
            this.$emit('prev');
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](_ => {
          this.submitLoading = false;
        });
    },
    getStoreType(payload) {
      this.bkRequest
        .httpRequest('dataStorage/getStorageType', { query: { data_type: payload.data_type } })
        .then(res => {
          if (res.result) {
            this.StorageConfigModuleData.storageTypeList = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
    },
    async getCluster(id, clusterType) {
      this.isStorageConfigLoading = true;
      const params = { raw_data_id: id, clusterType };
      const query = {
        tags: [this.region, 'usr', 'enable'],
      };
      const res = await this.bkRequest.httpRequest('auth/getClusterWithAccess', { params, query });
      this.isStorageConfigLoading = false;
      return res.map(item => {
        const rArr = item.children.map(item => {
          let colorClass = '';
          let colorTip = '';
          if (item.priority >= 0 && item.priority <= 10) {
            colorClass = 'color-danger';
            colorTip = this.$t('容量不足');
          } else if (item.priority > 10 && item.priority <= 30) {
            colorClass = 'color-warning';
            colorTip = this.$t('容量紧张');
          } else {
            colorClass = 'color-success';
            colorTip = this.$t('容量充足');
          }
          return {
            name: item.alias,
            id: item.cluster_name,
            colorClass,
            colorTip,
            isEnableReplica: item.isEnableReplica,
            disabled: item.isReadonly,
          };
        });
        return {
          name: item.alias,
          id: item.cluster_name,
          children: rArr,
        };
      });
    },
    getDataSourceList() {
      const query = {
        raw_data_id: this.$route.params.did,
      };
      this.StorageConfigModuleData.dataSourceLoading = true;
      this.bkRequest
        .httpRequest('dataStorage/getDataSourceList', { query })
        .then(res => {
          if (res.result) {
            const { data } = res;
            // console.log('getDataSourceList:', res)
            /** 数据源 */
            this.StorageConfigModuleData.rawDataList = data || []; //.filter(item => item.id !== 'raw_data')
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](_ => {
          this.StorageConfigModuleData.dataSourceLoading = false;
        });
    },
    async changeType(item) {
      this.changeClusterByType(item);
      this.changeOptions.type = item.name;
      this.StorageConfigModuleData.storageClusterList = await this.getCluster(this.$route.params.did, item.id);
    },
    changeCluster(item) {
      this.changeOptions.cluster = item.name;
    },
    changeClusterByType(item) {
      this.FieldConfigModuleData = {};
      if (this.StorageConfigModuleData.rawData.data_type === 'raw_data') {
        this.bkRequest.httpRequest('dataStorage/getRawdataFields', { params: { clusterType: item.id } }).then(res => {
          this.FieldConfigModuleData = res.data.filter(field => !field.isReadonly);
        });
      } else {
        const params = {
          rtid: `${this.details.bk_biz_id}_${this.StorageConfigModuleData.dataName}`,
          clusterType: item.id,
        };
        const query = {
          flag: 'all',
        };

        this.bkRequest.httpRequest('meta/getSchemaAndSqlByClusterType', { params, query }).then(res => {
          this.FieldConfigModuleData = res;
          this.backupField = JSON.parse(JSON.stringify(res));
          this.processFieldData(false); // 新增入库时，默认过滤掉
        });
      }
    },
    onDataSourceSelect(item) {
      this.StorageConfigModuleData.storageType = {};
      this.StorageConfigModuleData.storageCluster = {};
      this.StorageConfigModuleData.deadline = item.data_type === 'raw_data' ? '--' : '';
      this.FieldConfigModuleData = {};
      /** 获取数据类型 */
      this.getStoreType(item);
    },
  },
};
</script>

<style lang="scss" scoped>
::v-deep .layout-body {
  overflow: unset !important;
}
.data-edit-container {
  padding: 0 15px;
}

.container-with-shadow {
  ::v-deep .layout-content {
    position: relative;
    .tredis-keylen-warn {
      position: absolute;
      top: 0;
      transform: translateY(-100%);
      z-index: 9;
      left: 150px;
      height: 40px;
      line-height: 40px;
      color: red;
      font-weight: 600;
    }
  }
}

.submit-btn {
  margin-top: 20px;
  text-align: center;
}
</style>
<style lang="scss">
.bk-clickhouse-confirm {
  .bk-dialog-sub-header {
    .bk-dialog-header-inner {
      color: #ea3636 !important;
    }
  }
}
</style>



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
  <div class="data-preview-container">
    <div class="mb10">
      <bkdata-button size="small"
        theme="primary"
        @click="$emit('prev')">
        {{ $t('返回列表') }}
      </bkdata-button>
    </div>

    <Layout
      class="container-with-shadow"
      :crumbName="$t('存储配置')"
      :withMargin="false"
      :headerMargin="false"
      :isOpen="true"
      :headerBackground="'inherit'"
      :collspan="true"
      height="auto">
      <StorageConfigModule
        ref="storageConfigModule"
        :bizId="details.bk_biz_id"
        :loading="isStorageConfigLoading"
        :hasReplica="hasReplica"
        :mode="'edit'"
        :storageConfigModuleData="StorageConfigModuleData"
        :validate="validate"
        :disabled="true"
        :displayItem="itemObj"
        @changeDuplication="changeDuplication" />
    </Layout>
    <Layout
      class="container-with-shadow"
      :crumbName="$t('字段设置')"
      :withMargin="false"
      :headerMargin="false"
      :isOpen="true"
      :headerBackground="'inherit'"
      :collspan="true"
      height="auto">
      <div class="storage-edit">
        <FieldConfigModule
          :fieldConfigModuleData="FieldConfigModuleData"
          :type="storageType"
          :backupField.sync="backupField" />

        <div class="submit-btn">
          <bkdata-button theme="primary"
            :loading="submitLoading"
            @click="submitInfo">
            {{ $t('提交') }}
          </bkdata-button>
        </div>
      </div>
    </Layout>
    <template v-if="!isRawData">
      <Layout
        class="container-with-shadow"
        :crumbName="$t('结果查询')"
        :withMargin="false"
        :headerMargin="false"
        :headerBackground="'inherit'"
        :collspan="true"
        height="auto">
        <div style="margin: -30px -15px -15px; width: 100%">
          <template v-if="isSqlQuery">
            <SqlQuery
              :nodeSearch="true"
              :formData="sqlFormData"
              :showTitle="false"
              :resultTable="itemObj.result_table_id"
              :activeType="itemObj.storage_type" />
          </template>
          <template v-if="isEsQuery">
            <EsQuery :nodeSearch="true"
              :showTitle="false"
              :resultTable="itemObj.result_table_id" />
          </template>
        </div>
      </Layout>
    </template>
    <OperationHistory :storageItem="itemObj" />
  </div>
</template>

<script>
import Layout from '@/components/global/layout';
import StorageConfigModule from './storage-config-module';
import FieldConfigModule from './field-config-module';
import SqlQuery from '@/pages/DataQuery/Components/SqlQuery';
import EsQuery from '@/pages/DataQuery/Components/EsQuery';
import OperationHistory from './operatorList.vue';
import optionsMixin from './mixin/options';
export default {
  components: {
    Layout,
    StorageConfigModule,
    FieldConfigModule,
    SqlQuery,
    EsQuery,
    OperationHistory,
  },
  mixins: [optionsMixin],
  props: {
    details: {
      type: Object,
      default: () => {
        {
        }
      },
    },
    itemObj: {
      type: Object,
      default: () => {
        {
        }
      },
    },
  },
  data() {
    return {
      curPage: 1,
      pageCount: 10,
      submitLoading: false,
      isFirstValidate: true,
      validate: {
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
      },
      isStorageConfigLoading: false,
      hasReplica: false,
    };
  },
  computed: {
    isRawData() {
      return this.itemObj.data_type === 'raw_data';
    },
    totalPage() {
      return Math.ceil(this.logList.length / this.pageCount) || 1;
    },
    logListView() {
      return this.logList.slice(this.pageCount * (this.curPage - 1), this.pageCount * this.curPage);
    },
    isSqlQuery() {
      return !/^es/i.test(this.itemObj.storage_type);
    },
    isEsQuery() {
      return /^es/i.test(this.itemObj.storage_type);
    },
    sqlFormData() {
      const storageType = this.itemObj.storage_type;
      const formData = {
        order: storageType,
        storage: {
          [storageType]: {
            sql: this.FieldConfigModuleData.sql,
          },
        },
      };

      return formData;
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
  mounted() {
    /** 获取数据入库详情（查询rt以及rt相关配置信息需整合存储配置项接口 */
    this.getStorageDetail();

    /** 获取详情页面字段设置配置 */
    this.getFieldConfigModuleData();

    this.getStoreType(this.itemObj);
  },
  // computed: {
  //     // 数据信息添加区域
  //     region () {
  //         try {
  //             return this.details.access_raw_data.tags.manage.geog_area[0].code
  //         } catch (error) {
  //             return '-'
  //         }
  //     }
  // },
  methods: {
    // 数据信息添加区域
    getRegion() {
      try {
        return this.details.access_raw_data.tags.manage.geog_area[0].code;
      } catch (error) {
        return '-';
      }
    },
    submitInfo() {
      this.isFirstValidate = false;
      if (this.validateForms()) {
        this.submitLoading = true;
        const params = {
          data_type: this.itemObj.data_type,
          result_table_id:
            this.itemObj.data_type === 'raw_data' ? this.$route.params.did : this.itemObj.result_table_id,
          raw_data_id: this.$route.params.did,
          result_table_name: this.itemObj.result_table_name,
          result_table_name_alias: this.StorageConfigModuleData.dataAlias,
          storage_type: this.itemObj.storage_type,
          storage_cluster: this.StorageConfigModuleData.storageCluster.id,
          expires: this.StorageConfigModuleData.deadline.replace(this.$t('天'), 'd'),
          fields: this.FieldConfigModuleData.fields,
          index_fields: this.FieldConfigModuleData.index_fields,
          // esnode 海外版暂时注释，恢复时，加回校验
          config: this.generateConfigParams(),
        };
        this.bkRequest
          .httpRequest('dataStorage/updateStorage', { params: params })
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
      }
    },
    getStoreType(payload) {
      this.bkRequest
        .httpRequest('dataStorage/getStorageType', { query: { data_type: payload.data_type } })
        .then(res => {
          if (res.result) {
            this.$set(this.StorageConfigModuleData, 'storageTypeList', res.data);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
    },
    async getStorageDetail() {
      /** 存储表名 */
      this.StorageConfigModuleData.dataName = this.isRawData ? '--' : this.itemObj.data_name;
      this.StorageConfigModuleData.dataAlias = this.isRawData ? '--' : this.itemObj.data_alias;

      /** 数据源 */ this.StorageConfigModuleData.rawData = {
        id: this.itemObj.data_name,
        data_type: this.itemObj.data_type,
      };

      /** 存储类型 */
      this.StorageConfigModuleData.storageType = this.itemObj.storage_type;

      /** 存储集群 */
      this.StorageConfigModuleData.storageCluster = { id: this.itemObj.storage_cluster };

      /** 过期时间 */
      this.StorageConfigModuleData.deadline = this.isRawData ? '--' : this.itemObj.expire_time.replace('天', 'd');

      this.StorageConfigModuleData.storageClusterList = await this.getCluster(
        this.$route.params.did,
        this.itemObj.storage_type
      );
    },
    getFieldConfigModuleData() {
      const params = {
        rtid: this.itemObj.result_table_id,
        clusterType: this.itemObj.storage_type,
      };

      const query = {
        flag: 'all',
      };
      this.bkRequest.httpRequest('meta/getSchemaAndSqlByClusterType', { params, query }).then(res => {
        this.FieldConfigModuleData = res;
        this.backupField = JSON.parse(JSON.stringify(res));
        this.StorageConfigModuleData.maxRecords = this.FieldConfigModuleData.config.max_records;
        this.StorageConfigModuleData.zoneID = this.FieldConfigModuleData.config.zone_id;
        this.StorageConfigModuleData.setID = this.FieldConfigModuleData.config.set_id;
        if (this.FieldConfigModuleData.config.has_replica !== undefined) {
          this.StorageConfigModuleData.copyStorage = this.hasReplica = this.FieldConfigModuleData.config.has_replica;
        }
        this.StorageConfigModuleData.allowDuplication = this.FieldConfigModuleData.config.has_unique_key;
        this.StorageConfigModuleData.index_fields = (
          this.FieldConfigModuleData.config.order_by || []
        ).map((by, index) => ({ physical_field: by, index }));
        this.backupField.index_fields = this.StorageConfigModuleData.index_fields;
        this.FieldConfigModuleData.index_fields = this.StorageConfigModuleData.index_fields;
        this.processFieldData(this.StorageConfigModuleData.allowDuplication); // 新增入库时，默认过滤掉
      });
    },
    async getCluster(id, clusterType) {
      this.isStorageConfigLoading = true;
      const params = { raw_data_id: id, clusterType };
      const query = {
        tags: [this.getRegion(), 'usr'],
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
  },
};
</script>

<style lang="scss" scoped>
::v-deep .layout-body {
  overflow: unset !important;
}

::v-deep .crumb-right {
  display: flex;
  align-items: center;
}
.no_data {
  height: 120px;
  display: flex;
  align-items: center;
  flex-direction: column;
  justify-content: center;
}
.data-preview-container {
  padding: 0 15px;
}

.operation-history {
  width: 100%;
  table {
    border: 1px solid #e6e6e6;
  }
}

.storage-edit {
  width: 100%;
}
.submit-btn {
  text-align: center;
  margin-top: 20px;
}
</style>

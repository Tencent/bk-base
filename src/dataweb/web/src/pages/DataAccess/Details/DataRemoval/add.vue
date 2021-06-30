

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
        :mode="'add'"
        refs="storageConfigModule"
        :bizId="details.bk_biz_id"
        :validate="validate"
        :storageConfigModuleData="StorageConfigModuleData" />
    </Layout>

    <div class="submit-btn">
      <bkdata-button theme="primary"
        :loading="submitLoading"
        @click="submitInfo">
        {{ $t('提交') }}
      </bkdata-button>
    </div>
  </div>
</template>

<script>
import Layout from '@/components/global/layout';
import StorageConfigModule from './storage-config-module';
import { validateRules } from '../../NewForm/SubformConfig/validate.js';

export default {
  components: {
    Layout,
    StorageConfigModule,
  },
  props: {
    details: {
      type: Object,
      default: () => ({}),
    },
    name: {
      type: String,
    },
  },
  data() {
    return {
      submitLoading: false,
      StorageConfigModuleData: {
        dataSourceLoading: false,
        rawDataList: [],
        sourceStorage: '',
        targetStorage: '',
        initDateTime: [
          this.forMateDate(new Date(new Date().getTime() - 24 * 60 * 60 * 1000)),
          this.forMateDate(new Date()),
        ],
        coverData: true,
        rawData: {
          id: this.$route.query.result_table_id || this.$route.query.data_id,
          name: '',
        },
      },
      isFirstValidate: true,
      validate: {
        rawData: {
          regs: { required: true, error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
          objKey: 'id',
        },
        sourceStorage: {
          regs: { required: true, error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
        },
        targetStorage: {
          regs: { required: true, error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
        },
        coverData: {
          regs: { required: true, error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
        },
      },
    };
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
    /** 查询数据源信息列表 */
    this.getDataSourceList();
  },
  methods: {
    forMateDate(time) {
      let preArr = Array.apply(null, Array(10)).map((elem, index) => {
        return '0' + index;
      });
      let date = new Date(time);
      let year = date.getFullYear();
      let month = date.getMonth() + 1; // 月份是从0开始的
      let day = date.getDate();
      let hour = date.getHours();
      let min = date.getMinutes();
      let sec = date.getSeconds();
      let newTime =        year
        + '-'
        + (preArr[month] || month)
        + '-'
        + (preArr[day] || day)
        + ' '
        + (preArr[hour] || hour)
        + ':'
        + (preArr[min] || min)
        + ':'
        + (preArr[sec] || sec);
      return newTime;
    },
    submitInfo() {
      this.isFirstValidate = false;
      if (this.validateForms()) {
        this.submitLoading = true;
        const options = {
          params: {
            // result_table_id: `${this.bizId}_${this.StorageConfigModuleData.rawData.id}`,
            result_table_id: this.StorageConfigModuleData.rawData.id,
            source: this.StorageConfigModuleData.sourceStorage,
            dest: this.StorageConfigModuleData.targetStorage,
            start: this.forMateDate(this.StorageConfigModuleData.initDateTime[0]),
            end: this.forMateDate(this.StorageConfigModuleData.initDateTime[1]),
            overwrite: this.StorageConfigModuleData.coverData,
          },
        };
        this.bkRequest
          .httpRequest('dataRemoval/createDataRemovalTask', options)
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

    validateForms() {
      if (!this.isFirstValidate) {
        let isValidate = true;
        Object.keys(this.validate).forEach(key => {
          this.validate[key].visible = false;
          const field = this.validate[key].objKey
            ? this.StorageConfigModuleData[key][this.validate[key].objKey]
            : this.StorageConfigModuleData[key];
          if (!validateRules(this.validate[key].regs, field, this.validate[key])) {
            isValidate = false;
          }
        });

        return isValidate;
      }

      return true;
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
            this.StorageConfigModuleData.rawDataList = (data || []).filter(item => item.id !== 'raw_data');
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](_ => {
          this.StorageConfigModuleData.dataSourceLoading = false;
        });
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

.submit-btn {
  text-align: center;
}
</style>

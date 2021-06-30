

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
  <bkdata-form ref="form"
    :labelWidth="200"
    :model="formData"
    :rules="rules">
    <bkdata-form-item :label="$t('申请流程')">
      <TextStep :steps="[$t('申请'), $t('业务运维审批'), $t('数据平台实施'), $t('完成')]" />
    </bkdata-form-item>
    <bkdata-form-item :label="$t('提单人')">
      <span>{{ userName }}</span>
    </bkdata-form-item>
    <bkdata-form-item :label="$t('资源组')">
      <span>{{ groupDisName }}</span>
    </bkdata-form-item>
    <bkdata-form-item :label="$t('资源区域')"
      :required="true"
      extCls="form-item-input">
      <bkdata-selector :list="regionList"
        :settingKey="'id'"
        :selected.sync="formData.geog_area_code" />
    </bkdata-form-item>
    <bkdata-form-item :label="$t('资源类型')"
      property="service_type"
      :required="true"
      extCls="form-item-input">
      <bkdata-selector v-if="resourceType === 'processing'"
        :list="resourceTypeList"
        :selected.sync="formData.service_type"
        :settingKey="'service_type'"
        :displayKey="'service_name'"
        :isLoading="resourceTypeLoading"
        @item-selected="getResourceUnit" />
      <bkdata-selector v-else
        :list="resourceTypeList"
        :hasChildren="true"
        :selected.sync="formData.service_type"
        :optionTip="true"
        :toolTipTpl="getOptionTpl"
        :isLoading="resourceTypeLoading"
        @item-selected="getResourceUnit" />
    </bkdata-form-item>
    <bkdata-form-item :label="$t('资源单元型号')"
      property="resource_unit_id"
      :required="true"
      extCls="form-item-input">
      <bkdata-selector :list="resourceUnitTypeList"
        :selected.sync="formData.resource_unit_id"
        :settingKey="'resource_unit_id'"
        :isLoading="resourceUnitLoading"
        :displayKey="'name'" />
    </bkdata-form-item>
    <bkdata-form-item :label="$t('数量')"
      property="num"
      :required="true"
      extCls="form-item-input">
      <bkdata-input v-model="formData.num"
        :min="1"
        type="number" />
      <span v-bk-tooltips="$t('申请的总容量')"
        class="tips-icon icon-question-circle" />
    </bkdata-form-item>
    <bkdata-form-item :label="$t('申请原因')"
      :required="true"
      property="description"
      extCls="form-item-input">
      <bkdata-input v-model="formData.description"
        type="textarea"
        :rows="3" />
    </bkdata-form-item>
  </bkdata-form>
</template>

<script>
import TextStep from './TextStep';
export default {
  components: {
    TextStep,
  },
  props: {
    userName: {
      type: String,
      default: '',
    },
    groupID: {
      type: String,
      default: '',
    },
    resourceType: {
      type: String,
      default: '',
    },
    groupDisName: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      formData: {
        bk_username: this.userName,
        resource_group_id: '',
        geog_area_code: 'inland',
        apply_type: 'increase',
        resource_type: 'processing',
        service_type: '',
        resource_unit_id: '',
        num: 1,
        description: '',
      },
      rules: {
        service_type: [
          {
            required: true,
            message: this.$t('必填项不可为空'),
            trigger: 'change',
          },
        ],
        resource_unit_id: [
          {
            required: true,
            message: this.$t('必填项不可为空'),
            trigger: 'blur',
          },
        ],
        num: [
          {
            required: true,
            message: this.$t('必填项不可为空'),
            trigger: 'change',
          },
        ],
        description: [
          {
            required: true,
            message: this.$t('必填项不可为空'),
            trigger: 'blur',
          },
        ],
      },
      resourceTypeLoading: false,
      resourceUnitLoading: false,
      regionList: [
        {
          id: 'inland',
          name: this.$t('中国内地'),
        },
      ],
      resourceTypeList: [],
      resourceUnitTypeList: [],
    };
  },
  watch: {
    resourceType: {
      immediate: true,
      handler(val) {
        this.formData.resource_type = val;
        this.resourceTypeList = [];
        this.getResourceType();
        this.clearForm();
      },
    },
    groupID(val) {
      this.formData.resource_group_id = val;
    },
  },
  created() {
    this.getResourceType();
  },
  methods: {
    getOptionTpl(option) {
      return `<span style="white-space: pre-line;">${option.description || option.name}</span>`;
    },
    validateForm() {
      return this.$refs.form.validate().then(
        validator => {
          return Promise.resolve(true);
        },
        validator => {
          return Promise.reject(false);
        }
      );
    },
    clearForm() {
      this.formData.service_type = '';
      this.formData.resource_unit_id = '';
      this.resourceUnitTypeList = [];
    },
    getResourceType() {
      let url, params;
      if (this.resourceType === 'storage') {
        url = 'dataStorage/getStorageType';
        params = {};
      } else {
        url = 'resourceManage/getResourceTypeList';
        params = {
          query: {
            resource_type: this.formData.resource_type,
          },
        };
      }
      this.resourceTypeLoading = true;
      this.bkRequest
        .httpRequest(url, params)
        .then(res => {
          if (res.result) {
            this.resourceTypeList = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['catch'](res => {
          this.getMethodWarning(res.message, res.code);
        })
        ['finally'](() => {
          this.resourceTypeLoading = false;
        });
    },
    getResourceUnit() {
      this.resourceUnitLoading = true;
      this.bkRequest
        .httpRequest('resourceManage/getResourceUnitList', {
          query: {
            resource_type: this.formData.resource_type,
            service_type: this.formData.service_type,
          },
        })
        .then(res => {
          if (res.result) {
            this.resourceUnitTypeList = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.resourceUnitLoading = false;
        });
    },
  },
};
</script>

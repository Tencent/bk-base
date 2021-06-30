

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
  <bkdata-form :labelWidth="145">
    <div class="bk-form-item">
      <label class="bk-label">
        {{ $t('存储集群') }}
        <span class="required">*</span>
      </label>
      <div class="bk-form-content">
        <!--eslint-disable vue/no-mutating-props-->
        <cluster-selector
          v-model="cluster"
          v-tooltip.notrigger="{
            content: validate.storageType.content,
            visible: validate.storageType.visible,
            class: 'error-red',
          }"
          :clusterLoading.sync="clusterLoading"
          :colorLevel="clusterColorLevel"
          :selfConfig="selfConfig"
          :clusterType="clusterType"
          @change="validate.storageType.visible = !cluster" />
      </div>
    </div>
    <slot name="customize-form-item" />
    <div v-if="showLifeCycle"
      class="bk-form-item">
      <label class="bk-label">
        {{ $t('过期时间') }}
        <span class="required">*</span>
      </label>
      <div class="bk-form-content">
        <bkdata-selector
          v-tooltip.notrigger="{
            content: validate.expires.content,
            visible: validate.expires.visible,
            class: 'error-red',
          }"
          :class="{ error: validate.expires.visible }"
          :list="expiresList"
          :disabled="!cluster"
          :selected.sync="expires"
          :settingKey="'id'"
          :displayKey="'text'" />
      </div>
    </div>
  </bkdata-form>
</template>

<script>
import clusterSelector from '../clusterSelect/cluster_type_selector';
import childMixin from '../config/child.global.mixin.js';
export default {
  components: {
    clusterSelector,
  },
  mixins: [childMixin],
  model: {
    prop: 'nodeParams',
    event: 'change',
  },
  props: {
    selfConfig: {
      type: [Object, String],
      default: () => ({}),
    },
    clusterType: {
      type: String,
      default: '',
    },
    clusterColorLevel: {
      type: Boolean,
      default: true,
    },
    nodeParams: {
      type: Object,
      default: () => ({}),
    },
    validate: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      clusterLoading: true,
      cluster: '',
      expiresList: [],
      expires: null,
      localValidate: [],
      expiresConfig: {},
    };
  },
  computed: {
    nodeParamsValue: {
      get() {
        return this.nodeParams;
      },
      set(val) {
        Object.assign(this.nodeParams, val);
      },
    },
    showLifeCycle() {
      return !['tcaplus_storage', 'pgsql_storage'].includes(this.nodeParams.node_type);
    },
  },
  watch: {
    cluster(val) {
      this.nodeParamsValue.config.cluster = val;
      this.$emit('change', this.nodeParams);
      this.getStorageClusterExpiresConfigs();
    },
    expires(val) {
      this.nodeParamsValue.config.expires = this.expires;
      this.$emit('change', this.nodeParams);
    },
    nodeParams: {
      deep: true,
      handler(val) {
        if (val) {
          this.cluster = val.config.cluster;
          this.expires = val.config.expires;
        }
      },
    },
  },
  methods: {
    getStorageClusterExpiresConfigs() {
      this.expiresList = [];
      this.$store
        .dispatch('api/flows/getStorageClusterExpiresConfigs', { type: this.clusterType, name: this.cluster })
        .then(res => {
          if (res.result) {
            res.data.expires = JSON.parse(res.data.expires);
            this.expiresConfig = res.data;
            this.getClusterExpiresConfig();
            this.$emit('gotClusterExpiresConfig', this.expiresConfig);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
    },
    getClusterExpiresConfig() {
      this.getExpiresListCommon(false, 'expires');
      this.nodeParamsValue.config.expires = this.expires;
      this.$emit('change', this.nodeParams);
    },
  },
};
</script>

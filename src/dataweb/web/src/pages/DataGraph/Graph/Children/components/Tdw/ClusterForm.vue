

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
  <bkdata-form :labelWidth="125"
    :model="formData"
    :rules="validate">
    <bkdata-form-item :label="$t('存储集群')"
      :required="true"
      property="cluster_id">
      <!--eslint-disable-next-line vue/no-mutating-props-->
      <cluster-selector
        v-model="formData.cluster_id"
        :clusterLoading.sync="clusterLoading"
        :colorLevel="true"
        :clusterType="'tdw'" />
    </bkdata-form-item>
    <bkdata-form-item :label="$t('DB')"
      :required="true"
      property="db_name">
      <bkdata-selector
        :list="dbList"
        :isLoading="dbLoading"
        :selected.sync="formData.db_name"
        :settingKey="'id'"
        :displayKey="'name'" />
    </bkdata-form-item>
    <bkdata-form-item :label="$t('过期时间')"
      :required="true"
      property="expires">
      <bkdata-selector
        :list="expiresList"
        :selected.sync="expiresSelected"
        :settingKey="'id'"
        :displayKey="'text'"
        @item-selected="expireChangeHandle" />
    </bkdata-form-item>
  </bkdata-form>
</template>

<script>
export default {
  model: {
    prop: 'formData',
    event: 'update',
  },
  props: {
    formData: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      expiresSelected: '',
      dbLoading: false,
      dbList: [],
      cluster_id: '',
      clusterLoading: false,
      expiresList: [],
      // eslint-disable-next-line vue/no-dupe-keys
      formData: {
        cluster_id: '',
        db_name: '',
        expires: '',
        expires_unit: '',
      },
      validate: {
        db_name: [
          {
            required: true,
            message: this.$t('必填项不可为空'),
            trigger: 'blur',
          },
        ],
        cluster_id: [
          {
            required: true,
            message: this.$t('必填项不可为空'),
            trigger: 'blur',
          },
        ],
        expires: [
          {
            required: true,
            message: this.$t('必填项不可为空'),
            trigger: 'blur',
          },
        ],
      },
    };
  },
  watch: {
    'formData.cluster_id'(val) {
      val && this.getStorageClusterExpiresConfigs() && this.getDbListForTdw();
    },
  },
  methods: {
    /** 存储集群、db相关 */
    getDbListForTdw() {
      this.dbLoading = true;
      this.bkRequest
        .httpRequest('tdw/getDbListWithCreate', {
          query: {
            cluster_id: this.cluster_id,
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
            this.getMethodWarning(res.message, 'error');
          }
          this.dbLoading = false;
        });
    },
    getStorageClusterExpiresConfigs() {
      this.expiresList = [];
      this.$store
        .dispatch('api/flows/getStorageClusterExpiresConfigs', { type: 'tdw', name: this.formData.cluster_id })
        .then(res => {
          if (res.result) {
            res.data.expires = JSON.parse(res.data.expires);
            const expiresConfig = res.data;
            this.expiresList = expiresConfig.expires.list_expire.map(d => {
              return { id: d.value, text: d.name };
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
    },
    expireChangeHandle(expires) {
      const config = JSON.parse(JSON.stringify(this.formData));
      expires.replace(/^(\d+)(\w?)$/, (match, group1, group2) => {
        config.expires = group1;
        config.expires_unit = group2;
      });
      this.$emit('update', config);
    },
  },
};
</script>

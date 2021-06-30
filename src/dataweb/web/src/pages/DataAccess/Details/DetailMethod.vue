

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
  <Layout
    class="container-with-shadow"
    :crumbName="$t('接入方式')"
    :withMargin="false"
    :headerMargin="false"
    :headerBackground="'inherit'"
    :collspan="true"
    height="auto">
    <span
      v-if="isRefresh"
      slot="header"
      @click="
        () =>
          $router.push({
            path: `/data-access/updatedataid/${$route.params.did}`,
            query: { from: $route.path },
          })
      ">
      <i class="bk-icon icon-edit" />
    </span>
    <div class="running-log-container">
      <AccessMethod ref="acc_method"
        :schemaConfig="schemaConfig"
        :formData="formData" />
    </div>
  </Layout>
</template>

<script>
import Layout from '@/components/global/layout';
import AccessMethod from '../Components/AccessMethod/display';
import getFormSchemaConfig from '../NewForm/SubformConfig/formSchema.js';
export default {
  components: { Layout, AccessMethod },
  props: {
    dataScenario: {
      type: String,
      default: 'log',
    },
    formData: {
      type: Object,
      default: () => {
        return {};
      },
    },
    isRefresh: {
      type: Boolean,
      default: true,
    },
  },
  data() {
    return {
      accessType: 'log',
      formConfig: {},
    };
  },
  computed: {
    schemaConfig() {
      const config = {};
      return (this.formConfig[this.dataScenario] && this.formConfig[this.dataScenario].schema.accessMethod) || {};
    },
  },
  async mounted() {
    this.formConfig = await getFormSchemaConfig();
  }
};
</script>
<style lang="scss" scoped>
.running-log-container {
  margin-top: -20px;
  margin-left: -15px;
  margin-right: -15px;
  width: 100%;
}

.icon-edit {
  cursor: pointer;
  color: #3a84ff;
  &:hover {
    color: #5354a5;
  }
}
</style>

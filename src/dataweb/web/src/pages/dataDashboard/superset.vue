

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
  <div class="superset-wrapper">
    <Layout :showHead="false"
      :showSubNav="true">
      <template slot="subNav">
        <superset-nav :activeName.sync="tabActiveName"
          @supersetChangeTab="supersetChangeTab" />
      </template>
      <div v-bkloading="{ isLoading: loading }"
        class="superset-wrap">
        <iframe :src="src"
          frameborder="0"
          @load="load()" />
      </div>
    </Layout>
  </div>
</template>
<script>
import Layout from '@/components/global/layout';
import SupersetNav from './components/supersetNav';

export default {
  components: {
    Layout,
    SupersetNav,
  },
  data() {
    return {
      loading: true,
      src: '',
      tabActiveName: 'SupersetWelcome',
      iframeRouters: {
        SupersetWelcome: {
          subUrl: '/',
        },
        MyDashboard: {
          subUrl: 'dashboard/list/',
        },
        MyChart: {
          subUrl: 'chart/list/',
        },
        ScheduleEmail: {
          subUrl: 'dashboardemailscheduleview/list/',
        },
      },
    };
  },
  mounted() {
    this.supersetChangeTab(this.tabActiveName);
  },
  methods: {
    load() {
      this.loading = false;
    },
    supersetChangeTab(routerName) {
      this.tabActiveName = routerName;
      this.loading = true;
      const hostUrl = `${window.BKBASE_Global.supersetUrl}/${this.iframeRouters[routerName].subUrl}`;
      this.src = `${hostUrl}?in_iframe=1&ts=${Date.now()}`;
    },
  },
};
</script>
<style lang="scss" scoped>
.superset-wrapper {
  height: 100%;
  ::v-deep .layout-body {
    height: 100%;
    overflow: auto;
    .layout-content.with-margin {
      width: 100%;
      height: 100%;
      margin: 0;
      padding: 0;
      .superset-wrap {
        position: relative;
        top: -60px;
        width: 100%;
        height: calc(100% + 60px);
        iframe {
          width: 100%;
          height: 100%;
        }
      }
    }
  }
}
</style>

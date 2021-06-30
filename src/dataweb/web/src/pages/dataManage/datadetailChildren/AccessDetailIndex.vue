

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
  <div v-bkloading="{ isLoading: isLoading }"
    class="access-detail-container">
    <!--整体状态预览-->
    <AccessPreView :details="details"
      :accessSummaryStatus="accessSummaryStatus" />

    <template v-if="isOfflineFile">
      <Info class="mb20">
        <p slot="tipContent"
          class="content ml10">
          <span>{{ $t('使用文件类型的数据源的提示') }}</span>
          <span class="link"
            @click="linkToNotebook">
            {{ $t('数据探索') }}
          </span>
        </p>
      </Info>
    </template>
    <template v-if="isAccessObjectExist">
      <!-- 接入对象 -->
      <DynamicAccessObj :details="details"
        :ipList="lists.Iplists" />
    </template>

    <template v-if="isAccessMethodExist">
      <!-- 接入方式 -->
      <DetailMethod
        :isRefresh="dataChannel.includes(details.bk_app_code)"
        :data-scenario="dataScenario"
        :formData="details" />
    </template>
    <!-- 运行日志 -->
    <RunningLog />

    <!-- 操作历史 -->
    <OperationHistory />
  </div>
</template>
<script>
import AccessPreView from '@/pages/DataAccess/Details/AccessPreView';
import DynamicAccessObj from './dynamicAccessObj';
import RunningLog from '@/pages/DataAccess/Details/RunningLog';
import OperationHistory from '@/pages/DataAccess/Details/OperationHistory';
import DetailMethod from '@/pages/DataAccess/Details/DetailMethod';
import Info from '@/components/TipsInfo/TipsInfo.vue';

import mixin from './mixin';
export default {
  components: {
    AccessPreView,
    DynamicAccessObj,
    RunningLog,
    OperationHistory,
    DetailMethod,
    Info,
  },
  mixins: [mixin],
  props: {
    details: {
      type: Object,
      default() {
        return {
          scopes: [],
          bk_app_code: 'dataweb',
        };
      },
    },
    lists: {
      type: Object,
      default() {
        return {};
      },
    },
    dataScenario: {
      type: String,
      default: '',
    },
    accessSummaryStatus: {
      type: Object,
      default() {
        return {};
      },
    },
  },
  data() {
    return {
      isLoading: false,
      dataChannel: ['dataweb', 'data'], // 允许编辑的接入渠道
    };
  },
  computed: {
    accessConfInfo() {
      return this.details['access_conf_info'];
    },
    /** 是否有接入对象 */
    isAccessObjectExist() {
      return this.accessConfInfo && this.accessConfInfo.resource;
    },
    /** 是否有采集方式 */
    isAccessMethodExist() {
      return this.accessConfInfo && this.accessConfInfo['collection_model'];
    },
  },
  methods: {
    linkToNotebook() {
      const { href } = this.$router.resolve({
        name: 'DataExploreNotebook',
        query: {
          type: 'personal',
        },
      });
      window.open(href, '_blank');
    },
  },
};
</script>
<style lang="scss" scoped>
.access-detail-container {
  position: relative;
}
</style>

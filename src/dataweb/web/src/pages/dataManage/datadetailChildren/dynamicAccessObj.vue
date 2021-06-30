

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
    :crumbName="$t('接入对象')"
    :withMargin="false"
    :headerMargin="false"
    :headerBackground="'inherit'"
    :collspan="true"
    :isOpen="true"
    height="auto">
    <span v-if="dataChannel.includes(details.bk_app_code)"
      slot="header"
      style="margin-right: 10px"
      @click="routerTo">
      <i class="bk-icon icon-edit"
        style="color: #3a84ff; cursor: pointer" />
    </span>
    <div class="acc-obj-content">
      <div
        :is="exactAccessObjContent"
        :details="details"
        :display="true"
        :bizid="details.bk_biz_id"
        :ipList="ipList"
        @componentMounted="handleComponentMounted" />
    </div>
  </Layout>
</template>

<script>
import Layout from '@/components/global/layout';
import { getCurrentComponent } from '@/pages/DataAccess/Config/index.js';
export default {
  components: { Layout },
  props: {
    details: {
      type: Object,
      default: () => {
        return {
          bk_app_code: 'dataweb',
        };
      },
    },
    ipList: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      componentData: null,
      dataChannel: ['dataweb', 'data'], // 允许编辑的接入渠道
      exactAccessObjContent: undefined,
    };
  },
  computed: {
    scenario() {
      // 消息队列下有kafka类型的数据源
      return this.details.data_scenario === 'queue'
        ? this.details.access_conf_info.resource.type
        : this.details.data_scenario;
    },
  },
  watch: {
    details: {
      handler(val) {
        val && this.handleComponentMounted();
      },
      deep: true,
      immediate: true,
    },
    scenario: {
      immediate: true,
      handler(val) {
        getCurrentComponent(val, 'detail').then(resp => {
          this.exactAccessObjContent = (resp || {}).component;
        });
      }
    }
  },
  methods: {
    handleComponentMounted() {
      this.details
        && Array.prototype.forEach.call(this.$children, child => child.$nextTick(() => {
          this.deepRenderEditData(child, this.details);
        })
        );
    },

    /**
     * 编辑页面，初始化已有数据
     * @param node: 根节点
     * @param data: 接口获取数据
     */
    deepRenderEditData(node, data) {
      if (node.renderData) {
        typeof node.renderData === 'function' && node.renderData(data);
        node.$forceUpdate();
      }
      !node.isDeepTarget
        && Array.prototype.forEach.call(node.$children, child => child.$nextTick(() => {
          this.deepRenderEditData(child, data);
        })
        );
    },
    routerTo() {
      this.$router.push({
        path: `/data-access/updatedataid/${this.$route.params.did}`,
        query: { from: this.$route.path },
      });
    },
  },
};
</script>
<style lang="scss" scoped>
.acc-obj-content {
  margin: -15px -15px 0 -15px;
  width: 100%;
}
</style>

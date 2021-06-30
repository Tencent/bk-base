

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
  <div class="wrap">
    <bkdata-tab :active.sync="activeTab"
      @tab-change="tabChange">
      <bkdata-tab-panel name="dataDiscovery"
        :label="$t('数据发现')">
        <DataDiscoveryChart ref="dataDiscovery" />
      </bkdata-tab-panel>
      <bkdata-tab-panel name="dataValue"
        :label="$t('数据价值')">
        <DataValueChart v-if="activeTab === 'dataValue'"
          ref="dataValue" />
      </bkdata-tab-panel>
      <bkdata-tab-panel disabled
        name="dataQuality"
        :label="$t('数据质量')" />
    </bkdata-tab>
  </div>
</template>

<script>
import DataDiscoveryChart from './DictInventory/DataDiscoveryChart';
import DataValueChart from './DictInventory/DataValueChart';

export default {
  components: {
    DataDiscoveryChart,
    DataValueChart,
  },
  inject: ['getParams'],
  data() {
    return {
      activeTab: 'dataDiscovery',
      tabMap: {
        dataDiscovery: false,
        dataValue: false,
      },
    };
  },
  methods: {
    updateData() {
      this.tabMap = {
        dataDiscovery: true,
        dataValue: true,
      };
      for (const key in this.$refs) {
        if (this.activeTab === key) {
          this.getData(this.activeTab);
        }
      }
    },
    tabChange(tab) {
      if (this.tabMap[tab]) {
        this.getData(tab);
      }
      this.$emit('tabChange', tab);
    },
    getData(tabName) {
      this.setAdvanceParamsHistory();
      console.log(tabName);
      this.$refs[tabName].$children.forEach(instance => {
        instance.initData && instance.initData();
      });
      this.tabMap[tabName] = false;
    },
    setAdvanceParamsHistory() {
      this.bkRequest.httpRequest('dataDict/getSearchHistory', {
        params: this.getParams(),
      });
    },
  },
};
</script>

<style lang="scss" scoped>
::v-deep .bk-tab-section {
  padding-top: 0;
}
</style>

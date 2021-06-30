

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
  <div class="resource-detail-wrapper">
    <div class="detail-navi">
      <span class="icon-resource-detail" />
      <span class="resource-group-id"
        @click="$router.push({ name: 'user_center', query: { tab: 'resourceGroup' } })">
        {{ resourceGroupId }}
      </span>
      <span class="split">/</span>
      <span class="current-page">{{ pageTitle }}</span>
    </div>
    <div class="detail-content">
      <div class="control-panel">
        <bkdata-form formType="inline">
          <bkdata-form-item :label="$t('资源类型')">
            <div style="width: 260px">
              <bkdata-selector :list="resourceTypeList"
                :selected.sync="serviceType"
                :settingKey="'service_type'"
                :displayKey="'service_name'"
                :isLoading="resourceTypeLoading"
                @item-selected="resourceTypeChange" />
            </div>
          </bkdata-form-item>
          <bkdata-form-item :label="$t('集群')">
            <div style="width: 260px">
              <bkdata-selector :list="clusterList"
                :selected.sync="clusterId"
                :settingKey="'cluster_id'"
                :displayKey="'cluster_name'"
                :isLoading="clusterLoading"
                @item-selected="getChartsData" />
            </div>
          </bkdata-form-item>
        </bkdata-form>
        <div class="button-group">
          <bkdata-button icon="plus-bold"
            extCls="icon-button"
            @click="openAppliedResource('increase')">
            {{ $t('扩容申请') }}
          </bkdata-button>
        </div>
      </div>
      <div class="chard-wrapper">
        <template v-if="resourceType === 'processing'">
          <div class="cards-column">
            <Card class="cards"
              :chartsOptions="chartsOptions[0]"
              :titleName="$t('当天CPU占集群使用率')"
              :serviceType="serviceType"
              :clusterId="clusterId"
              :timeUnit="'5min'"
              data-type="cpu" />
            <Card class="cards"
              :chartsOptions="chartsOptions[0]"
              :titleName="$t('日均CPU占集群使用率')"
              :serviceType="serviceType"
              :clusterId="clusterId"
              :timeUnit="'day'"
              data-type="cpu" />
          </div>
          <div class="cards-column">
            <Card class="cards"
              :chartsOptions="chartsOptions[1]"
              :titleName="$t('当天内存占集群使用率')"
              :serviceType="serviceType"
              :clusterId="clusterId"
              :timeUnit="'5min'"
              data-type="memory" />
            <Card class="cards"
              :chartsOptions="chartsOptions[1]"
              :titleName="$t('日均内存占集群使用率')"
              :serviceType="serviceType"
              :clusterId="clusterId"
              :timeUnit="'day'"
              data-type="memory" />
          </div>
          <div class="cards-column">
            <Card class="cards"
              :chartsOptions="chartsOptions[2]"
              :titleName="$t('当天app')"
              :serviceType="serviceType"
              :clusterId="clusterId"
              :timeUnit="'5min'"
              data-type="app" />
            <Card class="cards"
              :chartsOptions="chartsOptions[2]"
              :titleName="$t('日均app')"
              :serviceType="serviceType"
              :clusterId="clusterId"
              :timeUnit="'day'"
              data-type="app" />
          </div>
        </template>
        <div v-if="resourceType === 'storage'"
          class="cards-column">
          <Card class="cards"
            :chartsOptions="chartsOptions[2]"
            :titleName="$t('当天磁盘使用率')"
            :serviceType="serviceType"
            :clusterId="clusterId"
            :timeUnit="'5min'"
            data-type="disk" />
          <Card class="cards"
            :chartsOptions="chartsOptions[2]"
            :titleName="$t('日均磁盘使用率')"
            :serviceType="serviceType"
            :clusterId="clusterId"
            :timeUnit="'day'"
            data-type="disk" />
        </div>
        <!-- <div class="cards-column">
                    <Card class="cards"
                        type="pie"
                        :chartsOptions="chartsOptions[2]"
                        :titleName="$t('当天项目任务数')"
                        :serviceType="serviceType"
                        :clusterId="clusterId"
                        :timeUnit="'5min'"
                        dataType="task"/>
                    <Card class="cards"
                        type="pie"
                        :chartsOptions="chartsOptions[2]"
                        :titleName="$t('日均项目任务数')"
                        :serviceType="serviceType"
                        :clusterId="clusterId"
                        :timeUnit="'day'"
                        dataType="task"/>
                </div> -->
      </div>
    </div>
    <AppliedResource ref="AppliedResource"
      :focusCard="resourceIndex"
      :resourceGroupId="resourceGroupId"
      :request="applyRequest"
      :resourceCard="resourceGroup" />
  </div>
</template>
<script>
import Bus from '@/common/js/bus.js';
const Card = () => import('./components/ResourceDetaileCard');
const AppliedResource = () => import('./components/AppliedResource');

export default {
  components: {
    Card,
    AppliedResource,
  },
  data() {
    return {
      applyRequest: 'increase',
      titleMap: {
        processing: window.$t('计算资源详情'),
        storage: window.$t('存储资源详情'),
        databus: window.$t('总线资源详情'),
      },
      chartsOptions: [
        {
          stroke: '#4CD59F',
          fill: '#D6F4E9',
          areaTitle: d => d.value,
        },
        {
          stroke: '#1F90FF',
          fill: '#D6E3FD',
          areaTitle: d => d.value,
        },
        {
          stroke: '#9023FE',
          fill: '#E3C4FE',
          areaTitle: d => d.value,
        },
      ],
      getChartDataParams: {
        resource_group_id: '',
        geog_area_code: 'inland',
        service_type: '',
        start_time: '',
        end_time: '',
        time_unit: 'hour',
      },
      resourceTypeList: [],
      resourceTypeLoading: false,
      clusterLoading: false,
      serviceType: '',
      clusterId: '',
      clusterList: [],
    };
  },
  computed: {
    resourceGroup() {
      return JSON.parse(this.$route.params.resourceGroup);
    },
    pageTitle() {
      return this.titleMap[this.resourceType];
    },
    resourceGroupId() {
      return this.$route.params.resourceId;
    },
    resourceType() {
      return this.$route.params.type;
    },
    resourceTypeUrlName() {
      return `${this.resourceType}_metrics`;
    },
    resourceIndex() {
      const sourceArray = ['processing', 'storage', 'databus'];
      return sourceArray.findIndex(item => item === this.resourceType);
    },
  },
  created() {
    this.getResourceType();
  },
  mounted() {
    console.log(this.$refs, this.$refs.currentCard);
  },
  methods: {
    resourceTypeChange() {
      this.clusterId = '';
      this.getClusterList();
    },
    openAppliedResource(val) {
      this.applyRequest = val;
      this.$refs.AppliedResource.dialogConfig.isShow = true;
    },
    getChartsData() {
      Bus.$emit('getResourceChartsData');
    },
    getResourceType() {
      this.resourceTypeLoading = true;
      this.bkRequest
        .httpRequest('resourceManage/getServiceList', {
          params: {
            resource_group_id: this.resourceGroupId,
            resourceType: this.resourceTypeUrlName,
          },
        })
        .then(res => {
          if (res.result) {
            this.resourceTypeList = res.data;
            this.serviceType = this.resourceTypeList[0].service_type;
            this.getClusterList(true);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.resourceTypeLoading = false;
        });
    },
    getClusterList(autoSelected = false) {
      this.clusterLoading = true;
      this.bkRequest
        .httpRequest('resourceManage/getClusterList', {
          params: {
            resource_group_id: this.resourceGroupId,
            resourceType: this.resourceTypeUrlName,
          },
          query: {
            service_type: this.serviceType,
          },
        })
        .then(res => {
          if (res.result) {
            this.clusterList = res.data;
            this.clusterList.push({
              cluster_id: 'all',
              cluster_name: '全部',
            });
            if (autoSelected) {
              this.clusterId = this.clusterList[0].cluster_id;
              this.$nextTick(() => {
                this.getChartsData();
              });
            }
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.clusterLoading = false;
        });
    },
  },
};
</script>

<style lang="scss" scoped>
.resource-detail-wrapper {
  width: 100%;
  height: 100%;
  overflow: auto;
  .detail-navi {
    padding-left: 16px;
    width: 100%;
    height: 50px;
    background: #f0f1f5;
    line-height: 50px;
    .icon-resource-detail {
      margin-right: 6px;
      color: #979ba5;
    }
    .resource-group-id {
      font-weight: 700;
      color: #303133;
      transition: color 0.2s cubic-bezier(0.645, 0.045, 0.355, 1);
      cursor: pointer;
      &:hover {
        color: #3a84ff;
      }
    }
    .split {
      color: #c4c6cc;
      font-weight: 700;
      margin: 0 6px;
    }
  }
  .detail-content {
    padding: 37px 35px;
    .control-panel {
      display: flex;
      justify-content: space-between;
      ::v-deep .button-group {
        .mtl-5 {
          margin-left: -5px;
        }
        .icon-button .left-icon {
          margin-right: 9px;
          top: -1px;
        }
      }
    }
    .chard-wrapper {
      margin-top: 20px;
      .cards-column {
        display: flex;
        margin-bottom: 20px;
        .cards {
          flex: 1;
          width: 0;
          &:nth-child(2n + 1) {
            margin-right: 20px;
          }
        }
      }
    }
  }
}
</style>

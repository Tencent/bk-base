

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
  <div class="access-object-container">
    <carousel
      v-if="details.access_conf_info"
      :pageSize="scopePageSize"
      :height="260"
      @change="handleCarouselChange"
      @layoutItems="handlLayoutItems">
      <carousel-item
        v-for="(item, index) in details.access_conf_info.resource.scope"
        :key="index"
        :itemIndex="index"
        :title="$t('点击查看接入对象详情')"
        @itemChecked="handleScopeItemClick(index)">
        <ScopeItem
          :ref="'scope_item_' + index"
          :step="index + 1"
          :width="'100%'"
          :height="260"
          :class="[activeScope === index && 'active', 'scope-item']">
          <FieldX2 :label="$t('采集范围')">
            <span class="collection-scope">
              <span>
                {{ $t('已选择') }} {{ item.module_scope && item.module_scope.length }} {{ $t('个模块和') }}
                {{ item.host_scope && item.host_scope.length }} {{ $t('个IP') }}
              </span>
              <StatusIcons :status="accessObjStatusSummary[item.deploy_plan_id]"
                @refresh="refreshObjStatus" />
            </span>
          </FieldX2>

          <FieldCol1 class="access-preview-file"
            :label="$t('文件路径')">
            <template v-if="item.scope_config">
              <div v-for="(log, pathIndex) of item.scope_config.paths"
                :key="pathIndex">
                <div v-for="(pathItem, pIndex) in log.path"
                  :key="pIndex"
                  class="file-path mb10">
                  <SystemPicker :selectedItem="log.system"
                    :isDisabled="true" />
                  <bkdata-input :value="pathItem"
                    disabled />
                </div>
              </div>
            </template>
          </FieldCol1>
        </ScopeItem>
      </carousel-item>
    </carousel>
    <div v-if="details.access_conf_info"
      class="module-ip-preview model-container">
      <div class="arraw-tip"
        :style="arrawTipsStyle" />

      <div class="module-panel">
        <ModulePreview
          :deployPlanId="activeScopeDetails.deploy_plan_id"
          :bizid="details.bk_biz_id"
          :data-scenario="details.data_scenario"
          :details="activeScopeDetails"
          :ipList="ipList" />
      </div>
      <div class="ip-panel">
        <IpPreview
          :deployPlanId="activeScopeDetails.deploy_plan_id"
          :details="details.access_conf_info.resource.scope[activeScope]"
          :data-scenario="details.data_scenario"
          :ipList="ipList" />
      </div>
    </div>
  </div>
</template>
<script>
import ScopeItem from '../Components/Scope/ScopeForm';
import carousel from '@/components/carousel/carousel';
import carouselItem from '@/components/carousel/carouselItem';
import FieldCol1 from '../NewForm/FormItems/FieldComponents/FieldCol1';
import FieldX2 from '../NewForm/FormItems/FieldComponents/FieldX2';
import StatusIcons from './StatusIcons';
import SystemPicker from '../NewForm/FormItems/Components/SystemPicker';
import ModulePreview from './ModulePreview.vue';
import IpPreview from './IpPreview.vue';
import { mapState } from 'vuex';

export default {
  components: {
    ScopeItem,
    carousel,
    carouselItem,
    FieldCol1,
    StatusIcons,
    SystemPicker,
    ModulePreview,
    IpPreview,
    FieldX2,
  },
  props: {
    details: {
      type: Object,
      default: () => ({}),
    },
    ipList: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      activeScope: 0,
      scopePageSize: 2,
      activePageIndex: 0,
      aciveItemInPage: {},
      arrawTipsStyle: {},
      accessObjStatusSummary: {},
    };
  },
  computed: {
    ...mapState({
      accessSummaryInfo: state => state.common.accessSummaryInfo,
    }),
    activeScopeDetails() {
      if (!this.details.access_conf_info) {
        return {};
      }

      const deployPlanId = this.details.access_conf_info.resource.scope[this.activeScope].deploy_plan_id;
      // accessSummaryInfo.deploy_plans中存在module信息的为module信息集合，反之则为ip集合
      const moduleInfo =        this.accessSummaryInfo.deploy_plans
        && this.accessSummaryInfo.deploy_plans.find(item => item.deploy_plan_id === deployPlanId && item.module);

      return moduleInfo || { deploy_plan_id: deployPlanId };
    },
    rawDataId() {
      return this.$route.params.did;
    },
  },
  watch: {
    activeScope: {
      handler(val) {
        this.$nextTick(() => {
          this.setArrawTipsStyle();
        });
      },
      immediate: true,
    },
    'details.access_conf_info.resource.scope': {
      immediate: true,
      handler(val) {
        if (val.length) {
          val.forEach(item => {
            this.accessObjStatus(item.deploy_plan_id);
          });
        }
      },
    },
  },
  methods: {
    handlLayoutItems() {
      this.setArrawTipsStyle();
    },
    handleCarouselChange(index, position) {
      this.activePageIndex = index;
      this.$nextTick(() => {
        const preActiveIndex = this.aciveItemInPage[`${this.activePageIndex}`];
        this.activeScope = (this.activePageIndex - 1) * this.scopePageSize;
        this.$set(this.aciveItemInPage, this.activePageIndex, this.activeScope);
        this.setArrawTipsStyle(index);
      });
    },
    handleScopeItemClick(index) {
      this.$set(this.aciveItemInPage, this.activePageIndex, index);
      this.activeScope = index;
    },
    setArrawTipsStyle(index) {
      const activeComp = this.$refs[`scope_item_${this.activeScope}`];
      const activeItem = (activeComp && activeComp[0] && activeComp[0].$el) || null;

      if (!activeItem) {
        return;
      }

      const container = activeItem.closest('.bk-carousel-items');
      const parentOffsetMath = container && container.style.transform.match(/.*\((-?\d{0,10})px/);
      const parentOffset = (parentOffsetMath && Number(parentOffsetMath[1])) || 1;
      if (activeItem) {
        const width = activeItem.offsetWidth;
        let offsetleft = activeItem.offsetLeft + width / 2 + parentOffset;
        offsetleft = offsetleft > 240 ? offsetleft : 240;
        this.arrawTipsStyle = {
          left: `${offsetleft}px`,
        };
      }
    },
    moduleStatus(moduleItem) {
      const activeModuleSummary =        (this.accessSummaryInfo.deploy_plans
          && this.accessSummaryInfo.deploy_plans.find(item => item.deploy_plan_id === moduleItem.deploy_plan_id))
        || {};

      return {
        success: (activeModuleSummary && activeModuleSummary.summary.success) || 0,
        error: (activeModuleSummary && activeModuleSummary.summary.failure) || 0,
        warning: (activeModuleSummary && activeModuleSummary.summary.running) || 0,
      };
    },
    /**
     * 刷新当前接入对象采集状态信息
     */
    refreshObjStatus(closeLoadingFn) {
      this.bkRequest
        .httpRequest('dataAccess/getAccessSummary', {
          params: {
            raw_data_id: this.rawDataId,
            deploy_plan_id: this.activeScopeDetails.deploy_plan_id,
          },
          query: {
            deploy_plan_id: this.activeScopeDetails.deploy_plan_id,
          },
        })
        .then(res => {
          if (res.result) {
            this.$set(this.accessObjStatusSummary, this.activeScopeDetails.deploy_plan_id, res.data.summary);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          closeLoadingFn && closeLoadingFn();
        });
    },
    accessObjStatus(deployPlanId) {
      this.bkRequest
        .httpRequest('dataAccess/getAccessSummary', {
          params: {
            raw_data_id: this.rawDataId,
            deploy_plan_id: deployPlanId,
          },
          query: {
            deploy_plan_id: deployPlanId,
          },
        })
        .then(res => {
          if (res.result) {
            this.$set(this.accessObjStatusSummary, deployPlanId, res.data.summary);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
    },
  },
};
</script>
<style lang="scss" scoped>
.access-object-container {
  margin: -20px 15px 15px 15px;
}

.file-path {
  display: flex;
  align-items: center;
}

.scope-item {
  cursor: pointer;
  //   margin: 0 5px;
  &.active {
    cursor: auto;
    box-shadow: 1px 1px 6px rgba(83, 84, 165, 0.6);
  }
}

::v-deep .access-preview-file {
  align-items: flex-start;
}

.collection-scope {
  min-width: 280px;
}

.model-container {
  display: flex;
  border: solid 3px #ddd;
  border-radius: 2px;
  position: relative;
  padding: 10px;

  .arraw-tip {
    position: absolute;
    top: -1px;
    left: 0;
    transform: translate(50%, -100%);
    border: 10px solid #ddd;
    border-color: transparent transparent #ddd transparent;
  }
}
.module-ip-preview {
  display: flex;
  height: 355px;

  .module-panel {
    width: 60%;
    padding-right: 20px;
  }

  .module-panel {
    width: 60%;
    padding-right: 20px;
  }

  .ip-panel {
    width: 40%;
  }
}
.icon-edit {
  cursor: pointer;

  &:hover {
    color: #5354a5;
    font-weight: bold;
  }
}
</style>

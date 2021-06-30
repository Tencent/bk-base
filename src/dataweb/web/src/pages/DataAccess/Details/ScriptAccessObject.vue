

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
      v-if="details.access_conf_info && resetCarouse"
      :pageSize="scopePageSize"
      :height="carouselHeight"
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
          :height="carouselHeight"
          :class="[activeScope === index && 'active', 'scope-item']">
          <Item class="x2">
            <label class="bk-label">{{ $t('采集范围') }}：</label>
            <div class="bk-form-content">
              <div class="pick-type">
                <span>
                  {{ $t('已选择') }} {{ item.module_scope && item.module_scope.length }} {{ $t('个模块和')
                  }}{{ item.host_scope && item.host_scope.length }} {{ $t('个IP') }}
                </span>
              </div>
            </div>
          </Item>
          <Item class="x2 script-item">
            <label class="bk-label">{{ $t('脚本') }}：</label>
            <div class="bk-form-content">
              <a href="javascript:;"
                class="editor-preview-btn"
                @click="showScriptEditor(index, item)">
                {{ `${showScript ? $t('关闭脚本') : $t('点击查看脚本')}` }}
              </a>
            </div>
          </Item>
        </ScopeItem>
      </carousel-item>
    </carousel>
    <div v-if="details.access_conf_info"
      class="module-ip-preview model-container">
      <div class="arraw-tip"
        :style="arrawTipsStyle" />

      <div class="module-panel">
        <ModulePreview
          :deployPlanId="activePlanId"
          :bizid="details.bk_biz_id"
          :data-scenario="details.data_scenario"
          :details="activeScopeDetails"
          :ipList="ipList" />
      </div>
      <div class="ip-panel">
        <IpPreview
          :deployPlanId="activePlanId"
          :data-scenario="details.data_scenario"
          :details="details.access_conf_info.resource.scope[activeScope]"
          :ipList="ipList" />
      </div>
    </div>

    <bkdata-dialog
      v-model="showScript"
      extCls="bkdata-dialog"
      :content="'component'"
      :hasHeader="false"
      :closeIcon="true"
      :hasFooter="false"
      :maskClose="false"
      :width="707"
      :height="455"
      @confirm="showScriptEditor(0)">
      <div class="debug-list">
        <Container>
          <Item class="x2 script-item">
            <div class="bk-form-content editor">
              <Monaco
                height="355px"
                language="shell"
                theme="vs-dark"
                :options="{ readOnly: true, fontSize: 10 }"
                :tools="tools"
                :code="(activeItem && activeItem.scope_config && activeItem.scope_config.content) || ''" />
            </div>
          </Item>
        </Container>
      </div>
    </bkdata-dialog>
  </div>
</template>
<script>
import ScopeItem from '../Components/Scope/ScopeForm';
import carousel from '@/components/carousel/carousel';
import carouselItem from '@/components/carousel/carouselItem';
import ModulePreview from './ModulePreview.vue';
import IpPreview from './IpPreview.vue';
import { mapState } from 'vuex';
import Monaco from '@/components/monaco';
import Container from '../NewForm/FormItems/ItemContainer';
import Item from '../NewForm/FormItems/Item';

export default {
  components: {
    ScopeItem,
    carousel,
    carouselItem,
    ModulePreview,
    IpPreview,
    Monaco,
    Item,
    Container,
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
      tools: {
        enabled: true,
        title: '',
        toolList: {
          format_sql: false,
          view_data: false,
          guid_help: false,
          full_screen: true, // 是否启用全屏设置
          event_fullscreen_default: true, // 是否启用默认的全屏功能，如果设置为false需要自己监听全屏事件组后续处理
        },
      },
      activeScope: 0,
      scopePageSize: 2,
      activePageIndex: 0,
      aciveItemInPage: {},
      arrawTipsStyle: {},
      carouselHeight: 190,
      resetCarouse: true,
      showScript: false,
      activeItem: {
        scope_config: {
          content: '',
        },
      },
    };
  },

  computed: {
    ...mapState({
      accessSummaryInfo: state => state.common.accessSummaryInfo,
    }),
    activePlanId() {
      return this.details.access_conf_info.resource.scope[this.activeScope].deploy_plan_id;
    },
    activeScopeDetails() {
      if (!this.details.access_conf_info) {
        return {};
      }
      /**
       * accessSummaryInfo.deploy_plans中存在module信息的为module信息集合，反之则为ip集合
       */
      return (
        (this.accessSummaryInfo.deploy_plans
          && this.accessSummaryInfo.deploy_plans.find(item => item.deploy_plan_id === this.activePlanId))
        || {}
      );
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
  },
  methods: {
    showScriptEditor(index, item) {
      this.showScript = !this.showScript;
      this.activeItem = item;
    },
    handlLayoutItems() {
      this.setArrawTipsStyle();
    },
    handleCarouselChange(index, position) {
      this.activePageIndex = index;
      this.$nextTick(() => {
        const preActiveIndex = this.aciveItemInPage[`${this.activePageIndex}`];
        this.activeScope = preActiveIndex !== undefined ? preActiveIndex : this.scopePageSize * (index - 1) + 1;
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
  },
};
</script>
<style lang="scss" scoped>
.access-object-container {
  margin: -20px 15px 15px 15px;
}

.module-count {
  padding: 0px 5px;
  font-size: 12px;
  border-radius: 2px;
  background: #3a84ff;
  color: #fff;
  margin: 0 5px;
}

.editor-preview-btn {
  display: inline-block;
  //   margin-top: 4px;
  color: #3a84ff;
  //   font-weight: bold;
}

.file-path {
  display: flex;
  align-items: center;
}

.scope-item {
  cursor: pointer;
  margin: 0 5px;
  &.active {
    cursor: auto;
    box-shadow: 1px 1px 6px rgba(83, 84, 165, 0.6);
  }
}

::v-deep .access-preview-file {
  align-items: flex-start;
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

  .ip-panel {
    width: 40%;
  }
}

.debug-list {
  .script-item {
    margin-left: 24px;
    padding-top: 12px;
    padding-bottom: 24px;
  }
}
</style>

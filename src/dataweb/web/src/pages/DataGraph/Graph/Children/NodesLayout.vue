

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
  <Layout>
    <template #left-pane>
      <Datainput
        ref="dataInput"
        :params.sync="params.config"
        :hasNodeId="hasNodeId"
        :data-input-list="dataInputList"
        :resultList="parentResultList"
        :inputFieldsList.sync="inputFieldsList" />
    </template>
    <template #mid-pane>
      <div class="data-process-wrapper bk-scroll-y">
        <bkdata-collapse v-model="activeCollapse"
          accordion
          :extCls="collapseStyle">
          <bkdata-collapse-item :name="$t('计算配置')">
            <div class="title">
              {{ processName }}
            </div>
            <div slot="content"
              class="f13 content">
              <div class="upper-section">
                <slot name="topForm" />
                <bkdata-form
                  ref="headerForm"
                  :data="params.config"
                  :labelWidth="80"
                  :model="params.config"
                  :rules="rules"
                  extCls="bk-common-form">
                  <bkdata-form-item
                    prop="name"
                    :label="$t('节点名称')"
                    required
                    :class="{ 'full-length': !hasProcessName }"
                    :property="'name'">
                    <bkdata-input v-model="params.config.name"
                      :placeholder="$t('节点的中文名')"
                      style="width: 496px" />
                  </bkdata-form-item>
                  <bkdata-form-item
                    v-if="hasProcessName"
                    prop="processing_name"
                    :label="$t('英文名称')"
                    :property="'processing_name'"
                    required>
                    <bkdata-input
                      v-model="params.config.processing_name"
                      :placeholder="$t('由英文字母_下划线和数字组成_且字母开头')"
                      style="width: 496px"
                      :disabled="hasNodeId" />
                  </bkdata-form-item>
                </bkdata-form>
                <slot />
              </div>
              <div class="second-section">
                <bkdata-tab :active.sync="activeName"
                  type="unborder-card">
                  <bkdata-tab-panel
                    v-for="(panel, index) in tabConfig"
                    :key="index"
                    v-bind="panel"
                    :disabled="isPanelDisabled(panel.disabled)">
                    <component
                      :is="panel.component"
                      :ref="panel.name"
                      :params.sync="params.config"
                      :filteWindowTypes="filterWindow"
                      :parentResultList="parentResultList"
                      :loading="loading"
                      :nodeId="selfConfig.node_id"
                      :nodeType="nodeType"
                      :parentConfig="parentConfig"
                      :selfConfig="selfConfig" />
                  </bkdata-tab-panel>
                  <bkdata-tab-panel v-bind="referTaskPanel"
                    :renderLabel="tabHeaderRender">
                    <ReferTasks v-model="referTaskPanel.count"
                      :rtid="currentRtid"
                      :nodeId="selfConfig.node_id" />
                  </bkdata-tab-panel>
                </bkdata-tab>
              </div>
            </div>
          </bkdata-collapse-item>
          <bkdata-collapse-item
            v-if="params.config.data_correct && params.config.data_correct.is_open_correct"
            :name="$t('数据修正')">
            <div class="title">
              {{ $t('数据修正') }}
            </div>
            <div slot="content"
              class="f13">
              <QualityFix
                :data-set-id="dataSetId"
                :sourceSql="sourceSql"
                :source-data-set-id="sourceDataSetId"
                :correctConfig.sync="params.config.data_correct.correct_configs"
                tableSize="min-table" />
            </div>
          </bkdata-collapse-item>
        </bkdata-collapse>
      </div>
    </template>
    <slot slot="right-pane"
      name="custom-output">
      <component
        :is="customOutputComponent"
        ref="outputPanel"
        :fullParams="params"
        :params.sync="params.config.outputs"
        :bizList="bizList"
        :configList.sync="params.config.config"
        :hasNodeId="hasNodeId"
        :selfConfig="selfConfig"
        :is-show-data-fix="isShowDataFix"
        :isOpenCorrect="isOpenCorrect"
        @open-data-fix="openDataFix" />
    </slot>
  </Layout>
</template>

<script>
import Datainput from './components/Datainput';
import Layout from './components/TribleColumnLayout';
import DataOutput from './components/DataOutput';
import DataMultipleOutput from './components/DataMultiOutput';
import childMixin from './config/child.global.mixin.js';
import generalMixin from './config/child.general.mixin.js';
import ReferTasks from './components/ReferTask';
import { getNodeConfig } from './config/nodeConfig.js';
export default {
  name: 'NodesLayout',
  components: {
    Datainput,
    DataMultipleOutput,
    DataOutput,
    Layout,
    ReferTasks,
    QualityFix: () => import('@/pages/DataGraph/Graph/Children/components/QualityFix/QualityFix.index.vue'),
  },
  mixins: [childMixin, generalMixin],
  props: {
    nodeType: {
      type: String,
      default: '',
    },
    loading: {
      type: Boolean,
      default: false,
    },
    processName: {
      type: String,
      default: $t('计算配置'),
    },
  },
  data() {
    return {
      nodeConfig: {},
      activeTab: '',
      inputFieldsList: [],
      selfConfig: {},
      parentConfig: {},
      dataInputGroup: [],
      bizList: [],
      params: {},
      dataInputList: [],
      parentResultList: {},
      referTaskPanel: {
        name: 'referTask',
        label: this.$t('关联任务'),
        count: 0,
      },
      activeName: '',
      rules: {
        name: [
          {
            required: true,
            message: this.$t('必填项不可为空'),
            trigger: 'blur',
          },
          {
            max: 50,
            message: this.$t('格式不正确_请输入五十个以内字符'),
            trigger: 'blur',
          },
        ],
        processing_name: [
          {
            required: true,
            message: this.$t('必填项不可为空'),
            trigger: 'blur',
          },
          {
            regex: /^[0-9a-zA-Z_]{1,}$/,
            message: this.$t('格式不正确_内容由字母_数字和下划线组成_且以字母开头'),
            trigger: 'blur',
          },
        ],
      },
      activeCollapse: $t('计算配置'),
    };
  },
  computed: {
    filterWindow() {
      return this.nodeConfig[this.nodeType].filterWindow || [];
    },
    isOpenCorrect() {
      return this.params.config.data_correct && this.params.config.data_correct.is_open_correct;
    },
    isShowDataFix() {
      // 节点在数据输出部分是否展示数据修正的switch开关
      return (
        this.nodeType
        && this.nodeConfig[this.nodeType].isShowDataFix
        && this.$modules.isActive('data_fix')
        && this.isSingleInput
      );
    },
    hasProcessName() {
      return (this.nodeType && this.nodeConfig[this.nodeType].hasProcessName) || false;
    },
    currentMidPanel() {
      return (this.nodeType && this.nodeConfig[this.nodeType].component) || null;
    },
    tabConfig() {
      return (this.nodeType && this.nodeConfig[this.nodeType].tabConfig) || [];
    },
    currentRtid() {
      return (this.selfConfig.result_table_ids && this.selfConfig.result_table_ids[0]) || '';
    },
    hasNodeId() {
      return this.selfConfig.hasOwnProperty('node_id');
    },
    customOutputComponent() {
      let component = null;
      switch (this.nodeConfig[this.nodeType].output) {
        case 'multipleOutput':
          component = DataMultipleOutput;
          break;
        case 'singleOutput':
          component = DataOutput;
          break;
        default:
          component = nodeConfig[this.nodeType].output;
      }
      return component;
    },
    dataSetId() {
      return `${this.params.config.bk_biz_id}_${this.params.config.table_name}`;
    },
    sourceSql() {
      return this.params.config.sql;
    },
    sourceDataSetId() {
      return this.params.config.from_nodes[0].from_result_table_ids;
    },
    isSingleInput() {
      return this.params.config && this.params.config.from_nodes.length === 1;
    },
    collapseStyle() {
      return this.activeCollapse === $t('计算配置') ? 'no-collapse-style' : 'collapse-background-style';
    },
  },
  watch: {
    params: {
      deep: true,
      handler(val) {
        this.$emit('updateParams', val);
      },
    },
    inputFieldsList(val) {
      this.$emit('updateInputFieldsList', val);
    },
    parentConfig: {
      deep: true,
      handler(val) {
        this.$emit('updateParentConfig', val);
      },
    },
  },
  async created() {
    this.nodeConfig = await getNodeConfig();
    this.$set(this.params, 'config', JSON.parse(JSON.stringify(this.nodeConfig[this.nodeType].config)));
    this.$emit('initParams', this.params);
  },
  methods: {
    tabHeaderRender() {
      return (
        <div>
          <span class="panel-name">{this.referTaskPanel.label}</span>
          <span class="panel-count">{this.referTaskPanel.count}</span>
        </div>
      );
    },
    initParams() {
      this.$set(this.params, 'config', JSON.parse(JSON.stringify(this.nodeConfig[this.nodeType].config)));
      this.$emit('initParams', this.params);
    },
    openDataFix(isOpenCorrect) {
      if (this.params.config.data_correct) {
        this.params.config.data_correct.is_open_correct = isOpenCorrect;
      }
      this.activeCollapse = isOpenCorrect ? $t('数据修正') : $t('计算配置');
    },
    isPanelDisabled(condition) {
      if (typeof condition === 'boolean') return condition;
      return !this.hasNodeId;
    },
    validateBaseForm() {
      const headerForm = this.$refs.headerForm.validate().then(
        validator => {
          return true;
        },
        validator => {
          return false;
        }
      );

      const subForm = [...this.tabConfig.map(tab => this.$refs[tab.name][0].validateForm())];
      return Promise.all([...subForm, headerForm]).then(validateResult => {
        let result = true;
        validateResult.forEach(validator => {
          if (validator === false) {
            result = false;
          }
        });
        return result;
      });
    },

    /**
                提交/保存节点
            */
    async validateFormData() {
      const baseValidate = await this.validateBaseForm();
      const outputResult = await this.$refs.outputPanel.validateForm();
      if (baseValidate && outputResult) {
        this.formatParams();
        return true;
      } else {
        return false;
      }
    },
  },
};
</script>

<style lang="scss">
.vertical-panes {
  .bk-form .bk-label {
    font-weight: normal;
  }
}
</style>
<style lang="scss" scoped>
.vertical-panes {
  width: 100%;
  height: 100%;
  border-top: 1px solid #ccc;
  border-bottom: 1px solid #ccc;
  .pane {
    text-align: left;
    overflow: hidden;
    background: #eee;
  }
  .data-input {
    min-width: 250px;
    width: 384px;
  }
  .data-process {
    min-width: 864px;
  }
  .data-output {
    flex-grow: 1;
    min-width: 300px;
  }
}
::v-deep .recal-tip {
  margin-left: 100px;
}
.data-process-wrapper {
  width: 100%;
  height: 100%;
  background: #fff;
  overflow-y: auto;
  border-right: 1px solid #dcdee5;
  .title {
    height: 22px;
    font-size: 16px;
    font-family: PingFangSC, PingFangSC-Medium;
    font-weight: bold;
    text-align: left;
    color: #313238;
    line-height: 22px;
  }
  .content {
    ::v-deep .upper-section {
      .full-length {
        // width: 100% !important;
      }
      .bk-common-form {
        display: flex;
        flex-wrap: wrap;
        justify-content: space-between;
        &::before,
        &::after {
          display: none;
        }
        .bk-form-item {
          width: auto;
          margin-bottom: 0;
        }
      }
    }
    ::v-deep .second-section {
      margin-top: 15px;
      .bk-tab-header {
        // transform: translate(6px, 0px);
        .bk-tab-label-list {
          li {
            padding: 0;
            min-width: auto;
            margin-right: 20px;
          }
          .active::after {
            left: 0;
            width: 100%;
          }
        }
      }
      .bk-tab-section {
        padding: 20px 0;
        min-height: 0 !important;
      }
      .panel-count {
        display: inline-block;
        width: 16px;
        height: 16px;
        line-height: 16px;
        border-radius: 50%;
        background: #f0f1f5;
        font-size: 12px;
        color: #979ba5;
      }
      .active .panel-count {
        background: #3a84ff;
        color: #fff;
      }
    }
  }
}

.wrapper-padding {
  padding: 17px 17px 24px 17px;
}

::v-deep .bk-collapse-item {
  .bk-collapse-item-content {
    padding: 20px 24px 0 24px;
  }
}

.no-collapse-style {
  ::v-deep .bk-collapse-item {
    .bk-collapse-item-header {
      display: flex;
      align-items: center;
      padding: 0 20px;
      cursor: inherit;
      pointer-events: none;
      padding-top: 17px;
    }
    .collapse-expand {
      display: none;
    }
    .bk-collapse-item-content {
      padding: 20px 24px 0 24px;
    }
  }
}
</style>

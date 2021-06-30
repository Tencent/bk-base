

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
  <div class="data-process-wrapper">
    <div class="title">
      {{ $t('计算配置') }}
    </div>
    <div class="content">
      <div class="upper-section">
        <bkdata-form ref="headerForm"
          :data="params"
          :labelWidth="80"
          :model="params"
          :rules="rules"
          formType="inline">
          <bkdata-form-item prop="name"
            :label="$t('节点名称')"
            required
            extCls="node-name"
            :property="'name'">
            <bkdata-input v-model="paramsValue.name"
              :placeholder="$t('节点的中文名')"
              style="width: 100%" />
          </bkdata-form-item>
          <bkdata-form-item
            prop="processing_name"
            :label="$t('英文名称')"
            :property="'processing_name'"
            required
            extCls="node-name">
            <bkdata-input
              v-model="paramsValue.processing_name"
              :placeholder="$t('由英文字母_下划线和数字组成_且字母开头')"
              :disabled="hasNodeId" />
          </bkdata-form-item>
        </bkdata-form>
        <div class="sql-wrapper">
          <Monaco
            ref="monacoEditor"
            :code="params.code"
            :height="'520px'"
            :language="params.programming_language"
            :options="{ fontSize: '14px' }"
            :tools="{
              guidUrl: docUrl,
              toolList: {
                font_size: true,
                full_screen: true,
                event_fullscreen_default: true,
                editor_fold: true,
              },
              title: params.programming_language.toUpperCase(),
            }"
            @codeChange="changeCode" />
        </div>
      </div>
      <div class="second-section">
        <bkdata-tab :active.sync="activeName"
          type="unborder-card">
          <bkdata-tab-panel v-for="(panel, index) in tabConfig"
            :key="index"
            v-bind="panel"
            :disabled="panel.disabled">
            <component :is="panel.VueComp"
              :ref="panel.name"
              :params.sync="params"
              :nodeType="nodeType" />
          </bkdata-tab-panel>
          <bkdata-tab-panel v-bind="referTaskPanel">
            <template slot="label">
              <span class="panel-name">{{ referTaskPanel.label }}</span>
              <span class="panel-count">{{ referTaskPanel.count }}</span>
            </template>
            <ReferTasks v-model="referTaskPanel.count"
              :rtid="currentRtid"
              :nodeId="selfConfig.node_id" />
          </bkdata-tab-panel>
        </bkdata-tab>
      </div>
    </div>
  </div>
</template>

<script>
import Monaco from '@/components/monaco';
import Bus from '@/common/js/bus.js';
import ReferTasks from './ReferTask';
import { validateRules } from '../../../../DataAccess/NewForm/SubformConfig/validate';
export default {
  components: {
    Monaco,
    ReferTasks,
  },
  props: {
    params: {
      type: Object,
      default: () => ({}),
    },
    selfConfig: {
      type: Object,
      default: () => ({}),
    },
    tabConfig: {
      type: Array,
      default: () => [],
    },
    resultList: {
      type: Object,
      default: () => ({}),
    },
    nodeType: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
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
      referTaskPanel: {
        name: 'referTask',
        label: this.$t('关联任务'),
        count: 0,
      },
      activeName: this.tabConfig[0].name,
    };
  },
  computed: {
    paramsValue: {
      get() {
        return this.params;
      },
      set(value) {
        Object.assign(this.params, value);
      },
    },
    docUrl() {
      const nodeName = this.nodeType === 'flink_streaming' ? 'flink' : 'spark';
      return this.$store.getters['docs/getNodeDocs'][nodeName];
    },
    inputResultTableIds() {
      return Object.keys(this.resultList);
    },
    hasNodeId() {
      return this.selfConfig.hasOwnProperty('node_id');
    },
    nodeId() {
      if (this.hasNodeId) {
        return this.selfConfig.node_id;
      }
      return null;
    },
    currentRtid() {
      return (this.selfConfig.result_table_ids && this.selfConfig.result_table_ids[0]) || '';
    },
  },
  watch: {
    inputResultTableIds(val) {
      val && this.params.code === '' && this.getCode();
    },
  },
  created() {
    this.tabConfig.forEach(tab => {
      tab.VueComp = () => import(`${tab.path}/${tab.component}.vue`);
    });
    Bus.$on('insert-monaco', text => {
      this.insertToMonaco(text);
    });
  },
  methods: {
    insertToMonaco(content) {
      this.$refs.monacoEditor.editor.trigger('keyboard', 'type', { text: `\"${content}\"` });
    },
    getCode() {
      this.bkRequest
        .httpRequest('dataFlow/getDefaultCode', {
          query: {
            programming_language: this.params.programming_language,
            node_type: this.nodeType,
            input_result_table_ids: this.inputResultTableIds,
          },
        })
        .then(res => {
          if (res.result) {
            this.paramsValue.code = res.data.code;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
    },
    changeCode(content) {
      this.paramsValue.code = content;
    },
    validate() {
      return this.$refs.headerForm.validate().then(
        validator => {
          return true;
        },
        validator => {
          return false;
        }
      );
    },
    async validateForm() {
      const subForm = [...this.tabConfig.map(tab => this.$refs[tab.name][0].validateForm())];
      return Promise.all([...subForm, this.validate()]).then(validateResult => {
        let result = true;
        validateResult.forEach(validator => {
          if (validator === false) {
            result = false;
          }
        });
        return result;
      });
    },
  },
};
</script>

<style lang="scss" scoped>
.data-process-wrapper {
  width: 100%;
  height: 100%;
  background: #fff;
  padding: 17px 24px 24px 24px;
  overflow-y: auto;
  border-right: 1px solid #dcdee5;
  .title {
    width: 64px;
    height: 22px;
    font-size: 16px;
    font-family: PingFangSC, PingFangSC-Medium;
    font-weight: 500;
    text-align: left;
    color: #313238;
    line-height: 22px;
    margin-bottom: 30px;
  }
  .content {
    ::v-deep .upper-section {
      .node-name {
      }
      .bk-form {
        width: 100%;
        .bk-form-item {
          margin-bottom: 0;
          width: 49%;
          .bk-form-content {
            width: calc(100% - 71px);
          }
        }
      }
    }
    .sql-wrapper {
      margin-top: 20px;
    }
    ::v-deep .second-section {
      margin-top: 15px;
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
</style>

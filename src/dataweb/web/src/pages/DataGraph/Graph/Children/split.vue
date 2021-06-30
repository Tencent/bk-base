

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
  <div class="split-node">
    <bkdata-tab :active="activeTab"
      :class="'node-editing'">
      <bkdata-tab-panel :label="$t('节点配置')"
        name="config">
        <div v-bkloading="{ isLoading: loading }"
          class="bk-form bk-scroll-y">
          <!--原始数据节点编辑 -->
          <div class="add-data-source node">
            <div class="content-up">
              <div class="bk-form-item sideslider-select">
                <label class="bk-label">
                  {{ $t('节点名称') }}
                  <span class="required">*</span>
                </label>
                <div class="bk-form-content">
                  <bkdata-input
                    v-model="params.config.name"
                    v-tooltip.notrigger.right="validate['name']"
                    :disabled="selfConfig.hasOwnProperty('node_id')" />
                </div>
              </div>
            </div>
          </div>

          <div class="content-down">
            <div class="bk-form-item sideslider-select">
              <label class="bk-label">{{ $t('数据输入') }}</label>
              <div class="bk-form-content source-origin">
                <span
                  v-for="(item, index) in parentConfig"
                  :key="index"
                  :title="item.node_name"
                  :disabled="true"
                  class="bk-form-input data-src">
                  {{ item.result_table_ids[0] }}({{ item.node_name }})
                </span>
                <i v-bk-tooltips="$t('来自上一节点的输出')"
                  class="node-tips bk-icon icon-question-circle" />
              </div>
            </div>
            <div class="bk-form-item cal-label clearfix">
              <label class="bk-label">
                {{ $t('数据输出') }}
                <span class="required">*</span>
              </label>
              <div
                v-for="(item, index) in params.config.config"
                :key="index"
                class="bk-form-content clearfix output-container mt5">
                <div class="bk-form-exp">
                  <bkdata-input
                    v-model="item.logic_exp"
                    :placeholder="$t('例如field')"
                    :title="item.logic_exp"
                    :maxlength="255"
                    name="validation_name"
                    type="text" />
                </div>
                <!-- <div class="bk-form-biz-id"> -->
                <div class="biz_id biz_id_shunt">
                  <bkdata-selector
                    :placeholder="$t('请选择')"
                    :displayKey="'display_bk_biz_id'"
                    :isLoading="bkBizLoading"
                    :list="bizList"
                    :searchable="true"
                    :selected.sync="item.bk_biz_id"
                    :settingKey="'bk_biz_id'"
                    searchKey="bk_biz_name" />
                </div>
                <div class="output">
                  <bkdata-input
                    v-model="params.config.table_name"
                    :placeholder="$t('由英文字母_下划线和数字组成_且字母开头')"
                    :title="params.config.table_name"
                    :disabled="index !== 0 || !isNewForm"
                    :maxlength="50"
                    name="validation_name"
                    type="text" />
                </div>
                <!-- </div> -->
                <i
                  v-if="index === params.config.config.length - 1"
                  class="node-tips bk-icon icon-plus-circle option-position"
                  @click="handleExpOption('add', index)" />
                <i
                  v-else
                  class="node-tips bk-icon icon-minus-circle option-position"
                  @click="handleExpOption('reduce', index)" />
              </div>
            </div>
            <div class="bk-form-item">
              <label class="bk-label">
                {{ $t('输出中文名') }}
                <span class="required">*</span>
              </label>
              <div class="bk-form-content">
                <bkdata-input
                  v-model="params.config.output_name"
                  v-tooltip.notrigger.right="validate['output_name']"
                  :placeholder="$t('输出中文名')"
                  :maxlength="50"
                  name="validation_name"
                  type="text" />
              </div>
            </div>
            <div class="bk-form-item">
              <label class="bk-label">
                {{ $t('节点描述') }}
                <span class="required">*</span>
              </label>
              <div class="bk-form-content">
                <textarea
                  v-model="params.config.description"
                  v-tooltip.notrigger.right="validate['description']"
                  :placeholder="$t('节点描述')"
                  class="bk-form-textarea"
                  :maxlength="50"
                  name="description"
                  type="text" />
              </div>
            </div>
            <fieldset>
              <legend>{{ $t('分流结果字段') }}</legend>
              <div class="lastest-data-content">
                <div v-bkloading="{ isLoading: fieldLoading }">
                  <bkdata-table :border="true"
                    :data="fieldConfig.fields"
                    :emptyText="$t('暂无数据')">
                    <bkdata-table-column :label="$t('字段名称')"
                      prop="field_name" />
                    <bkdata-table-column :label="$t('类型')"
                      prop="field_type" />
                    <bkdata-table-column :label="$t('描述')"
                      prop="field_alias" />
                  </bkdata-table>
                </div>
              </div>
            </fieldset>
          </div>
        </div>
        <!-- <div class="bk-form-item bk-form-action content-button"
                    v-show="!loading">
                    <bkdata-button :loading="buttonStatus.Disabled"
                        @click="getParams($event)"
                        theme="success">{{$t('保存')}}</bkdata-button>
                    <bkdata-button @click="closeSider"
                        theme="default">{{$t('关闭')}}</bkdata-button>
                </div> -->
      </bkdata-tab-panel>
      <bkdata-tab-panel :label="`${$t('关联任务')}(${referTaskLength})`"
        name="referTasks">
        <ReferTasks v-model="referTaskLength"
          :rtid="currentRtid"
          :nodeId="selfConfig.node_id" />
      </bkdata-tab-panel>
    </bkdata-tab>
  </div>
</template>

<script>
import { validateScope } from '@/common/js/validate.js';
import ReferTasks from './components/ReferTask';
import childMixin from './config/child.global.mixin.js';
import { mapGetters } from 'vuex';
export default {
  components: {
    ReferTasks,
  },
  mixins: [childMixin],
  props: {
    project: Object,
    nodeDisname: String,
  },
  data() {
    return {
      activeTab: 'config',
      referTaskLength: 0,
      bkBizLoading: false,
      fieldLoading: false,
      resultData: {
        placeholder: '',
        disabled: true,
        dataSet: '',
      },
      buttonStatus: {
        isDisabled: false,
        Disabled: false,
      },
      loading: true,
      selfConfig: {}, // 传回的配置信息
      parentConfig: {},
      /* 新建节点的params */
      params: {
        node_type: 'split',
        config: {
          from_result_table_ids: [],
          output_name: '',
          table_name: '',
          name: '',
          bk_biz_id: 0,
          description: '',
          config: [
            {
              bk_biz_id: '',
              logic_exp: '',
              display_bk_biz_id: '',
            },
          ],
        },
        from_links: [],
        frontend_info: {},
      },
      dataId: 0,
      validate: {
        name: {
          regs: [
            { required: true, error: window.$t('不能为空') },
            { length: 50, error: window.$t('长度不能大于50') },
          ],
          content: '',
          visible: false,
          class: 'error-red',
        },
        bk_biz_id: {
          regs: { required: true, type: 'string', error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
        },
        output_name: {
          regs: [
            { required: true, error: window.$t('不能为空') },
            { length: 50, error: window.$t('长度不能大于50') },
          ],
          content: '',
          visible: false,
          class: 'error-red',
        },
        description: {
          regs: { required: true, type: 'string', error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
        },
      },
      rtId: '',
      fieldConfig: {
        fields: [],
      },
    };
  },
  computed: {
    ...mapGetters({
      allBizList: 'global/getAllBizList',
    }),
    currentRtid() {
      return (this.selfConfig.result_table_ids && this.selfConfig.result_table_ids[0]) || '';
    },
    isNewForm() {
      return !this.selfConfig.hasOwnProperty('node_id');
    },
    bizList() {
      return (this.allBizList || []).map(item => {
        item.display_bk_biz_id = item.bk_biz_id + '_';
        return item;
      });
    },
  },
  watch: {
    parentConfig(newVal) {
      if (newVal.length) {
        this.params.config.bk_biz_id = newVal[0].bk_biz_id;
        this.params.config.table_name = newVal[0].result_table_id === undefined
          ? newVal[0].table_name
          : newVal[0].result_table_id.split('_')[1];
        this.rtId = newVal[0].result_table_id || newVal[0].result_table_ids[0];
        this.getSplitTableFields();
      }
    },
    selfConfig(newVal) {
      if (newVal && newVal.hasOwnProperty('node_id')) {
        newVal.node_config.config.forEach(item => {
          item.bk_biz_id += '';
        });
        this.params.config = JSON.parse(JSON.stringify(newVal.node_config));
      }
    },
  },
  // mounted() {
  //     this.getSplitNodeInfo()
  // },
  methods: {
    /* 配置项目回填 */
    setConfigBack(self, source, fl, option = {}) {
      this.activeTab = option.elType === 'referTask' ? 'referTasks' : 'config';
      this.params.frontend_info = self.frontend_info;
      this.parentConfig = source;
      this.selfConfig = self;
      this.params.config.result_table_id = '';
      this.params.frontend_info = self.frontend_info;
      this.params.from_links = fl;
      if (self.hasOwnProperty('node_id') || self.isCopy) {
        this.params.config.name = self.node_config.name;
      }

      // this.judgeBizIsEqual()
      if (this.bizList.length) {
        this.params.config.bk_biz_id = this.bizList[0].biz_id;
      }

      this.loading = false;
    },

    /**/
    closeSider() {
      this.buttonStatus.Disabled = false;
      this.$emit('closeSider');
    },
    addRelatedLocation() {
      this.$emit('add-location', this.latestMsg);
    },
    /* 获取参数 */
    validateFormData() {
      if (this.validateForm()) {
        this.buttonStatus.Disabled = true;
        this.params.config.from_result_table_ids = this.getFromResultTableIds(this.parentConfig);
        if (this.selfConfig.hasOwnProperty('node_id')) {
          this.params.flow_id = this.$route.params.fid;
        }
      } else {
        this.$forceUpdate();
        return false;
      }
      return true;
    },
    changeButtonStatus() {
      // this.loading = true
      this.buttonStatus.Disabled = false;
      this.addRelatedLocation();
    },
    validateForm() {
      return validateScope(this.params.config, this.validate);
    },
    handleExpOption(optionFlag, index) {
      if (optionFlag === 'add') {
        this.params.config.config.push({
          bk_biz_id: '',
          logic_exp: '',
        });
      } else if (optionFlag === 'reduce') {
        if (index !== this.params.config.config.length - 1) {
          this.params.config.config.splice(index, 1);
        }
      }
    },
    getSplitTableFields() {
      this.fieldLoading = true;
      let options = {
        params: {
          rtid: this.rtId,
        },
      };
      this.bkRequest
        .httpRequest('meta/getResultTables', options)
        .then(res => {
          if (res.result) {
            this.fieldConfig.fields = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.fieldLoading = false;
        });
    },
  },
};
</script>
<style lang="scss" scoped>
.bk-form-item {
  width: calc(100% - 20px) !important;
  .bk-label {
    position: relative;
    .required {
      color: red;
      position: absolute;
      right: 18px;
    }
  }
  .bk-form-content {
    position: relative;
    .bk-form-input {
      line-height: 32px;
    }
    .node-tips {
      position: absolute;
      top: 10px;
      right: -20px;
      cursor: pointer;
    }
  }
  .output-container {
    justify-content: space-between;
    .bk-form-exp,
    .biz_id_shunt {
      flex: 0.38;
      margin-right: 5px;
    }
    .output {
      flex: 0.9;
      margin-right: -3px;
    }
  }
}
</style>

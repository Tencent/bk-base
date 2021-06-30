

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
                  :disabled="selfConfig.hasOwnProperty('node_id') || isGlobalDisable" />
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
                class="bk-form-input data-src text-overflow">
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
            <div class="bk-form-content clearfix output-container">
              <div class="biz-id">
                <bkdata-selector
                  v-tooltip.notrigger.right="validate['bk_biz_id']"
                  :list="bizList"
                  :disabled="selfConfig.hasOwnProperty('node_id') || isGlobalDisable"
                  :selected.sync="params.config.bk_biz_id"
                  :settingKey="'biz_id'"
                  :displayKey="'name'" />
              </div>
              <div v-if="selfConfig.hasOwnProperty('node_id')"
                class="bk-form-content output">
                <bkdata-input
                  v-model="params.config.table_name"
                  :placeholder="$t('由英文字母_下划线和数字组成_且字母开头')"
                  :title="params.config.table_name"
                  disabled
                  :maxlength="50"
                  name="validation_name"
                  type="text" />
              </div>
              <div v-else
                class="bk-form-content output">
                <bkdata-input
                  v-model="params.config.table_name"
                  v-tooltip.notrigger.right="validate['table_name']"
                  :placeholder="$t('由英文字母_下划线和数字组成_且字母开头')"
                  :title="params.config.table_name"
                  :maxlength="50"
                  name="validation_name"
                  type="text"
                  :disabled="isGlobalDisable" />
              </div>
            </div>
            <div class="table-name-container">
              <div :title="params.config.bk_biz_id + '_' + params.config.table_name"
                class="bk-form-content table-name">
                {{ params.config.bk_biz_id }}_{{ params.config.table_name }}
              </div>
              <i
                v-bk-tooltips="$t('数据输出ID_作为下一个节点的数据输入')"
                class="node-tips bk-icon icon-question-circle" />
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
                type="text"
                :disabled="isGlobalDisable" />
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
                type="text"
                :disabled="isGlobalDisable" />
            </div>
          </div>
          <fieldset>
            <legend>{{ $t('合流结果字段') }}</legend>
            <div class="explain">
              <label>{{ $t('说明') }}：</label><span>{{ $t('红色文字代表该字段在多个表中存在差异') }}</span>
            </div>
            <div class="lastest-data-content">
              <InterflowTable :rtids="rtids"
                @disabled="isGlobalDisable = false" />
              <!-- :isDisable.sync="isGlobalDisable" -->
            </div>
          </fieldset>
        </div>
      </div>
      <!-- <div class="bk-form-item bk-form-action content-button"
				v-show="!loading">
				<bkdata-button :loading="buttonStatus.Disabled"
					@click="getParams($event)"
					:disabled="isGlobalDisable"
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
</template>

<script>
import { postMethodWarning } from '@/common/js/util';
import InterflowTable from './components/InterflowTable';
import { validateScope } from '@/common/js/validate.js';
import ReferTasks from './components/ReferTask';
import childMixin from './config/child.global.mixin.js';
export default {
  components: {
    InterflowTable,
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
      isGlobalDisable: true,
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
      bizList: [],
      selfConfig: {}, // 传回的配置信息
      parentConfig: [],
      /* 新建节点的params */
      params: {
        node_type: 'merge',
        config: {
          from_result_table_ids: [],
          output_name: '',
          table_name: '',
          name: '',
          bk_biz_id: 0,
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
        table_name: {
          regs: [
            { required: true },
            { regExp: /^[a-z][a-zA-Z|0-9|_]*$/, error: window.$t('NameValidate1') },
            { length: 50, error: window.$t('长度不能大于50') },
          ],
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
      fieldsData: {},
      fieldArr: [],
    };
  },
  computed: {
    currentRtid() {
      return (this.selfConfig.result_table_ids && this.selfConfig.result_table_ids[0]) || '';
    },
    isNewForm() {
      return !this.selfConfig.hasOwnProperty('node_id');
    },
    rtids() {
      return [...new Set(...this.parentConfig.map(element => element.result_table_ids))];
    },
  },
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
        this.params.config.output_name = self.node_config.output_name;
        this.params.config.table_name = self.node_config.table_name;
        this.params.config.description = self.node_config.description;
      }

      this.bizList = [];
      for (let i = 0; i < this.parentConfig.length; i++) {
        let bizId = this.parentConfig[i].bk_biz_id;
        if (!this.bizList.some((biz, _index) => biz.biz_id === bizId)) {
          this.bizList.push({
            name: this.getBizNameByBizId(bizId),
            biz_id: bizId,
          });
        }
      }

      // this.judgeBizIsEqual()
      if (this.bizList.length) {
        this.params.config.bk_biz_id = this.bizList[0].biz_id;
      }

      this.loading = false;
    },

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
  },
};
</script>
<style lang="scss" scoped>
.node-tips {
  position: absolute;
  top: 10px;
  right: -20px;
  cursor: pointer;
}
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
    .data-src {
      background-color: #efefef;
    }
  }
}
.output-container {
  display: block;
  .biz-id {
    width: 38%;
    float: left;
    margin-right: 5px;
  }
}
.table-name-container {
  position: relative;
  .table-name {
    position: relative;
    margin-top: 5px;
    line-height: 28px;
    height: 28px;
    background: #3a84ff;
    border-radius: 2px;
    color: #fff;
    margin-left: 95px;
    padding-left: 10px;
  }
}
</style>

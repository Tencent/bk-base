

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
    <bkdata-tab-panel :label="$t('基本配置')"
      name="config">
      <div class="bk-form">
        <div v-bkloading="{ isLoading: loading }"
          class="acompute-node-edit node">
          <div class="content-up">
            <div class="bk-form-item">
              <label class="bk-label">
                {{ $t('节点名称') }}
                <span class="required">*</span>
              </label>
              <div class="bk-form-content">
                <bkdata-input
                  v-model="params.config.name"
                  v-tooltip.notrigger="{
                    content: validata.name.errorMsg,
                    visible: validata.name.status,
                    class: 'error-red',
                  }"
                  :class="{ 'bk-form-input-error': validata.name.status }"
                  :placeholder="$t('节点的中文名')"
                  :maxlength="50"
                  name="validation_name"
                  type="text"
                  @keyup="checkNodeName" />
              </div>
            </div>
            <div class="bk-form-item">
              <label class="bk-label no-wrap">
                {{ $t('英文名称') }}
                <span class="required">*</span>
              </label>
              <div class="bk-form-content">
                <bkdata-input
                  v-model="params.config.processing_name"
                  v-tooltip.notrigger="{
                    content: validata.processing_name.errorMsg,
                    visible: validata.processing_name.status,
                    class: 'error-red',
                  }"
                  :class="{ 'bk-form-input-error': validata.processing_name.status }"
                  :placeholder="$t('由英文字母_下划线和数字组成_且字母开头')"
                  :maxlength="50"
                  :disabled="selfConfig.hasOwnProperty('node_id')"
                  name="validation_name"
                  type="text"
                  @keyup="checkDataFormat(params.config, 'processing_name')" />
              </div>
            </div>
          </div>
          <div class="content-down">
            <div class="bk-form-item sideslider-select">
              <label class="bk-label">{{ $t('数据输入') }}</label>
              <div v-for="(item, index) in parentConfig"
                :key="index"
                class="bk-form-content source-origin mb15">
                <template v-if="item.result_table_ids.length > 1">
                  <bkdata-selector
                    :list="dataInputList[index]"
                    :selected.sync="params.config.from_nodes[index].from_result_table_ids"
                    :settingKey="'id'"
                    :displayKey="'name'" />
                </template>
                <template v-else>
                  <span :key="index"
                    class="bk-form-input data-src"
                    disabled>
                    {{ getOutputDisplay(item) }}
                  </span>
                </template>
                <span v-bk-tooltips="$t('查看数据格式')"
                  class="icon-empty"
                  @click="openTableDetail(index)" />
              </div>
              <div v-show="showTableDetail"
                class="dialog-edit">
                <div class="dialog-edit-head">
                  {{ rtId }}
                  <i class="bk-icon icon-close"
                    @click="showTableDetail = false" />
                </div>
                <div class="dialog-edit-table">
                  <bkdata-table :data="mySqlTable"
                    :border="true">
                    <bkdata-table-column :label="$t('字段名称')"
                      prop="name" />
                    <bkdata-table-column :label="$t('类型')"
                      prop="type" />
                    <bkdata-table-column :label="$t('描述')"
                      prop="des" />
                  </bkdata-table>
                </div>
              </div>
            </div>
            <div class="bk-form-item clearfix form-extent-width">
              <label class="bk-label">
                {{ $t('数据输出') }}
                <span class="required">*</span>
              </label>
              <div class="bk-form-content clearfix data-table">
                <bkdata-button
                  icon="plus"
                  theme="primary"
                  size="small"
                  class="add-btn"
                  :disabled="isAddOutputDisable"
                  @click="editAddOutputs">
                  {{ $t('新增') }}
                </bkdata-button>
                <span
                  v-show="isAddOutputDisable"
                  v-bk-tooltips="$t('暂不支持多输出')"
                  class="icon-info ouput-info-icon" />
                <bkdata-table :data="beforeUpdateValue.outputs"
                  :emptyText="$t('请添加数据输出')">
                  <bkdata-table-column :label="$t('输出表')"
                    prop="table_name" />
                  <bkdata-table-column :label="$t('中文名')"
                    prop="output_name" />
                  <bkdata-table-column :label="$t('操作')"
                    :width="95">
                    <template slot-scope="props">
                      <bkdata-button theme="primary"
                        text
                        @click="editOutputs(props)">
                        {{ $t('编辑') }}
                      </bkdata-button>
                      <bkdata-popover :ref="`popover${props.$index}`"
                        placement="top"
                        theme="light"
                        trigger="click">
                        <bkdata-button theme="primary"
                          text
                          class="del-btn">
                          {{ $t('删除') }}
                        </bkdata-button>
                        <div slot="content">
                          <bkdata-button theme="primary"
                            text
                            style="font-size: 12px"
                            @click="delOutputs(props)">
                            {{ $t('确定') }}
                          </bkdata-button>
                          <span style="color: #dcdee5">|</span>
                          <bkdata-button
                            theme="primary"
                            text
                            class="del-btn"
                            style="font-size: 12px"
                            @click="cancleDel(props.$index)">
                            {{ $t('取消') }}
                          </bkdata-button>
                        </div>
                      </bkdata-popover>
                    </template>
                  </bkdata-table-column>
                </bkdata-table>
              </div>
              <div
                v-show="validata.outputs.status"
                class="help-block"
                style="margin-left: 125px"
                v-text="validata.outputs.errorMsg" />
              <!-- 点击编辑后弹出框 -->
              <div v-show="visiable"
                class="dialog-edit">
                <div class="dialog-edit-head">
                  {{ $t('输出编辑') }}
                  <i class="bk-icon icon-close"
                    @click="visiable = false" />
                </div>
                <div class="dialog-edit-body">
                  <div class="bk-form-item clearfix">
                    <label class="bk-label">
                      {{ $t('数据输出') }}
                      <span class="required">*</span>
                    </label>
                    <div class="bk-form-content clearfix">
                      <div class="biz_id">
                        <bkdata-selector
                          :list="bizList"
                          :disabled="selfConfig.hasOwnProperty('node_id')"
                          :selected.sync="editDetail.bk_biz_id"
                          :settingKey="'biz_id'"
                          :displayKey="'name'" />
                      </div>
                      <div class="bk-form-content output">
                        <bkdata-input
                          v-model="editDetail.table_name"
                          v-tooltip.notrigger="{
                            content: validata.table_name.errorMsg,
                            visible: validata.table_name.status,
                            class: 'error-red',
                          }"
                          :placeholder="$t('由英文字母_下划线和数字组成_且字母开头')"
                          :title="editDetail.table_name"
                          :disabled="isOuputEditDisable"
                          :maxlength="50"
                          name="validation_name"
                          type="text"
                          style="margin-left: 6px"
                          @keyup="checkDataFormat(editDetail, 'table_name')" />
                      </div>
                    </div>
                    <div
                      :title="editDetail.biz_id + '_ ' + editDetail.table_name"
                      class="bk-form-content table-name clearfix">
                      {{ editDetail.bk_biz_id }}_{{ editDetail.table_name }}
                      <i v-bk-tooltips="$t('数据输出ID_作为下一个节点的数据输入')"
                        class="bk-icon icon-question-circle">
                        <!-- <span>{{ $t('数据输出ID_作为下一个节点的数据输入') }}</span> -->
                      </i>
                    </div>
                  </div>
                  <div class="bk-form-item mt20">
                    <label class="bk-label">
                      {{ $t('输出中文名') }}
                      <span class="required">*</span>
                    </label>
                    <div class="bk-form-content">
                      <bkdata-input
                        v-model="editDetail.output_name"
                        v-tooltip.notrigger="{
                          content: validata.output_name.errorMsg,
                          visible: validata.output_name.status,
                          class: 'error-red',
                        }"
                        :class="{ 'bk-form-input-error': validata.output_name.status }"
                        :placeholder="$t('输出中文名')"
                        :maxlength="50"
                        name="validation_name"
                        type="text"
                        @keyup="checkOutputChineseName(editDetail)" />
                    </div>
                  </div>
                  <div class="bk-form-item mt20">
                    <div class="ml20">
                      <OutputField
                        :fields="editDetail.fields"
                        :typeList="typeList"
                        @addFieldHandle="addField"
                        @vailidateField="checkFieldContent"
                        @removeFieldHandle="removeField" />
                    </div>
                    <div
                      v-show="validata.field_config.status"
                      class="help-block ml20"
                      v-text="validata.field_config.errorMsg" />
                  </div>
                  <div class="bk-form-item clearfix mt20"
                    style="padding-left: 125px">
                    <div class="button-group fr">
                      <bkdata-button theme="primary"
                        class="mr10"
                        @click="outputEditHandle">
                        {{ $t('确定') }}
                      </bkdata-button>
                      <bkdata-button theme="default"
                        @click="cancelEditOutputs">
                        {{ $t('取消') }}
                      </bkdata-button>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
          <div class="content-down">
            <div class="bk-form-item">
              <label class="bk-label">
                {{ $t('编程语言') }}
                <span class="required">*</span>
              </label>
              <div class="bk-form-content">
                <bkdata-radio-group v-model="params.config.programming_language">
                  <bkdata-radio value="java"
                    :disabled="nodeType === 'spark_structured_streaming'">
                    Java
                  </bkdata-radio>
                  <bkdata-radio value="python"
                    :disabled="nodeType === 'flink_streaming'">
                    Python
                  </bkdata-radio>
                </bkdata-radio-group>
              </div>
            </div>
            <SDKUpload
              ref="fileUpload"
              :error="validata.file"
              :loadedList="params.config.package"
              :tip="uploadFileTip"
              :validate="fileTypeSet"
              :accept="uploadFileType"
              :handleResCode="uploadHandle" />
            <div class="bk-form-item">
              <label class="bk-label">
                {{ $t('程序入口') }}
                <span class="required">*</span>
              </label>
              <div class="bk-form-content">
                <bkdata-input
                  v-model="params.config.user_main_class"
                  v-tooltip.notrigger="{
                    content: validata.main_class.errorMsg,
                    visible: validata.main_class.status,
                    class: 'error-red',
                  }"
                  :class="[{ 'bk-form-input-error': validata.main_class.status }]"
                  :placeholder="$t('包含全路径的入口类')"
                  :maxlength="256"
                  name="main_class"
                  type="text"
                  @keyup="checkProgramEntry" />
              </div>
            </div>
            <div class="bk-form-item">
              <label class="bk-label">
                {{ $t('程序参数') }}
              </label>
              <div class="bk-form-content">
                <textarea
                  v-model.trim="params.config.user_args"
                  :class="['bk-form-textarea', { 'bk-form-input-error': validata.program_specific_params.status }]"
                  :placeholder="$t('参数以空格分割')"
                  rows="3"
                  :maxlength="4096"
                  name="program_specific_params"
                  type="text" />
              </div>
            </div>
          </div>
        </div>
      </div>
    </bkdata-tab-panel>
    <bkdata-tab-panel :label="$t('高级配置')"
      name="advanceConfig">
      <div class="bk-form">
        <div v-bkloading="{ isLoading: loading }"
          class="acompute-node-edit node">
          <div class="content-up">
            <div class="bk-form-item">
              <label class="bk-label">
                Checkpoint
                <span class="required">*</span>
              </label>
              <div class="bk-form-content">
                <bkdata-radio :checked="true"
                  :disabled="true">
                  {{ $t('开') }}
                </bkdata-radio>
              </div>
            </div>
            <div class="bk-form-item">
              <label class="bk-label">
                Savepoint
                <span class="required">*</span>
              </label>
              <div class="bk-form-content">
                <bkdata-radio-group v-model="params.config.advanced.use_savepoint">
                  <bkdata-radio :value="true">
                    {{ $t('开') }}
                  </bkdata-radio>
                  <bkdata-radio :value="false">
                    {{ $t('关') }}
                  </bkdata-radio>
                </bkdata-radio-group>
              </div>
            </div>
            <div class="bk-form-item">
              <div class="box-wrapper">
                <p class="box-title">
                  {{ $t('配置说明') }}
                </p>
                <p class="box-desp">
                  {{ $t('Checkpoint配置强制开启_用于任务的故障恢复') }}
                </p>
                <p class="box-desp">
                  {{ $t('Savepoint启动可选_用于任务停止后再次启动时') }}
                  <a class="box-link"
                    :href="$store.getters['docs/getNodeDocs'].sdk"
                    target="_blank">
                    {{ $t('详见SDK文档__') }}
                  </a>
                </p>
              </div>
            </div>
          </div>
        </div>
      </div>
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
import SDKUpload from './components/SDKUpload';
import OutputField from './components/OutputField';
import ReferTasks from './components/ReferTask';
import { isNullOrUndefined } from 'util';
import childMixin from './config/child.global.mixin.js';
export default {
  components: {
    SDKUpload,
    OutputField,
    ReferTasks,
  },
  mixins: [childMixin],
  props: {
    nodeType: {
      type: String,
      default: '',
    },
    nodeDisname: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      typeList: [], // 输出字段类型
      from_result_table_ids: '',
      showTableDetail: false,
      editIndex: '',
      beforeUpdateValue: {},
      visiable: false, // 输出编辑框是否打开
      activeTab: 'config',
      output: '',
      referTaskLength: 0,
      buttonStatus: {
        isDisabled: false,
      },
      uploadMap: {
        java: {
          tip: this.$t('支持扩展名jar'),
          type: '.jar',
        },
        python: {
          tip: this.$t('支持扩展名zip'),
          type: 'aplication/zip ',
        },
      },
      loading: true,
      selfConfig: {}, // 自身配置
      parentConfig: {}, // 本地保存父节点config
      validata: {
        processing_name: {
          // 数据处理名称
          status: false,
          errorMsg: '',
        },
        table_name: {
          // 输出编辑框 数据输出
          status: false,
          errorMsg: '',
        },
        name: {
          // 节点名称
          status: false,
          errorMsg: '',
        },
        output_name: {
          // 输出编辑框 中文名称
          status: false,
          errorMsg: '',
        },
        file: {
          status: false,
          errorMsg: this.validator.message.required,
        },
        main_class: {
          // 程序入口
          status: false,
          errorMsg: this.validator.message.required,
        },
        program_specific_params: {
          // 程序参数
          status: false,
          errorMsg: this.validator.message.required,
        },
        field_config: {
          // 输出 字段配置
          status: false,
          errorMsg: this.$t('字段配置不可为空'),
        },
        options: {
          status: false,
          errorMsg: this.validator.message.required,
        },
        outputs: {
          status: false,
          errorMsg: this.validator.message.required,
        },
      },
      params: {
        node_type: this.nodeType,
        config: {
          advanced: {
            use_savepoint: true, // 启用SavePoint
          },
          bk_biz_id: '',
          name: '', // 节点名称
          processing_name: '', // 数据处理名称
          programming_language: 'python', // 编程语言
          user_main_class: '', // 程序入口
          user_args: '', // 程序参数
          bk_biz_name: '',
          output_name: '', // 输出中文名
          package: [], // 待提交的包名
          outputs: [],
          from_nodes: [],
        },
        from_links: [],
        frontend_info: [],
      },
      dataInputList: [],
      bizList: [], // 父节点业务类型
      rtId: '',
      editDetail: {},
      isEditOutput: false,
      parentResultList: {},
    };
  },
  computed: {
    mySqlTable() {
      return this.activeResultTable.fields || [];
    },
    activeResultTable() {
      return this.parentResultList[this.rtId] || {};
    },
    fileTypeSet() {
      return this.params.config.programming_language === 'python' ? /.zip$/ : /.jar$/;
    },
    isOuputEditDisable() {
      if (this.selfConfig.hasOwnProperty('node_id')) {
        return this.isEditOutput;
      }
      return false;
    },
    isAddOutputDisable() {
      return this.params.config.outputs.length > 0;
    },
    uploadFileType() {
      return this.uploadMap[this.params.config.programming_language].type;
    },
    uploadFileTip() {
      return this.uploadMap[this.params.config.programming_language].tip;
    },
    currentRtid() {
      return (this.selfConfig.result_table_ids && this.selfConfig.result_table_ids[0]) || '';
    },
  },
  created() {
    this.init();
  },
  methods: {
    getOutputDisplay(item) {
      const rtid = item.result_table_ids[0];
      const result = this.parentResultList[rtid] || {};
      const alias = (result.resultTable || {}).alias;
      return `${rtid}(${alias || rtid})`;
    },
    cleanUploadList(file, fileList) {
      /*
       * 文件上传成功回掉，清洗控制文件列表
       * 只允许上传一个文件，手动修改index与fileList
       */
      const uploader = this.$refs.fileUpload.$refs.upload;
      uploader.fileIndex--;
      uploader.fileList = fileList.filter(item => item === file);
    },
    openTableDetail(index) {
      /*
       *   计算rtID， 暂时只考虑单输出
       *   多输出时，需要根据from_nodes进行计算
       *   from_nodes: [{
       *       node_id: 1,
       *       from_result_table_ids: [x]
       *   }]
       *   目前是在提交时，硬塞入的参数，需要新的数据结构与多选时的selected进行绑定
       */
      const resultTable = this.params.config.from_nodes[index].from_result_table_ids;
      this.rtId = Array.isArray(resultTable) ? resultTable[0] : resultTable;
      // this.getResultList(true)
      this.showTableDetail = true;
    },
    /*
     *   文件上传回掉
     */
    uploadHandle(res) {
      if (res && res.result === true) {
        const { id, name } = res.data;
        const createdAt = res.data['created_at'];
        const createdBy = res.data['created_by'];
        const uploadItem = {
          id: id,
          name: name,
          created_at: createdAt,
          created_by: createdBy,
        };
        // 保证只有一个上传文件
        while (this.params.config['package'].length) {
          this.params.config['package'].pop();
        }
        this.params.config['package'].push(uploadItem);
        return true;
      }
      return false;
    },
    /*
     * @改变按钮的状态
     * */
    changeButtonStatus() {
      this.buttonStatus.isDisabled = false;
    },
    /*
     *   检验计算名称
     *   验证内容由字母、数字和下划线组成，且以字母开头&内容不为空
     */
    checkDataFormat(data, key) {
      let val = data[key].trim();
      let reg = new RegExp('^[0-9a-zA-Z_]{1,}$');
      if (this.validator.validWordFormat(val)) {
        this.validata[key].status = false;
      } else {
        this.validata[key].status = true;
        if (val.length === 0) {
          this.validata[key].errorMsg = this.validator.message.required;
        } else {
          this.validata[key].errorMsg = this.validator.message.wordFormat;
        }
      }
      return !this.validata[key].status;
    },
    /*
     *  校验节点名称
     */
    checkNodeName(e) {
      let val = this.params.config.name.trim();
      if (val.length < 51 && val.length !== 0) {
        this.validata.name.status = false;
      } else {
        this.validata.name.status = true;
        if (val.length === 0) {
          this.validata.name.errorMsg = this.validator.message.required;
        } else {
          this.validata.name.errorMsg = this.validator.message.max50;
        }
      }
    },
    // 数据输出表单验证
    checkOutputs(e) {
      this.validata.outputs.status = !this.beforeUpdateValue.outputs;
    },
    // 数据输出字段配置验证
    checkFieldConfig() {
      let result = false;
      if (this.editDetail.fields.length > 0) {
        this.validata.field_config.status = false;
        result = true;
      } else {
        this.validata.field_config.status = true;
      }
      return result;
    },
    validateFieldFormat(val, reg, reg2) {
      if (reg.test(val) || reg2.test(val)) {
        return true;
      }
      return false;
    },
    // 数据输出字段配置表内容验证
    checkFieldContent() {
      let result = true;
      const deduplicatedObj = {}; // 去重检测
      this.editDetail.fields.forEach(item => {
        if (this.validateFieldFormat(item.field_name, /^[a-zA-Z_][a-zA-Z0-9_]*$/, /^\$f\d+$/)) {
          if (deduplicatedObj.hasOwnProperty(item.field_name)) {
            item.validate.name.errorMsg = this.$t('字段名称不可重复');
            item.validate.name.status = true;
          } else {
            deduplicatedObj[item.field_name] = true;
            item.validate.name.status = false;
          }
        } else {
          item.validate.name.status = true;
          if (item.field_name.length === 0) {
            item.validate.name.errorMsg = this.validator.message.required;
          } else {
            item.validate.name.errorMsg = this.validator.message.wordFormat;
          }
        }
        item.validate.alias.status = item.field_alias === '';
        if (item.validate.name.status || item.validate.alias.status) {
          result = false;
        }
      });
      return result;
    },
    /*
     *  校验 输出编辑框的所有项
     */
    validateEditOutputs() {
      let result = false;
      if (
        !this.checkOutputChineseName(this.editDetail)
        && this.checkDataFormat(this.editDetail, 'table_name')
        && this.checkFieldConfig()
      ) {
        result = this.checkFieldContent();
      }
      return result;
    },
    // 输出中文名表单验证
    checkOutputChineseName(data) {
      if (data.output_name) {
        let val = data.output_name.trim();
        if (val.length < 51 && val.length !== 0) {
          this.validata.output_name.status = false;
        } else {
          this.validata.output_name.status = true;
          if (val.length === 0) {
            this.validata.output_name.errorMsg = this.validator.message.required;
          } else {
            this.validata.output_name.errorMsg = this.validator.message.max50;
          }
          return true;
        }
      } else {
        this.validata.output_name.status = true;
        this.validata.output_name.errorMsg = this.validator.message.required;
        return true;
      }
    },
    /**
     * 验证程序入口
     */
    checkProgramEntry() {
      let val = this.params.config.user_main_class.trim();
      if (val.length < 256 && val.length !== 0) {
        this.validata.main_class.status = false;
      } else {
        this.validata.main_class.status = true;
        if (val.length === 0) {
          this.validata.main_class.errorMsg = this.validator.message.required;
        } else {
          this.validata.main_class.errorMsg = this.validator.message.max50;
        }
      }
    },
    /**
     * 验证 program_specific_params
     */
    checkProgramParams() {
      let val = this.params.config.program_specific_params.trim();
      if (val.length < 4096 && val.length !== 0) {
        this.validata.program_specific_params.status = false;
      } else {
        this.validata.program_specific_params.status = true;
        if (val.length === 0) {
          this.validata.program_specific_params.errorMsg = this.validator.message.required;
        } else {
          this.validata.program_specific_params.errorMsg = this.validator.message.max50;
        }
      }
    },
    /**
     * 验证 options
     */
    checkExtendParams() {
      let val = this.params.config.options.trim();
      if (val.length < 4096 && val.length !== 0) {
        this.validata.options.status = false;
      } else {
        this.validata.options.status = true;
        if (val.length === 0) {
          this.validata.options.errorMsg = this.validator.message.required;
        } else {
          this.validata.options.errorMsg = this.validator.message.max50;
        }
      }
    },
    // 初始化输入框校验状态度
    initValidata() {
      for (const k in this.validata) {
        this.validata[k].status = false;
      }
    },
    /*
                codemiror初始化
            */
    init() {
      this.initValidata();
    },

    /*
                数据回填
            */
    async setConfigBack(self, source, fl, option = {}) {
      this.activeTab = option.elType === 'referTask' ? 'referTasks' : 'config';
      this.selfConfig = self;
      this.parentConfig = JSON.parse(JSON.stringify(source));
      for (let i = 0; i < this.parentConfig.length; i++) {
        // 对parentConfig去重
        for (let j = 0; j < this.parentConfig.length; j++) {
          if (this.parentConfig[i].id === this.parentConfig[j].id && i !== j) {
            this.parentConfig.splice(i, 1);
          }
        }
      }
      this.parentConfig.forEach(parent => {
        // 在离线节点的父节点是离线节点时，传来的父节点对象里result_table_id为null，需要赋值
        const parentRts = parent.result_table_ids;
        if (!parent.result_table_id) {
          parent.result_table_id = parent.result_table_ids[0];
        }

        /** 数据输入相关 */
        this.params.config.from_nodes.push({
          id: parent.node_id,
          from_result_table_ids: parentRts[0],
        });

        const selectInputList = [];
        parentRts.forEach(item => {
          const listItem = {
            id: item,
            name: `${item}(${parent.output_description})`,
          };
          selectInputList.push(listItem);
        });
        this.dataInputList.push(selectInputList);
      });
      this.output = this.parentConfig[0].result_table_ids[0];
      this.params.frontend_info = self.frontend_info;
      this.params.from_links = fl;
      this.rtId = this.parentConfig[0].result_table_ids[0];
      this.loading = true;
      this.getResultList(this.parentConfig.reduce((output, config) => [...output, ...config.result_table_ids], []));
      await this.getOutputTypeList();
      this.loading = false;
      this.bizList = [];
      let defaultFixedDelay = 0;
      for (let i = 0; i < this.parentConfig.length; i++) {
        const parent = this.parentConfig[i];
        const _config = parent;
        let bizId = _config.bk_biz_id;
        if (!this.bizList.some(item => item.biz_id === bizId)) {
          this.bizList.push({
            name: this.getBizNameByBizId(bizId),
            biz_id: bizId,
          });
        }

        if (parent.node_type === this.params.node_type && _config.delay) {
          defaultFixedDelay = this.parentConfig[i].delay;
        }
      }

      if (!this.params.config.bk_biz_id) {
        this.params.config.bk_biz_id = this.bizList[0].biz_id;
      }
      // this.judgeBizIsEqual()
      const defaultLanguage = this.nodeType === 'spark_structured_streaming' ? 'python' : 'java';

      if (self.hasOwnProperty('node_id') || self.isCopy) {
        for (let key in self.node_config) {
          // 去掉 null、undefined, [], '', fals
          if (key === 'from_nodes') continue;
          if (self.node_config[key] !== null) {
            this.params.config[key] = self.node_config[key];
          }
        }
      }

      /** 克隆nodeConfig， 并为其ouputs的table_name添加前缀，用于显示 */
      this.beforeUpdateValue = JSON.parse(JSON.stringify(self.node_config || {}, this.cloneOutputs));

      // 加载完毕，上传组件回填list
      // this.params.config.package && this.$refs.fileUpload.updateList(this.params.config.package)
    },
    /*
     *   判断业务类型是否重复
     */
    // judgeBizIsEqual() {
    //     for (let i = 0; i < this.bizList.length; i++) {
    //         for (let j = 0; j < this.bizList.length; j++) {
    //             if (this.bizList[i].biz_id === this.bizList[j].biz_id && i !== j) {
    //                 this.bizList.splice(i, 1)
    //             }
    //         }
    //     }
    // },
    /*
                获取输出字段类型
            */
    getOutputTypeList() {
      return this.bkRequest.httpRequest('/dataFlow/getSdkTypeList').then(res => {
        if (res.result) {
          this.typeList = res.data;
        }
      });
    },

    loadingStatusChange(status) {
      this.loading = status;
    },

    /*
                获取结果字段表
            */
    getResultList(resultTableIds = []) {
      this.getAllResultList(resultTableIds)
        .then(res => {
          this.parentResultList = res;
        })
        ['catch'](err => {
          this.getMethodWarning(err.message, 'error');
        });
    },

    /*
                关闭侧边弹框
            */
    closeSider() {
      this.buttonStatus.isDisabled = false;
      this.loading = true;
      this.$emit('closeSider');
    },
    formatParams() {
      this.params.config.from_nodes.forEach(item => {
        if (!Array.isArray(item.from_result_table_ids)) {
          item.from_result_table_ids = [item.from_result_table_ids];
        }
      });

      /** 更新节点时需要flowid */
      if (this.selfConfig.hasOwnProperty('node_id')) {
        this.params.flow_id = this.$route.params.fid;
      }
    },
    validateForm() {
      // 统一配置项验证
      this.checkNodeName(); // 校验节点名称
      this.checkDataFormat(this.params.config, 'processing_name'); // 校验数据处理名称
      this.checkUploadfile(); // 验证上传文件
      this.checkProgramEntry(); // 验证程序入口
      this.checkOutputs(); // 验证数据输出

      // 输出编辑框打开时，校验框内内容
      if (this.visiable) {
        this.checkDataFormat(this.params.config, 'table_name');
        this.checkOutputChineseName(this.params.config);
      }
    },
    /*
                获取参数
             */
    validateFormData(e) {
      this.validateForm(); // 表单验证
      let valideResult = true;
      for (const key in this.validata) {
        if (this.validata[key].status) {
          valideResult = false;
          return false; // 当任何校验项的status为true，校验不通过，返回
        }
      }

      /*  参数格式化，提交表单  */
      this.buttonStatus.isDisabled = true;
      if (valideResult) {
        this.formatParams();
        return true;
      } else {
        return false;
      }
    },
    /**
     * 输出编辑 表格相关
     */
    addField(item) {
      this.editDetail.fields.push(item);
      this.checkFieldConfig();
    },
    removeField(item, index) {
      if (item.id) {
        this.editDetail.fields = this.editDetail.filter(field => field.id !== item.id);
      } else {
        this.editDetail.fields.splice(index, 1);
      }
    },
    checkUploadfile() {
      // 验证上传文件
      this.validata.file.status = !this.params.config['package'].length;
    },
    // 编辑弹框确定按钮
    outputEditHandle() {
      let isTrue = false;
      // isTrue = this.checkOutputChineseName(this.editDetail)
      // 新增点击确定
      if (!this.isEditOutput) {
        if (this.validateEditOutputs()) {
          this.visiable = false;
          this.params.config.outputs.push(this.editDetail);
          this.beforeUpdateValue = JSON.parse(JSON.stringify(this.params.config, this.cloneOutputs));
        }
      } else {
        // 编辑点击确定
        if (this.validateEditOutputs()) {
          this.visiable = false;
          this.params.config.outputs[this.editIndex] = this.editDetail;
          this.beforeUpdateValue = JSON.parse(JSON.stringify(this.params.config, this.cloneOutputs));
        }
      }
    },
    /** 克隆ouputs,并为其table_name添加前缀，用于显示 */
    cloneOutputs(key, value) {
      if (key === 'outputs') {
        const outputs = value.map(output => {
          let retObj = {};
          for (key in output) {
            if (key === 'table_name') {
              console.log(this);
              retObj[key] = `${this.params.config.bk_biz_id}_${output.table_name}`;
            } else {
              retObj[key] = output[key];
            }
          }
          return retObj;
        });
        return outputs;
      }
      return value;
    },
    // 数据输出编辑
    editOutputs(props) {
      const data = this.params.config.outputs[props.$index];
      this.editIndex = props.$index;
      this.initValidata();
      this.visiable = true;
      this.isEditOutput = true;
      // 编辑
      this.editDetail = JSON.parse(JSON.stringify(data));
      this.editDetail.fields.map(item => {
        item.edit = false;
        item.validate = {
          alias: {
            status: false,
            errorMsg: this.validator.message.required,
          },
          name: {
            status: false,
            errorMsg: this.validator.message.required,
          },
        };
      });
    },
    //  取消新增&编辑
    cancelEditOutputs() {
      this.editDetail.fields.forEach(item => {
        item.validate.name.status = false;
        item.validate.alias.status = false;
      });
      this.visiable = false;
    },
    // 数据输出新增
    editAddOutputs() {
      this.initValidata();
      this.isEditOutput = false;
      this.visiable = true;
      const options = {
        bk_biz_id: this.params.config.bk_biz_id,
        table_name: '',
        output_name: '',
        fields: [],
      };
      this.editDetail = options;
    },
    // 确认删除数据输出
    delOutputs(props) {
      this.beforeUpdateValue.outputs = this.beforeUpdateValue.outputs.filter((item, index) => index !== props.$index);
      this.params.config = this.beforeUpdateValue;
    },
    cancleDel(index) {
      this.$refs[`popover${index}`].instance.hide();
    },
  },
};
</script>

<style lang="scss" scoped>
$borderLightColor: #e6e6e6;

.node-editing {
  position: relative;
  .title {
    color: #212232;
    font-size: 16px;
    font-weight: normal;
    margin: 0 0px 60px;
  }
  .table-name {
    margin-top: 5px;
    line-height: 28px;
    height: 28px;
    background: #3a84ff;
    border-radius: 2px;
    color: #fff;
    margin-left: 125px;
    padding-left: 10px;
    overflow: hidden;
    .bk-icon {
      color: #3a84ff;
      font-size: 14px;
      top: 5px !important;
    }
  }
  .content-up {
    border-bottom: 1px solid #efefef;
  }
  .content-down {
    padding: 20px 0px !important;
    border-bottom: 1px solid #efefef;
    .biz_id {
      width: 30%;
      float: left;
    }
  }
  .dialog-edit {
    width: 538px;
    margin: 0 auto;
    padding: 20px 0px 30px;
    background: rgba(255, 255, 255, 1);
    box-shadow: 0px 3px 6px 0px rgba(0, 0, 0, 0.5);
    border-radius: 2px;
    position: absolute;
    left: 0;
    top: -80px;
    z-index: 20;
    .dialog-edit-head {
      font-size: 18px;
      color: #444;
      padding-left: 24px;
      line-height: 18px;
      i {
        color: #979ba5;
        font-size: 12px;
        font-weight: 900;
        position: absolute;
        right: 10px;
        top: 10px;
        cursor: pointer;
      }
    }
    .dialog-edit-table {
      margin-top: 20px;
      padding: 0 20px;
    }
    .dialog-edit-body {
      margin-top: 20px;
      .bk-form-item {
        width: 518px !important;
        margin-bottom: 0 !important;
      }
      .bk-form .mr10 {
        margin-right: 10px;
      }
    }
  }
  ::v-deep .data-table {
    position: relative;
    width: 379px;
    .bk-table-body-wrapper {
      line-height: 16px;
    }
    .bk-table-empty-block {
      max-height: 90px !important;
      .bk-table-empty-text div {
        line-height: 1;
      }
    }
    .del-btn {
      font-size: 12px;
      margin-left: 10px;
      position: relative;
      &::before {
        content: '';
        display: inline-block;
        width: 1px;
        height: 12px;
        background: #cbcdd2;
        position: absolute;
        left: -7px;
        top: 6px;
      }
    }
  }
  .ouput-info-icon {
    position: absolute;
    top: 15px;
    right: -32px;
    cursor: pointer;
  }
  .add-btn {
    position: absolute;
    top: 10px;
    right: 20px;
    z-index: 1;
  }
  .help-block {
    // width: 100%;
    margin-top: 10px;
    color: red;
  }
  .bk-form-item {
    width: 442px;
    &.form-extent-width {
      width: 450px !important;
    }
    .box-wrapper {
      width: 510px;
      padding: 16px 18px 19px 15px;
      background: #fafbfd;
      border-radius: 2px;
      border: 1px solid #f0f1f5;
      font-size: 14px;
      .box-title {
        line-height: 18px;
        color: #313238;

        margin-bottom: 8px;
      }
      .box-desp {
        line-height: 18px;
        color: #91929c;
        .box-link {
          color: #538fff;
          cursor: pointer;
        }
      }
    }
    i.icon-question-circle {
      position: absolute;
      top: 10px;
      right: -30px;
      cursor: pointer;
      span {
        z-index: 9999;
        max-width: 150px;
        width: max-content;
        display: none;
        position: absolute;
        background: #212232;
        line-height: 26px;
        padding: 0px 10px;
        color: #fff;
        border-radius: 2px;
        top: 45px;
        left: 50%;
        transform: translate(-50%, -50%);
        &:after {
          content: '';
          z-index: 200;
          top: -5px;
          left: 50%;
          transform: translate(-50%, -50%);
          position: absolute;
          border: 5px solid #212232;
          border-left-color: transparent;
          border-top-color: transparent;
          border-right-color: transparent;
        }
      }
      &:hover span {
        display: block;
        width: max-content;
      }
    }
  }
  .bk-form-input {
    color: #444;
  }
  .bk-form-input[disabled] {
    background-color: #fafafd;
  }
  .content-button {
    position: fixed;
    bottom: 0;
    right: 0;
    padding: 30px;
    width: 600px;
    text-align: center;
    border-top: 1px solid #efefef;
    background: #fff;
    .bk-button {
      width: 97px;
      &:first-of-type {
        margin-right: 35px;
      }
    }
  }
  ::v-deep .bk-label {
    width: 125px;
    padding-right: 20px;
    color: #737987;
    position: relative;
    padding: 11px 28px 11px 0;
    &.no-warp {
      white-space: nowrap;
    }
    .required {
      color: red;
      position: absolute;
      top: 14px;
      right: 18px;
    }
  }
  .bk-parent-name {
    padding: 18px 0px 18px 40px;
    text-align: left;
  }
  .bk-form-content {
    .bk-time-picker-cells-list {
      li {
        padding: 0 !important;
      }
    }
    margin-left: 125px;
    .el-date-editor.el-input {
      width: 100%;
      .el-input__prefix,
      .el-input__suffix {
        top: -3px;
      }
    }
  }
  .output {
    margin-left: 0 !important;
    float: left;
    margin-left: 5px;
    width: 100%;
  }
  .source-origin {
    .data-src {
      margin-right: 0;
      line-height: 32px;
      width: 100%;
      overflow: hidden;
      display: inline-block;
    }
    .icon-empty {
      cursor: pointer;
      position: absolute;
      right: -35px;
    }
    .table-detail {
      margin-left: 15px;
      color: #3a84ff;
      font-size: 14px;
      cursor: pointer;
    }
  }
  .sideslider-select {
    width: 443px;
  }
  .acompute-node-edit {
    .bk-form-radio {
      margin-right: 15px;
    }
    .mask {
      position: relative;
      width: 54px;
      height: 12px;
      background: #fff;
      margin-right: 8px;
      flex: 1;
    }
    .slide-box {
      width: 67px;
      height: 12px;
      background: #3a84ff;
      position: absolute;
      left: 11px;
      display: none;
    }
  }
}

.bk-form-input-error {
  border-color: red !important;
  &:focus {
    border-color: red !important;
  }
}
.input-number {
  width: 442px !important;
  .count-freq {
    display: inline-block;
    width: 30%;
    vertical-align: 2px;
  }
  .count-freq-unit {
    display: inline-block;
    width: 20%;
  }
  .formate-error {
    padding: 6px 20px 0px 123px;
    color: red;
  }
  .formate-error-selected {
    padding-left: 261px;
    margin-top: -25px;
  }
  .pad-right {
    padding-left: 264px;
  }
  .dependency-rule-width {
    width: 51%;
  }
}
.bk-select-input {
  line-height: 37px;
}
</style>

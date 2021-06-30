

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
  <div class="output-result-wrppaer">
    <bkdata-form-item :label="$t('结果数据表')"
      :required="true"
      style="margin-bottom: 10px">
      <div class="rt-wrapper">
        <bkdata-selector
          :list="bizList"
          :selected.sync="output.bk_biz_id"
          :settingKey="'biz_id'"
          :disabled="hasNodeId"
          style="width: 120px"
          :displayKey="'name'" />
        <div v-if="isIndicatorNode"
          class="indicator-name text-overflow"
          :title="indicatorName">
          {{ indicatorName }}
        </div>
        <bkdata-input
          v-model="outputValue[tableNameKey]"
          v-tooltip.notrigger="{
            content: output.validate.table_name.errorMsg,
            visible: output.validate.table_name.status,
            class: 'error-red',
          }"
          :placeholder="$t('由英文字母_下划线和数字组成_且字母开头')"
          :title="output[tableNameKey]"
          :maxlength="50"
          :disabled="hasNodeId"
          name="validation_name"
          type="text"
          style="margin-left: 6px"
          @keyup="checkDataFormat(output, tableNameKey)" />
        <div v-if="isIndicatorNode"
          class="indicator-time">
          {{ indicatorTime }}
        </div>
      </div>
    </bkdata-form-item>
    <div v-if="!isMultiple"
      class="form-display mt10 mb10">
      {{ displayName }}
    </div>

    <bkdata-form-item :label="$t('中文名称')"
      :required="true">
      <bkdata-input
        v-model="outputValue.output_name"
        v-tooltip.notrigger="{
          content: output.validate.output_name.errorMsg,
          visible: output.validate.output_name.status,
          class: 'error-red',
        }"
        :placeholder="$t('输出中文名')"
        :maxlength="50"
        name="validation_name"
        type="text"
        @keyup="checkOutputChineseName(output)" />
    </bkdata-form-item>
  </div>
</template>

<script>
import Bus from '@/common/js/bus.js';
import { translateUnit } from '@/common/js/util';

export default {
  props: {
    bizList: {
      type: Array,
      default: () => [],
    },
    output: {
      type: Object,
      default: () => ({}),
    },
    isMultiple: {
      type: Boolean,
      default: true,
    },
    hasNodeId: {
      type: Boolean,
      default: false,
    },
    selfConfig: {
      type: Object,
      default: () => ({}),
    },
    params: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      indicatorName: '',
      indicatorTime: '',
      validate: {
        table_name: {
          // 输出编辑框 数据输出
          status: false,
          errorMsg: '',
        },
        output_name: {
          // 输出编辑框 中文名称
          status: false,
          errorMsg: '',
        },
      },
    };
  },
  computed: {
    outputValue: {
      get() {
        return this.output;
      },
      set(value) {
        Object.assign(this.output, value);
      },
    },
    isIndicatorNode() {
      return ['data_model_stream_indicator', 'data_model_batch_indicator'].includes(this.selfConfig.node_type);
    },
    displayName() {
      const output = this.output;
      return this.isIndicatorNode
        ? `${output.bk_biz_id}_${this.indicatorName}_${output[this.tableNameKey]}${
            this.indicatorTime ? '_' + this.indicatorTime : ''
          }`
        : `${output.bk_biz_id}_${output[this.tableNameKey]}`;
    },
    tableNameKey() {
      return this.isIndicatorNode ? 'table_custom_name' : 'table_name';
    },
  },
  watch: {
    'params.config.window_config': {
      deep: true,
      immediate: true,
      handler(val) {
        if (val && val.window_type && this.isIndicatorNode) {
          this.indicatorTime = this.timeUnit(val);
          this.outputValue.table_name = `${this.indicatorName}_${this.output[this.tableNameKey]}${
            this.indicatorTime ? '_' + this.indicatorTime : ''
          }`;
        }
      },
    },
    displayName(val) {
      if (val && this.isIndicatorNode) {
        this.outputValue.table_name = `${this.indicatorName}_${this.output[this.tableNameKey]}${
          this.indicatorTime ? '_' + this.indicatorTime : ''
        }`;
      }
    },
  },
  created() {
    Bus.$on('setIndicatorFormula', val => {
      this.indicatorName = val;
    });
  },
  beforeDestroy() {
    Bus.$off('setIndicatorFormula');
  },
  methods: {
    timeUnit(nodeConfig) {
      // windowTypeLength取值，单位：s、min、h、d、w、m
      // 窗口类型：
      // 滚动(scroll) => 等于统计频率
      // 滑动(slide) => 窗口长度
      // 累加(accumulate) => ''
      // 固定窗口(fixed) => 窗口长度
      if (this.selfConfig.node_type === 'data_model_stream_indicator') {
        if (nodeConfig.window_type === 'scroll') {
          return translateUnit(nodeConfig.count_freq);
        } else if (nodeConfig.window_type === 'slide') {
          return translateUnit(nodeConfig.window_time * 60);
        } else {
          return '';
        }
      } else if (this.selfConfig.node_type === 'data_model_batch_indicator') {
        const unitMap = {
          hour: 'h',
          day: 'd',
          week: 'w',
          month: 'm',
        };
        if (nodeConfig.window_type === 'fixed') {
          return nodeConfig.unified_config.window_size + unitMap[nodeConfig.unified_config.window_size_period];
        } else {
          return '';
        }
      }
    },
    // 输出中文名表单验证
    checkOutputChineseName(data) {
      if (data.output_name) {
        let val = data.output_name.trim();
        if (val.length < 51 && val.length !== 0) {
          data.validate.output_name.status = false;
          return true;
        } else {
          data.validate.output_name.status = true;
          if (val.length === 0) {
            data.validate.output_name.errorMsg = this.validator.message.required;
          } else {
            data.validate.output_name.errorMsg = this.validator.message.max50;
          }
          return false;
        }
      } else {
        data.validate.output_name.status = true;
        data.validate.output_name.errorMsg = this.validator.message.required;
        return false;
      }
    },
    /*
     *   检验计算名称
     *   验证内容由字母、数字和下划线组成，且以字母开头&内容不为空
     */
    checkDataFormat(data, key) {
      let val = data[key].trim();
      let reg = new RegExp('^[0-9a-zA-Z_]{1,}$');
      if (this.validator.validWordFormat(val)) {
        data.validate.table_name.status = false;
      } else {
        data.validate.table_name.status = true;
        if (val.length === 0) {
          data.validate.table_name.errorMsg = this.validator.message.required;
        } else {
          data.validate.table_name.errorMsg = this.validator.message.wordFormat;
        }
      }
      return !data.validate.table_name.status;
    },
    async validateForm() {
      let validateResult = true;
      const aliasResult = await this.checkOutputChineseName(this.output);
      const tableNameResult = await this.checkDataFormat(this.output, this.tableNameKey);

      const result = [aliasResult, tableNameResult].every(item => item === true);
      return result;
    },
  },
};
</script>

<style lang="scss" scoped>
.output-result-wrppaer {
  ::v-deep .bk-form-item {
    width: 100% !important;
    &.output-field .bk-form-content {
      flex-direction: column;
      align-items: flex-start;
    }
  }
  .form-display {
    width: 100%;
    height: 32px;
    background: #f5f6fa;
    border-radius: 1px;
    color: #979ba5;
    display: flex;
    align-items: center;
    padding-left: 10px;
  }
  .rt-wrapper {
    width: 100%;
    display: flex;
    color: #63656e;
    font-size: 12px;
    ::v-deep .bk-form-control {
      flex: 1;
      width: auto;
    }
    .indicator-name {
      width: 100px;
      height: 32px;
      margin-left: 6px;
      background: #fafbfd;
      border: 1px solid #dcdee5;
      border-radius: 2px;
      padding: 0 10px;
    }
    .indicator-time {
      width: 52px;
      height: 32px;
      background: #fafbfd;
      border: 1px solid #dcdee5;
      border-radius: 2px;
      margin-left: 6px;
      text-align: center;
      line-height: 30px;
    }
  }
}
</style>

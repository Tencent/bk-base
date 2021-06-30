

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
  <div class="output-wrapper">
    <div class="title">
      {{ $t('数据输出') }}
    </div>
    <div class="content">
      <bkdata-form v-if="params.length"
        formType="vertical">
        <OutputBaseForm
          ref="baseForm"
          :bizList="bizList"
          :output="params[0]"
          :isMultiple="false"
          :hasNodeId="hasNodeId"
          :selfConfig="selfConfig"
          :params="fullParams" />
        <div v-if="isShowDataFix"
          class="data-fix-section mt10">
          <span v-bk-tooltips="$t('以数据字段内容做修正，从而提升数据质量')"
            class="data-fix-label">
            {{ $t('数据修正') }}
          </span>
          <bkdata-switcher v-model="isSwitchCorrect"
            @change="$emit('open-data-fix', isSwitchCorrect)" />
        </div>
        <bkdata-form-item v-if="params[0].fields.length"
          :label="$t('字段设置')"
          :required="true"
          extCls="output-field">
          <OutputField
            ref="outputFields"
            :fields.sync="params[0].fields"
            :readOnly="true"
            :openTimeOption="false"
            :rowBorder="true" />
        </bkdata-form-item>
      </bkdata-form>
    </div>
  </div>
</template>

<script>
import OutputBaseForm from './OutputBaseForm';
import mixin from '../config/child.output.mixin.js';
import OutputField from './OutputField';
import Bus from '@/common/js/bus.js';

export default {
  components: {
    OutputBaseForm,
    OutputField,
  },
  mixins: [mixin],
  props: {
    hasNodeId: {
      type: Boolean,
      default: false,
    },
    isShowDataFix: {
      type: Boolean,
      default: false,
    },
    isOpenCorrect: {
      type: Boolean,
      default: false,
    },
    selfConfig: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      isSwitchCorrect: false,
    };
  },
  watch: {
    isOpenCorrect: {
      immediate: true,
      handler(val) {
        this.isSwitchCorrect = val;
        this.$emit('open-data-fix', this.isSwitchCorrect);
      },
    },
  },
  created() {
    Bus.$on('setOutputFields', val => {
      this.params[0].fields = val.map(item => {
        return {
          field_name: item.field,
          field_type: item.type,
          field_alias: item.description || item.field,
          validate: {
            alias: {
              status: false,
              errorMsg: this.validator.message.required,
            },
            name: {
              status: false,
              errorMsg: this.validator.message.required,
            },
          },
        };
      });
    });
  },
  methods: {
    initOutput() {
      this.params.length === 0 && this.processOutput();
    },
    async validateForm() {
      const validateResult = await this.$refs.baseForm.validateForm();
      return validateResult;
    },
  },
};
</script>
<style lang="scss" scoped>
@import '~./scss/dataOutput.scss';
</style>

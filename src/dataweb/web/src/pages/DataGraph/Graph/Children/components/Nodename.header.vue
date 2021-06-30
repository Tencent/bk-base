

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
  <bkdata-form ref="nodeName"
    :labelWidth="125"
    :model="nodeParams.config"
    :rules="validate">
    <bkdata-form-item :label="$t('节点名称')"
      :required="true"
      property="name">
      <bkdata-input
        v-model="nodeParamsValue.config.name"
        :placeholder="$t('节点的中文名')"
        :maxlength="50"
        name="validation_name"
        type="text" />
    </bkdata-form-item>
    <bkdata-form-item :label="$t('英文名称')"
      :required="true"
      property="processing_name">
      <bkdata-input
        v-model="nodeParamsValue.config.processing_name"
        :placeholder="$t('由英文字母_下划线和数字组成_且字母开头')"
        :maxlength="50"
        :disabled="hasNodeId"
        name="validation_name"
        type="text" />
    </bkdata-form-item>
  </bkdata-form>
</template>

<script>
export default {
  model: {
    prop: 'nodeParams',
    event: 'change',
  },
  props: {
    hasNodeId: {
      type: Boolean,
      default: () => undefined,
    },
    nodeParams: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      validate: {
        name: [
          {
            required: true,
            message: this.$t('该值不能为空'),
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
            regex: /^[0-9a-zA-Z_]{1,}$/,
            message: this.$t('格式不正确_内容由字母_数字和下划线组成_且以字母开头'),
            trigger: 'blur',
          },
        ],
      },
    };
  },
  computed: {
    nodeParamsValue: {
      get() {
        return this.nodeParams;
      },
      set(val) {
        Object.assign(this.nodeParams, val);
      },
    },
  },
  methods: {
    validateForm() {
      return this.$refs.nodeName.validate().then(
        validator => {
          return Promise.resolve(validator);
        },
        validator => {
          return Promise.reject(validator);
        }
      );
    },
  },
};
</script>

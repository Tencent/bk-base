

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
  <prism-editor v-model="codeSync"
    class="my-editor"
    :highlight="highlighter"
    :readonly="true"
    lineNumbers />
</template>

<script>
import { PrismEditor } from 'vue-prism-editor';
import 'vue-prism-editor/dist/prismeditor.min.css';
import { highlight, languages } from 'prismjs/components/prism-core';
import 'prismjs/components/prism-clike';
import 'prismjs/components/prism-javascript';
import 'prismjs/themes/prism-tomorrow.css';

export default {
  components: {
    PrismEditor,
  },
  props: {
    code: {
      type: String,
      default: '',
    },
  },
  computed: {
    codeSync: {
      get() {
        return this.code;
      },
      set(val) {
        this.$emit('update:code', val);
      },
    },
  },
  methods: {
    highlighter(code) {
      return highlight(code, languages.js);
    },
  },
};
</script>

<style lang="scss" scoped>
.my-editor {
  background: #fafbfd;
  border: 1px solid #e6e7eb;
  border-radius: 3px;
  font-size: 14px;
  line-height: 1.4;

  /deep/.prism-editor__line-numbers {
    width: 33px;
    line-height: 20px !important;
    background: #f0f2f5 !important;
    border-right: 1px solid #e6e7eb;
    padding: 10px 0 !important;
    .prism-editor__line-number {
      text-align: center !important;
      font-family: monospace !important;
    }
  }
  /deep/.prism-editor__container {
    margin: 10px 0 !important;
    font-family: monospace !important;
    .token.operator,
    .token.entity,
    .token.url {
      color: #aa22ff;
      font-weight: bold;
      font-family: monospace !important;
    }
    .token.boolean,
    .token.number,
    .token.function {
      color: #3a84ff;
      font-family: monospace !important;
    }
    .token.selector,
    .token.important,
    .token.atrule,
    .token.keyword,
    .token.builtin {
      color: #3a84ff;
      font-weight: bold;
      font-family: monospace !important;
    }
    .token.property,
    .token.class-name,
    .token.constant,
    .token.symbol,
    .token.string,
    .token.char,
    .token.attr-value,
    .token.regex,
    .token.variable {
      color: #63656e;
      font-family: monospace !important;
    }
  }
}
</style>



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
  <NodeLayout
    ref="baseLayout"
    v-bkloading="{ isLoading: loading }"
    :nodeType="nodeType"
    :loading="loading"
    @updateParams="updateHandler"
    @updateInputFieldsList="updateInputList">
    <div class="sql-wrapper mt20">
      <Monaco
        ref="monacoEditor"
        :code="params.config.sql"
        :height="'520px'"
        :options="{ fontSize: '14px' }"
        :tools="{
          guidUrl: $store.getters['docs/getPaths'].realtimeSqlRule,
          toolList: { font_size: true, full_screen: true, event_fullscreen_default: true, editor_fold: true },
          title: 'SQL',
        }"
        @codeChange="changeCode" />
    </div>
  </NodeLayout>
</template>

<script>
import NodeLayout from './NodesLayout.vue';
import Monaco from '@/components/monaco';
import Bus from '@/common/js/bus.js';
import mixin from './config/node.mixin.js';
import dataCorrectMixin from './config/child.dataCorrect.mixin.js';

export default {
  components: {
    NodeLayout,
    Monaco,
  },
  mixins: [mixin, dataCorrectMixin],
  data() {
    return {
      params: {
        config: {
          sql: '',
        },
      },
    };
  },
  created() {
    Bus.$on('insert-monaco', text => {
      this.insertToMonaco(text);
    });
  },
  methods: {
    insertToMonaco(content) {
      this.$refs.monacoEditor && this.$refs.monacoEditor.editor.trigger('keyboard', 'type', { text: `\"${content}\"` });
    },
    changeCode(content) {
      this.params.config.sql = content;
    },
    getCode() {
      const Inputs = this.$refs.baseLayout.parentResultList;
      const rtId = Object.keys(Inputs)[0];
      const parentResult = Inputs[rtId];
      this.sql = '';
      for (let i = 0; i < parentResult.fields.length; i++) {
        this.sql += parentResult.fields[i].name + ', ';
        if (i % 3 === 0 && i !== 0) {
          this.sql += '\n    ';
        }
      }
      let index = this.sql.lastIndexOf(',');
      this.sql = this.sql.slice(0, index);
      if (!this.sql) {
        this.sql = '*';
      }
      this.params.config.sql = 'select ' + this.sql + '\nfrom ' + rtId;
    },
    async setConfigBack(self, source, fl, option = {}) {
      // 存储初始修正配置的拷贝
      this.copyCorrectConfig(self);

      await this.$refs.baseLayout.setConfigBack(self, source, fl, (option = {}));

      /**  为了兼容添加，待支持outputs后，可删除 */
      this.params.config.outputs[0].output_name = this.params.config.output_name;
      this.params.config.outputs[0].table_name = this.params.config.table_name;

      !this.params.config.sql && (await this.getCode());
      this.loading = false;
    },
    validateFormData() {
      /**  为了兼容添加，待支持outputs后，可删除 */
      this.params.config.output_name = this.params.config.outputs[0].output_name;
      this.params.config.table_name = this.params.config.outputs[0].table_name;
      this.params.config.from_result_table_ids = this.params.config.from_nodes.map(item => {
        return typeof item.from_result_table_ids === 'string'
          ? item.from_result_table_ids
          : item.from_result_table_ids[0];
      });

      // 修正开关打开时，修正配置需要调试成功才能保存节点
      if (this.params.config.data_correct.is_open_correct && !this.checkCorrectChange()) return;

      /** ----------- 分割线 ------------*/
      return this.$refs.baseLayout.validateFormData();
    },
  },
};
</script>

<style lang="scss" scoped></style>



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
    @updateParams="updateHandler"
    @updateInputFieldsList="updateInputList">
    <div class="merge-wrapper mt10">
      <bkdata-form ref="mergeForm"
        formType="inline"
        :labelWidth="80"
        :model="params.config"
        :rules="rules">
        <bkdata-form-item style="width: 100%"
          property="description"
          :label="$t('节点描述')"
          :required="true">
          <bkdata-input
            v-model="params.config.description"
            type="textarea"
            :placeholder="$t('节点描述')"
            :rows="3"
            :maxlength="50"
            :disabled="isGlobalDisable" />
        </bkdata-form-item>
      </bkdata-form>
      <fieldset class="mt20">
        <legend>{{ $t('合流结果字段') }}</legend>
        <div class="explain">
          <label>{{ $t('说明') }}：</label>
          <span>{{ $t('红色文字代表该字段在多个表中存在差异') }}</span>
        </div>
        <div class="lastest-data-content">
          <InterflowTable :rtids="rtids"
            @disabled="isGlobalDisable = false" />
        </div>
      </fieldset>
    </div>
  </NodeLayout>
</template>

<script>
import NodeLayout from './NodesLayout.vue';
import InterflowTable from './components/InterflowTable';
import mixin from './config/node.mixin.js';

export default {
  components: {
    NodeLayout,
    InterflowTable,
  },
  mixins: [mixin],
  data() {
    return {
      rtids: [],
      isGlobalDisable: true,
      params: {
        config: {},
      },
      rules: {
        description: [
          {
            required: true,
            message: this.$t('必填项不可为空'),
            trigger: 'blur',
          },
        ],
      },
    };
  },
  methods: {
    async setConfigBack(self, source, fl, option = {}) {
      await this.$refs.baseLayout.setConfigBack(self, source, fl, (option = {}));

      /**  为了兼容添加，待支持outputs后，可删除 */
      console.log(this.params.config);
      this.params.config.outputs[0].output_name = this.params.config.output_name;
      this.params.config.outputs[0].table_name = this.params.config.table_name;

      this.rtids = [...new Set(...source.map(element => element.result_table_ids))];

      this.loading = false;
    },
    async validateFormData() {
      /**  为了兼容添加，待支持outputs后，可删除 */
      this.params.config.output_name = this.params.config.outputs[0].output_name;
      this.params.config.table_name = this.params.config.outputs[0].table_name;
      this.params.config.from_result_table_ids = this.params.config.from_nodes
        .map(item => item.from_result_table_ids)
        .flat();

      /** ----------- 分割线 ------------*/
      const mergeFormValidate = this.$refs.mergeForm
        .validate()
        .then(validate => true)
        ['catch'](validate => false);
      const validates = await Promise.all([mergeFormValidate, this.$refs.baseLayout.validateFormData()]);
      return validates.every(result => result);
    },
  },
};
</script>

<style lang="scss" scoped></style>

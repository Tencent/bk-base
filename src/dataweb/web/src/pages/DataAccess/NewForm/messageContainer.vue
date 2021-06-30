

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
  <div>
    <template v-if="formData.data_scenario === 'queue'">
      <span class="bk-item-des">{{ '接入成功后是不允许修改的，请慎重填写！' }}</span>
      <Item>
        <label class="bk-label">所属消息队列：</label>
        <div class="bk-form-content">
          <bkdata-selector
            :selected.sync="type"
            :list="[{ id: 1, name: 'Kafka' }]"
            :settingKey="'name'"
            :displayKey="'name'"
            searchKey="name"
            @item-seclected="changeScenario" />
        </div>
      </Item>
    </template>
    <AccessObject :activeType="scenario"
      :bizid="formData.bk_biz_id"
      v-bind="$attrs"
      :permissionHostList="ipList" />
  </div>
</template>

<script>
import Item from './FormItems/Item';
import { AccessObject } from './FormItems/index.js';

export default {
  components: {
    Item,
    AccessObject,
  },
  props: {
    formData: {
      type: Object,
      default: () => ({
        data_scenario_id: 0,
        data_scenario: '',
        bk_biz_id: 0,
        description: '',
        permission: 'all',
      }),
    },
    ipList: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      type: '',
    };
  },
  computed: {
    scenario() {
      return this.formData.data_scenario === 'queue' ? this.type.toLocaleLowerCase() : this.formData.data_scenario;
    },
  },
  watch: {
    'formData.data_scenario': {
      immediate: true,
      handler(val) {
        if (val === 'queue') {
          this.type = 'Kafka';
          this.$emit('changeScenario', this.type.toLocaleLowerCase());
        }
      },
    },
  },
  methods: {
    changeScenario(val) {
      this.$emit('changeScenario', val.toLocaleLowerCase());
    },
  },
};
</script>

<style lang="scss" scoped></style>

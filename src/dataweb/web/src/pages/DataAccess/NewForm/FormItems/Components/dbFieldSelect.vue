

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
  <div class="bk-dbfield-filter">
    <bkdata-selector
      :disabled="disabled"
      :isLoading="loadTb"
      :list="calFieldList"
      settingKey="Field"
      displayKey="Field"
      :selected.sync="localSelected"
      :placeholder="$t('字段')"
      @item-selected="handleFieldsSelected" />
  </div>
</template>
<script>
import Bus from '@/common/js/bus';
import mixin from '@/pages/DataAccess/Config/mixins.js';
export default {
  mixins: [mixin],
  props: {
    selected: {
      type: Boolean,
      default: undefined,
    },
    disabled: {
      type: Boolean,
      default: false,
    },
    fieldList: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      loadTb: false,
      dbTableFields: this.fieldList,
      localSelected: this.selected,
    };
  },
  computed: {
    calFieldList() {
      return (this.fieldList && this.fieldList.length && this.fieldList) || this.dbTableFields;
    },
  },
  watch: {
    selected(val) {
      this.localSelected = val;
    },
  },
  mounted() {
    Bus.$on('access.obj.dbtableSearching', isStart => {
      this.loadTb = isStart;
    });

    Bus.$on('access.obj.dbtableselected', fields => {
      this.dbTableFields = fields;
    });
  },
  methods: {
    handleFieldsSelected(index, item) {
      this.$emit('update:selected', index);
    },
  },
};
</script>
<style lang="scss" scoped></style>

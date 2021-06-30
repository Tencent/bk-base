

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
  <div class="clearfix fl">
    <div class="operator-param clearfix fl">
      <div class="fl">
        <bkdata-checkbox v-model="isChecked">
          {{ $t('默认值') }}
        </bkdata-checkbox>
      </div>
      <template v-if="isChecked">
        <div class="title fl">
          {{ $t('类型') }}
        </div>
        <div class="bk-form-content param-input fl mr10">
          <bkdata-selector
            :placeholder="hoder.default_type"
            :list="defaultTypeList"
            :selected.sync="calcDefaultType"
            :settingKey="'id'"
            :displayKey="'name'" />
        </div>
        <div class="title fl">
          {{ $t('值') }}
        </div>
        <div class="bk-form-content param-input fl">
          <bkdata-input
            v-model="calcDefaultValue"
            :disabled="calcDefaultType === 'null'"
            :placeholder="hoder.default_value"
            @focus="focusActive" />
        </div>
      </template>
    </div>
  </div>
</template>
<script>
export default {
  props: {
    defaultValue: {
      type: [String, Number],
      default: '',
    },
    defaultType: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      hoder: {
        default_type: this.$t('请输入默认值类型'),
        default_value: this.$t('请输入默认值'),
      },
      defaultTypeList: [
        { name: 'text', id: 'text' },
        { name: 'string(512)', id: 'string' },
        { name: 'int', id: 'int' },
        { name: 'double', id: 'double' },
        { name: 'long', id: 'long' },
        { name: 'null', id: 'null' },
      ],
    };
  },

  computed: {
    isChecked: {
      get() {
        return this.calcDefaultType !== 'null';
      },

      set(val) {
        this.$emit('update:defaultType', val ? 'string' : 'null');
        this.$emit('update:defaultValue', '');
      },
    },
    calcDefaultValue: {
      get() {
        return this.defaultValue;
      },

      set(val) {
        this.$emit('update:defaultValue', val);
      },
    },

    calcDefaultType: {
      get() {
        return this.defaultType === '' ? 'null' : this.defaultType;
      },

      set(val) {
        this.$emit('update:defaultType', val);
        if (val === 'null') {
          this.calcDefaultValue = '';
        }
      },
    },
  },

  methods: {
    focusActive() {
      this.$emit('focus', arguments);
    },
  },
};
</script>
<style lang="scss" scoped>
.operator-param {
  .title {
    border-right: none !important;
  }
}
</style>

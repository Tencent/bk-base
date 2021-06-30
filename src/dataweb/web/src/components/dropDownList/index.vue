

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
  <ul class="panel-list">
    <li
      v-for="item in list"
      :key="item.value"
      class="panel-item"
      :class="{
        'is-selected': selected === item.value,
        'is-disabled': !!item.disabled,
      }"
      @click="handleSelect(item)">
      {{ item.text }}
    </li>
  </ul>
</template>

<script>
import Tippy from 'tippy.js';
export default {
  props: {
    list: {
      type: Array,
      default: () => [],
    },
    options: {
      type: Object,
      default: () => ({}),
    },
    selected: {
      type: [String, Number],
      default: '',
    },
    target: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      instance: null,
      tippyConfig: null,
    };
  },
  created() {
    const tippyConfig = {
      trigger: 'click',
      placement: 'bottom-start',
      onShow: () => {},
      onHide: () => {},
    };
    this.tippyConfig = Object.assign(tippyConfig, this.options);
  },
  mounted() {
    const target = document.querySelector(this.target);
    this.instance = Tippy(target, {
      appendTo: target,
      theme: 'light',
      trigger: this.tippyConfig.trigger,
      interactive: true,
      placement: this.tippyConfig.placement,
      content: this.$el,
      onShow: this.tippyConfig.onShow,
      onHide: this.tippyConfig.onHide,
    });
  },
  methods: {
    handleSelect(item) {
      if (item.disabled) {
        return;
      }
      this.$emit('update:selected', item.value);
    },
  },
};
</script>

<style lang="scss" scoped>
.panel-list {
  padding: 5px 0;
  margin: 0;
  max-height: 250px;
  list-style: none;
  overflow-y: auto;
  .panel-item {
    padding: 0 10px;
    font-size: 12px;
    line-height: 26px;
    cursor: pointer;
    &:hover {
      background-color: #eaf3ff;
      color: #3a84ff;
    }
    &.is-selected {
      background-color: #f4f6fa;
      color: #3a84ff;
    }
    &.is-disabled {
      color: #c4c6cc;
    }
  }
}
</style>

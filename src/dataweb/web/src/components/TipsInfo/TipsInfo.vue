

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
  <div :class="['tips-wrapper', type, size]"
    @click="handleClick">
    <span :class="[icon, 'tips-icon']" />
    <slot name="tipContent">
      <p class="tips-content">
        {{ text }}
        <a class="tips-link"
          :href="link.url"
          target="_blank">
          {{ link.text }}
        </a>
      </p>
    </slot>
  </div>
</template>
<script>
export default {
  props: {
    link: {
      type: Object,
      default: () => ({}),
    },
    icon: {
      type: String,
      default: 'icon-info',
    },
    text: {
      type: String,
      default: '',
    },
    type: {
      type: String,
      default: '',
    },
    size: {
      type: String,
      default: 'normal',
    },
  },
  methods: {
    handleClick() {
      this.$emit('tipsClick');
    },
  },
};
</script>

<style lang="scss" scoped>
.tips-wrapper,
.normal {
  display: flex;
  align-items: center;
  border-radius: 2px;
  background: #e1ecff;
  padding: 13px 20px;
  font-size: 14px;
  &.warning {
    background: #fff4e2;
  }
  &.success {
    background: #f2fff4;
  }
  &.error {
    background: #ffeded;
  }
  &.small {
    font-size: 12px;
    padding: 8px 12px;
    .tips-content {
      line-height: 16px;
    }
  }
  .tips-icon {
    color: #3a84ff;
    font-size: 14px;
  }
  .tips-content {
    color: #63656e;
    line-height: 18px;
    margin-left: 10px;
    .tips-link {
      color: #538fff;
      cursor: pointer;
    }
  }
}
</style>

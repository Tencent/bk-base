

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
  <span class="breadcrumb-item pointer"
    @click="changeRouter">
    <slot />
    <span class="breadcrumb-separator">
      <i v-if="separatorClass"
        :class="separatorClass" />
      <span v-else
        role="presentation">
        {{ separator }}
      </span>
    </span>
  </span>
</template>

<script>
export default {
  name: 'breadcrumb-item',
  inject: ['breadcrumb'],
  props: {
    to: {
      type: Number,
      default: undefined,
    },
  },
  data() {
    return {
      separator: '',
      separatorClass: '',
    };
  },
  mounted() {
    this.separator = this.breadcrumb.separator;
    this.separatorClass = this.breadcrumb.separatorClass;
  },
  methods: {
    changeRouter() {
      if (this.to === undefined) {
        return false;
      } else if (this.to === 0) {
        this.$router.go(0);
      } else {
        this.$router.push(this.to);
      }
    },
  },
};
</script>

<style scoped lang="scss">
.breadcrumb-item {
  position: relative;
  margin-right: 20px;
  .breadcrumb-separator {
    position: absolute;
    right: -15px;
    font-size: 10px;
    transform: scale(0.7);
  }
}
</style>

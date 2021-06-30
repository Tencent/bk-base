

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
  <header class="breadcrumb">
    <bk-button v-if="needBack"
      @click="back">
      {{ $t('返回') }}
    </bk-button>
    <span class="title-list">
      <slot />
    </span>
  </header>
</template>

<script>
export default {
  name: 'breadcrumb',
  provide() {
    return {
      breadcrumb: this,
    };
  },
  props: {
    separator: {
      type: String,
      default: '>',
    },
    separatorClass: {
      type: String,
      default: '',
    },
    needBack: {
      type: Boolean,
      default: true,
    },
  },
  mounted() {
    const itemSeparators = this.$el.querySelectorAll('.breadcrumb-separator');
    if (itemSeparators.length) {
      itemSeparators[itemSeparators.length - 1].style.display = 'none';
    }
  },
  methods: {
    back() {
      this.$router.go(-1);
      // 返回上一级
      // const items = this.$el.querySelectorAll('.breadcrumb-item')
      // if (items.length > 1) {
      //     items[items.length - 2].click()
      // } else {
      //     this.$router.go(-1)
      // }
    },
  },
};
</script>

<style scoped lang="scss">
.breadcrumb {
  color: #737987;
  height: 51px;
  line-height: 51px;
  /*border-bottom: 1px solid #dfe2ef;*/
  padding: 0 75px;
  background: #efefef;
  button {
    background-color: #3a84ff;
    width: 60px;
    height: 26px;
    line-height: 25px;
    color: #fff;
    font-size: 11px;
    padding: 0;
    border: none;
    display: inline-block;
    vertical-align: 2px;
  }
  .title-list {
    margin-left: 20px;
    .breadcrumb-item:last-child {
      color: #212232;
    }
  }
}
</style>

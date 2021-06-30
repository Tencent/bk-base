

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
  <div class="operation-group">
    <slot />
  </div>
</template>

<script>
export default {
  name: 'operation-group',
  props: {
    separator: {
      type: String,
      default: '|',
    },
    separatorClass: {
      type: String,
      default: '',
    },
  },
  mounted() {
    this.renderSplit();
  },
  methods: {
    renderSplit() {
      let length = this.$el.children.length;
      let children = this.$el.children;
      if (length > 1) {
        this.removeAllEmptyNode();
        for (let i = 0; i < this.$el.children.length - 1; i += 2) {
          let newI;
          if (this.separatorClass) {
            newI = document.createElement('i');
            newI.classList.add(this.separatorClass);
          } else {
            newI = document.createElement('span');
            newI.classList.add('operation-separator');
            newI.innerText = this.separator;
          }
          this.$el.insertBefore(newI, children[i + 1]);
        }
      }
    },
    removeAllEmptyNode() {
      for (let item of this.$el.childNodes) {
        if (item.nodeType === 3 && /^\s+$/.test(item.nodeValue)) {
          item.remove();
        }
      }
    },
  },
};
</script>

<style>
.operation-separator {
  color: grey;
  margin-left: 3px;
  margin-right: 3px;
}
</style>



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
  <div v-scroll-bottom="{ callback: handleScrollCallback }">
    <slot :data="calcList" />
  </div>
</template>
<script>
import scrollBottom from '@/common/directive/scroll-bottom.js';
export default {
  name: 'bkdata-scroll-list',
  directives: {
    scrollBottom,
  },
  props: {
    pageSize: {
      type: Number,
      default: 100,
    },
    list: {
      type: Array,
      default: () => [],
    },
    hasChildren: {
      type: [Boolean, String],
      default: false,
    },
  },
  data() {
    return {
      currentPage: 1,
      currentPageUpdating: false,
    };
  },
  computed: {
    calcHasChild() {
      return this.hasChildren === 'auto'
        ? this.list.some(item => item.children && Array.isArray(item.children))
        : this.hasChildren;
    },
    calcList() {
      if (!this.calcHasChild) {
        return this.list.slice(0, this.currentPage * this.pageSize);
      } else {
        if (this.list.length) {
          const cloneList = JSON.parse(JSON.stringify(this.list));
          let nodeSign = { groupIndex: 0, nodeIndex: 0, counted: 0 };
          const lastIndex = this.currentPage * this.pageSize;
          cloneList.some((item, index) => {
            nodeSign.groupIndex = index;
            const childeLen = Array.isArray(item.children) ? item.children.length : 0;
            if (nodeSign.counted + childeLen >= lastIndex) {
              nodeSign.nodeIndex = lastIndex - nodeSign.counted;
              return true;
            } else {
              nodeSign.counted += childeLen;
              return false;
            }
          });

          let target = cloneList.slice(0, nodeSign.groupIndex + 1);
          if (nodeSign.nodeIndex) {
            target[nodeSign.groupIndex].children = target[nodeSign.groupIndex].children.slice(0, nodeSign.nodeIndex);
          }
          return target;
        } else {
          return this.list;
        }
      }
    },
  },
  watch: {
    list(val) {
      this.currentPage = 1;
    },
  },
  methods: {
    handleScrollCallback() {
      const pageCount = this.getTotalCount();
      if (pageCount > this.currentPage && !this.currentPageUpdating) {
        this.currentPageUpdating = true;
        this.currentPage++;
        this.$nextTick(() => {
          setTimeout(() => {
            this.currentPageUpdating = false;
          }, 300);
        });
      }
    },

    getTotalCount() {
      if (!this.hasChildren) {
        return Math.ceil(this.list.length / this.pageSize);
      } else {
        const total = this.list.reduce((pre, node) => {
          const childLen = Array.isArray(node.children) ? node.children.length : 0;
          pre += childLen + 1;
          return pre;
        }, 0);
        return Math.ceil(total / this.pageSize);
      }
    },
  },
};
</script>

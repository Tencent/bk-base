

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

<!--
 * 表格双击显示全部信息
 -->
<template>
  <div class="table-cell"
    @dblclick.stop="dblclickHandle"
    @click="clickHandle">
    <template v-if="html">
      <!--eslint-disable vue/no-v-html-->
      <span v-html="content" />
    </template>
    <template v-else>
      {{ content }}
    </template>
  </div>
</template>
<script>
import { once } from '@/common/js/events.js';
export default {
  props: {
    content: [String, Number],
    html: {
      type: Boolean,
      default: true,
    },
  },
  data() {
    return {
      searchContainer: null,
      isOpen: false,
    };
  },
  computed: {
    bindAttribute() {
      return (this.html && { 'v-html': this.content }) || { 'v-text': this.content };
    },
    isSearchContainerEmpty() {
      return this.searchContainer !== null && typeof this.searchContainer === 'object';
    },
  },
  methods: {
    clickHandle(event) {
      if (this.isOpen === true) {
        event.stopPropagation();
      }
    },
    dblclickHandle() {
      if (this.isOpen === true) return;
      this.isOpen = true;
      let targetNode = this.$el;
      const targetClassList = targetNode.classList || [];
      let divEl = document.createElement('div');
      divEl.className = 'text-popup';
      if (this.html) {
        divEl.innerHTML = this.content;
      } else {
        divEl.innerText = this.content;
      }

      divEl.contentEditable = 'true';
      let divElStyle = {
        left: targetNode.offsetLeft + 'px',
        top: targetNode.offsetTop + 'px',
        'min-height': targetNode.offsetHeight + 'px',
      };
      targetNode.appendChild(divEl);
      this.$nextTick(() => {
        let selection = window.getSelection();
        let range = document.createRange();
        range.selectNodeContents(divEl);
        selection.removeAllRanges();
        selection.addRange(range);
        const targetRect = targetNode.getBoundingClientRect();
        const popRect = divEl.getBoundingClientRect();
        const popStyle = {};

        if (isSearchContainerEmpty) {
          let containerRect = {};
          let parent = this.$el;
          while (parent !== document.body) {
            if (parent.classList.contains('bk-table-body-wrapper')) {
              containerRect = parent.getBoundingClientRect();
              break;
            }
            parent = parent.parentNode;
          }
          if (containerRect !== undefined && this.containerRect !== null) {
            this.$set(this, 'searchContainer', {
              left: containerRect.left,
              top: containerRect.top,
              right: containerRect.right,
            });
          }
        }

        if (popRect.width > this.searchContainer.right - this.searchContainer.left) {
          let popWidth = 0;
          let left = targetNode.offsetLeft;
          let top = 0;
          const targetWidth = targetRect.right - targetRect.left;
          const targetLeftRect = targetRect.left - this.searchContainer.left - 30;
          const targetRight = this.searchContainer.right - targetRect.right + targetWidth;
          if (popRect.width <= targetLeftRect) {
            popWidth = targetLeftRect;
          } else {
            if (targetLeftRect > targetRight) {
              left = targetLeftRect - targetNode.offsetWidth;
              popWidth = targetLeftRect;
            } else {
              popWidth = targetRight;
            }
          }

          Object.assign(popStyle, {
            left: -left + 'px',
            top: targetNode.offsetTop + 'px',
            'min-height': targetNode.offsetHeight + 'px',
            width: popWidth + 'px',
            'white-space': 'normal',
          });
        } else {
          if (popRect.right > this.searchContainer.right) {
            popStyle.left = this.searchContainer.right - popRect.right + 'px';
          }
        }

        Object.assign(divElStyle, popStyle);
        let cssText = '';
        Object.keys(divElStyle).map(key => {
          cssText += key + ':' + divElStyle[key] + ';';
        });
        divEl.style.cssText = cssText;
      });
      once(document, 'click', () => {
        // once只执行一次事件, 执行完自动注销
        this.isOpen = false;
        this.$el.removeChild(divEl);
      });
    },
  },
};
</script>

<style lang="scss" scoped>
.table-cell {
  overflow: inherit;
  text-overflow: ellipsis;
  white-space: normal;
  .text-popup {
    outline: none;
    padding: 4px 5px;
    position: absolute;
    top: 0px;
    word-break: break-all;
    white-space: nowrap;
    z-index: 10;
    background: #fff;
    border-radius: 4px;
    border: 1px solid #bfc0c0;
    box-shadow: 0px 2px 2px 1px rgba(33, 34, 50, 0.5);
  }
}
</style>

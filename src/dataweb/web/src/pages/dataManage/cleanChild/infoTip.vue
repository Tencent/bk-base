

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
  <div class="info-tip"
    :class="{ bottom: direction === 'bottom' }"
    :style="{ top: tops + 'px' }">
    <i class="triangular"
      :style="{ left: left + 'px' }" />
    <span class="close"
      @click="close">
      <i :title="$t('关闭')"
        class="bk-icon icon-close-circle-shape" />
    </span>
    <slot />
  </div>
</template>
<script>
import { getActualTop, getActualLeft } from '@/common/js/util.js';
export default {
  props: {
    direction: {
      type: String,
      default: '',
    },
    dom: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      top: 0,
      left: 0,
    };
  },
  computed: {
    tops() {
      if (this.direction === 'top') {
        let selfHeight = 0;
        this.$nextTick(() => {
          let self = document.querySelectorAll('.clean-rules .info-tip');
          selfHeight = self[0].offsetHeight;
          this.top = getActualTop(this.dom[0]) - 275 - selfHeight - 20; // 275:头部60,面包屑51 边距20+20 124原始数据
        });
      } else if (this.direction === 'bottom') {
        let h = 0;
        this.$nextTick(function () {
          let index = this.dom.length - 1;
          h = this.dom[index].offsetHeight;
          this.top = getActualTop(this.dom[index]) - 275 + h + 20;
        });
      }
      let w = this.dom[0].offsetWidth / 2;
      // eslint-disable-next-line vue/no-side-effects-in-computed-properties
      this.left = getActualLeft(this.dom[0]) - 125 + w; // 小箭头定位
      return this.top;
    },
  },
  methods: {
    close() {
      this.$emit('closeTip');
    },
  },
};
</script>
<style lang="scss">
.info-tip {
  position: absolute;
  left: 50px;
  right: 125px;
  top: 220px;
  background: #fafafa;
  border: 1px solid #d9dfe5;
  box-shadow: 0 0 5px 5px rgba(33, 34, 50, 0.15);
  padding: 0px 20px 15px;
  z-index: 2;
  .triangular {
    position: absolute;
    bottom: -15px;
    left: 70px;
    &:after {
      content: '';
      position: absolute;
      bottom: 0px;
      left: 0px;
      width: 0;
      height: 0;
      border: 8px solid #f00;
      border-color: #fff transparent transparent transparent;
    }
    &:before {
      content: '';
      position: absolute;
      bottom: -1px;
      left: 0px;
      width: 0;
      height: 0;
      border: 8px solid #d9dfe5;
      border-color: #d9dfe5 transparent transparent transparent;
    }
  }
  &.bottom {
    .triangular {
      top: -15px;
      bottom: inherit;
      &:after {
        border-color: transparent transparent #fff transparent;
        top: 0px;
      }
      &:before {
        border-color: transparent transparent #d9dfe5 transparent;
        top: -1px;
      }
    }
  }
  .close {
    position: absolute;
    top: 7px;
    right: 10px;
    cursor: pointer;
  }
}
</style>

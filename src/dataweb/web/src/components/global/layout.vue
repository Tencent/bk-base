

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
  <div class="global-layout-container"
    :style="{ height: calcHeight }">
    <div
      v-if="showHead"
      :class="['layout-header', (!headerMargin && 'without-margin') || '', collspan && 'collspan']"
      :style="headerStyle">
      <Breadcrumb :crumbName="crumbName"
        @crumbNameClick="handleHeaderClick">
        <slot name="header" />
        <slot slot="button"
          name="defineTitle" />
        <template v-if="collspan">
          <i
            style="color: #3a84ff"
            :class="['__bk-collspan bk-icon', (isCollspaned && 'icon-angle-up') || 'icon-angle-down']"
            @click="handleHeaderClick" />
        </template>
      </Breadcrumb>
    </div>
    <div v-if="showSubNav"
      class="layout-header">
      <slot name="subNav" />
    </div>
    <div v-scroll-bottom="handleScrollBottom"
      class="layout-body">
      <div v-if="!collspan || isCollspaned"
        :class="['layout-content', (withMargin && 'with-margin') || '']">
        <slot />
      </div>
    </div>
  </div>
</template>
<script>
import Breadcrumb from './breadcrumb';
import scrollBottom from '@/common/directive/scroll-bottom.js';
export default {
  directives: {
    scrollBottom,
  },
  components: { Breadcrumb },
  props: {
    /** 监听Click事件 */
    watchClick: {
      type: Boolean,
      default: false,
    },
    /** 整体高度 */
    height: {
      type: [String, Number],
      default: '',
    },
    /** 导航跳转配置 */
    crumbName: {
      type: [String, Array],
      default: '',
    },
    /** 整体是否自动填充margin */
    withMargin: {
      type: Boolean,
      default: true,
    },
    /** 是否显示默认顶部导航条 */
    showHead: {
      type: Boolean,
      default: true,
    },
    /** 是否允许添加自定义顶部子组件导航， showHead = true起作用 */
    showSubNav: {
      type: Boolean,
      default: false,
    },
    /** 顶部是否自动填充margin,showHead = true起作用 */
    headerMargin: {
      type: Boolean,
      default: true,
    },
    /** 顶部背景颜色,showHead = true起作用 */
    headerBackground: {
      type: String,
      default: '#efefef',
    },
    /** 是否启用展开、收起功能 */
    collspan: {
      type: Boolean,
      default: false,
    },
    /** 是否默认展开，需要collspan设置为True才起作用 */
    isOpen: {
      type: Boolean,
      default: false,
    },
    handleScrollBottom: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      isCollspaned: this.isOpen,
    };
  },
  computed: {
    headerStyle() {
      return {
        background: this.headerBackground,
      };
    },
    calcHeight() {
      return (this.height && ((/^\d+$/.test(this.height) && `${this.height}px`) || this.height))
            || 'calc(100% - 1px)';
    },
  },
  methods: {
    handleHeaderClick() {
      if (this.collspan) {
        this.isCollspaned = !this.isCollspaned;
      }
    },
    handleLayoutClick() {
      this.watchClick && this.$emit('click');
    },
  },
};
</script>
<style lang="scss" scoped>
$layoutMargin: 0 55px !default;
$layoutBackground: #fff;
$layoutBodyPadding: 15px;
$globalHeadHeight: 60px;

html,
body {
  overflow-y: hidden;
}

.global-layout-container {
  width: 100%;
  display: flex;
  flex-direction: column;
  background: $layoutBackground;
  //   height: calc(100% - 60px);
  //   padding-bottom: 15px;
  margin-bottom: 20px;
  .layout-header {
    $layoutMargin: 0 70px;
    padding: $layoutMargin;
    font-weight: 500;
    height: 40px;
    line-height: 40px;
    background: #efefef;
    color: #1a1b2d;
    font-size: 14px;
    z-index: 1;

    &.without-margin {
      padding: 0 20px !important;
    }

    ::v-deep .__bk-collspan {
      font-size: 26px;
      vertical-align: middle;
      &:hover {
        color: #5354a5;
        cursor: pointer;
      }
    }
  }

  .layout-body {
    height: 100%;
    overflow: scroll;

    &::-webkit-scrollbar {
      width: 8px;
      height: 8px;
      background-color: transparent;
    }

    &::-webkit-scrollbar-thumb {
      border-radius: 8px;
      background-color: #a0a0a0;
    }

    .layout-content {
      &.with-margin {
        margin: $layoutMargin;
      }
      height: auto;
      background: #fff;
      padding: $layoutBodyPadding;
    }
  }
}
</style>

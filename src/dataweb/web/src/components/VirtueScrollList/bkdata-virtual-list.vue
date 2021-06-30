

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
  <div :class="['bkdata-virtue-list', scrollXName, scrollYName, className]"
    :style="wrapperStyle">
    <div
      v-virtue-render="{ callback: handleScrollCallback, listLength, lineHeight, startIndex, endIndex }"
      class="bkdata-virtue-content"
      :style="innerContentStyle">
      <slot :data="calcList" />
      <template v-if="!calcList.length">
        <NoneData :height="wrapperStyle.height" />
      </template>
    </div>
    <div class="bkdata-virtue-section"
      :style="innerStyle" />
  </div>
</template>
<script>
import virtueRender from './virtual-render';
import NoneData from '@/components/global/NoData.vue';
export default {
  name: 'bkdata-virtue-list',
  components: { NoneData },
  directives: {
    virtueRender,
  },
  props: {
    className: {
      type: String,
      default: '',
    },
    scrollXName: {
      type: String,
      default: 'bk-scroll-x',
    },
    scrollYName: {
      type: String,
      default: 'bk-scroll-y',
    },
    pageSize: {
      type: Number,
      default: 100,
    },
    list: {
      type: Array,
      default: () => [],
    },
    lineHeight: {
      type: Number,
      default: 30,
    },
    minHeight: {
      type: Number,
      default: 30,
    },
    height: {
      type: [Number, String],
      default: '100%',
    },
    /** 分组展示，一行数据可能有多条数据 */
    groupItemCount: {
      type: Number,
      default: 1,
    },
    /** 预加载行数，避免空白渲染 */
    preloadItemCount: {
      type: Number,
      default: 1,
    },
  },
  data() {
    return {
      currentPageUpdating: false,
      startIndex: 0,
      endIndex: 0,
      scrollTop: 1,
      translateY: 0,
    };
  },
  computed: {
    listLength() {
      return Math.ceil((this.localList || []).length / this.groupItemCount);
    },
    calcList() {
      return this.localList.slice(
        this.startIndex * this.groupItemCount,
        (this.endIndex + this.preloadItemCount) * this.groupItemCount
      );
    },
    wrapperStyle() {
      return {
        height: typeof this.height === 'number' ? `${this.height}px` : this.height,
      };
    },
    innerHeight() {
      return this.lineHeight * this.listLength;
    },

    innerContentStyle() {
      return {
        top: `${this.scrollTop}px`,
        transform: `translateY(-${this.translateY}px)`,
      };
    },

    innerStyle() {
      return {
        height: `${this.innerHeight < this.minHeight ? this.minHeight : this.innerHeight}px`,
      };
    },

    localList() {
      return (this.list || []).map((item, index) => Object.assign(item, { $index: index }));
    },
  },
  watch: {
    list(val) {
      /** 数据改变时激活当前表单，使其渲染DOM */
      this.$nextTick(() => this.scrollToIndex(0));
    },
  },
  methods: {
    handleScrollCallback(event, startIndex, endIndex, scrollTop) {
      this.startIndex = startIndex;
      this.endIndex = endIndex;
      this.scrollTop = scrollTop;
      this.$emit('content-scroll', event);
      this.translateY = scrollTop % this.lineHeight;
    },
    scrollToIndex(index) {
      if (index >= 0) {
        const scrollTop = this.lineHeight * index;
        this.$el
          && this.$el.scrollTo({
            top: scrollTop,
            behavior: 'smooth',
          });
      }
    },
  },
};
</script>
<style lang="scss" scoped>
.bkdata-virtue-list {
  position: relative;
  .bkdata-virtue-content {
    position: absolute;
    left: 0;
    top: 0;
    width: 100%;
    height: 100%;
  }
  .bkdata-virtue-section {
    background: transparent;
    width: 1px;
  }
}
</style>

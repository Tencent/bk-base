

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
  <div class="bk-carousel"
    :style="calcStyle">
    <span
      v-if="offset && itemLens > pageSize"
      class="bk-carousel-pointer bk-carousel-left"
      :style="leftTriggerStyle"
      @click="handleLeftClick">
      <i class="bk-icon icon-angle-left" />
    </span>
    <span
      v-if="!offset && itemLens > pageSize"
      :style="rightTriggerStyle"
      class="bk-carousel-pointer bk-carousel-right"
      @click="handleRightClick">
      <i class="bk-icon icon-angle-right" />
    </span>
    <div class="bk-carousel-items"
      :style="itemPositoinStyle">
      <slot />
    </div>
  </div>
</template>
<script>
export default {
  props: {
    /** 每个滑块显示的Item数量 */
    pageSize: {
      type: Number,
      default: 1,
    },

    height: {
      type: Number,
      default: 90,
    },

    activeIndex: {
      type: Number,
      default: 1,
    },

    itemOffsetRight: {
      type: Number,
      default: 15,
    },
    /** inline , outline */
    triggerPosition: {
      type: String,
      default: 'inline',
    },
  },
  data() {
    return {
      itemWidth: 0,
      calActiveIndex: this.activeIndex,
      offset: 0,
      itemLens: 0,
    };
  },
  computed: {
    calcStyle() {
      return {
        padding: (this.isOutlineTrrigger && '0px 20px') || '0',
        height: `${this.height + 30}px`,
      };
    },
    itemPositoinStyle() {
      // eslint-disable-next-line vue/no-side-effects-in-computed-properties
      this.offset = (this.calActiveIndex - 1) * this.pageSize;
      const appendOffset = (this.isOutlineTrrigger && 20) || 0;
      // eslint-disable-next-line vue/no-side-effects-in-computed-properties
      this.offset = this.offset > 0 ? this.offset : 0;
      let transX = this.itemWidth * this.offset;
      transX = transX > 0 ? -(transX + appendOffset) : 0;
      return {
        transform: `translate(${transX}px, -50%)`,
        'padding-left': `${appendOffset}px`,
      };
    },
    isOutlineTrrigger() {
      return this.triggerPosition === 'outline';
    },
    calcOffsetTrgger() {
      return (this.isOutlineTrrigger && -10) || 10;
    },
    leftTriggerStyle() {
      return {
        left: `${this.calcOffsetTrgger}px`,
      };
    },
    rightTriggerStyle() {
      return {
        right: `${this.calcOffsetTrgger}px`,
      };
    },
  },
  watch: {
    pageSize: {
      handler(val) {
        val && this.layoutItems(val);
      },
      immediate: true,
    },
    activeIndex(val) {
      val && this.$set(this, 'calActiveIndex', val);
    },
  },
  mounted() {
    this.layoutItems(this.pageSize);
  },
  methods: {
    handleLeftClick() {
      this.calActiveIndex = this.calActiveIndex - 1;
      this.$emit('change', this.calActiveIndex, 'left');
    },
    handleRightClick() {
      this.calActiveIndex = this.calActiveIndex + 1;
      this.$emit('change', this.calActiveIndex, 'right');
    },
    layoutItems(pageSize) {
      this.$parent.$nextTick(() => {
        this.$nextTick(() => {
          let items = document.querySelectorAll('.bk-carousel > .bk-carousel-items');
          let itemList = document.querySelectorAll('.bk-carousel-items > .bk-carousel-item');
          let itemLen = itemList.length;
          if (items && items.length) {
            if (itemLen === 1) {
              let emptyDiv = document.createElement('div');
              emptyDiv.classList = 'bk-carousel-item empty-item';
              emptyDiv.setAttribute('style', 'background: #F8F8F8;border: dashed 1px #D1CECE;');
              items[0].appendChild(emptyDiv);
              itemList = document.querySelectorAll('.bk-carousel-items > .bk-carousel-item');
              itemLen = itemList.length;
            }

            this.itemLens = itemLen;
            const realSize = pageSize; // itemLen > pageSize ? pageSize : itemLen
            const $elWidth = this.$el.offsetWidth - ((this.isOutlineTrrigger && 40) || 0);
            this.itemWidth = Math.floor(($elWidth / realSize) * 10) / 10;
            const calcWidth = itemLen * this.itemWidth;
            items[0].style.width = `${calcWidth}px`;
            Array.prototype.forEach.call(itemList, (item, index) => {
              if (index < itemLen) {
                item.style.width = `${this.itemWidth - this.itemOffsetRight / 2}px`;
                // item.style['margin-left'] = '5px'
                if (index % 2) {
                  item.style['margin-right'] = '0px'; // `${this.itemOffsetRight + 5}px`
                } else {
                  item.style['margin-right'] = `${this.itemOffsetRight}px`;
                }
              } else {
                item.style.width = `${this.itemWidth}px`;
              }
            });
          }

          this.$emit('layoutItems');
        });
      });
    },
  },
};
</script>
<style lang="scss">
.bk-carousel {
  position: relative;
  overflow-x: hidden;
  display: flex;

  .bk-carousel-pointer {
    position: absolute;
    top: 50%;
    transform: translateY(-50%);
    z-index: 100;
    font-size: 36px;
    cursor: pointer;
    // color: transparent;

    // &:hover{
    //     color: #5354a5;
    // }

    &.bk-carousel-left {
      left: 15px;
    }

    &.bk-carousel-right {
      right: 15px;
    }
  }

  .bk-carousel-items {
    display: flex;
    position: absolute;
    top: 50%;
    left: 0;
    transition: all 0.5s ease;
  }
}
</style>

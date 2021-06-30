

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
  <div class="window-example scroll-animate"
    :class="[`scroll-type-${type}`]">
    <div class="example-box box1" />
    <div class="example-box box2" />
    <div class="example-box box3" />
    <div class="example-box box4" />
    <div class="example-box box5" />
    <template v-if="type === 'slide'">
      <div class="example-box slider-box" />
    </template>
  </div>
</template>
<script>
export default {
  props: {
    type: {
      type: String,
      default: 'scroll',
      validator(val) {
        /**
         * 校验支持的类型
         * scroll：滚动窗口
         * accumulate：累加窗口
         * slide：滑动窗口
         */
        if (/^scroll|accumulate|slide$/gi.test(val)) {
          return true;
        } else {
          console.error(`${val} is not avaliable type of 'scroll|accumulate|slid'`);
          return false;
        }
      },
    },
  },
};
</script>
<style lang="scss" scoped>
.window-example {
  width: 300px;
  height: 24px;
  display: flex;
  align-items: center;
  margin-top: 6px;
  background: rgb(238, 240, 245);
  border-radius: 3px;
  padding: 6px 11px;
  position: relative;

  .example-box {
    width: 54px;
    height: 12px;
    margin-right: 8px;
    background: rgb(255, 255, 255);
    flex: 1 1 0%;

    &.slider-box {
      background: #3a84ff;
      position: absolute;
    }
  }
}
.scroll-animate {
  &.scroll-type-scroll {
    .example-box {
      animation: scroll-animate 5s infinite normal;
      &.box1 {
        animation-delay: 0.1s;
      }
      &.box2 {
        animation-delay: 1.1s;
      }
      &.box3 {
        animation-delay: 2.1s;
      }
      &.box4 {
        animation-delay: 3.1s;
      }
      &.box5 {
        animation-delay: 4.1s;
        margin-right: 0;
      }
    }
  }

  &.scroll-type-accumulate {
    .example-box {
      &.box1 {
        animation: scroll-animate-step1 5s infinite normal;
        animation-delay: 0.1s;
      }
      &.box2 {
        animation: scroll-animate-step2 5s infinite normal;
        animation-delay: 0.1s;
      }
      &.box3 {
        animation: scroll-animate-step3 5s infinite normal;
        animation-delay: 0.1s;
      }
      &.box4 {
        animation: scroll-animate-step4 5s infinite normal;
        animation-delay: 0.1s;
      }
      &.box5 {
        animation: scroll-animate-step5 5s infinite normal;
        animation-delay: 0.1s;
        margin-right: 0;
      }
    }
  }

  &.scroll-type-slide {
    .slider-box {
      animation: slider-animate 6s infinite;
    }
  }
}
@keyframes scroll-animate {
  0% {
    background: #3a84ff;
  }
  20% {
    background: #3a84ff;
  }
  21% {
    background: #fff;
  }
  100% {
    background: #fff;
  }
}

@keyframes scroll-animate-step1 {
  0% {
    background: #fff;
  }

  1% {
    background: #3a84ff;
  }

  99% {
    background: #3a84ff;
  }

  100% {
    background: #fff;
  }
}

@keyframes scroll-animate-step2 {
  0% {
    background: #fff;
  }

  20% {
    background: #fff;
  }
  21% {
    background: #3a84ff;
  }

  99% {
    background: #3a84ff;
  }

  100% {
    background: #fff;
  }
}

@keyframes scroll-animate-step3 {
  0% {
    background: #fff;
  }

  40% {
    background: #fff;
  }
  41% {
    background: #3a84ff;
  }

  99% {
    background: #3a84ff;
  }

  100% {
    background: #fff;
  }
}

@keyframes scroll-animate-step4 {
  0% {
    background: #fff;
  }

  60% {
    background: #fff;
  }
  61% {
    background: #3a84ff;
  }

  99% {
    background: #3a84ff;
  }

  100% {
    background: #fff;
  }
}

@keyframes scroll-animate-step5 {
  0% {
    background: #fff;
  }

  80% {
    background: #fff;
  }
  81% {
    background: #3a84ff;
  }

  99% {
    background: #3a84ff;
  }

  100% {
    background: #fff;
  }
}

@keyframes slider-animate {
  0% {
    left: 10px;
  }
  100% {
    left: calc(100% - 60px);
  }
}
</style>

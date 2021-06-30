

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
  <div class="slide-cutover"
    @mouseover="clearInv"
    @mouseout="runInv">
    <div class="slide-content">
      <!-- <a :href="slides[nowIndex].href"> -->
      <transition :name="newTr ? 'slide-trans' : 'slide-back'">
        <div class="step">
          <div class="step-content">
            <div class="step-content-img">
              <img v-if="isShow"
                :src="slides[nowIndex].src">
            </div>
            <div class="step-content-text">
              <h3 class="title">
                {{ slides[nowIndex].title }}
              </h3>
              <ol class="step-list">
                <li v-for="(item, index) in slides[nowIndex].step"
                  :key="index">
                  {{ index + 1 }}.{{ item.text }}
                </li>
              </ol>
            </div>
          </div>
          <!-- <div><img v-if="isShow" :src="slides[nowIndex].src"></div> -->
        </div>
        <!-- <img v-if="isShow" :src="slides[nowIndex].src"> -->
      </transition>
      <transition :name="newTr ? 'slide-trans-old' : 'slide-back-old'">
        <div v-if="!isShow"
          class="step">
          <div class="step-content">
            <div class="step-content-img">
              <img :src="slides[nowIndex].src">
            </div>
            <div class="step-content-text">
              <h3 class="title">
                {{ slides[nowIndex].title }}
              </h3>
              <ol class="step-list">
                <li v-for="(item, index) in slides[nowIndex].step"
                  :key="index">
                  {{ index + 1 }}.{{ item.text }}
                </li>
              </ol>
            </div>
          </div>
        </div>
      </transition>
      <!-- </a> -->
      <div class="prev-button"
        @click="goto(prevIndex)">
        <i class="bk-icon icon-angle-left" />
      </div>
      <div class="next-button"
        @click="goto(nextIndex)">
        <i class="bk-icon icon-angle-right" />
      </div>
    </div>
    <ul class="slide-pages">
      <li
        v-for="(item, index) in slides"
        :key="index"
        :class="{ on: index === nowIndex }"
        @mouseenter="runInv"
        @mouseout="clearInv"
        @click="goto(index)" />
    </ul>
  </div>
</template>
<script>
export default {
  props: {
    slides: {
      type: Array,
      default: () => [],
    },
    inv: {
      type: Number,
      default: 2000,
    },
  },
  data() {
    return {
      nowIndex: 0,
      isShow: true,
      newTr: true,
    };
  },
  computed: {
    prevIndex() {
      if (this.nowIndex === 0) {
        return this.slides.length - 1;
      } else {
        return this.nowIndex - 1;
      }
    },
    nextIndex() {
      if (this.nowIndex === this.slides.length - 1) {
        return 0;
      } else {
        return this.nowIndex + 1;
      }
    },
  },
  mounted() {
    this.runInv();
  },
  methods: {
    goto(index) {
      this.isShow = false;
      if (index < this.nowIndex) {
        this.newTr = false;
      } else {
        this.newTr = true;
      }
      setTimeout(() => {
        this.isShow = true;
        this.nowIndex = index;
      }, 10);
    },
    runInv() {
      this.invId = setInterval(() => {
        this['goto'](this.nextIndex);
      }, this.inv);
    },
    clearInv() {
      clearInterval(this.invId);
    },
  },
};
</script>

<style lang="scss">
.slide-cutover {
  position: relative;
  margin: 0 auto;
  width: 100%;
  height: 380px;
  overflow: hidden;
  .step {
    width: 100%;
    height: 100%;
    position: absolute;
    background: #fff;
    top: 0;
    .step-content {
      padding: 60px;
    }
    .step-content-img {
      float: left;
      width: 50%;
      text-align: left;
    }
    .step-content-text {
      float: left;
      max-width: calc(50% - 17px);
      margin-left: 17px;
      padding: 13px;
    }
    .title {
      font-size: 28px;
    }
    .step-list {
      margin-top: 10px;
      line-height: 32px;
    }
  }
  .slide-content {
    width: 100%;
    height: 100%;
    position: relative;
    .prev-button,
    .next-button {
      width: 50px;
      height: 50px;
      line-height: 50px;
      text-align: center;
      position: absolute;
      top: 40%;
      font-size: 20px;
      cursor: pointer;
    }
    .prev-button {
      left: 10px;
    }
    .next-button {
      right: 10px;
    }
  }
  .slide-pages {
    position: absolute;
    bottom: 15px;
    left: 50%;
    transform: translate(-50%, 0);
    li {
      display: inline-block;
      /* padding: 0 3px; */
      width: 10px;
      height: 10px;
      cursor: pointer;
      background: #fff;
      border: 1px solid #7b7d8a;
      border-radius: 50%;
      font-size: 16px;
      margin: 3px;
      &.on {
        background: #3a84ff;
        border-color: #3a84ff;
      }
    }
  }
  .slide-back-enter-active {
    transition: all 0.5s;
  }
  .slide-back-enter {
    transform: translateX(-980px);
  }
  .slide-back-old-leave-active {
    transition: all 0.5s;
    transform: translateX(980px);
  }
  .slide-trans-enter-active {
    transition: all 0.5s;
  }
  .slide-trans-enter {
    transform: translateX(980px);
  }
  .slide-trans-old-leave-active {
    transition: all 0.5s;
    transform: translateX(-980px);
  }
}
</style>

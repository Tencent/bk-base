

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
  <div class="bk-circle">
    <svg :width="config.width"
      :height="config.height"
      :stroke-width="config.strokeWidth"
      fill="none">
      <circle
        :cx="config.width / 2"
        :cy="config.height / 2"
        :r="config.r"
        :stroke-width="config.strokeWidth"
        :stroke="config.bgColor"
        fill="none" />
      <circle
        :cx="config.width / 2"
        :cy="config.height / 2"
        :r="config.r"
        :stroke-width="config.strokeWidth"
        :stroke="config.activeColor"
        :class="'circle' + config.index"
        fill="none"
        stroke-dasharray="0 1609"
        transform="matrix(0,-1,1,0,0,65)" />
    </svg>
    <div class="num">
      {{ num }}
    </div>
    <div class="title">
      {{ title }}
    </div>
  </div>
</template>

<script>
export default {
  props: {
    config: {
      type: Object,
      default() {
        return {
          width: 50,
          height: 50,
          strokeWidth: 5,
          r: 5,
          fillWidth: 50,
          bgColor: 'gray',
          activeColor: 'green',
          index: 0,
        };
      },
    },
    percent: {
      type: Number,
      default: 0,
    },
    title: {
      type: String,
    },
    num: {
      type: Number,
      default: 0,
    },
  },
  data() {
    return {};
  },
  computed: {
    perimeter() {
      return Math.PI * 2 * 30;
    },
  },
  watch: {
    percent: function () {
      this.drawData();
    },
  },
  mounted() {
    this.$nextTick(() => {
      this.drawData();
    });
  },
  methods: {
    drawData() {
      let className = `.circle${this.config.index}`;
      const circle = document.querySelectorAll(className);
      circle.forEach(c => {
        c.setAttribute('stroke-dasharray', this.perimeter * this.percent + ' ' + this.perimeter * (1 - this.percent));
      });
    },
  },
};
</script>

<style lang="scss" scoped>
.bk-circle {
  position: relative;
}
.bk-circle circle {
  line-height: 1;
  transition: stroke-dasharray 0.25s;
}
.bk-circle .title {
  text-align: center;
  position: absolute;
  left: 50%;
  width: 100%;
  overflow: auto;
  transform: translate(-50%, 0);
}
.bk-circle .num {
  position: absolute;
  left: 50%;
  top: 50%;
  transform: translate(-50%, -60%);
  color: #737987;
}
</style>

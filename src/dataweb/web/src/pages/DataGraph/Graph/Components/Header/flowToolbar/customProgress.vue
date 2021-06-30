

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
  <div id="customProgressbar"
    class="progress-bar blue stripes">
    <span :style="`width:${percent}%`" />
    <p class="text">
      {{ percent }}%
    </p>
  </div>
</template>
<script>
export default {
  props: {
    barWidth: {
      type: String,
      default: '250px',
    },
    percent: {
      type: Number,
      default: 10,
    },
  },
};
</script>

<style lang="scss" scoped>
/*=====进度条外框样式======*/
.progress-bar {
  background-color: #f0f1f5;
  height: 21px;
  padding: 1px;
  width: 250px;
  -moz-border-radius: 5px;
  -webkit-border-radius: 5px;
  border-radius: 5px;
  position: relative;
  margin-right: 40px;
  p {
    position: absolute;
    right: -37px;
    top: 1px;
  }
}
/*=====填充条的默认风格=====*/
.progress-bar span {
  display: inline-block;
  height: 100%;
  background-color: #f0f1f5;
  -moz-border-radius: 3px;
  -webkit-border-radius: 3px;
  border-radius: 3px;
  -moz-transition: width 0.4s ease-in-out;
  -webkit-transition: width 0.4s ease-in-out;
  -ms-transition: width 0.4s ease-in-out;
  -o-transition: width 0.4s ease-in-out;
  transition: width 0.4s ease-in-out;
}
/*=======blue bar=======*/
.blue span {
  background-color: #0484ff;
}
/*==============stripes bar=============*/
.stripes span {
  -webkit-background-size: 30px 30px;
  -moz-background-size: 30px 30px;
  -ms-background-size: 30px 30px;
  -o-background-size: 30px 30px;
  background-size: 30px 30px;
  background-image: -webkit-gradient(
    linear,
    left top,
    right bottom,
    color-stop(0.25, rgba(255, 255, 255, 0.15)),
    color-stop(0.25, transparent),
    color-stop(0.5, transparent),
    color-stop(0.5, rgba(255, 255, 255, 0.15)),
    color-stop(0.75, rgba(255, 255, 255, 0.15)),
    color-stop(0.75, transparent),
    to(transparent)
  );
  background-image: linear-gradient(
    135deg,
    rgba(255, 255, 255, 0.15) 25%,
    transparent 25%,
    transparent 50%,
    rgba(255, 255, 255, 0.15) 50%,
    rgba(255, 255, 255, 0.15) 75%,
    transparent 75%,
    transparent
  );
  animation: animate-stripes 3s linear infinite;
}

@keyframes animate-stripes {
  0% {
    background-position: 0 0;
  }
  100% {
    background-position: 60px 0;
  }
}
</style>

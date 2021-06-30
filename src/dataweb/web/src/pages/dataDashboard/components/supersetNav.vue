

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
  <div class="subnav-tabs">
    <div class="subnav-tab"
      :class="{ active: activeName === 'SupersetWelcome' }"
      @click="jump('SupersetWelcome')">
      {{ $t('Welcome') }}
    </div>
    <div class="subnav-tab"
      :class="{ active: activeName === 'MyDashboard' }"
      @click="jump('MyDashboard')">
      {{ $t('我的仪表板') }}
    </div>
    <div class="subnav-tab"
      :class="{ active: activeName === 'MyChart' }"
      @click="jump('MyChart')">
      {{ $t('我的图表') }}
    </div>
    <div class="subnav-tab"
      :class="{ active: activeName === 'ScheduleEmail' }"
      @click="jump('ScheduleEmail')">
      {{ $t('周期邮件') }}
    </div>
  </div>
</template>

<script>
export default {
  props: {
    activeName: {
      type: String,
      default: 'SupersetWelcome',
    },
  },
  data() {
    return {};
  },
  async mounted() {},
  methods: {
    jump(routeName) {
      this.$emit('supersetChangeTab', routeName);
    },
  },
};
</script>

<style lang="scss" scoped>
@import '~@/common/scss/conf.scss';

.subnav-tabs {
  .subnav-tab {
    padding: 0 20px;
    height: 40px;
    -webkit-box-sizing: border-box;
    box-sizing: border-box;
    line-height: 40px;
    display: inline-block;
    list-style: none;
    font-size: 14px;
    font-weight: 500;
    color: #303133;
    position: relative;

    &:last-child {
      padding-right: 0px;
    }

    &:first-child {
      padding-left: 0px;
    }

    &:hover {
      cursor: pointer;
      color: $primaryColor;
    }
    &.active {
      color: $primaryColor;
    }

    &.active:after {
      content: '';
      position: absolute;
      bottom: 0;
      left: 20px;
      width: calc(100% - 40px);
      height: 2px;
      background-color: $primaryColor;
      z-index: 1;
      -webkit-transition: -webkit-transform 0.3s cubic-bezier(0.645, 0.045, 0.355, 1);
      transition: -webkit-transform 0.3s cubic-bezier(0.645, 0.045, 0.355, 1);
      transition: transform 0.3s cubic-bezier(0.645, 0.045, 0.355, 1);
      transition: transform 0.3s cubic-bezier(0.645, 0.045, 0.355, 1),
        -webkit-transform 0.3s cubic-bezier(0.645, 0.045, 0.355, 1);
      list-style: none;
    }
    &.active:last-child:after,
    &.active:first-child:after {
      width: calc(100% - 20px);
    }
    &.active:first-child:after {
      left: 0px;
    }

    .count {
      position: absolute;
      left: 100%;
      top: 2px;
      min-width: 16px;
      height: 16px;
      font-size: 12px;
      color: #fff;
      line-height: 16px;
      text-align: center;
      background: #ff5656;
      border-radius: 8px;
      padding: 0 3px;
    }
  }
}
</style>



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
  <div v-if="!isExpand"
    :class="{ 'side-bar-hearder': true, collspaned: !isExpand }">
    <div id="task-list-menu"
      class="menu fl"
      :title="$t('点击展开任务列表')"
      @click="handeExpand">
      <i :class="menuIconClass" />
      <div v-if="!$route.params.fid"
        class="tips">
        {{ $t('请选择或创建任务') }}
      </div>
    </div>
  </div>
</template>
<script>
export default {
  data() {
    return {
      isExpand: false,
    };
  },
  computed: {
    menuIconClass() {
      return {
        'bk-icon': true,
        'icon-indent': !this.isExpand,
        'icon-dedent': this.isExpand,
      };
    },
  },
  methods: {
    handeExpand() {
      this.$emit('onToolExpand', !this.isExpand);
      this.$store.commit('ide/setTaskSidebarExpand', !this.isExpand);
    },
  },
};
</script>
<style lang="scss" scoped>
.side-bar-hearder {
  background: inherit;
  border-right: 1px solid rgba(221, 221, 221, 1);
  border-bottom: 1px solid rgba(221, 221, 221, 1);
  .menu {
    width: 57px;
    height: 49px;
    line-height: 49px;
    text-align: center;
    cursor: pointer;
    position: relative;
    .tips {
      color: #fff;
      position: absolute;
      left: 115%;
      top: 50%;
      font-size: 12px;
      -webkit-transform: translateY(-50%);
      transform: translateY(-50%);
      min-width: 116px;
      padding: 0 10px;
      height: 28px;
      white-space: nowrap;
      line-height: 28px;
      background: #000;
      border-radius: 2px;

      &::after {
        position: absolute;
        top: 30%;
        left: -6px;
        content: '';
        width: 0;
        height: 0;
        border: 6px solid;
        border-color: transparent #000 transparent transparent;
        -webkit-transform: translate(-50%);
        transform: translate(-50%);
      }
    }
  }
}
</style>

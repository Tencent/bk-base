

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
  <bkdata-popover ref="popover"
    theme="light"
    :trigger="'click'">
    <slot />
    <div slot="content">
      <div id="dev-config-tootip"
        class="dev-config-tootip">
        <span @click="confirm">确定</span>
        <span @click="cancle">取消</span>
      </div>
    </div>
  </bkdata-popover>
</template>

<script>
export default {
  methods: {
    confirm() {
      this.$emit('confirmHandle');
    },
    cancle() {
      this.$emit('cancleHandle');
    },
    closeTip() {
      this.$refs.popover.instance.hide();
    },
  },
};
</script>

<style lang="scss" scoped>
.dev-config-tootip {
  display: flex;
  span {
    font-size: 12px;
    font-family: MicrosoftYaHei;
    color: rgba(58, 132, 255, 1);
    line-height: 16px;
    cursor: pointer;
  }
  span:nth-child(1) {
    margin-right: 9px;
  }
  &::after {
    content: '';
    position: absolute;
    height: 12px;
    border-right: 1px solid rgba(240, 241, 245, 1);
    left: 42px;
    top: 10px;
  }
}
</style>

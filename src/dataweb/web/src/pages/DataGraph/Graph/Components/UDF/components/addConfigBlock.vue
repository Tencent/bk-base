

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
  <div class="wrapper"
    @click="addBlock">
    <span class="icon-plus" />
    <span class="desp">{{ $t('添加代码依赖') }}</span>
  </div>
</template>
<script>
export default {
  methods: {
    addBlock() {
      this.$emit('addBlock');
    },
  },
};
</script>

<style lang="scss" scoped>
.wrapper {
  width: 100%;
  height: 32px;
  border: 1px dashed rgba(196, 198, 204, 1);
  display: flex;
  justify-content: center;
  align-items: center;
  cursor: pointer;
  .span {
    color: rgba(151, 155, 165, 1);
    font-size: 14px;
    font-family: MicrosoftYaHeiUI;
    color: rgba(99, 101, 110, 1);
    line-height: 18px;
  }
  .icon-plus {
    margin-right: 9px;
  }
}
</style>



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
  <div class="palette-panel">
    <slot name="palette">
      <ul class="palette-group">
        <li>
          <div class="palette-default-item type-a"
            data-type="type-a">
            A
          </div>
        </li>
        <li>
          <div class="palette-default-item type-b"
            data-type="type-b">
            B
          </div>
        </li>
        <li>
          <div class="palette-default-item type-c"
            data-type="type-c">
            C
          </div>
        </li>
        <li>
          <div class="palette-default-item type-d"
            data-type="type-d">
            D
          </div>
        </li>
      </ul>
    </slot>
  </div>
</template>
<script>
export default {
  name: 'PalettePanel',
  // props: ['selector'],
  data() {
    return {};
  },
  mounted() {
    this.$emit('registerPaletteEvent');
  },
};
</script>
<style lang="scss">
.palette-group {
  margin: 0;
  padding: 0;
  background: #fcfcfc;
  text-align: center;
  list-style: none;
  & > li {
    padding: 14px 0;
  }
}
.palette-default-item {
  display: inline-block;
  width: 40px;
  height: 40px;
  line-height: 40px;
  font-size: 12px;
  color: #888888;
  border: 1px solid #999999;
  border-radius: 2px;
  cursor: pointer;
  user-select: none;
  &:hover {
    box-shadow: 0px 0px 8px rgba(50, 50, 50, 0.3);
  }
}
.type-a {
  border-radius: 50%;
}
.type-b {
  border-radius: 50%;
  border: 4px solid #333333;
}
.type-c,
.type-d {
  height: 30px;
  line-height: 30px;
  border: 1px solid #999;
}
.type-d {
  position: relative;
  &:after {
    content: '';
    position: absolute;
    top: 0;
    right: 0;
    width: 5px;
    height: 5px;
    border-radius: 50%;
    background: #cccccc;
  }
}
</style>



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
  <div :class="shape">
    <span class="item-content">{{ displayText }}</span>
  </div>
</template>
<script>
export default {
  props: {
    text: {
      type: [String, Object],
      default: '',
    },
    shape: {
      type: String,
      default: 'radius',
    },
  },
  computed: {
    displayText() {
      return typeof this.text === 'string' ? this.text : this.text.content;
    },
  },
};
</script>

<style lang="scss" scoped>
.radius {
  width: 48px;
  height: 24px;
  background: rgba(240, 241, 245, 1);
  border-radius: 12px;
  display: flex;
  justify-content: center;
  align-items: center;
  span {
    font-size: 12px;
    font-family: MicrosoftYaHei;
    color: rgba(49, 50, 57, 1);
    line-height: 16px;
  }
}

.rect {
  width: 48px;
  height: 24px;
  margin-bottom: 4px;
  background: rgba(250, 251, 253, 1);
  border-radius: 2px;
  border: 1px solid rgba(220, 222, 229, 1);
  display: flex;
  justify-content: center;
  align-items: center;
  span {
    font-size: 12px;
    font-family: MicrosoftYaHei;
    color: rgba(99, 101, 110, 1);
    line-height: 16px;
  }
}
</style>

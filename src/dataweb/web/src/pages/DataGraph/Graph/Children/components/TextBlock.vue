

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
  <div class="text-block">
    <div class="text"
      @click="handleFocus">
      <ul>
        <li v-for="(item, index) in contentList"
          :key="index"
          class="block">
          {{ item }}
          <div class="block-close icon-close"
            @click="handleClose(index)" />
        </li>
      </ul>
      <bkdata-input ref="ipt"
        v-model="content"
        type="text"
        @enter="handleContent" />
    </div>
  </div>
</template>

<script>
export default {
  name: 'TextBlock',
  props: {
    msg: String,
  },
  data() {
    return {
      content: '',
      contentList: [],
    };
  },
  mounted() {
    this.$refs.ipt.focus();
  },
  methods: {
    handleContent(item) {
      this.content.trim() && this.contentList.push(this.content);
      this.content = '';
    },
    handleClose(index) {
      this.contentList.splice(index, 1);
    },
    handleFocus() {
      this.$refs.ipt.focus();
    },
  },
};
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style lang="scss" scoped>
* {
  margin: 0;
  padding: 0;
}
li {
  list-style: none;
}
.text {
  border: 1px solid #c3cdd7;
  width: 100%;
  min-height: 100px;
  padding: 5px;
  overflow: hidden;
  input {
    display: block;
    width: 100px;
    font-size: 16px;
    height: 30px;
    border: none;
    outline: none;
    float: left;
  }
  ul {
    .block {
      border-radius: 2px;
      height: 28px;
      line-height: 15px;
      float: left;
      margin-right: 10px;
      margin-bottom: 5px;
      position: relative;
      border: 1px solid #ddd;
      padding: 5px 20px;
      .block-close {
        position: absolute;
        right: 0px;
        top: 0px;
        cursor: pointer;
        &::before {
          content: '\E97D';
        }
      }
    }
  }
}
</style>

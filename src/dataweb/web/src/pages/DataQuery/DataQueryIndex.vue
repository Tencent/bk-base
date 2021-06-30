

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
  <div class="resultQuery">
    <!--侧边栏-->
    <div class="sub-sidebar">
      <QueryPanel />
    </div>
    <div class="main">
      <QueryDetails />
    </div>
  </div>
</template>
<script>
import QueryPanel from './QueryLeftPanel';
import QueryDetails from './QueryDetails';
export default {
  components: {
    QueryPanel,
    QueryDetails,
  },
};
</script>

<style media="screen" lang="scss">
.resultQuery {
  display: flex;
  height: 100%;
  /*侧边栏*/
  .sub-sidebar {
    width: 300px;
    border-right: 1px solid #ddd;
    background: #fafafa;
    position: absolute;
    top: 60px;
    bottom: 0;
  }
  .main {
    height: 100%;
    margin-left: 300px;
    position: relative;
    min-height: 500px;
    width: 100%;
    overflow: scroll;
    &::-webkit-scrollbar {
      width: 8px;
      background-color: transparent;
    }

    &::-webkit-scrollbar-thumb {
      border-radius: 8px;
      background-color: #a0a0a0;
    }
  }
}
</style>

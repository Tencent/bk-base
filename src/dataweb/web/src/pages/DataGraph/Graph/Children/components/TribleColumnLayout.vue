

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
  <multipane class="vertical-panes"
    layout="vertical">
    <div class="left-column pane">
      <slot name="left-pane" />
    </div>
    <multipane-resizer />
    <div class="mid-column pane">
      <slot name="mid-pane" />
    </div>
    <multipane-resizer />
    <div class="right-column pane">
      <slot name="right-pane" />
    </div>
  </multipane>
</template>
<script>
import { Multipane, MultipaneResizer } from 'vue-multipane';
export default {
  components: {
    Multipane,
    MultipaneResizer,
  },
};
</script>
<style lang="scss" scoped>
.vertical-panes {
  width: 100%;
  height: 100%;
  border-top: 1px solid #ccc;
  border-bottom: 1px solid #ccc;
  .pane {
    text-align: left;
    overflow: hidden;
    background: #eee;
  }
  .left-column {
    min-width: 250px;
    width: 22%;
    max-width: 25%;
  }
  .mid-column {
    min-width: 50%;
    width: 50%;
    max-width: 65%;
  }
  .right-column {
    flex: 1;
  }
}
</style>

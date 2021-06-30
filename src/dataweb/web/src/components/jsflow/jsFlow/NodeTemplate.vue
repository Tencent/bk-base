

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
  <div
    class="bk-flow-location"
    @mousedown="moveFlag = false"
    @mousemove="moveFlag = true"
    @mouseup="onNodeClick(node, $event)">
    <div v-if="node.type === 'type-a'"
      class="type-a">
      start
    </div>
    <div v-else-if="node.type === 'type-b'"
      class="type-b">
      end
    </div>
    <div v-else-if="node.type === 'type-c'"
      class="type-c">
      taskNode
    </div>
    <div v-else-if="node.type === 'type-d'"
      class="type-d">
      subFlow
    </div>
    <div v-else
      class="node-default" />
  </div>
</template>
<script>
export default {
  name: 'NodeTemplate',
  props: {
    node: {
      type: Object,
      default() {
        return {};
      },
    },
  },
  data() {
    return {
      moveFlag: false,
    };
  },
  methods: {
    onNodeClick(node, event) {
      if (!this.moveFlag) {
        console.log(node.x, node.y);
      }
    },
  },
};
</script>
<style lang="scss">
.bk-flow-location {
  .type-c,
  .type-d {
    width: 120px;
    height: 60px;
    line-height: 60px;
    border: 1px solid #cccccc;
    text-align: center;
    cursor: pointer;
    &:hover {
      border-color: #348aff;
    }
  }
  .type-d {
    position: relative;
    &:after {
      content: '';
      position: absolute;
      top: 0;
      right: 0;
      width: 10px;
      height: 10px;
      border-radius: 50%;
      background: #cccccc;
    }
  }

  .type-a,
  .type-b {
    width: 60px;
    height: 60px;
    line-height: 60px;
    border-radius: 50%;
    border: 1px solid #cccccc;
    text-align: center;
  }
  .type-b {
    border: 4px solid #333333;
  }
  .node-default {
    width: 120px;
    height: 80px;
    line-height: 80px;
    border: 1px solid #cccccc;
    border-radius: 2px;
    text-align: center;
    &.selected {
      border: 1px solid #3a84ff;
    }
  }
}
</style>

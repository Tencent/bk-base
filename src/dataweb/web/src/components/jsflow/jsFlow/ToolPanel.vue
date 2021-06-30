

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
  <div class="tool-panel">
    <div class="tools-list clearfix">
      <ul class="list">
        <li
          v-for="(tool, index) in tools"
          :key="index"
          :class="['tool-item', tool.cls, { actived: tool.type === 'frameSelect' && isFrameSelecting }]"
          @click="onToolClick(tool)">
          <template v-if="tool.showType === 'text'">
            {{ tool.name || tool.type }}
          </template>
          <template v-if="tool.showType !== 'text'">
            <span :class="tool.icon"
              :title="tool.name || tool.type">
              {{ (tool.showType === 'full' && (tool.name || tool.type)) || '' }}
            </span>
          </template>
        </li>
      </ul>
    </div>
  </div>
</template>
<script>
export default {
  name: 'ToolPanel',
  props: {
    isFrameSelecting: {
      type: Boolean,
    },
    tools: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {};
  },
  methods: {
    onToolClick(tool) {
      this.$emit('onToolClick', tool);
    },
  },
};
</script>
<style lang="scss" scoped>
.tool-panel {
  // margin: 20px 10px 0px 0px;
  width: 38px;
  right: 0px;
  .tools-list {
    float: right;
    border: 1px solid #dddddd;
    border-radius: 2px;
    .list {
      font-size: 14px;
      float: left;
      background: #fafafa;
      border-left: 1px solid #ddd;
      &:first-of-type {
        border-left: none;
      }
      > li {
        float: left;
        width: 36px;
        height: 34px;
        line-height: 34px;
        border-radius: 2px;
        text-align: center;
        position: relative;
        &:hover {
          background: #3a84ff;
          color: #fff;
          cursor: pointer;
        }
      }
    }
  }
}
</style>

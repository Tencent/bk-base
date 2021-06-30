

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
  <div class="list-wrapper">
    <div v-for="(flow, index) in flowList"
      :key="index"
      class="flow-item"
      @click="linkToFlow(flow)">
      <span :class="['icon-dataflow', flow.related_flow.status]" />
      <span class="flow-name">{{ flow.related_flow.flow_name }}</span>
    </div>
    <div v-if="flowList.length === 0"
      class="empty-content">
      <span class="icon-empty" />
      <p class="text">
        {{ $t('暂无数据') }}
      </p>
    </div>
  </div>
</template>

<script>
export default {
  props: {
    flowList: {
      type: Array,
      default: () => [],
    },
  },
  methods: {
    linkToFlow(flow) {
      this.$router.push({
        name: 'dataflow_ide',
        params: { fid: flow.flow_id },
        query: { NID: flow.node_id, project_id: flow.related_flow.project_id },
      });
    },
  },
};
</script>

<style lang="scss" scoped>
.list-wrapper {
  .flow-item {
    cursor: pointer;
    padding: 0 10px 0 38px;
    display: -webkit-box;
    position: relative;
    height: 42px;
    width: 300px;
    border-bottom: 1px solid #eee;
    &:hover {
      background: #eee;
      color: #3a84ff;
      .flow-name {
        width: 163px;
        margin-right: 10px;
      }
    }
    .flow-name {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      padding: 11px 0;
      width: calc(100% - 108px);
      position: absolute;
      left: 38px;
    }
    .icon-dataflow {
      font-size: 14px;
      position: absolute;
      left: 13px;
      top: 14px;
      transition: all linear 0.1s;
      &.running {
        color: #a3ce74;
      }
    }
  }
  .empty-content {
    display: flex;
    flex-direction: column;
    align-items: center;
    margin-top: 100px;
    .icon-empty {
      font-size: 65px;
      color: #c3cdd7;
    }
    .text {
      font-size: 12px;
    }
  }
}
</style>

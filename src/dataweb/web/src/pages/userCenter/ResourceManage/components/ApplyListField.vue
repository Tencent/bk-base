

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
  <div v-bkloading="{ isLoading: listLoading }"
    class="wrapper bk-scroll-y">
    <div class="apply-status">
      <div class="status-item">
        <span class="success" />{{ $t('已申请') }}
      </div>
      <div class="status-item">
        <span class="applying" />{{ $t('申请中') }}
      </div>
      <div class="status-item">
        <span class="rejected" />{{ $t('被驳回') }}
      </div>
    </div>
    <div
      v-for="(item, index) in applyGroupList"
      :key="index"
      v-bk-tooltips="item.name"
      class="apply-process"
      :class="{
        'apply-processing': item.status === 'processing',
        'apply-succeeded': item.status === 'succeeded',
        'apply-failed': item.status === 'failed',
      }">
      {{ item.name }}
    </div>
  </div>
</template>

<script>
export default {
  props: {
    listLoading: {
      type: Boolean,
      default: false,
    },
    applyGroupList: {
      type: Array,
      default: () => [],
    },
  },
};
</script>
<style lang="scss" scoped>
.wrapper {
  width: 100%;
  height: 100%;
  .apply-status {
    display: flex;
    flex-wrap: nowrap;
    justify-content: space-between;
    margin-bottom: 10px;
    .status-item {
      height: 20px;
      line-height: 20px;
      font-size: 14px;
      color: #63656f;
      span {
        display: inline-block;
        width: 12px;
        height: 12px;
        margin-right: 6px;
      }
      .success {
        background: #00d042;
      }
      .applying {
        background: #0484ff;
      }
      .rejected {
        background: #ff0b28;
      }
    }
  }
  .apply-process {
    width: 100%;
    margin-right: 10px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    background-color: #deecff;
    padding: 3px 8px 5px 8px;
    font-size: 12px;
    color: #3a84ff;
    margin-bottom: 6px;
    outline: none;
    cursor: pointer;
    &.apply-processing {
      background: #3a84ff;
      color: white;
    }
    &.apply-succeeded {
      background: #30d878;
      color: white;
    }
    &.apply-failed {
      background: #ff5656;
      color: white;
    }
  }
}
</style>

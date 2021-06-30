

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
  <div :class="['applied-card-wrapper', { active: active }, extCls]"
    @click="$emit('cardClick')">
    <div class="icon-area">
      <span :class="card.icon" />
    </div>
    <div class="text-area">
      <div class="card-title">
        {{ card.title }}
      </div>
      <div class="content">
        {{ card.content }}
      </div>
    </div>
    <div v-show="active"
      class="footer-triangle" />
    <span class="icon-check-1 footer-icon" />
  </div>
</template>
<script>
export default {
  props: {
    card: {
      type: Object,
      default: () => ({}),
    },
    active: {
      type: Boolean,
      default: false,
    },
    extCls: {
      type: String,
      default: '',
    },
  },
};
</script>
<style lang="scss" scoped>
.applied-card-wrapper {
  position: relative;
  width: 250px;
  height: 98px;
  background: #fafbfd;
  border: 1px solid #c4c6cc;
  border-radius: 2px;
  padding-right: 13px;
  display: flex;
  align-items: center;
  cursor: pointer;
  .icon-area {
    font-size: 24px;
    margin-left: 21px;
    color: #a3c5fd;
  }
  .text-area {
    margin-left: 20px;
    font-size: 12px;
    line-height: 14px;
    color: #979ba5;
    .card-title {
      font-size: 18px;
      color: #63656e;
      line-height: 22px;
      margin-bottom: 6px;
    }
  }
  &.active {
    width: 250px;
    height: 98px;
    background: #e1ecff;
    border: 1px solid #3a84ff;
    border-radius: 2px;
    .icon-area {
      font-size: 29px;
      margin-left: 18px;
      color: #3a84ff;
    }
    .text-area {
      margin-left: 17px;
    }
  }
  .footer-triangle {
    border-bottom: 17px solid #3a84ff;
    border-left: 33px solid transparent;
    width: 0;
    height: 0;
    position: absolute;
    right: 0;
    background-color: rgba(255, 255, 255, 0);
    bottom: 0;
  }
  .footer-icon {
    color: #fff;
    position: absolute;
    right: 2px;
    bottom: 0;
    font-size: 12px;
  }
  &.disabled {
    background-color: #fff;
    border-color: #dcdee5;
    cursor: not-allowed;
    .text-area {
      .card-title,
      .content {
        color: #c4c6cc;
      }
    }
  }
}
</style>

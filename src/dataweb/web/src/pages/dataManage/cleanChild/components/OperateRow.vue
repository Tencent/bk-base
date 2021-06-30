

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
  <div class="addDelect">
    <span class="add"
      @click="$emit('add-row')">
      <i :title="$t('添加')"
        class="bk-icon icon-plus bk-icon-new"
        :class="{ 'is-disabled': isAdd }" />
    </span>
    <span class="delect"
      :class="{ 'is-disabled': isDelete }"
      @click="$emit('delete-row')">
      <i :title="$t('删除')"
        class="bk-icon icon-minus" />
    </span>
  </div>
</template>

<script lang="ts">
import { Component, Vue, Prop } from 'vue-property-decorator';

@Component({})
export default class QualityFixConfig extends Vue {
  @Prop({ type: Boolean, default: false }) isAdd: boolean;
  @Prop({ type: Boolean, default: false }) isDelete: boolean;
}
</script>

<style lang="scss" scoped>
.addDelect {
  display: flex;
  height: 36px;
  padding: 8px 0 0 0;
  span {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 18px;
    height: 18px;
    border-radius: 50%;
    background: #737987;
    color: #fff;
    font-size: 10px;
    cursor: pointer;
  }
  .bk-icon {
    &.icon-minus {
      transform: scale(0.8);
    }
  }
  .disable {
    opacity: 0.8;
  }
  span + span {
    margin-left: 8px;
  }
  .is-disabled {
    color: #666 !important;
    opacity: 0.6 !important;
    cursor: not-allowed !important;
  }
}
</style>

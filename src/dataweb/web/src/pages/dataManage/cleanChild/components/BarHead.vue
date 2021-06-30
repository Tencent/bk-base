

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
  <div class="bar-head-container">
    <div class="header">
      <p class="title">
        {{ title }}
      </p>
      <div v-if="btnText"
        class="result-preview-btn">
        <bkdata-button :loading="btnLoading"
          theme="primary">
          {{ btnText }}
        </bkdata-button>
      </div>
      <div v-else>
        <span class="fr collspan"
          @click="changeContentStatus">
          <i :class="['bk-icon', 'collspan-icon', isContentShow ? 'icon-angle-up' : 'icon-angle-down']" />
        </span>
        <span class="fr guide pr10">
          <i :title="$t('数据修正指南')"
            class="bk-icon icon-question-circle" />
          {{ $t('数据修正指南') }}
        </span>
      </div>
    </div>
    <slot />
  </div>
</template>

<script lang="ts">
import { Component, Vue, Prop } from 'vue-property-decorator';

@Component({})
export default class QualityFixConfig extends Vue {
  @Prop({ type: String, default: '' }) title: string;
  @Prop({ type: String, default: '' }) btnText: string;
  @Prop({ type: Boolean, default: false }) btnLoading: boolean;

  isContentShow = true;

  changeContentStatus() {
    this.isContentShow = !this.isContentShow;
    this.$emit('changeContentStatus', this.isContentShow);
  }
}
</script>

<style lang="scss" scoped>
.bar-head-container {
  margin-top: 20px;
  border: 1px solid #c3cdd7;
  box-shadow: 2px 4px 5px rgba(33, 34, 50, 0.15);
  background: #fff;
  .header {
    height: 50px;
    padding-left: 20px;
    padding-right: 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    .guide {
      color: #3a84ff;
      cursor: pointer;
    }
    .collspan {
      cursor: pointer;
      .collspan-icon {
        display: inline-block;
        height: 20px;
        line-height: 20px;
        font-size: 26px;
      }
    }
    .title {
      line-height: 20px;
      font-weight: bold;
      padding-left: 18px;
      position: relative;
      display: inline-block;
      margin-right: 15px;
      &:before {
        content: '';
        width: 4px;
        height: 20px;
        position: absolute;
        left: 0px;
        top: 50%;
        background: #3a84ff;
        transform: translate(0%, -50%);
      }
    }
  }
}
</style>

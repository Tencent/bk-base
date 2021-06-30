

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
  <BarHead :btnText="btnText"
    :title="$t('结果预览')">
    <div class="result-preview-container">
      <div class="result-no-text mb20">
        {{ $t('请进行修正DEBUG或全局调试预览结果数据！') }}
      </div>
      <FixResultPreview :isShowDebug="false" />
    </div>
  </BarHead>
</template>

<script lang="ts">
import { Component, Vue, Prop } from 'vue-property-decorator';
import BarHead from './components/BarHead.vue';
// eslint-disable-next-line max-len
import FixResultPreview from '@/pages/DataGraph/Graph/Children/components/QualityFix/QualityFixOperate/FixResultPreview.vue';

@Component({
  components: {
    BarHead,
    FixResultPreview,
  },
})
export default class ResultPreview extends Vue {
  @Prop({ type: String, default: '' }) btnText: string;
  @Prop({ type: Boolean, default: false }) btnLoading: boolean;
}
</script>

<style lang="scss" scoped>
.result-preview-container {
  padding: 0 20px 20px 20px;
  .result-no-text {
    padding: 20px 0;
    text-align: center;
    border: 1px solid #c4c6cc;
    color: #c4c6cc;
    font-size: 14px;
  }
}
</style>

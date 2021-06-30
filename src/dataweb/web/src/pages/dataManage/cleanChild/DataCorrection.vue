

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
  <div>
    <BarHead :title="$t('数据修正')"
      @changeContentStatus="changeContentStatus">
      <QualityFixConfig v-show="isShowContent" />
    </BarHead>
  </div>
</template>

<script lang="ts">
import { Component, Vue, Prop } from 'vue-property-decorator';
import BarHead from './components/BarHead.vue';
import QualityFixConfig from '@/pages/DataGraph/Graph/Children/components/QualityFix/QualityFixConfig.vue';

@Component({
  components: {
    BarHead,
    QualityFixConfig,
  },
})
export default class DataCorrection extends Vue {
  @Prop({ type: String, default: '' }) title: string;
  @Prop({ type: String, default: '' }) btnText: string;
  @Prop({ type: Boolean, default: false }) btnLoading: boolean;

  isShowContent = true;

  changeContentStatus(isShow: boolean) {
    this.isShowContent = isShow;
  }
}
</script>

<style lang="scss" scoped></style>

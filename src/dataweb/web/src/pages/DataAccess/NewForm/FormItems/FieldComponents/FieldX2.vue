

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
  <Item class="x2">
    <template v-if="labelTips.visible">
      <i v-bk-tooltips="labelTips"
        class="bk-label-icon icon-exclamation-circle-shape bk-icon-new" />
    </template>
    <label v-if="!hideLabel"
      :class="['bk-label', { required: requried }]">
      <template v-if="labelTips">
        <span v-bk-tooltips="labelTips"
          class="has-desc">
          {{ label }}
        </span>
      </template>
      <template v-else>
        {{ label }}
      </template>
    </label>
    <div class="bk-form-content"
      :style="extCls">
      <slot />
      <template v-if="tips.visible">
        <i v-bk-tooltips="tips.content"
          class="bk-icon icon-exclamation-circle-shape bk-icon-new ml10" />
      </template>
    </div>
  </Item>
</template>
<script>
import Item from '../Item';
export default {
  components: { Item },
  props: {
    requried: {
      type: Boolean,
      default: false,
    },
    extCls: {
      type: String,
      default: '',
    },
    hideLabel: {
      type: Boolean,
      default: false,
    },
    label: {
      type: String,
      default: '',
    },
    tips: {
      type: Object,
      default: () => {
        return { visible: false, content: '' };
      },
    },
    labelTips: {
      type: String,
      default: '',
    },
  },
};
</script>
<style lang="scss" scoped>
.x2 {
  .bk-label-icon {
    position: absolute;
    left: 16px;
    cursor: pointer;
  }
  .bk-form-content {
    position: relative;
    display: flex;
    .bk-icon-right {
      position: absolute;
      right: -25px;
      margin-left: 10px;
      line-height: 30px;
      cursor: pointer;
      top: 50%;
      transform: translateY(-50%);
    }
  }
}
</style>

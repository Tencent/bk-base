

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
  <div class="alert-form-container">
    <div class="alert-form-left"
      :style="formTitleStyle">
      <span>{{ formTitle }}</span>
      <span class="alert-form-subTitle">{{ subTitle }}</span>
    </div>
    <div class="alert-form-right">
      <slot />
    </div>
  </div>
</template>
<script>
export default {
  props: {
    formTitle: {
      type: String,
      default: '',
    },
    subTitle: {
      type: String,
      default: '',
    },
    titlePosition: {
      type: String,
      default: 'top',
    },
  },
  data() {
    return {
      titleStyle: {
        top: {
          top: 0,
          left: 0,
        },
        center: {
          top: '50%',
          transform: 'translateY(-50%)',
        },
      },
    };
  },
  computed: {
    formTitleStyle() {
      return this.titleStyle[this.titlePosition];
    },
  },
};
</script>
<style lang="scss" scoped>
.alert-form-container {
  display: flex;
  justify-content: flex-start;
  position: relative;
  margin-bottom: 10px;

  &:last-child {
    margin-bottom: 0px;
  }
  .alert-form-left {
    font-size: 14px;
    color: #212232;
    width: 100px;
    display: inline-flex;
    align-items: center;
    justify-content: flex-start;

    .alert-form-subTitle {
      font-size: 10px;
      color: #a5a5a5;
    }
  }

  .alert-form-right {
    width: 100%;
    display: inline-flex;
    align-items: center;
  }
}
</style>

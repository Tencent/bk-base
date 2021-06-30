

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
  <div class="bk-form-item access-item mt10 mr15">
    <slot />
  </div>
</template>
<style lang="scss">
.bk-form-item {
  &.access-item {
    display: flex;
    justify-content: flex-start;
    align-items: center;
    &.x2 {
      .bk-form-content {
        width: 669px;
      }
    }
    .bk-label {
      width: 120px;
      min-width: 120px;
      text-align: right;
      padding: 5px 15px 5px 0;
      &.required::after {
        height: 8px;
        line-height: 1;
        content: '*';
        color: #ea3636;
        font-size: 12px;
        position: absolute;
        display: inline-block;
        vertical-align: middle;
        top: 50%;
        -webkit-transform: translate(3px, -50%);
        transform: translate(3px, -50%);
      }
    }

    .bk-form-content {
      width: 267px;
      .el-cascader {
        width: 100%;

        input {
          border-radius: 2px;
          padding: 0 10px;
          border-color: #c3cdd7;

          &[disabled] {
            background: #fafafa;
            color: #aaa;
            cursor: not-allowed;
          }
        }

        &.is-disabled {
          .el-cascader__label {
            cursor: not-allowed;
            color: #aaa;
          }
        }
        .el-cascader__label {
          padding-left: 10px;
        }
      }

      .bk-form-input[disabled] {
        background: #fafafa !important;
        color: #aaa;
        cursor: not-allowed;
      }
    }

    .bkdata-selector-input {
      height: 30px;
      line-height: 30px;

      &[type='radio'],
      &[type='checkbox'] {
        height: 18px;
        line-height: 18px;
      }
    }
    .bkdata-selector-icon {
      top: 9px;
    }
  }
}
</style>

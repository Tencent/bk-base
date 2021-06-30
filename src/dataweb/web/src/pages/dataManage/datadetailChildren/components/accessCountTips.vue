

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
  <div class="access-count-tips">
    <div class="lists">
      <div class="counts">
        <span class="count running" />
        <span class="num">{{ counts[0] ? counts[0] : 0 }}</span>
        <span class="count abnormal" />
        <span class="num">{{ counts[1] ? counts[1] : 0 }}</span>
        <span class="count normal" />
        <span class="num">{{ counts[2] ? counts[2] : 0 }}</span>
        <span class="count no-start" />
        <span class="num">{{ counts[3] ? counts[3] : 0 }}</span>

        <div class="tips">
          <div class="tip">
            <div class="types">
              {{ $t('成功') }} <span class="count running" />：
            </div>
            <div class="count">
              {{ counts[0] ? counts[0] : 0 }}
            </div>
          </div>

          <div class="tip">
            <div class="types">
              {{ $t('失败') }} <span class="count abnormal" />：
            </div>
            <div class="count">
              {{ counts[1] ? counts[1] : 0 }}
            </div>
          </div>

          <div class="tip">
            <div class="types">
              {{ $t('接入中') }} <span class="count normal" />：
            </div>
            <div class="count">
              {{ counts[2] ? counts[2] : 0 }}
            </div>
          </div>

          <div class="tip">
            <div class="types">
              {{ $t('等待中') }} <span class="count no-start" />：
            </div>
            <div class="count">
              {{ counts[3] ? counts[3] : 0 }}
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  props: {
    counts: {
      type: Array,
      default() {
        return [0, 0, 0, 0];
      },
    },
  },
};
</script>

<style lang="scss" scoped>
.access-count-tips {
  .counts {
    display: flex;
    flex-direction: row;
    position: relative;
    &:hover .tips {
      display: block;
    }
    .count {
      border-radius: 50%;
      width: 12px;
      height: 12px;
      margin-right: 5px;
    }
    .running {
      background: #9dcb6b;
    }
    .normal {
      background: #3a84ff;
    }
    .abnormal {
      background: #fe771d;
    }
    .no-start {
      background: #737987;
    }
    .num {
      color: #3a84ff;
      position: relative;
      top: -4px;
      vertical-align: middle;
      margin-right: 20px;
      &:last-of-type {
        margin: 0px;
      }
    }
    .tips {
      z-index: 1000;
      display: none;
      position: absolute;
      background: #212222;
      left: 50%;
      top: -5px;
      transform: translate(-50%, -100%);
      padding: 5px;
      border-radius: 2px;
      &:after {
        content: '';
        position: absolute;
        left: 50%;
        top: calc(100% + 5px);
        transform: translate(-50%, -50%);
        border: 5px solid #212222;
        border-right-color: transparent;
        border-left-color: transparent;
        border-bottom-color: transparent;
      }
      .tip {
        display: flex;
      }
      .types {
        color: #fff;
        text-align: right;
        width: 100px;
      }
      .count {
        color: #fff;
        display: inline-block;
      }
    }
  }
}
</style>

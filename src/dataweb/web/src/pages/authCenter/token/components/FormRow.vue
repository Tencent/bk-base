

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
  <div class="form-layout-container">
    <div class="form-layout-left">
      <span v-if="step"
        class="form-layout-step">
        {{ step }}
      </span>
      <span class="form-layout-stepname">{{ stepName }}</span>
    </div>
    <div class="form-layout-right">
      <bkdata-collapse v-if="collspan"
        v-model="activeName">
        <bkdata-collapse-item class="header-right-float"
          name="1"
          :arrowActive="isActive"
          :transition="false">
          <template slot="content">
            <slot />
          </template>
        </bkdata-collapse-item>
      </bkdata-collapse>
      <template v-else>
        <slot />
      </template>
    </div>
  </div>
</template>
<script>
export default {
  props: {
    step: {
      type: Number,
      default: 1,
    },
    stepName: {
      type: String,
      default: '',
    },
    collspan: {
      type: Boolean,
      default: true,
    },
    isActive: {
      type: Boolean,
      default: true,
    },
    isOpen: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      isEdit: false,
      activeName: (this.isOpen && '1') || '',
    };
  },
  watch: {
    isOpen(val) {
      this.activeName = (val && '1') || '';
    },
  },
};
</script>
<style lang="scss">
$rowMinHeight: 70px !default;
.form-layout-container {
  display: flex;
  justify-content: flex-start;
  background: #f5f5f5;
  box-shadow: 0px 0px 10px #ddd;
  border: 1px solid #d9dfe5;

  .form-layout-left {
    width: 180px;
    background: transparent;
    position: relative;
    height: 100%;
    margin: 20px 0 0 20px;

    .form-layout-step {
      display: inline-block;
      width: 24px;
      height: 24px;
      background: #3a84ff;
      color: white;
      line-height: 24px;
      text-align: center;
      font-style: italic;
      font-weight: bold;
    }

    .form-layout-stepname {
      margin-left: 10px;
      font-size: 16px;
      color: #212232;
      &::after {
        position: absolute;
        border: 10px solid #fff;
        border-color: transparent #fff transparent transparent;
        top: 10px;
        left: calc(100% - 10px);
        transform: translate(-50%, -50%);
        content: '';
      }
    }
  }

  .form-layout-right {
    width: calc(100% - 180px);
    background: #fff;
    height: 100%;
    padding: 20px 30px;
    position: relative;

    > .bk-collapse {
      .bk-collapse-item {
        > .bk-collapse-item-header {
          width: 100%;
          height: auto;
          line-height: inherit;
        }

        &.header-right-float {
          position: relative;
          display: flex;
          align-items: center;
          justify-content: flex-end;

          &.bk-collapse-item-active {
            display: inherit;
            > .bk-collapse-item-header {
              line-height: inherit;
              position: absolute;
              width: auto;
              right: 0;

              z-index: 100;
            }
          }
        }

        &.bk-position-relative {
          position: relative;
          > .bk-collapse-item-header {
            position: relative;
            height: auto;
            line-height: inherit;
            left: 0;
            padding-left: 0;
          }
        }
      }
    }

    .bkdata-selector {
      .bkdata-selector-wrapper {
        .bkdata-selector-icon {
          top: 50%;
          transform: translateY(-50%);
        }
      }
    }
  }
}
</style>

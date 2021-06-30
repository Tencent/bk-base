

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
  <div class="dict-params-detail">
    <div v-bkloading="{ isLoading: isLoading }"
      class="data-detail-left">
      <div class="data-detail-left-content">
        <div class="shadows data-info">
          <template v-for="(item, index) in tableInfo">
            <div v-if="Object.keys(item).length"
              :key="index"
              class="info data">
              <div v-if="Object.keys(item).length">
                <div class="type">
                  <span>
                    {{ item.title }}
                  </span>
                </div>

                <div class="info-detail">
                  <form class="bk-form"
                    style="width: 370px">
                    <div v-for="(child, idx) in item.list"
                      :key="idx"
                      class="dict-params-item">
                      <slot :name="`${child.name}-wraper`">
                        <!-- 自定义左侧名称 -->
                        <div class="label-container">
                          <slot :name="`${child.name}-label`">
                            <label class="bk-label info-left">
                              {{ child.name }}
                            </label>
                          </slot>
                        </div>
                        <div class="dict-params-content"
                          :class="{ 'data-manager': child.name === $t('标签') }">
                          <slot :slot="child"
                            :name="child.name">
                            <span :title="child.value"
                              class="text-overflow">
                              {{ child.value }}
                            </span>
                          </slot>
                        </div>
                      </slot>
                    </div>
                  </form>
                </div>
              </div>
            </div>
          </template>
        </div>
      </div>
    </div>
  </div>
</template>
<script>
export default {
  props: {
    data: {
      type: Array,
      default: () => [],
    },
    isLoading: {
      type: Boolean,
      default: false,
    },
    rawInfo: {
      type: Object,
      default: () => {
        {
        }
      },
    },
  },
  data() {
    return {
      tableInfo: [],
    };
  },
  watch: {
    data: {
      immediate: true,
      handler(val) {
        this.tableInfo = val;
      },
    },
  },
};
</script>
<style lang="scss" scoped>
template {
  display: block;
}
.dict-params-detail {
  .type {
    height: 55px;
    line-height: 55px;
    padding: 0 15px;
    font-weight: 700;
    span {
      padding: 0 !important;
      margin: 0 !important;
    }
    &::before {
      content: '';
      width: 2px;
      height: 19px;
      background: #3a84ff;
      display: inline-block;
      margin-right: 15px;
      position: relative;
      top: 4px;
    }
  }
  .shadows {
    box-shadow: 2px 3px 5px 0 rgba(33, 34, 50, 0.15);
    border-radius: 2px;
    border: 1px solid rgba(195, 205, 215, 0.6);
  }
  .info-detail {
    display: flex;
    margin: 0 15px;
    form {
      border: 1px solid #d9dfe5;
      .dict-params-item {
        &:first-child {
          border-top: none;
        }
        display: flex;
        border-top: 1px solid #d9dfe5;
        background-color: #efefef;
        .label-container {
          width: 140px;
        }
        .dict-params-content {
          width: calc(100% - 140px);
          margin-left: 0;
          padding-left: 15px;
          background-color: white;
          line-height: 34px;
        }
      }
      label.info-left {
        position: relative;
        width: 140px;
        background: #efefef;
        white-space: nowrap;
        .icon-question-circle {
          position: absolute;
          font-size: 12px;
          color: #ff9c01;
          right: 10px;
          top: 50%;
          transform: translateY(-50%);
        }
        .color-blue {
          color: #3a84ff;
        }
      }

      span.name {
        float: left;
        padding: 0px 5px;
        border-radius: 2px;
        background: #f7f7f7;
        border: 1px solid #dbe1e7;
        color: #737987;
        line-height: 20px;
        font-size: 12px;
        margin: 4px 3px 0px 0px;
      }
    }
  }
  .data-detail-left {
    display: flex;
    margin-bottom: 20px;
    .data-detail-left-content {
      width: 400px;
      margin-right: 15px;
    }
    .data-info {
      width: 100%;
      min-height: 700px;
      padding-bottom: 20px;
    }
  }
}
</style>



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
  <div class="quality-index-container">
    <div class="header">
      <slot name="header">
        <span class="title">{{ name }}</span>
        <span class="unit">（{{ unit }}）</span>
      </slot>
    </div>
    <div class="content"
      :class="{ 'content-no-average': !isShowAverage }">
      <slot name="content">
        <div class="recent-data">
          <div class="rate-container"
            @mouseenter="rateHover($event, 'rate')">
            <span class="data-count width100 text-overflow"
              :class="[alertLevel]"
              :title="tranNumber(dataCount)">
              {{ tranNumber(dataCount) }}
            </span>
            <span v-if="dataRate !== ''"
              class="font12">
              （{{ dataRate + '%' }}）
            </span>
          </div>
          <span class="font12"
            :class="{ mt10: !isShowAverage }">
            {{ dataDes }}
          </span>
        </div>
        <div v-if="isShowAverage"
          class="average-data">
          <div class="rate-container"
            @mouseenter="rateHover($event, 'average')">
            <span class="average-value width100 text-overflow"
              :title="tranNumber(averageData)">
              {{ tranNumber(averageData) }}
            </span>
            <span v-if="averageRate"
              class="font12">
              （{{ averageRate + '%' }}）
            </span>
          </div>
          <span class="font12">
            {{ averageDes }}
          </span>
        </div>
      </slot>
    </div>
  </div>
</template>

<script>
import { tranNumber } from '@/pages/datamart/common/utils.js';

export default {
  props: {
    name: String,
    unit: String,
    dataCount: [String, Number],
    dataDes: String,
    averageData: {
      type: [String, Number],
      default: 0,
    },
    averageDes: String,
    alertLevel: {
      type: String,
      default: 'normal',
    },
    isShowAverage: {
      type: Boolean,
      default: true,
    },
    dataRate: {
      type: [String, Number],
      default: '',
    },
    averageRate: [String, Number],
  },
  methods: {
    tranNumber(num, pointer = 3) {
      return tranNumber(num, pointer);
    },
    rateHover(e, type) {
      this.$emit('rateHover', e, type);
    },
  },
};
</script>

<style lang="scss" scoped>
.quality-index-container {
  width: 157px;
  min-height: 100px;
  border: 1px solid #c4c6cc;
  color: #313238;

  .header {
    display: flex;
    justify-content: center;
    align-items: flex-end;
    border-bottom: 1px solid #c4c6cc;
    padding: 10px;
    .title {
      font-size: 16px;
      font-weight: 550;
    }
    .unit {
      font-size: 12px;
    }
  }
  .content {
    .font12 {
      font-size: 12px;
    }
    .recent-data {
      width: 100%;
      display: flex;
      flex-direction: column;
      align-items: center;
      padding: 8px;
      .data-count {
        font-size: 20px;
      }
      .danger {
        color: #ea3636;
        font-weight: bold;
      }
      .normal {
        // color: #45e35f;
      }
    }
    .average-data {
      display: flex;
      flex-wrap: wrap;
      flex-direction: column;
      align-items: center;
      padding: 8px;
      .average-value {
        font-size: 20px;
        margin-bottom: 5px;
      }
    }
    .rate-container {
      width: 100%;
      display: flex;
      align-items: center;
      justify-content: center;
      flex-wrap: nowrap;
      .data-count,
      .average-value {
        max-width: 98px;
      }
    }
    .width100 {
      text-align: center;
    }
  }
  .content-no-average {
    height: calc(100% - 42px);
    display: flex;
    justify-content: center;
    align-items: center;
  }
}
</style>

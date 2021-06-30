

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
    <HeaderType>
      <div slot="title"
        class="header">
        剖析结果
        <span class="explain">
          共计<span class="primary-color">{{ count }}条</span>采样数据参与剖析
        </span>
      </div>
    </HeaderType>
    <!-- <div class="container">
            <div class="left-content">
                <i
                    class="icon-data-analyze mr10"
                    v-bk-tooltips="
                    $t(
                        '数据剖析是指数据集内部为达一致性、单值性和逻辑性而进行的数值质量的统计分析及评估；通过数据剖析，用户能够深入了解数据的结构，数值的有效性，分布等重要信息'
                    )
                "
                ></i>
                <h5>{{ $t('剖析结果') }}</h5>
                <div class="explain">
                    共计<span class="primary-color m5">{{ count }}条</span>采样数据参与剖析
                </div>
            </div>
            <bkdata-button
                theme="primary"
                disabled
                :title="$t('自定义剖析')"
                @click="openAnalyzeConfig"
            >
                {{ $t('自定义剖析') }}
            </bkdata-button>
            <bkdata-alert
                v-if="isSampling"
                type="info"
                title="当前剖析结果是基于最近1分钟到达的采样数据计算而来，无法保证此表数据完全符合当前剖析结论，如需准确的剖析结果，用户可自定义配置剖析，或在计算任务中配置剖析节点"
                closable
            ></bkdata-alert>
        </div> -->
  </div>
</template>

<script>
import HeaderType from '@/pages/datamart/common/components/HeaderType';

export default {
  components: {
    HeaderType,
  },
  props: {
    count: {
      type: Number,
      default: 0,
    },
    isSampling: {
      type: Boolean,
      default: false,
    },
  },
  methods: {
    openAnalyzeConfig() {
      this.$emit('openAnalyzeConfig');
    },
  },
};
</script>

<style lang="scss" scoped>
.header {
  display: inline-block;
  font-size: 14px;
  font-weight: 700;
  color: #63656e;
  line-height: 55px;
  .explain {
    font-size: 12px;
    margin-left: 10px;
  }
  .primary-color {
    color: #3a84ff;
  }
}
.container {
  position: relative;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 10px 20px;
  background-color: #f0f1f5;
  border-bottom: 1px solid #ddd;
  .left-content {
    display: flex;
    align-items: center;
    h5 {
      display: inline-block;
      font-weight: 700;
      font-size: 14px;
      margin-right: 10px;
    }
    .explain {
      display: inline-block;
      font-size: 12px;
      .primary-color {
        color: #3a84ff;
      }
    }
    .icon-data-analyze {
      font-size: 18px;
      color: #3a84ff;
    }
  }
  .bk-alert {
    position: absolute;
    left: 50%;
    top: 50%;
    transform: translate(-50%, -15%);
  }
}
</style>

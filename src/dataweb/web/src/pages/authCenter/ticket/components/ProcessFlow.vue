

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
  <div class="auth-process-flow clearfix">
    <div class="steps-container">
      <div class="step-box">
        <div class="step-item succeeded">
          <div class="step-title">
            {{ $t('提交申请') }}
          </div>
          <div class="step-processors">
            {{ applicant }}
          </div>
        </div>
      </div>
      <div class="step-link" />
      <template v-for="(s, index) in localSteps">
        <div :key="index"
          :class="['step-box', { 'step-box-processing': index === currentStep }]">
          <div v-for="(state, sIndex) in s.states"
            :key="sIndex"
            :class="[state.class]"
            class="step-item">
            <div class="step-title">
              {{ state.name }}
            </div>
            <div v-tooltip="state.processors.join(', ')"
              :title="state.processors.join(', ')"
              class="step-processors">
              {{ state.processors.join(',') }}
            </div>
          </div>
        </div>
        <div v-if="index < localSteps.length - 1"
          :key="index"
          class="step-link" />
      </template>
    </div>
  </div>
</template>

<script>
export default {
  props: {
    steps: {
      type: Array,
      required: true,
    },
    applicant: {
      type: String,
      required: true,
    },
    currentStep: {
      type: Number,
      default: 0,
    },
  },
  data() {
    return {
      localSteps: [
        {
          step: 1,
          states: [
            {
              name: '负责人审批',
              processors: ['user_a', 'user_b', 'user_c'],
            },
            {
              name: '负责人审批',
              processors: ['user_a', 'user_b', 'user_c'],
            },
            {
              name: '负责人驳回',
              processors: ['user_a'],
              class: 'failed',
            },
          ],
        },
        {
          step: 2,
          states: [
            {
              name: '负责人审批',
              processors: ['user_a', 'user_b', 'user_c'],
              class: 'succeeded',
            },
            {
              name: '负责人审批',
              processors: ['user_a', 'user_b', 'user_c'],
              class: 'default',
            },
            {
              name: '负责人审批',
              processors: ['user_a', 'user_b', 'user_c'],
              class: 'default',
            },
          ],
        },
      ],
    };
  },
  watch: {
    steps: {
      immediate: true,
      handler(newVal) {
        if (newVal) {
          this.localSteps = this.steps;
        }
      },
    },
    localSteps() {
      this.locateCurrentStep();
    },
  },
  methods: {
    locateCurrentStep() {
      // 自动定位到当前步骤的flow
      setTimeout(() => {
        let dom = document.getElementsByClassName('auth-process-flow')[0];
        if (dom) {
          // 每个步骤的宽度为170
          dom.scrollLeft = 170 * this.currentStep;
        }
      }, 200);
    },
  },
};
</script>

<style lang="scss" scoped>
@import '~@/common/scss/conf.scss';

.auth-process-flow {
  position: relative;
  width: 100%;
  overflow-x: auto;

  .steps-container {
    padding: 20px;
    margin-top: 10px;
    margin-bottom: 10px;
    font-size: 12px;
    white-space: nowrap;
    background: url('../../../../bkdata-ui/common/images/dataflow_back.png');
    .step-box {
      display: inline-block;
      width: 130px;
      padding: 10px;
      border: 2px dashed $primaryLightColor;
      vertical-align: top;

      .step-item {
        margin-bottom: 20px;
        border: 1px solid $primaryLightColor;

        .step-title {
          padding: 3px 8px;
          background: $primaryLightColor;
          text-align: center;
        }
        .step-processors {
          padding: 3px 8px;
          text-align: center;
          overflow: hidden;
          white-space: nowrap;
          text-overflow: ellipsis;
        }
      }

      .step-item.succeeded {
        border: 1px solid $successLightColor;

        .step-title {
          background: $successLightColor;
        }
      }

      .step-item.failed {
        border: 1px solid $dangerLightColor;

        .step-title {
          background: $dangerLightColor;
        }
      }

      .step-item.pending {
        border: 1px solid #eee;
        color: #ccc;

        .step-title {
          background: #eee;
        }
      }

      .step-item.stopped {
        border: 1px solid #eee;
        color: #ccc;

        .step-title {
          background: #eee;
        }
      }
    }

    .step-box-processing {
      border: 2px dashed $primaryActiveColor;
    }

    .step-link {
      display: inline-block;
      width: 40px;
      margin-top: 30px;
      border-bottom: 2px solid $primaryLightColor;
    }
  }
}
</style>

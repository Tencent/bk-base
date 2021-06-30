

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
  <div v-bkloading="{ isLoading }"
    class="model-right-body">
    <template v-if="isEmptyModel">
      <EmptyView class="empty-data"
        tips="请选择模型" />
    </template>
    <template v-else>
      <bkdata-steps
        id="modeManagerSteps"
        ref="steps"
        extCls="right-body-steps"
        :controllable="modelExisted"
        :steps="objectSteps"
        :curStep.sync="currentActiveStep"
        :beforeChange="handleStepBeforeChange" />
      <div :key="'main_' + modelId"
        class="model-right-content bk-scroll-y">
        <component
          :is="activeComponent"
          ref="stepComponent"
          :isLoading.sync="isLoading"
          :preNextBtnManage.sync="preNextBtnManage" />
      </div>
      <div :key="'footer_' + modelId"
        class="model-right-bottom">
        <template v-if="preNextBtnManage.isPreviewBtnVisible">
          <template v-if="preNextBtnManage.isPreNextBtnConfirm && !isSaving">
            <bkdata-popconfirm
              trigger="click"
              :extCls="'bk-data-model-confirm'"
              :confirmButtonIsText="false"
              :cancelButtonIsText="false"
              confirmText="保存并上一步"
              cancelText="直接上一步"
              @confirm="handleConfirmPreNext('preview', true)"
              @cancel="handleConfirmPreNext('preview', false)">
              <div slot="content"
                class="bkdmodel-confirm-content">
                <div class="title">
                  确认返回上一步？
                </div>
                <div class="content">
                  有未保存的修改，是否返回上一步
                </div>
              </div>
              <bkdata-button
                :theme="'default'"
                :disabled="!preNextBtnManage.isPreNextBtnEnable"
                extCls="data-model-stepbtn"
                class="mr10">
                {{ $t('上一步') }}
              </bkdata-button>
            </bkdata-popconfirm>
          </template>
          <template v-else>
            <bkdata-button
              :theme="'default'"
              :disabled="!preNextBtnManage.isPreNextBtnEnable || isSaving"
              extCls="data-model-stepbtn"
              class="mr10"
              @click="handlePreNextBtnClick('preview')">
              {{ $t('上一步') }}
            </bkdata-button>
          </template>
        </template>
        <template v-if="activeStep.isShowSaveBtn">
          <bkdata-button
            :theme="'primary'"
            extCls="data-model-stepbtn"
            class="mr10"
            :loading="isSaving"
            :disabled="saveBtnDisabled"
            @click="handleSubmitClick">
            {{ modelExisted ? activeStep.saveBtnText || $t('保存') : $t('创建模型') }}
          </bkdata-button>
        </template>

        <template v-if="preNextBtnManage.isNextBtnVisible">
          <template v-if="preNextBtnManage.isPreNextBtnConfirm && !isSaving">
            <bkdata-popconfirm
              trigger="click"
              :extCls="'bk-data-model-confirm'"
              :confirmButtonIsText="false"
              :cancelButtonIsText="false"
              confirmText="保存并下一步"
              cancelText="直接下一步"
              @confirm="handleConfirmPreNext('next', true)"
              @cancel="handleConfirmPreNext('next', false)">
              <div slot="content"
                class="bkdmodel-confirm-content">
                <div class="title">
                  确认进入下一步？
                </div>
                <div class="content">
                  有未保存的修改，是否进入下一步
                </div>
              </div>
              <bkdata-button
                :theme="'default'"
                :disabled="!preNextBtnManage.isPreNextBtnEnable || nextBtnDisabled"
                extCls="data-model-stepbtn"
                class="mr10">
                {{ $t('下一步') }}
              </bkdata-button>
            </bkdata-popconfirm>
          </template>
          <template v-else>
            <bkdata-button
              :theme="currentActiveStep === 4 || currentActiveStep === 3 ? 'primary' : 'default'"
              :disabled="!preNextBtnManage.isPreNextBtnEnable || isSaving || nextBtnDisabled"
              extCls="data-model-stepbtn"
              class="mr10"
              @click="handlePreNextBtnClick('next')">
              {{ $t('下一步') }}
            </bkdata-button>
          </template>
        </template>
      </div>
      <div
        v-show="stepConfirm.show"
        ref="stepConfirmTips"
        v-bkClickoutside="hideStepConfirmTips"
        class="step-confirm-tips">
        <div class="bkdmodel-confirm-content">
          <div class="title">
            {{ $t('确认离开当前步骤？') }}
          </div>
          <div class="content">
            {{ $t('有未保存的修改，是否离开当前步骤') }}
          </div>
        </div>
        <div class="popconfirm-operate">
          <bkdata-button :loading="isSaving"
            theme="primary"
            @click.stop="handleConfirmJump(true)">
            {{ $t('保存并跳转') }}
          </bkdata-button>
          <bkdata-button theme="primary"
            @click.stop="handleConfirmJump(false)">
            {{ $t('直接跳转') }}
          </bkdata-button>
        </div>
      </div>
    </template>
  </div>
</template>
<script lang="ts" src="./ModeRightBody.ts"></script>
<style lang="scss" scoped>
.model-right-body {
  padding: 24px 30px;
  min-height: 300px;
  height: calc(100% - 91px);

  .empty-data {
    min-height: 300px;
  }
  .right-body-steps {
    padding: 0 100px 32px 100px;
  }
  ::v-deep .bk-step {
    &:not(.current) {
      .bk-step-icon {
        color: #979ba5;
        background: #ffffff;
        border: 1px solid #c4c6cc;
      }
    }

    &.current {
      .bk-step-content {
        .bk-step-title {
          color: #313238;
        }
      }
    }

    .bk-step-content {
      .bk-step-title {
        color: #63656e;
      }
    }
  }
  .model-right-content {
    text-align: center;
    margin: 0 auto;
    height: calc(100% - 84px);
  }

  .model-right-bottom {
    position: absolute;
    left: 0;
    right: 0;
    bottom: 0;
    height: 52px;
    background: #ffffff;
    border-top: 1px solid #dcdee5;

    display: flex;
    justify-content: flex-end;
    align-items: center;

    .data-model-stepbtn {
      width: 120px;
    }
  }
}
</style>
<style lang="scss">
.bk-data-model-confirm {
  .tippy-content {
    width: 220px;
  }
  .bkdmodel-confirm-content {
    .title {
      height: 21px;
      font-size: 16px;
      font-family: MicrosoftYaHei;
      text-align: left;
      color: #313238;
      line-height: 21px;
    }

    .content {
      height: 18px;
      font-size: 12px;
      font-family: MicrosoftYaHei;
      text-align: left;
      color: #63656e;
      line-height: 18px;
      margin: 10px 0;
    }
  }
  .bk-popconfirm-content.popconfirm-more .popconfirm-operate {
    text-align: left;
  }
  .popconfirm-operate {
    button {
      width: 96px;
      height: 24px;
      line-height: 22px;
      font-size: 12px;
      border-radius: 2px;
      margin-right: 10px;

      &.default-operate-button {
        height: 24px;
        border-radius: 2px;
        padding: 1px 6px;
        margin-left: 0;
        &:hover {
          color: #ffffff;
        }
      }

      &:last-child {
        width: 84px;
        height: 24px;
        background: #ffffff;
        border: 1px solid #c4c6cc;
        border-radius: 2px;
        font-size: 12px;
        font-family: MicrosoftYaHei;
        text-align: left;
        color: #63656e;
        line-height: 16px;
        text-align: center;
      }
    }
  }

  .step-confirm-tips {
    padding: 10px 6px;
  }
}
</style>

/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
 */

import { Component, Emit, Prop, PropSync, Ref, Vue, Watch } from 'vue-property-decorator';
import { DataModelManageBase } from '../../Controller/DataModelManageBase';

export namespace DataModelManage {
  export interface IStepBtnStatu {
    /** 上一步按钮是否可见 */
    isPreviewBtnVisible: boolean;

    /** 下一步按钮是否可见 */
    isNextBtnVisible: boolean;

    /** 上一步|下一步按钮是否可用 */
    isPreNextBtnEnable: boolean;

    /** 上一步|下一步按钮是否需要二次确认 */
    isPreNextBtnConfirm: boolean;

    /** 当前激活Step ID */
    currentActiveStep: number;
  }

  export class IStepsManage extends DataModelManageBase {
    /** 上一步、下一步、保存按钮状态 */
    @PropSync('preNextBtnManage', {
      default: () => ({
        /** 上一步按钮是否可见 */
        isPreviewBtnVisible = true,

        /** 下一步按钮是否可见 */
        isNextBtnVisible = true,

        /** 上一步|下一步按钮是否可用 */
        isPreNextBtnEnable = true,

        /** 上一步|下一步按钮是否需要二次确认 */
        isPreNextBtnConfirm = false,

        /** 当前激活Step ID */
        currentActiveStep = 1,
      }),
    })
    public syncPreNextBtnManage: IStepBtnStatu;

    /**
     * 提交表单事件
     * 所有Step组件都需要实现此方法
     */
    public submitForm(): Promise<boolean> {
      return Promise.resolve(true);
    }

    /**
     * 下一步事件
     * 可选择性实现方法
     */
    public nextStepClick(): void { }

    /**
     * 初始化上一步、下一步、保存按钮状态
     * @param isPreviewBtnVisible 上一步按钮是否可见 默认 true
     * @param isPreNextBtnEnable 上一步|下一步按钮是否可用 默认 true
     * @param isNextBtnVisible 下一步按钮是否可见 默认 true
     * @param isPreNextBtnConfirm 上一步|下一步按钮是否需要二次确认 默认 false
     */
    public initPreNextManage(
      isPreviewBtnVisible = true,
      isPreNextBtnEnable = true,
      isNextBtnVisible = true,
      isPreNextBtnConfirm = false
    ) {
      Object.assign(this.syncPreNextBtnManage, {
        isPreviewBtnVisible,
        isPreNextBtnEnable,
        isNextBtnVisible,
        isPreNextBtnConfirm,
      });
    }

    /**
     * 更新当前激活Step
     * @param stepIndex 当前激活Index 从1开始
     */
    public updateActiveStep(stepIndex: number) {
      this.syncPreNextBtnManage.currentActiveStep = stepIndex;
    }
  }
}

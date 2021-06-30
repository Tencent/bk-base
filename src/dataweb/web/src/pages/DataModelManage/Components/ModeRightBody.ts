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

import { EmptyView } from '@/pages/DataModelManage/Components/Common/index';
import { Component, Ref, Watch } from 'vue-property-decorator';
import { DataModelManageBase } from '../Controller/DataModelManageBase';
import { DataModelPreview, DataModelRelease, IndexDesign, MainTableDesign, ModelInfo } from './Steps/index';
import { DataModelManage } from './Steps/IStepsManage';
@Component({
  components: { DataModelPreview, DataModelRelease, IndexDesign, MainTableDesign, ModelInfo, EmptyView },
})
export default class ModeRightBody extends DataModelManageBase {
  get isEmptyModel() {
    return !(this.modelId >= 0);
  }

  get activeStep() {
    return this.objectSteps[this.currentActiveStep - 1];
  }

  get activeComponent() {
    return this.activeStep.comp();
  }

  get cancelKey() {
    return `getModelInfo${this.modelId}`;
  }

  get currentActiveStep() {
    return this.preNextBtnManage.currentActiveStep;
  }

  set currentActiveStep(val: number) {
    this.preNextBtnManage.currentActiveStep = val;
  }

  get curStepNode() {
    return this.stepNodes[this.currentActiveStep - 1];
  }

  get modelExisted() {
    return this.activeModelTabItem && !this.activeModelTabItem.isNew;
  }

  get published() {
    return this.activeModelTabItem && !(this.activeModelTabItem.publishStatus === 'developing');
  }

  public isLoading = false;
  public saveBtnDisabled = false;
  public nextBtnDisabled = false;
  @Ref() public readonly steps!: bkdataSteps;

  /** 步骤组件 */
  @Ref('stepComponent') public readonly refStepComponent!: DataModelManage.IStepsManage;

  @Ref() public readonly stepConfirmTips!: HTMLDivElement;

  public preNextBtnManage = {
    /** 上一步按钮是否可用 */
    isPreviewBtnVisible: true,

    /** 下一步按钮是否可用 */
    isNextBtnVisible: true,

    /** 上一步|下一步按钮是否可用 */
    isPreNextBtnEnable: true,

    /** 上一步|下一步按钮是否需要二次确认 */
    isPreNextBtnConfirm: false,

    /** 当前激活Step ID */
    currentActiveStep: 1,
  };

  public stepNodes = [];

  public stepConfirm = {
    show: false,
    instance: null,
    resolve: null,
    reject: null,
    next: null,
  };

  public isSaving = false;
  /** 是否显示二次弹框确认 */
  public showConfirm = false;

  public iconSteps = ['edit2', 'image', 'check-1'];

  public objectSteps = [
    {
      title: '模型信息',
      route: 'modelinfo',
      icon: 1,
      isLoading: true,
      id: 'setBaseInfo',
      finished: false,
      isShowSaveBtn: true,
      comp: () => ModelInfo,
    },
    {
      title: '主表设计',
      route: 'sampleSets',
      icon: 2,
      isLoading: true,
      id: 'selectSample',
      finished: false,
      isShowSaveBtn: true,
      comp: () => MainTableDesign,
    },
    {
      title: '指标设计',
      route: 'experiment',
      icon: 3,
      isLoading: true,
      id: 'buildExperiment',
      finished: false,
      isShowSaveBtn: false,
      comp: () => IndexDesign,
    },
    {
      title: '模型预览',
      route: 'preview',
      icon: 4,
      isLoading: true,
      id: 'preiew',
      finished: false,
      isShowSaveBtn: false,
      comp: () => DataModelPreview,
    },
    {
      title: '模型发布',
      route: 'publish',
      icon: 5,
      isLoading: true,
      id: 'releaseModel',
      finished: false,
      isShowSaveBtn: true,
      saveBtnText: this.$t('发布'),
      comp: () => DataModelRelease,
    },
  ];
  @Watch('isEmptyModel', { immediate: true })
  public handleIsEmptyModel(isEmpty: boolean) {
    if (!isEmpty) {
      this.$nextTick(() => {
        this.steps.isDone = this.stepsDone;
        this.steps.$forceUpdate();
      });
    }
  }

  @Watch('modelId', { immediate: true })
  public handleModelIdChanged() {
    this.currentActiveStep = (this.activeModelTabItem && this.activeModelTabItem.activeStep) || 1;
    this.saveBtnDisabled = false;
    this.nextBtnDisabled = false;
  }

  @Watch('currentActiveStep')
  public updateTabActiveStep(val: number, old: number) {
    val && old && this.sendUserActionData({ name: `从 S${old} 进入 S${val}` });
    this.DataModelTabManage.updateTabActiveStep(val);
    this.saveBtnDisabled = false;
    this.nextBtnDisabled = false;
  }

  /**
   * steps 设置是否高亮处理
   */
  public stepsDone(index: number): boolean {
    if (!this.activeModelTabItem) return false;
    const highlight = index < this.activeModelTabItem.lastStep;
    // 设置鼠标手状态
    this.stepNodes[index] && (this.stepNodes[index].style.cursor = 'pointer');
    // 维度表可跳过step3
    if (this.activeModelTabItem.modelType === 'dimension_table' && this.activeModelTabItem.lastStep === 2) {
      this.stepNodes[this.stepNodes.length - 1].style.cursor = 'unset';
      return highlight;
    }
    // 查看上一个是否为高亮且当前不是高亮的情况，也可以点击
    const prevStatus = index - 1 < this.activeModelTabItem.lastStep;
    if (!highlight && !prevStatus && this.stepNodes[index]) {
      this.stepNodes[index].style.cursor = 'unset';
    }
    return highlight;
  }

  public handleSubmitClick() {
    this.isSaving = true;
    // 上报数据
    switch (this.currentActiveStep) {
      case 1:
        !this.modelExisted && this.sendUserActionData({ name: '点击【创建模型】' });
        break;
      case 2:
      case 5:
        const reportName = {
          '2': '保存【S2】',
          '5': '点击【发布】',
        };
        this.sendUserActionData({ name: reportName[this.currentActiveStep] });
        break;
    }
    return this.refStepComponent
      .submitForm()
      ['catch'](err => console.log(err))
      ['finally'](next => {
        this.isSaving = false;
      });
  }

  /**
   * 点击下一步|上一步按钮操作
   * @param type 操作类型
   */
  public handlePreNextBtnClick(type = 'next') {
    // 点击下一步可以做组件内一下处理
    if (typeof this.refStepComponent.nextStepClick === 'function' && type === 'next') {
      this.refStepComponent.nextStepClick();
    }
    const newStep = type === 'next' ? this.currentActiveStep + 1 : this.currentActiveStep - 1;
    this.activePreNextStep(newStep);
  }

  /**
   *
   * @param type
   * @param isSave
   */
  public handleConfirmPreNext(type = 'next', isSave = true) {
    if (isSave) {
      this.handleSubmitClick().then(res => {
        if (res) {
          this.handlePreNextBtnClick(type);
        }
      });
    } else {
      this.handlePreNextBtnClick(type);
    }
  }

  /**
   * 激活上一步/下一步操作
   */
  public activePreNextStep(step: number) {
    if (this.currentActiveStep <= 5 && step >= 0 && step <= 5) {
      this.currentActiveStep = step;
    }
  }

  public async handleConfirmJump(isSave = true) {
    if (isSave) {
      const res = await this.handleSubmitClick();
      if (!res) {
        this.hideStepConfirmTips();
        return false;
      }
    }
    this.activePreNextStep(this.stepConfirm.next);
    this.stepConfirm.resolve(true);
    this.hideStepConfirmTips();
  }

  public changeStepComfirm(next: number) {
    return new Promise((resolve, reject) => {
      this.stepConfirm.instance && this.stepConfirm.instance.destroy();
      this.stepConfirm.instance = this.$bkPopover(this.curStepNode, {
        content: this.stepConfirmTips,
        theme: 'light',
        zIndex: 9999,
        trigger: 'manual',
        boundary: 'window',
        arrow: true,
        interactive: true,
        extCls: 'bk-data-model-confirm',
        placement: 'bottom',
      });
      this.stepConfirm.show = true;
      this.stepConfirm.resolve = resolve;
      this.stepConfirm.reject = reject;
      this.stepConfirm.next = next;
      this.$nextTick(() => {
        this.stepConfirm.instance.show();
      });
    });
  }

  /**
   * 跳转前做校验
   */
  public async handleStepBeforeChange(next: number) {
    if (this.activeModelTabItem.isNew || this.isSaving) return false;

    // 维度表可跳过step3
    if (this.activeModelTabItem.modelType === 'dimension_table' && this.activeModelTabItem.lastStep === 2) {
      return (this.preNextBtnManage.isPreNextBtnConfirm && (await this.changeStepComfirm(next))) || next < 5;
    }

    // 最多可跳转到已完成步骤的下个步骤
    if (next <= this.activeModelTabItem.lastStep + 1) {
      return (this.preNextBtnManage.isPreNextBtnConfirm && (await this.changeStepComfirm(next))) || true;
    }
  }

  public hideStepConfirmTips() {
    this.stepConfirm.instance && this.stepConfirm.instance.hide();
    this.$nextTick(() => {
      this.stepConfirm.show = false;
    });
  }

  public mounted() {
    this.isMounted = true;
    this.stepNodes = Array.from(document.querySelectorAll('#modeManagerSteps .bk-step-indicator'));
  }
}

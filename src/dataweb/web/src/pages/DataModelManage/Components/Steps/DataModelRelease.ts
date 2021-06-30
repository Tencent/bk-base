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

import { Component, Ref } from 'vue-property-decorator';
import { DataModelManage } from './IStepsManage';
import { getModelVersionDiff, releaseModelVersion, getFieldContraintConfigs } from '../../Api';
import EmptyView from '@/components/emptyView/EmptyView.vue';
import { VNode } from 'vue/types/umd';
import { BizDiffContents } from '../Common/index';
@Component({
  components: { EmptyView, BizDiffContents },
})
export default class DataModelRelease extends DataModelManage.IStepsManage {
  @Ref() public readonly modelInfoForm!: VNode;

  get releaseTips() {
    const text = this.activeModelTabItem.isNew
      ? '发布后会创建一个版本，可以在数据开发中应用。'
      : '将本次修改内容置于最新版本。';
    return this.$t(text);
  }
  public onlyShowDiff = true;
  public diffData = {};
  public newContents: object = {};
  public origContents: object = {};
  public fieldContraintConfigList: object[] = [];
  public hasDiff = false;
  public isDiffLoading = true;
  public formData: object = {
    log: '',
  };
  public rules: object = {
    log: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
    ],
  };
  public comfirmRelease = {
    isShow: false,
    isSubmiting: false,
  };

  public getFieldContraintConfigs() {
    getFieldContraintConfigs().then(res => {
      res.setData(this, 'fieldContraintConfigList');
    });
  }

  public getModelVersionDiff() {
    this.isDiffLoading = true;
    getModelVersionDiff(this.modelId)
      .then(res => {
        res.setDataFn(data => {
          const { diff = {}, newContents = {}, origContents = {} } = data || {};
          this.diffData = diff;
          this.newContents = newContents;
          this.origContents = origContents;
          this.hasDiff = diff?.diffResult?.hasDiff ? true : false;
          // 禁用发布按钮
          if (!this.hasDiff) {
            this.$parent.saveBtnDisabled = true;
          }
        });
      })
      ['finally'](() => {
        this.isDiffLoading = false;
      });
  }

  public handleGoApplication() {
    let routeData = this.$router.resolve({
      name: 'dataflow_ide',
      query: {
        project_id: this.projectId,
      },
    });
    window.open(routeData.href, '_blank');
  }

  public handleConfirmRelease() {
    this.modelInfoForm.validate().then(validate => {
      this.comfirmRelease.isSubmiting = true;
      releaseModelVersion(this.modelId, this.formData.log)
        .then(res => {
          if (res.validateResult()) {
            const activeTabItem = this.DataModelTabManage.getActiveItem()[0];
            activeTabItem.lastStep = res.data.model_content.step_id;
            activeTabItem.publishStatus = res.data.model_content.publish_status;
            // 重置到第一步
            activeTabItem.activeStep = 1;
            this.DataModelTabManage.updateTabItem(activeTabItem);
            this.DataModelTabManage.dispatchEvent('updateModel', [
              {
                model_id: res.data.model_id,
                publish_status: res.data.model_content.publish_status,
                step_id: res.data.model_content.step_id,
              },
            ]);

            this.formData.log = '';
            this.comfirmRelease.isShow = false;
            this.getModelVersionDiff();
            this.changeRouterWithParams('dataModelView', {}, { showReleaseDialog: true });
          }
        })
        ['catch'](err => console.log(err))
        ['finally'](() => {
          this.comfirmRelease.isSubmiting = false;
        });
    });
  }

  public handleCancelRelease() {
    this.formData.log = '';
    this.comfirmRelease.isShow = false;
    this.comfirmRelease.isSubmiting = false;
    this.modelInfoForm.clearError();
  }

  public submitForm() {
    this.comfirmRelease.isShow = true;
    return Promise.resolve(true);
  }

  public async created() {
    this.initPreNextManage(true, true, false);
    await this.getFieldContraintConfigs();
    this.getModelVersionDiff();
  }
}

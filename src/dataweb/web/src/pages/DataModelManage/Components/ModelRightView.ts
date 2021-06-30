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

import { Component, Prop } from 'vue-property-decorator';
import { DataModelManageBase } from '../Controller/DataModelManageBase';
import SingleCollapse from '@/components/singleCollapse/SingleCollapse.vue';
import ModelInfoView from './views/ModelInfoView.vue';
import MainTableDesignView from './views/MainTableDesignView.vue';
import DataModelPreviewView from './views/DataModelPreviewView.vue';
import IndexDesignView from './views/IndexDesignView.vue';
import { getModelVersionDiff, getFieldContraintConfigs, getModelViewData } from '../Api';
import { BizDiffContents } from './Common/index';
import { EmptyView } from '@/pages/DataModelManage/Components/Common/index';

@Component({
  components: {
    SingleCollapse,
    BizDiffContents,
    ModelInfoView,
    MainTableDesignView,
    IndexDesignView,
    DataModelPreviewView,
    EmptyView,
  },
})
export default class ModelRightView extends DataModelManageBase {
  @Prop({ default: 'preview' }) readonly activeView!: string;

  get isEmptyModel() {
    return !(this.modelId >= 0);
  }

  get isUnpublished() {
    return [undefined, 'developing'].includes(this.activeModelTabItem?.publishStatus);
  }

  get versionInfo() {
    const { createdAt, createdBy } = this.origContents;
    return createdAt && createdBy ? `${createdBy} (${createdAt})` : '';
  }

  get viewList() {
    const masterTableInfo = {
      fields: this.modelViewData?.modelDetail?.fields || [],
      model_relation: this.modelViewData?.modelDetail?.modelRelation || [],
      model_id: this.modelViewData?.modelId,
      publish_status: this.modelViewData?.publishStatus,
      step_id: this.modelViewData?.stepId,
    };
    return [
      {
        name: this.$t('基本信息'),
        id: 'basicInfo',
        component: ModelInfoView,
        showEditBtn: false,
        data: this.modelViewData,
      },
      {
        name: this.$t('主表信息'),
        id: 'masterInfo',
        component: MainTableDesignView,
        showEditBtn: true,
        step: 2,
        updateCount: 0,
        data: {
          masterTableInfo,
          fieldContraintConfigList: this.fieldContraintConfigList,
        },
      },
      {
        name: this.$t('指标信息'),
        id: 'indicatorInfo',
        component: IndexDesignView,
        showEditBtn: true,
        step: 3,
        updateCount: 0,
        data: this.modelViewData?.modelDetail?.calculationAtoms || [],
      },
    ];
  }

  public isLoading = false;
  public isShowReleaseDialog = false;
  public modelViewData = {};
  public diffData = {};
  public newContents: object = {};
  public origContents: object = {};
  public fieldContraintConfigList: object[] = [];
  public diffSlider: object = {
    isShow: false,
    newContents: {},
    originContents: {},
  };
  public collapsedMap = {
    basicInfo: true,
    masterInfo: true,
    indicatorInfo: true,
  };

  public handleGoEditStep(step: number) {
    // 激活编辑步骤
    this.DataModelTabManage.updateTabActiveStep(step);
    this.$router.push({
      name: 'dataModelEdit',
      params: this.routeParams,
      query: this.routeQuery,
    });
  }

  public handleShowDiffSlider(id: string) {
    const contents = {
      new: [],
      origin: [],
    };
    if (id === 'masterInfo') {
      contents['new'] = (this.newContents.objects || []).filter(item => item.objectType === 'master_table');
      contents.origin = (this.origContents.objects || []).filter(item => item.objectType === 'master_table');
    } else if (id === 'indicatorInfo') {
      contents['new'] = (this.newContents.objects || [])
        .filter(item => ['indicator', 'calculation_atom'].includes(item.objectType)
        );
      contents.origin = (this.origContents.objects || [])
        .filter(item => ['indicator', 'calculation_atom'].includes(item.objectType)
        );
    } else {
      contents['new'] = this.newContents.objects || [];
      contents.origin = this.origContents.objects || [];
    }
    this.diffSlider.newContents = Object.assign({}, this.newContents, { objects: contents['new'] });
    this.diffSlider.originContents = Object.assign({}, this.origContents, { objects: contents.origin });
    this.diffSlider.isShow = true;
  }

  public getFieldContraintConfigs() {
    getFieldContraintConfigs().then(res => {
      res.setData(this, 'fieldContraintConfigList');
    });
  }

  public getModelVersionDiff() {
    getModelVersionDiff(this.modelId).then(res => {
      if (res.result) {
        const { diff = {}, newContents = {}, origContents = {} } = res.data || {};
        const { calculationAtom, indicator, masterTable } = res.data?.diff?.diffResult || {};
        this.diffData = diff;
        this.newContents = newContents;
        this.origContents = origContents;
        // 指标更新数量
        if (calculationAtom || indicator) {
          const viewIndex = this.viewList.findIndex(view => view.id === 'indicatorInfo');
          if (viewIndex >= 0) {
            const view = this.viewList[viewIndex];
            view.updateCount = calculationAtom + indicator;
            this.viewList.splice(viewIndex, 1, view);
          }
        }
        // 主表更新数量
        if (masterTable) {
          const viewIndex = this.viewList.findIndex(view => view.id === 'masterInfo');
          if (viewIndex >= 0) {
            const view = this.viewList[viewIndex];
            view.updateCount = masterTable;
            this.viewList.splice(viewIndex, 1, view);
          }
        }
      }
    });
  }

  public getModelViewData() {
    this.isLoading = true;
    getModelViewData(this.modelId, 'existed_in_stage')
      .then(res => {
        res.setData(this, 'modelViewData');
      })
      ['finally'](() => {
        this.isLoading = false;
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

  public async created() {
    await this.getFieldContraintConfigs();
    this.getModelVersionDiff();
    this.modelId && this.getModelViewData();
    this.isShowReleaseDialog = this.routeParams.showReleaseDialog;
    this.appendRouter(Object.assign(this.routeParams, { showReleaseDialog: undefined }));
  }
}

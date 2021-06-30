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

import { Component, Emit, Prop, PropSync, Vue, Watch, Provide, Ref } from 'vue-property-decorator';
import { VNode } from 'vue/types/umd';
import { updateModel } from '../Api/index';
import { DataModelManageBase } from '../Controller/DataModelManageBase';
import { IModeRightTabItem, IModeRightTabItems } from '../Interface/index';
import ModeRightBody from './ModeRightBody';

@Component({
  components: {
    ModeRightBody,
  },
})
export default class ModeRight extends DataModelManageBase {
  get isEditModel() {
    return this.routeName === 'dataModelEdit';
  }

  get isOperationModel() {
    return this.routeName === 'dataModelOperation';
  }

  get tabList() {
    return this.DataModelTabManage.tabManage.items || [];
  }

  get activeTabitem() {
    return this.tabList.find(item => !!item.isActive);
  }

  get isEmptyTab() {
    return !this.activeTabitem || !this.activeTabitem.name;
  }

  get activeModeDisplayName() {
    return this.activeTabitem ? this.activeTabitem.displayName : 'Undefined';
  }

  set activeModeDisplayName(val: string) {
    this.activeTabitem.displayName = val;
  }

  get publishStatus() {
    return this.activeTabitem?.publishStatus || 'developing';
  }

  get published() {
    return this.activeTabitem && !(this.activeTabitem.publishStatus === 'developing');
  }

  get activeModelTypeTips() {
    const textMap = {
      fact_table: this.$t('事实表数据模型'),
      dimension_table: this.$t('维度表数据模型'),
    };
    return textMap[this.activeTabitem.modelType] || this.$t('事实表数据模型');
  }

  @Ref() public readonly modeRightBody!: VNode;
  @Ref() public readonly modelRenameInput!: VNode;

  public status = {
    developing: '未发布',
    published: '已发布',
    're-developing': '发布有修改',
  };

  public activeViewModel: string = 'preview';

  /** 表名是否正在编辑 */
  public isModeNameEdit = false;

  public isLoading = false;

  /** 操作项列表 */
  public rightActions = [
    {
      name: '数据模型视图',
      icon: 'icon-model-info',
      disabled: true,
      callFn: item => {
        console.log(item);
      },
    },
    {
      name: '导入',
      icon: 'icon-import',
      disabled: true,
      callFn: item => {
        console.log(item);
      },
    },
    {
      name: '导出',
      icon: 'icon-export',
      disabled: true,
      callFn: item => {
        console.log(item);
      },
    },
    {
      name: '操作记录',
      icon: 'icon-history-2',
      disabled: true,
      callFn: item => {
        console.log(item);
      },
    },
    {
      name: '应用列表',
      icon: 'icon-application',
      disabled: true,
      callFn: item => {
        console.log(item);
      },
    },
  ];
  @Provide()
  public activeTabItem = () => this.activeTabitem;

  @Emit('activeTabChanged')
  public handleActivetabChanged(activeTab: IModeRightTabItem) {
    return activeTab;
  }

  @Watch('routeName')
  public handleRouteNameChange(name: string) {
    const activeTabItem = this.activeModelTabItem;
    if (name && activeTabItem) {
      activeTabItem.routeName = name;
      this.DataModelTabManage.updateTabItem(activeTabItem);
    }
  }

  @Watch('modelId')
  public handleModelIdChange() {
    this.activeViewModel = 'preview';
  }

  /**
   * handleTabClose
   * @param item IDataModelManage.IModeRightTabItem
   * @returns
   */
  public handleTabClose(item: IDataModelManage.IModeRightTabItem) {
    this.DataModelTabManage.removeItem(item);
    if (!item.isActive) {
      return;
    }
    const activeItem = this.DataModelTabManage.activeItemByIndex(0);
    if (activeItem) {
      this.changeRouterWithParams(
        this.activeTabitem?.routeName || 'dataModelView',
        { project_id: this.activeTabitem.projectId },
        { modelId: this.activeTabitem.modelId }
      );
      this.updateStorageModelId(this.activeTabitem.id);
      this.updateStorageProjectId(this.activeTabitem.projectId);
      this.handleActivetabChanged(this.activeTabitem);
    } else {
      this.changeRouterWithParams('dataModelView', { project_id: undefined }, { modelId: undefined });
    }
  }

  /**
   * handleLeaveConfirm
   * @returns
   */
  public async handleLeaveConfirm() {
    let res = true;
    if (this.isEditModel && this.modeRightBody.preNextBtnManage.isPreNextBtnConfirm) {
      res = await new Promise((resolve, reject) => {
        this.$bkInfo({
          title: this.$t('当前数据模型有修改，确认保存？'),
          subTitle: this.$t('取消操作可能会导致当前编辑的内容消失'),
          extCls: 'bk-dialog-sub-header-center',
          closeIcon: false,
          confirmLoading: true,
          width: 440,
          confirmFn: async () => {
            try {
              const confimRes = await this.modeRightBody.handleSubmitClick();
              if (confimRes) {
                resolve(true);
              } else {
                resolve(false);
              }
            } catch (e) {
              resolve(false);
            }
          },
          cancelFn: () => resolve(true),
        });
      });
    }
    return res;
  }

  /**
   * handleTabClose
   * @param item IDataModelManage.IModeRightTabItem
   * @returns
   */
  public async handleTabClose(item: IDataModelManage.IModeRightTabItem) {
    const res = await this.handleLeaveConfirm();
    if (res === false) {
      return;
    }
    this.DataModelTabManage.removeItem(item);
    if (!item.isActive) {
      return;
    }
    const activeItem = this.DataModelTabManage.activeItemByIndex(0);
    if (activeItem) {
      this.changeRouterWithParams(
        this.activeTabitem?.routeName || 'dataModelView',
        { project_id: this.activeTabitem.projectId },
        { modelId: this.activeTabitem.modelId }
      );
      this.updateStorageModelId(this.activeTabitem.id);
      this.updateStorageProjectId(this.activeTabitem.projectId);
      this.handleActivetabChanged(this.activeTabitem);
    } else {
      this.changeRouterWithParams('dataModelView', { project_id: undefined }, { modelId: undefined });
    }
  }

  public handleCloseAllTabs() {
    this.DataModelTabManage.clearTabItems();
    this.changeRouterWithParams('dataModelView', { project_id: undefined }, { modelId: undefined });
  }

  /**
   * handleTabItemClick
   * @param {IDataModelManage.IModeRightTabItem} item
   * @returns
   */
  public async handleTabItemClick(item: IDataModelManage.IModeRightTabItem) {
    if (this.activeTabitem?.modelId === item.modelId) {
      return;
    }
    const res = await this.handleLeaveConfirm();
    if (res === false) {
      return;
    }
    this.DataModelTabManage.activeItem(item);
    this.changeRouterWithParams(
      this.activeModelTabItem?.routeName || 'dataModelView',
      { project_id: item.projectId },
      { modelId: item.modelId }
    );
    this.updateStorageModelId(item.id);
    this.updateStorageProjectId(item.projectId);
    this.handleActivetabChanged(item);
  }

  public handleEditModeName() {
    this.isModeNameEdit = true;
    this.$nextTick(() => {
      this.modelRenameInput && this.modelRenameInput.focus();
    });
  }

  /**
   * handleEditNameBlur
   * @param {string} name
   */
  public handleEditNameBlur(name: string) {
    this.isLoading = true;
    updateModel(this.modelId, name)
      .then(res => {
        if (res.validateResult()) {
          this.DataModelTabManage.updateTabItem(this.activeTabitem);
          this.DataModelTabManage.dispatchEvent('updateModelName', [res.data]);
          this.isModeNameEdit = false;
        }
      })
      .finally(() => {
        this.isLoading = false;
      });
  }

  /**
   * handleChangeModel
   * @param name
   */
  public handleChangeModel(name: string) {
    const routeChange = () => this.$router.push({
      name,
      params: this.routeParams,
      query: this.routeQuery,
    });
    name && routeChange();
  }

  /**
   * handleGoBack
   */
  public handleGoBack() {
    this.$router.push({
      name: 'dataModelView',
      params: this.routeParams,
      query: this.routeQuery,
    });
  }

  /**
   * handleChangeView
   * @param view
   */
  public handleChangeView(view: string) {
    this.activeViewModel = view;
  }
}

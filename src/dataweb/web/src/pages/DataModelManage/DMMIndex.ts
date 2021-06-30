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

import { getMineProjectList } from '@/api/common';
import { generateId } from '@/common/js/util';
import PermissionApplyWindow from '@/pages/authCenter/permissions/PermissionApplyWindow';
import { EmptyView } from '@/pages/DataModelManage/Components/Common/index';
import { MenuItemList, ModelLeftMenu, ModeRight } from '@/pages/DataModelManage/Components/index';
import { Component, Ref, Watch } from 'vue-property-decorator';
import { VNode } from 'vue/types/umd';
import { cancelTop, deleteModel, getModelList, topModel } from './Api/index';
import { DataModelManageBase } from './Controller/DataModelManageBase';
import { IUpdateEvent, TabItem } from './Controller/DataModelTabManage';
@Component({
  components: { ModelLeftMenu, MenuItemList, EmptyView, ModeRight, PermissionApplyWindow },
})
export default class DMMIndex extends DataModelManageBase {
  @Ref() public readonly projectApply!: VNode;

  public projectList = [];

  public topTabList = [
    { name: 'project', label: '项目' },
    { name: 'market', label: this.$t('公开'), disabled: true, tips: '功能暂未开放' },
  ];
  /** 顶部tab选中| 项目或者数据集市 */
  public activeTypeName = 'project';

  /** fact_table/dimension_table **/
  public leftTabList = [
    { name: 'fact_table', label: '事实表数据模型', icon: 'fact-model' },
    { name: 'dimension_table', label: '维度表数据模型', icon: 'dimension-model' },
  ];

  /** 模型列表是否正在加载 */
  public isLoadingModelList = false;

  /** 模型类型选中 */
  public activeModelType = 'fact_table';

  /** 项目-模型列表 */
  public pDataSource: IDataModelManage.IModelList[] = [];

  /** 数据集市-模型列表 */
  public sDataSource: IDataModelManage.IModelList[] = [];

  /** 用于标识：项目-模型列表是否已经拉取，避免多次请求 */
  public isPDatasourceLoad = false;

  /** 用于标识：数据集市-模型列表是否已经拉取，避免多次请求 */
  public isSDatasourceLoad = false;

  /** 搜索值 */
  public searchValue = '';

  /** 选择项目Tab时，选择项目ID */
  public pProjectId: number | string = '';

  /** 初始化activeItem */
  public isInitActiveItem = false;

  public isShowLeftMenu = true;

  @Watch('pProjectId', { immediate: true })
  public handleProjectIdChanged(val) {
    this.getModelInfoByProjectId();
  }

  @Watch('updateEvents', { deep: true, immediate: true })
  public handleUpdateEventFired(val: IUpdateEvent) {
    /**
        /* updateModelName : 事件来自右侧顶部工具栏更新模型名称
         * updateModel : 事件来自右侧表单更新触发
         */
    if (val && (val.name === 'updateModel' || val.name === 'updateModelName')) {
      this.handleUpdateModel(val.params[0]);
    }
  }

  /** 根据当前选中Tab类型，计算当前模型列表数据 */
  get modelInfoList() {
    return this.isProject ? this.pDataSource : this.sDataSource;
  }

  /** 根据model_type过滤当前列表 */
  get filterTypeData() {
    return this.modelInfoList.filter(info => info.model_type === this.activeModelType);
  }

  get searchList() {
    return this.filterTypeData.filter(
      item => String.prototype.includes.call(item.model_name, this.searchValue)
                || String.prototype.includes.call(item.model_alias, this.searchValue)
    );
  }

  get isProject() {
    return this.activeTypeName === 'project';
  }

  get activeLeftTab() {
    return this.leftTabList.find(tab => tab.name === this.activeModelType) || {};
  }

  /** 置顶数据 */
  get pinedList() {
    return this.searchList.filter(item => item.sticky_on_top);
  }

  /** 不置顶数据 */
  get unPinedList() {
    return this.searchList.filter(item => !item.sticky_on_top);
  }

  /** 设置左侧栏收起/展开 icon */
  get expandIcon() {
    return this.isShowLeftMenu ? 'icon-shrink-fill' : 'icon-expand-fill';
  }

  get iconList() {
    return {
      project: {
        icon: 'plus-8',
        label: this.$t('新建') + this.activeLeftTab.label,
        callback() {
          if (this.pProjectId > 0) {
            if (
              this.DataModelTabManage.insertItem(
                new TabItem({
                  id: 0,
                  name: 'New Model',
                  displayName: this.$t('新建模型'),
                  type: this.activeTypeName,
                  isNew: true,
                  projectId: this.pProjectId,
                  modelType: this.activeLeftTab.name,
                  modelId: 0,
                  icon: this.activeLeftTab.icon,
                  lastStep: 0,
                }),
                true
              )
            ) {
              this.changeRouterWithParams('dataModelEdit',
                { project_id: this.pProjectId }, { modelId: 0 });
              // this.appendRouter({ modelId: 0 }, { project_id: this.pProjectId })
            }
          } else {
            this.$bkMessage({
              message: this.$t('请选择项目'),
              delay: 1000,
              theme: 'warning ',
              offsetY: 80,
              ellipsisLine: 5,
              limit: 1,
            });
          }
        },
      },
      market: {
        icon: 'share',
        label: '查看数据集市',
        callback: () => {
          window.open('#/data-mart/data-dictionary/search-result', '_blank');
        },
      },
    };
  }

  public handleNewModel() {
    const curType = this.iconList[this.activeTypeName];
    curType.callback.call(this);
    this.sendUserActionData({ name: '点击【新建模型】' });
  }

  /** 点击选中数据表 */
  public clickList(item) {
    /**
     * 数据集市暂时无处跳转，不做处理
     * 二期处理数据集市跳转
     */
    if (this.activeTypeName === 'project') {
      if (
        this.DataModelTabManage.activeItemById(item.model_id)
                || this.DataModelTabManage.insertItem(
                  new TabItem({
                    id: item.model_id,
                    modelId: item.model_id,
                    name: item.model_name,
                    displayName: item.model_alias,
                    type: this.activeTypeName,
                    modelType: this.activeLeftTab.name,
                    isNew: false,
                    projectId: item.project_id,
                    icon: this.activeLeftTab.icon,
                    lastStep: item.step_id,
                    publishStatus: item.publish_status,
                    activeStep: 1,
                    routeName: 'dataModelView',
                  }),
                  true
                )
      ) {
        // this.appendRouter({ modelId: item.model_id }, { project_id: item.project_id })
        this.changeRouterWithParams(
          this.activeModelTabItem?.routeName || 'dataModelView',
          { project_id: item.project_id },
          { modelId: item.model_id }
        );
        this.updateStorageModelId(item.model_id);
        this.updateStorageProjectId(item.project_id);
      }
    }
  }

  /**
   * 置顶 & 取消置顶
   * @param item 操作对象
   * @param stickyOnTop 是否置顶
   */
  public handleTopData(item, stickyOnTop) {
    const { model_id } = item;
    (stickyOnTop ? topModel(model_id) : cancelTop(model_id)).then(res => {
      if (res.data) {
        item.sticky_on_top = stickyOnTop;
      }
    });
  }

  /**
   * 删除数据表
   * @param item 操作对象
   */

  public handleDeleteModel(item) {
    const { model_id } = item;
    const h = this.$createElement;
    // const vdom = h('div', {}, [h('p', { class: 'mb10 mt10' },
    // '数据模型：' + `${item.model_name} (${item.model_alias})`), h('p', {}, '删除数据模型及所属指标统计口径和指标')]);
    this.$bkInfo({
      theme: 'primary',
      title: this.$t('确认删除数据模型?'),
      subTitle: this.$t('删除当前数据模型（附带删除所属的指标统计口径和指标），可能会影响其他模型草稿的使用'),
      confirmFn: () => {
        deleteModel(model_id).then(res => {
          if (res.validateResult()) {
            this.$bkMessage({
              message: this.$t('删除成功'),
              theme: 'success',
            });
            this.getModelInfoByProjectId();
            const findItem = (this.DataModelTabManage.tabManage.items || [])
              .find(tab => tab.modelId === item.model_id);
            if (findItem) {
              this.DataModelTabManage.removeItem(findItem);
              const activeItem = this.DataModelTabManage.activeItemByIndex(0);
              if (activeItem) {
                // this.appendRouter({ modelId: activeItem.modelId },
                // { project_id: activeItem.projectId })
                this.changeRouterWithParams(
                  activeItem.routeName || 'dataModelView',
                  { project_id: activeItem.projectId },
                  { modelId: activeItem.modelId }
                );
                this.updateStorageModelId(activeItem.id);
                this.updateStorageProjectId(activeItem.projectId);
                this.handleActiveTabChanged(activeItem);
              } else {
                // this.appendRouter({ modelId: undefined }, {}, true)
                this.changeRouterWithParams('dataModelView',
                  { project_id: undefined }, { modelId: undefined });
              }
            }
          }
        });
      },
    });
  }

  /** 项目模式下面获取模型列表 */
  public getModelInfoByProjectId() {
    if (this.pProjectId) {
      this.isPDatasourceLoad = true;
      getModelList({ project_id: this.pProjectId })
        .then(res => {
          res.setData(this, 'pDataSource');
          // 处理通过直接点链接进入数据模型的情况
          if (this.isInitActiveItem && this.modelId) {
            const model = this.pDataSource.find(item => item.model_id === Number(this.modelId));
            if (!model) {
              if (
                this.DataModelTabManage.insertItem(
                  new TabItem({
                    id: 0,
                    name: 'New Model',
                    displayName: this.$t('新建模型'),
                    type: this.activeTypeName,
                    isNew: true,
                    projectId: this.pProjectId,
                    modelType: this.activeLeftTab.name,
                    modelId: 0,
                    icon: this.activeLeftTab.icon,
                    lastStep: 0,
                  }),
                  true
                )
              ) {
                this.changeRouterWithParams('dataModelEdit',
                  { project_id: this.pProjectId }, { modelId: 0 });
                // this.appendRouter({ modelId: 0 }, { project_id: this.pProjectId })
              }
            } else {
              this.activeModelType = model.model_type;
              this.clickList(model);
              this.updateStorageModelType(model.model_type);
            }
          }
        })
        ['finally'](() => {
          this.isPDatasourceLoad = false;
        });
    }
  }

  /** 数据集市获取模型列表 */
  public getModelInfoByMart() {
    this.isSDatasourceLoad = true;
    getModelList({})
      .then(res => {
        res.setData(this, 'sDataSource');
      })
      ['finally'](() => {
        this.isSDatasourceLoad = false;
      });
  }

  public getProjectList() {
    getMineProjectList().then(res => {
      res.setData(this, 'projectList');
      const existProject = this.projectList.find(item => item.project_id === Number(this.pProjectId));
      this.pProjectId && !existProject && this.projectApply && this.projectApply.openDialog();
    });
  }

  public handleProjectChanged(id: number) {
    this.DataModelTabManage.clearTabItems();
    // this.appendRouter({ modelId: undefined }, { project_id: undefined })
    this.changeRouterWithParams('dataModelView', { project_id: undefined }, { modelId: undefined });
    this.updateStorageProjectId(id);
  }

  /**
   * 当前选中类型改变事件 项目|数据集市
   * 因为数据集市跳转新开Tab，此处不做缓存
   * 目前只处理项目类型下面的模型类型
   * activeType缓存暂时只能为 project
   * @param typeName
   */
  public handleActiveTypeChanged(typeName: string) {
    this.searchValue = '';
    // this.updateStorageActiveType(typeName)
  }

  public handleModelTypeChanged(typeName: string) {
    this.updateStorageModelType(typeName);
  }

  /**
   * 右侧已经打开的标签点击激活事件
   * 目前只有项目才能在右侧新开标签
   * 此处逻辑为数据集市也开新的标签时的处理逻辑
   * 目前状态下无影响
   * @param activeTab
   */
  public handleActiveTabChanged(activeTab: IDataModelManage.IModeRightTabItem) {
    this.activeTypeName = activeTab.type;
    this.activeModelType = activeTab.modelType;
  }

  public handleUpdateModel(item: any) {
    const index = this.modelInfoList.findIndex(ite => ite.model_id === item.model_id);
    if (index >= 0) {
      const oldItem = this.modelInfoList[index];
      Object.assign(oldItem, item);
      this.modelInfoList.splice(index, 1, oldItem);
    } else {
      this.modelInfoList.splice(0, 0, item);
    }
  }

  public handleToggleLeftMenu() {
    this.isShowLeftMenu = !this.isShowLeftMenu;
  }

  public created() {
    this.getProjectList();
    this.getModelInfoByMart();
    this.restoreLocalStorage();
    const projectId = this.projectId || this.storageData.pProjectId;
    // 通过分享链接进入
    if (this.projectId && Number(this.projectId) !== Number(this.storageData.pProjectId)) {
      this.DataModelTabManage.clearTabItems();
      this.updateStorageProjectId(projectId);
    }
    this.activeTypeName = this.storageData.activeType;
    const activeItem = this.DataModelTabManage.getActiveItem()[0];
    // 如果激活项和router modelId不同，以router modelId为准
    this.isInitActiveItem = !activeItem || (activeItem && activeItem.modelId !== this.modelId);
    if (!!activeItem) {
      this.activeModelType = activeItem.modelType;
      if (!this.modelId) {
        this.modelId = activeItem.modelId;
      }
    }
    this.pProjectId = Number(projectId) || projectId;
  }
}

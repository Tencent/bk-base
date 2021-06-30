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

import { TsRouteParams } from '@/common/ts/tsVueBase';
import { DataModelStorage } from './DataModelStorage';
import { DataModelTabManage } from './DataModelTabManage';

interface IDataModelStorage {
  /** 当前选中类型（项目|数据集市） */
  activeType?: string;

  /** 模型类型（fact_table|dimension_table）选择项目Tab时 */
  pModelType: string;

  /** 模型类型（fact_table|dimension_table）选择数据集市Tab时*/
  sModelType: string;

  /** 当前选中项目ID-选择项目Tab时 */
  pProjectId?: number | string;

  /** 当前选中模型ID-选择项目Tab时 */
  pModelId?: number;

  /** 当前选中项目ID-选择数据集市Tab时 */
  sProjectId?: number;

  /** 当前选中模型ID-选择数据集市Tab时 */
  sModelId?: number;

  /** 当前打开Tabs */
  tabItems?: any;
}

export class DataModelManageBase extends TsRouteParams {
  /** 模型ID */
  get modelId() {
    return (this.routeParams || {}).modelId;
  }

  /** 模型ID */
  set modelId(val: number) {
    this.appendRouter({ modelId: val });
  }

  /** 已选择项目ID */
  get projectId() {
    return this.routeQuery.project_id;
  }

  set projectId(val: number) {
    this.appendQuery({ project_id: val });
  }

  get storagePrefix() {
    return this.storageData.activeType === 'project' ? 'p' : 's';
  }

  /** 用于触发自定义事件监控，只能用于简单事件触发，多个事件请不要使用，会因为Vue的计算最终只能触发最后一个事件，复杂事件触发、共享请使用Vuex或者Bus */
  get updateEvents() {
    return DataModelTabManage.updateEvents;
  }

  /** 当前激活模型标签信息 */
  get activeModelTabItem() {
    return this.DataModelTabManage.getActiveItem()[0];
  }

  public DataModelTabManage = DataModelTabManage;
  public DataModelStorage = DataModelStorage;

  /** 缓存数据 */
  public storageData: IDataModelStorage = {
    /** 当前选中类型（项目|数据集市） */
    activeType: 'project',

    /** 模型类型（fact_table|dimension_table）选择项目Tab时 */
    pModelType: 'fact_table',

    /** 模型类型（fact_table|dimension_table）选择数据集市Tab时*/
    sModelType: 'fact_table',

    /** 当前选中项目ID-选择项目Tab时 */
    pProjectId: '',

    /** 当前选中模型ID-选择项目Tab时 */
    pModelId: 0,

    /** 当前选中项目ID-选择数据集市Tab时 */
    sProjectId: 0,

    /** 当前选中模型ID-选择数据集市Tab时 */
    sModelId: 0,
  };

  /** 更新缓存 */
  public updateStorageData(storage: IDataModelStorage = {}) {
    Object.assign(this.storageData, storage);
    this.DataModelStorage.updateAll(this.storageData);
  }

  /** 更新缓存 */
  public updateStorageActiveType(activeType: string) {
    this.updateStorageData({ activeType });
  }

  /** 更新缓存 */
  public updateStorageModelType(modelType: string) {
    this.updateStorageData({ [`${this.storagePrefix}ModelType`]: modelType });
  }

  /** 更新缓存 */
  public updateStorageProjectId(projectId: number) {
    this.updateStorageData({ [`${this.storagePrefix}ProjectId`]: projectId });
  }

  /** 更新缓存 */
  public updateStorageModelId(modelId: number) {
    this.updateStorageData({ [`${this.storagePrefix}ModelId`]: modelId });
  }

  /** 恢复缓存数据 */
  public restoreLocalStorage() {
    Object.assign(
      this.storageData,
      this.DataModelStorage.get([
        'activeType',
        'pModelType',
        'pProjectId',
        'pModelId',
        'sModelType',
        'sProjectId',
        'sModelId',
      ])
    );
    this.DataModelTabManage.restoreTabItems();
  }

  /** 上报用户数据 */
  public sendUserActionData(data: object = {}) {
    window.$bk_perfume.start('User_Action');
    const options = {
      page: 'dataModel',
      event: 'click',
      name: '',
      time: new Date().toISOString(),
      modelInfo: this.activeModelTabItem || {},
    };
    const value = Object.assign({}, options, data);
    window.$bk_perfume.end('User_Action', { data: value });
  }
}

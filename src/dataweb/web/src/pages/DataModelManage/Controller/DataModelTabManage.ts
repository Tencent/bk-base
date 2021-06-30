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

import Vue from 'vue';
import { IDataModelManage, IModeRightTabItem, IModeRightTabItems } from '../Interface/index';
import { DataModelStorage } from './DataModelStorage';

export class TabItem implements IDataModelManage.IModeRightTabItem {
  /** 是否新增 */
  public isNew: boolean;

  /** 是否当前激活条目 */
  public isActive: boolean;

  /** 唯一ID */
  public id: string;

  /** 缓存当前激活步骤 */
  public activeStep: number;

  /** name */
  public name: string;

  public displayName: string;

  /** type */
  public type: string;

  /** 模型类型 */
  public modelType: string;

  /** 模型ID */
  public modelId: number;

  public projectId?: number;

  public icon: string;

  /** 当前模型已完成最后步骤Index */
  public lastStep: number;

  /** 模型发布状态 */
  public publishStatus: string;

  /** 子路由 name */
  public routeName?: string;

  constructor(item: IDataModelManage.IModeRightTabItem) {
    const {
      name,
      displayName,
      type = 'project',
      isNew = false,
      isActive = true,
      id = '',
      activeStep = 0,
      projectId = 0,
      modelId = 0,
      modelType = '',
      icon = 'fact-model',
      lastStep = 1,
      publishStatus = 'developing',
      routeName = 'dataModelEdit',
    } = item;
    this.isNew = isNew;
    this.isActive = isActive;
    this.id = id;
    this.activeStep = activeStep;
    this.name = name;
    this.type = type;
    this.displayName = displayName;
    this.projectId = projectId;
    this.modelId = modelId;
    this.modelType = modelType;
    this.icon = icon;
    this.lastStep = lastStep;
    this.publishStatus = publishStatus;
    this.routeName = routeName;
  }
}

export interface ITabManage {
  items: IDataModelManage.IModeRightTabItem[];
}

export interface IUpdateEvent {
  /** 事件名称 */
  name: string;
  /** 参数 */
  params: string[];

  timestamp: 0;
}

export class DataModelTabManage implements IDataModelManage.IModeRightTabItems {
  public static tabManage: ITabManage = Vue.observable({
    items: [],
  });

  /** 用于监控触发不同组件的异步事件 */
  public static updateEvents: IUpdateEvent = Vue.observable({
    name: '',
    params: [],
    timestamp: 0,
  });

  /**
   * 分发事件
   * @param name
   * @param params
   */
  public static dispatchEvent(name: string, params: any[]) {
    this.updateEvents.name = name;
    this.updateEvents.params = JSON.parse(JSON.stringify(params));
    this.updateEvents.timestamp = new Date().getTime();
  }

  /** 从缓存恢复上次已缓存的Tab项 */
  public static restoreTabItems() {
    Object.assign(this.tabManage, DataModelStorage.get(this.storageKey) || { items: [] });
  }

  /** 缓存最新Tabs到LocalStorage */
  public static storeTabItems() {
    DataModelStorage.update(this.storageKey, this.tabManage);
  }

  public static getActiveItem(): IDataModelManage.IModeRightTabItem[] {
    return this.tabManage.items.filter(item => item.isActive);
  }

  public static getActiveItemIndex() {
    return this.tabManage.items.findIndex(item => item.isActive);
  }

  public static activeItem(item: IDataModelManage.IModeRightTabItem) {
    return this.activeItemById(item.id);
  }

  public static activeItemById(id: string): IDataModelManage.IModeRightTabItem {
    const item = this.tabManage.items.find(item => item.id === id);
    if (item) {
      this.deActiveItem();
      item.isActive = true;
    }
    this.storeTabItems();
    return item;
  }

  public static activeItemByIndex(index: number): IDataModelManage.IModeRightTabItem {
    const item = this.tabManage.items[index];
    if (item) {
      this.deActiveItem();
      item.isActive = true;
    }
    this.storeTabItems();
    return item;
  }

  public static deActiveItem(item?: IDataModelManage.IModeRightTabItem) {
    if (item) {
      item.isActive = false;
    } else {
      this.getActiveItem().forEach(ite => (ite.isActive = false));
    }
    this.storeTabItems();
  }

  public static appendItem(item: IDataModelManage.IModeRightTabItem) {
    if (this.insertBefore()) {
      if (!this.tabManage.items.some(ite => ite.id === item.id)) {
        this.tabManage.items.push(item);
      }

      this.activeItem(item);
      this.storeTabItems();
      return true;
    }

    return false;
  }

  /**
   * 添加新的Item到头部
   * @param item
   * @param force false 是否强制覆盖
   */
  public static insertItem(item: IDataModelManage.IModeRightTabItem, force = false) {
    if (this.insertBefore()) {
      const oldItem = this.tabManage.items.find(ite => ite.id === item.id);
      if (!oldItem) {
        this.tabManage.items.push(item);
        this.activeItem(item);
      } else if (force) {
        Object.assign(oldItem, item);
        this.activeItem(oldItem);
      }
      this.storeTabItems();
      return true;
    }

    return false;
  }

  public static insertBefore() {
    if (this.tabManage.items.length >= 16) {
      gVue.$bkMessage({
        message: '页签数量已到达上限，请删除后再添加',
        delay: 1000,
        theme: 'primary ',
        offsetY: 80,
        ellipsisLine: 5,
        limit: 1,
      });
      return false;
    }

    return true;
  }

  public static removeItem(item: IDataModelManage.IModeRightTabItem) {
    const itemIndex = this.tabManage.items.findIndex(ite => ite.id === item.id);
    if (itemIndex >= 0) {
      this.tabManage.items.splice(itemIndex, 1);
    }
    this.storeTabItems();
    return itemIndex;
  }

  /**
   * 更新
   * @param item
   * @param index 待更新条目Index，可选，不指定则使用item id匹配
   */
  public static updateTabItem(item: IDataModelManage.IModeRightTabItem, index?: number) {
    if (index >= 0) {
      this.tabManage.items.splice(index, 1, item);
    } else {
      const itemIndex = this.tabManage.items.findIndex(ite => ite.id === item.id);
      if (itemIndex >= 0) {
        this.tabManage.items.splice(itemIndex, 1, item);
      }
    }

    this.storeTabItems();
  }

  /**
   * 更新当前激活步骤
   * @param step
   */
  public static updateTabActiveStep(step: number) {
    if (step >= 1 && step <= 5) {
      const index = this.getActiveItemIndex();
      if (index >= 0) {
        const item = this.tabManage.items[index];
        item.activeStep = step;
        this.tabManage.items.splice(index, 1, item);
      }
    }

    this.storeTabItems();
  }

  /**
   * 清空items
   */

  public static clearTabItems() {
    this.tabManage.items = [];
    this.storeTabItems();
  }

  private static storageKey = 'tabItems';
}

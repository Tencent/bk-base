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

import bkStorage from '@/common/js/bkStorage';
import ValueCost from '@/pages/datamart/DataDictionaryComp/DictInventory/DataValue/ValueCost';

/**
 * 数据模型管理缓存管理静态类
 */
export class DataModelStorage {
  /**
   * 设置缓存
   * @param storage
   */
  public static set(storage: any) {
    bkStorage.set(this.__storageKey, storage);
  }

  /**
   * 获取缓存对象中指定的Key的值
   * @param key 指定key，如果不指定则返回数据模型管理所有缓存
   */
  public static get(key?: string | string[]) {
    const storage = bkStorage.get(this.__storageKey) || {};
    if (typeof key === 'string') {
      return storage[key];
    }

    if (Array.isArray(key)) {
      return key.reduce(
        (output, _key) => Object.assign(output, storage[_key] !== undefined ? { [_key]: storage[_key] } : {}),
        {}
      );
    }

    return storage;
  }

  /**
   * 更新指定的Key
   * @param key 指定key
   * @param value 更新值
   */
  public static update(key: string, value: any) {
    const oldValue = this.get() || {};
    if (typeof oldValue === 'object') {
      const target = Object.assign(oldValue, { [key]: value });
      this.set(target);
    }
  }

  /**
   * 更新所有
   */
  public static updateAll(value: any = {}) {
    const oldValue = this.get() || {};
    if (typeof oldValue === 'object') {
      const target = Object.assign(oldValue, value);
      this.set(target);
    }
  }

  private static __storageKey = '__bk_datamodelmanage_01';
}

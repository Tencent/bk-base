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

import { CreateElement } from 'vue/types/umd';
import { BKHttpResponseBase } from './BKHttpResponseBase';

/**
 * 服务器返回数据格式
 */
export class BKHttpResponse<T> extends BKHttpResponseBase {
  public result: boolean;
  public data: T;
  public message: string;
  public code: string;
  public errors: any[] = [];
  public lockMsgInstance = null;
  private originData: any;

  /**
   * 服务器返回数据格式构造函数
   * @param response 服务器返回数据
   * @param formatToWebSchema 是否格式化数据为前端格式（前端对象Key为驼峰式，接口返回Key为下划线）
   */
  constructor(response: IResponse<any>, formatToWebSchema = false): IResponse<T> {
    super(response.data);
    this.result = response.result;
    this.message = response.message;
    this.code = response.code;
    this.data = formatToWebSchema ? this.getWebData<T>(response.data) : response.data;
    this.errors = response.errors;
  }

  /**
   * 将前端对象格式化为服务器端数据格式
   * @param data 待格式化对象，如果为null则自动格式化实例中的data
   */
  public getServeFormatData(data = null): any {
    return this.getServeFormData(data || this.data);
  }

  /**
   * 格式化数据为前端格式
   * @param data
   */
  public getWebFormatData(data = null): any {
    return this.getWebData<T>(data);
  }

  /**
   * 设置API返回数据Data到指定对象
   * @param scope 当前作用域 this
   * @param target 赋值key
   * @param validateSuccess 是否校验错误，默认 true
   * @param warning 是否自动提示错误，默认true
   */
  public setData(scope: any, target: string, validateSuccess = true, warning = true) {
    if (!validateSuccess) {
      scope[target] = this.data;
      return Promise.resolve(this);
    }

    if (this.validateResult(null, null, warning)) {
      scope[target] = this.data;
      return Promise.resolve(this);
    } else {
      return Promise.reject(this);
    }
  }

  /**
   * 校验返回结果是否包含错误
   * @param customValidate 自定义校验 | 会优先校验返回result是否为true，然后才会校验 customValidate
   * @param messageFun 获取错误消息的函数
   * @param warning 是否自动提示错误，默认true
   */
  public validateResult(customValidate: Function | null = null, messageFun: Function | null = null, warning = true) {
    let isSuccess = Boolean(this.result);
    if (isSuccess) {
      if (typeof customValidate === 'function') {
        isSuccess = customValidate(this);
      }
    }

    if (warning && !isSuccess) {
      this.postMethodWarning(
        this.getErrorMessage() || (typeof messageFun === 'function' && messageFun(this)),
        this.code || 'Error',
        { ellipsisLine: 5 }
      );
    }

    return isSuccess;
  }

  /**
   * 需要对返回结果进行自定义处理
   * @param fn 自定义函数
   * @param warning 是否自动提示错误
   */
  public setDataFn(fn: Function, warning = true): void {
    if (this.result) {
      typeof fn === 'function' && fn(this.data);
    } else {
      warning && this.postMethodWarning(this.getErrorMessage(), this.code || 'Error', { ellipsisLine: 5 });
    }
  }

  private getErrorMessage() {
    return this.message || this.getErrorString();
  }

  private getErrorString() {
    const errors = this.getDeepArrayError(this.errors);
    if (Array.isArray(errors)) {
      return (errors as any[]).flatMap(err => err).join(',');
    } else {
      return errors;
    }
  }

  private getDeepArrayError(errors: any, key = '') {
    if (errors === null || errors === undefined || errors === '') {
      return errors;
    }

    if (Array.isArray(errors)) {
      return errors.map(error => this.getDeepArrayError(error, key));
    } else {
      if (typeof errors === 'object') {
        return Object.keys(errors).map(key => this.getDeepArrayError(errors[key], key));
      }

      if (/string|number|boolean/.test(typeof errors)) {
        return `${(key && `${key}：`) || ''}${errors}`;
      }
    }
  }

  private postMethodWarning(msg, code, options = {}) {
    gVue.$bkMessage(
      Object.assign(
        {
          message: `${code}: ${msg}`,
          delay: 0,
          theme: 'error',
          offsetY: 80,
          ellipsisLine: 5,
          limit: 1,
        },
        options || {}
      )
    );
  }
}

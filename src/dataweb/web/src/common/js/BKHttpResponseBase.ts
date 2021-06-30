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

import { getServeFormData, getWebData, initOutput, webFormatToServeFormat } from './Utils';
export class BKHttpResponseBase {
  private originData: any;
  constructor(origin: any) {
    this.originData = origin;
  }

  /**
   * 格式化服务器端接口返回数据
   * 前端数据命名规范：驼峰式
   * 服务器端命名规范：下划线
   * @param data ：服务器接口返回数据
   * @returns ： 前端驼峰式
   */
  protected getWebData<T>(data: any) {
    return getWebData(data);
  }

  /**
   * 前端定义接口格式化为服务器端格式
   * @param data ： 前端数据
   */
  protected getServeFormData(data: any): any {
    return getServeFormData(data);
  }

  /**
   * 前端格式转换为服务器格式
   * @param fieldName ：字段名称
   */
  protected webFormatToServeFormat(fieldName: string): string {
    return webFormatToServeFormat(fieldName);
  }

  /**
   * 服务器格式转换为前端格式
   * @param fieldName ：字段名称
   */
  protected serveFormatToWebFormat(fieldName: string): string {
    function serveToWebUpper(match) {
      return match.substring(match.lastIndexOf('_') + 1, match.length).toUpperCase();
    }
    return fieldName.replace(/_\w/g, serveToWebUpper);
  }
}

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

import { bkHttpRequest } from '@/common/js/ajax';
import { HttpRequestParams } from '@/controller/common';

export default class BKApiBase {
  public moduleName: string;
  public bkHttpRequest: () => Promise<IBKResponse<T>>;

  /** 是否格式化服务器端数据格式为Web端格式 */
  public formatWebSchema: boolean;

  /** 是否启用YApi Mock */
  public ymock: boolean;

  /**
   * BaseAPI
   * @param moduleName 模块名称
   * @param formatWebSchema 是否格式化服务器端数据格式为Web端格式, default: false
   * @param ymock 是否启用YApi Mock, default: false
   */
  constructor(moduleName: string, formatWebSchema = false, ymock = false) {
    this.moduleName = moduleName;
    this.formatWebSchema = formatWebSchema;
    this.bkHttpRequest = bkHttpRequest;
    this.ymock = ymock;
  }

  /**
   *
   * @param methodName
   * @param requestParams
   * @param queryParams
   * @param exts
   */
  bkHttp(methodName: string, requestParams = {}, queryParams = {}, ext = {}): Promise<IBKResponse<any>> {
    ext = Object.assign({}, { format: this.formatWebSchema, ymock: this.ymock }, ext);
    return this.bkHttpRequest(
            `${this.moduleName}/${methodName}`,
            new HttpRequestParams(requestParams, queryParams, false, true, ext)
    );
  }
}

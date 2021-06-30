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
import { ImodelList, ItableData } from '../interface/index';

const moduleName = 'dataGraph';

/**
 *
 * @param methodName
 * @param requestParams
 * @param queryParams
 * @param exts
 */
const bkHttp = (methodName: string, requestParams = {}, queryParams = {}, ext = {}): Promise<IBKResponse<any>> => {
  return bkHttpRequest(
    `${moduleName}/${methodName}`,
    new HttpRequestParams(requestParams, queryParams, false, true, ext)
  );
};

/**
 * 获取数据模型列表
 * @param query model_id
 */
export const getReleasedModelList = (query: object = {}): Promise<IBKResponse<ImodelList[]>> => {
  return bkHttp('getReleasedModelList', {}, query);
};

export const getModelApplication = (query: object = {}): Promise<IBKResponse<ItableData[]>> => {
  return bkHttp('getModelApplication', {}, query);
};

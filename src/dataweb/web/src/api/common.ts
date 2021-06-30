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

// 公共API定义
// import { bkRequest } from '@/common/js/ajax'
import { IBKResponse } from '@/@types/types';
import { bkHttpRequest } from '@/common/js/ajax';
import { HttpRequestParams } from '@/controller/common';

const moduleMeta = 'meta';

/**
 *
 * @param methodName
 * @param requestParams
 * @param queryParams
 * @param param3
 */
export const bkMetaHttp = (
  methodName: string,
  requestParams = {},
  queryParams = {},
  exts = {}
): Promise<IBKResponse<any>> => {
  return bkHttpRequest(
    `${moduleMeta}/${methodName}`,
    new HttpRequestParams(requestParams, queryParams, false, true, exts)
  );
};

/**
 * 获取当前用户有权限的项目列表
 * @param active
 */
export const getMineProjectList = (active = true) => {
  return bkMetaHttp('getProjectList', {}, { active });
};

/**
 * 获取标签
 * @param pagopen_recommende
 */
export const getRecommendsTags = (open_recommend = 1) => {
  return bkMetaHttp('getRecommendsTags', {}, { open_recommend });
};

/**
 * 获取全量业务列表
 */
export const getAllBizs = (open_recommend = 1) => {
  return bkMetaHttp('getAllBizs', {}, { open_recommend });
};

/**
 * 获取当前项目下面有权限的业务列表
 */
export const getBizListByProjectId = (requestParams = {}, queryParams = {}, exts = {}): Promise<IBKResponse<any>> => {
  return bkHttpRequest(
    'dataFlow/getBizListByProjectId',
    new HttpRequestParams(requestParams, queryParams, false, true, exts)
  );
};

/**
 * 根据bizid获取结果表列表
 */
export const getBizResultTableList = (open_recommend = 1, bk_biz_id: string) => {
  return bkMetaHttp('getBizResultTableList', {}, { open_recommend, bk_biz_id });
};

/**
 * 获取表结构相关接口基础接口地址
 */
export const getResultTablesBase = (rtid: number, related = 'fields') => {
  return bkMetaHttp('getResultTablesBase', { rtid }, { related });
};

/**
 * 获取表结构相关接口
 */
export const getDictDetailFieldInfo = (requestParams = {}, queryParams = {}, exts = {}): Promise<IBKResponse<any>> => {
  return bkHttpRequest(
    'dataDict/getDictDetailFieldInfo',
    new HttpRequestParams(requestParams, queryParams, false, true, exts)
  );
};

/**
 * 获取指定项目下面的业务数据
 */
export const getDataByProject = (requestParams = {}, queryParams = {}, exts = {}): Promise<IBKResponse<any>> => {
  return bkHttpRequest(
    'dataExplore/getDataByProject',
    new HttpRequestParams(requestParams, queryParams, false, true, exts)
  );
};

/**
 * 获取指定业务下的结果表数据
 */
export const getAuthResultTablesByProject = (
  requestParams = {},
  queryParams = {},
  exts = {}
): Promise<IBKResponse<any>> => {
  return bkHttpRequest(
    'auth/getAuthResultTablesByProject',
    new HttpRequestParams(requestParams, queryParams, false, true, exts)
  );
};

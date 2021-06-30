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

import { bkRequest } from '@/common/js/ajax';
import { HttpRequestParams } from '@/controller/common';
import {
  IScoreDistribution,
  IQualityScoreTrend,
  IStorageCapacityTrend,
  IStorageCapacityDistribution,
  IValueLevelDistribution,
  IRelyonNodeData,
  IApplyNodeData,
  IApplyAppData,
  IApplyProjData,
  IImportanceDistribution,
  IBizImportanceDistribution,
  IScoreLevelTrend,
} from '@/pages/datamart/InterFace/DataInventory';

// 以下接口参数类似
/** 参数样例
    * {
    "bk_biz_id": null,
    "project_id": null,
    "tag_ids": [

    ],
    "keyword": "",
    "tag_code": "virtual_data_mart",
    "me_type": "tag",
    "has_standard": 1,
    "cal_type": [
        "standard"
    ],
    "data_set_type": "all",
    "page": 1,
    "page_size": 10,
    "platform": "bk_data",
}
*/

/**
 *  价值、收益比、热度、广度、重要度等评分分布接口
 *  @params (params): score_type 类型(importance/heat/range/asset_value/assetvalue_to_cost)
 *  @param mock : 是否模拟数据
 */
export const getScoreDistribution = (
  score_type: string,
  params: Record<string, any>
): Promise<IResponse<IScoreDistribution>> => {
  return bkRequest.httpRequest(
    'dataMart/getScoreDistribution',
    new HttpRequestParams({ score_type, ...params }, {}, false, true, {})
  );
};

/**
 *  价值、重要度等评分趋势
 *  @params (params): score_type 类型(importance/heat/range/asset_value/assetvalue_to_cost)
 *  @param mock : 是否模拟数据
 */
export const getScoreTrend = (
  score_type: string,
  params: Record<string, any>
): Promise<IResponse<IQualityScoreTrend>> => {
  return bkRequest.httpRequest(
    'dataMart/getScoreTrend',
    new HttpRequestParams({ score_type, ...params }, {}, false, true, {})
  );
};

/**
 *  价值、重要度等等级分布
 *  @params (params): score_type 类型(importance/heat/range/asset_value/assetvalue_to_cost)
 *  @param mock : 是否模拟数据
 */
export const getLevelDistribution = (
  score_type: string,
  params: Record<string, any>
): Promise<IResponse<IValueLevelDistribution>> => {
  return bkRequest.httpRequest(
    'dataMart/getLevelDistribution',
    new HttpRequestParams({ score_type, ...params }, {}, false, true, {})
  );
};

/**
 *  总存储成本分布
 *  @param mock : 是否模拟数据
 */
export const getStorageCapacityDistribution = (
  params: Record<string, any>
): Promise<IResponse<IStorageCapacityDistribution>> => {
  return bkRequest.httpRequest(
    'dataMart/getStorageCapacityDistribution',
    new HttpRequestParams({ ...params }, {}, false, true, {})
  );
};

/**
 *  存储成本趋势
 *  @param mock : 是否模拟数据
 */
export const getStorageCapacityTrend = (params: Record<string, any>): Promise<IResponse<IStorageCapacityTrend>> => {
  return bkRequest.httpRequest(
    'dataMart/getStorageCapacityTrend',
    new HttpRequestParams({ ...params }, {}, false, true, {})
  );
};

/**
 *  获取数据查询次数分布
 *  @param mock : 是否模拟数据
 */
export const getQueryData = (params: Record<string, any>): Promise<IResponse<IStorageCapacityTrend>> => {
  return bkRequest.httpRequest(
    'dataInventory/getQueryData', new HttpRequestParams({ ...params }, {}, false, true, {}));
};

/**
 *  数据开发后继依赖节点个数
 *  @param mock : 是否模拟数据
 */
export const getRelyonNodeData = (params: Record<string, any>): Promise<IResponse<IRelyonNodeData>> => {
  return bkRequest.httpRequest(
    'dataInventory/getRelyonNodeData',
    new HttpRequestParams({ ...params }, {}, false, true, {})
  );
};

/**
 *  应用业务个数分布
 *  @param mock : 是否模拟数据
 */
export const getApplyNodeData = (params: Record<string, any>): Promise<IResponse<IApplyNodeData>> => {
  return bkRequest.httpRequest(
    'dataInventory/getApplyNodeData',
    new HttpRequestParams({ ...params }, {}, false, true, {})
  );
};

/**
 *  应用项目个数分布
 *  @param mock : 是否模拟数据
 */
export const getApplyProjData = (params: Record<string, any>): Promise<IResponse<IApplyProjData>> => {
  return bkRequest.httpRequest(
    'dataInventory/getApplyProjData',
    new HttpRequestParams({ ...params }, {}, false, true, {})
  );
};

/**
 *  应用APP个数分布
 *  @param mock : 是否模拟数据
 */
export const getApplyAppData = (params: Record<string, any>): Promise<IResponse<IApplyAppData>> => {
  return bkRequest.httpRequest(
    'dataInventory/getApplyAppData',
    new HttpRequestParams({ ...params }, {}, false, true, {})
  );
};

/**
 *  业务重要度、关联BIP、项目运营状态、数据敏感度分布、数据生成类型
 *  @param mock : 是否模拟数据
 */
export const getImportanceDistribution = (params: Record<string, any>): Promise<IResponse<IImportanceDistribution>> => {
  return bkRequest.httpRequest(
    'dataInventory/getImportanceDistribution',
    new HttpRequestParams({ ...params }, {}, false, true, {})
  );
};

/**
 *  业务星际&运营状态分布
 *  @param mock : 是否模拟数据
 */
export const getBizImportanceDistribution = (
  params: Record<string, any>
): Promise<IResponse<IBizImportanceDistribution>> => {
  return bkRequest.httpRequest(
    'dataInventory/getBizImportanceDistribution',
    new HttpRequestParams({ ...params }, {}, false, true, {})
  );
};

/**
 *  价值、重要度等等级分布&评分趋势
 *  @param mock : 是否模拟数据
 */
export const getScoreDistributionLevelTrend = (
  score_type: string,
  params: Record<string, any>
): Promise<IResponse<IScoreLevelTrend>> => {
  return bkRequest.httpRequest(
    'dataInventory/getScoreDistributionLevelTrend',
    new HttpRequestParams({ score_type, ...params }, {}, false, true, {})
  );
};

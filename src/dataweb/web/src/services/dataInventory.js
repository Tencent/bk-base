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

// { 参数结构
//     "bk_biz_id": '',
//     "project_id":null,
//     "tag_ids":[],
//     "keyword":"",
//     "tag_code": "virtual_data_mart",
//     "me_type": "tag",
//     "has_standard":1,
//     "cal_type":[
//         "standard"
//     ],
//     "data_set_type":"all",
//     "page":1,
//     "page_size":10,
//     "platform":"bk_data",
//     "order_time":null,
//     "order_heat":null,
//     "order_range":null,
//     "parent_tag_code":"all"
// }
/** 数据来源分布接口
 */
const getDataSource = {
  url: '/v3/datamanage/datastocktake/data_source/distribution/',
  method: 'POST',
};

/** 数据来源设备分布接口
 */
const getDataDevice = {
  url: 'v3/datamanage/datastocktake/data_source/detail_distribution/',
  method: 'POST',
};

/** 数据类型分布接口
 */
const getDataTypeDistributed = {
  url: 'v3/datamanage/datastocktake/data_type/distribution/',
  method: 'POST',
};

const getStandarDistribution = {
  url: 'v3/datamanage/datamap/retrieve/datamap_summary/',
  method: 'POST',
};

const getOverallStatistacs = {
  url: 'datamart/datastocktake/overall_statistic/',
  method: 'POST',
};

const getApplyData = {
  url: 'v3/datamanage/datastocktake/sankey_diagram/distribution/',
  method: 'POST',
};

const getRelyonNodeData = {
  url: 'v3/datamanage/datastocktake/node_count/distribution_filter/',
  method: 'POST',
};

const getHeatScore = {
  url: 'v3/datamanage/datastocktake/score_distribution/heat_filter/',
  method: 'POST',
};

const getRangeSCore = {
  url: 'v3/datamanage/datastocktake/score_distribution/range_filter/',
  method: 'POST',
};

// 获取数据查询次数分布
const getQueryData = {
  url: 'v3/datamanage/datastocktake/day_query_count/distribution_filter/',
  method: 'POST',
};

// 获取应用业务个数分布
const getApplyNodeData = {
  url: 'v3/datamanage/datastocktake/application/biz_count_distribution_filter/',
  method: 'POST',
};

// 获取应用项目个数分布
const getApplyProjData = {
  url: 'v3/datamanage/datastocktake/application/project_count_distribution_filter/',
  method: 'POST',
};

// 获取APP应用个数分布
const getApplyAppData = {
  url: 'v3/datamanage/datastocktake/application/app_code_count_distribution_filter/',
  method: 'POST',
};

/**
 * 业务重要度、关联BIP、项目运营状态、数据敏感度分布、数据生成类型
 *  @params (params): metric_type // 类型
 */
const getImportanceDistribution = {
  url: 'v3/datamanage/datastocktake/importance_distribution/:metric_type/',
  method: 'POST',
};

/**
 * 业务星际&运营状态分布
 */
const getBizImportanceDistribution = {
  url: 'v3/datamanage/datastocktake/importance_distribution/biz/',
  method: 'POST',
};

/**
 * 价值、重要度等等级分布&评分趋势
 *  @params (params): score_type 生命周期评分指标 importance/heat/range/asset_value/assetvalue_to_cost
 */
const getScoreDistributionLevelTrend = {
  url: 'v3/datamanage/datastocktake/score_distribution/level_and_trend/:score_type/',
  method: 'POST',
};

export {
  getDataSource,
  getDataDevice,
  getDataTypeDistributed,
  getStandarDistribution,
  getOverallStatistacs,
  getApplyData,
  getRelyonNodeData,
  getHeatScore,
  getRangeSCore,
  getQueryData,
  getApplyNodeData,
  getApplyProjData,
  getApplyAppData,
  getImportanceDistribution,
  getBizImportanceDistribution,
  getScoreDistributionLevelTrend,
};

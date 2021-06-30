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

/**
 * @query project_id
 */
const getResultInfo = {
  url: 'datamart/datadict/dataset_list/',
  method: 'POST',
};

/**
 * @query result_table_id
 * @query data_set_type
 * @query standard_id
 */
const getResultInfoDetail = {
  url: 'datamart/datadict/dataset_info/',
};

/**
 * 数据字典字段信息
 * @params result_table_id
 */
const getDictDetailFieldInfo = {
  url: 'result_tables/:result_table_id/list_all_field/',
};

/**
 * 数据字典结果表使用任务
 * @params result_table_id
 */
const getResultTaskInfo = {
  url: 'result_tables/:result_table_id/list_rela_by_rtid/',
};

/**
 * 数据字典数据源使用任务
 * @query raw_data_id 数据源ID
 * @query clean_config_name 任务名称
 * @query project_name 项目名称
 */
const getSourceTaskInfo = {
  url: '/etls/',
};

/**
 * 数据字典数据预览接口
 * @params result_table_id
 */
const getDataPreviewInfo = {
  url: 'result_tables/:result_table_id/get_data_by_recommend_storage/',
  // url: 'result_tables/591_apply_test_rt2/get_sql_query_rt/',
  method: 'POST',
};

/**
 * 数据字典详情里数据趋势图数据
 * @params raw_data_id
 * @params frequency
 * @params start_time
 * @params end_time
 */
const getDataTrendInfo = {
  url: 'dataids/:raw_data_id/list_data_count_by_time/',
};

/**
 * 数据字典详情判断某一数据是否有权限的接口
 * @query action_id
 * @query object_id
 */
const dataJudgePermission = {
  url: '/auth/judge_permission/',
};

/**
 * 数据字典血缘分析数据接口
 * @query type
 * @query qualified_name
 */
const getlineageSubGraghData = {
  url: '/datamart/datadict/lineage/',
};

/**
 * 数据字典渠道列表接口
 */
const getDataDictionaryPlatformList = {
  url: 'v3/meta/platform_configs/',
};

/**
 * TDW修改字段描述
 *  * @params cluster_id
 *  * @params db_name
 *  * @params table_name
 *  * @params ddls
 *  * @params bk_username
 */
const modifyTdwFieldDes = {
  url: '/v3/meta/tdw/tables/modify_table_column_comment/',
  method: 'POST',
};

/**
 * TDW修改表描述
 *  * @params cluster_id
 *  * @params db_name
 *  * @params table_name
 *  * @params comment
 *  * @params bk_username
 */
const modifyTdwTableDes = {
  url: '/v3/meta/tdw/tables/modify_table_comment/',
  method: 'POST',
};

/**
 * 结果表修改表描述
 * @params result_table_id
 */
const modifyResultTableDes = {
  url: '/v3/meta/result_tables/:result_table_id/',
  method: 'PUT',
};

/**
 * 结果表修改表描述
 * @params raw_data_id
 */
const modifyRawTableDes = {
  url: 'v3/access/rawdata/:raw_data_id/',
  method: 'patch',
};

/**
 * 结果表修改表描述和中文名
 * @params raw_data_id
 */
const modifyFieldInfo = {
  url: '/datamart/datadict/rt_field_edit/',
  method: 'POST',
};

/**
 * 数据字典详情存储列表
 *  * @params id (数据源id或rtID)
 */
const getDictionaryStorageList = {
  url: 'v3/meta/result_tables/:id/?related=storages',
};

/**
 * 数据字典生命周期详情广度及应用分布图形数据接口
 *  * @params dataset_id (数据源id或rtID)
 *  * @params start_time
 *  * @params end_time
 */
const getRangeTrendData = {
  url: 'v3/datamanage/lifecycle/range/list_range_metric_by_influxdb/',
};

/**
 * 数据字典生命周期详情热度及数据查询次数接口
 *  * @params dataset_id (数据源id或rtID)
 *  * @params start_time
 *  * @params end_time
 */
const getHeatTrendData = {
  url: 'v3/datamanage/lifecycle/heat/list_heat_metric_by_influxdb/',
};

/**
 * 数据字典生命周期获取热度和广度分值及排名的接口
 *  * @params dataset_id (数据源id或rtID)
 */
const getDictScore = {
  url: 'v3/datamanage/lifecycle/ranking/range_heat_ranking_by_influxdb/',
};

/**
 * 数据字典生命周期获取相似数据集的接口
 *  * @params dataset_id (数据源id或rtID)
 */
const getDictSimilarity = {
  url: 'v3/meta/analyse/similarity/info/',
  method: 'POST',
};
/*
 * 数据字典最近热门查询表接口
 */
const getRecentQuery = {
  url: 'datamart/datadict/popular_query/',
};

/**
 * 数据字典历史搜索参数
 */
// 请求示例
// {
//     "bk_biz_id":null,
//     "project_id":null,
//     "tag_ids":[
//         "login",
//         "logout"
//     ],
//     "keyword":"",
//     "tag_code":410,
//     "me_type":"standard",
//     "cal_type":[
//         "standard",
//         "only_standard"
//     ],
//     "data_set_type":"all",
//     "platform":"bk_data",
//     "order_time":null,
//     "order_heat":null,
//     "order_range":null
// }
const getSearchHistory = {
  url: 'datamart/datadict/dataset_list/set_search_history/',
  method: 'POST',
};

/*
 * 数据字典首页获取历史搜索的最近10条记录
 */
const getRecentSearchRecord = {
  url: 'datamart/datadict/dataset_list/get_search_history/',
};

/**
 * 获取数据集的敏感度信息
 *  * @params data_set_type 数据集类型
 *  * @params data_set_id 数据集id
 */
const getDataSetSensitivity = {
  url: 'v3/auth/sensitivity/retrieve_dataset/',
};

/**
 * 更新数据集的敏感度信息
 *  * @params data_set_type 数据集类型
 *  * @params data_set_id 数据集id
 *  * @params tag_method 标记范式
 *  * @params sensitivity 敏感度
 */
const updateDataSetSensitivity = {
  url: 'v3/auth/sensitivity/update_dataset/',
  method: 'POST',
};

/*
 * 查询数据质量度量汇总指标
 *  * @params measurement 数据质量指标
 */
const getDataQualitySummary = {
  url: 'v3/datamanage/dataquality/summary/:measurement/',
};

/*
 * 价值成本比变化趋势
 *  * @params dataset_id(string) rt_id/data_id
 *  * @params dataset_type(string) 数据集类型
 */
const getDataValueCost = {
  url: 'v3/datamanage/lifecycle/asset_value/trend/assetvalue_to_cost/',
};

/*
 * 价值评分变化趋势
 *  * @params dataset_id(string) rt_id/data_id
 *  * @params dataset_type(string) 数据集类型
 */
const getValueRateTrend = {
  url: 'v3/datamanage/lifecycle/asset_value/trend/asset_value/',
};

/*
 *  获取价值评分及其相关的热度、广度和重要度评分
 *  * @params dataset_id(string) rt_id/data_id
 *  * @params dataset_type(string) 数据集类型
 */
const getValueRate = {
  url: 'v3/datamanage/lifecycle/asset_value/metric/asset_value/',
};

/*
 *  获取重要度相关指标
 *  * @params dataset_id(string) rt_id/data_id
 *  * @params dataset_type(string) 数据集类型
 */
const getImportanceIndex = {
  url: 'v3/datamanage/lifecycle/asset_value/metric/importance/',
};

/*
 *   重要度评分变化趋势
 *  * @params dataset_id(string) rt_id/data_id
 *  * @params dataset_type(string) 数据集类型
 */
const getImportanceRateTrend = {
  url: 'v3/datamanage/lifecycle/asset_value/trend/importance/',
};

/*
 *   数据集数据应用情况
 *  * @params dataset_id(string) rt_id/data_id
 *  * @params dataset_type(string) 数据集类型
 */
const getLifeCycleApplication = {
  url: 'v3/datamanage/lifecycle/data_application/info/',
};

/*
 *   查看结果表存储总量和最近7天存储增量和数据增量
 *  * @params dataset_id(string) rt_id/data_id
 *  * @params dataset_type(string) 数据集类型
 */
const getLifeCycleStorage = {
  url: 'v3/datamanage/lifecycle/cost/metric/storage/',
};

/*
 *   数据集数据变更详情，包含数据迁移和数据标准化
 *  * @params dataset_id(string) rt_id/data_id
 *  * @params dataset_type(string) 数据集类型
 *  * @params is_standard(string) 是否要查询标准化数据变更
 */
const getDataChangeRecord = {
  url: 'v3/datamanage/lifecycle/data_change/info/',
};

/*
 * 查询字典详情页数据足迹
 *  * @params dataset_id
 *  * @params dataset_type
 */
const getFootPointData = {
  url: 'v3/datamanage/lifecycle/data_change/data_trace/',
};

/*
 * 查询字典详情页数据足迹 (新版接口)
 *  * @params dataset_id
 *  * @params dataset_type
 */
const getDataTraces = {
  url: 'v3/datamanage/lifecycle/data_traces/',
};

export {
  getResultInfo,
  getResultInfoDetail,
  getDictDetailFieldInfo,
  getResultTaskInfo,
  getSourceTaskInfo,
  getDataPreviewInfo,
  getDataTrendInfo,
  dataJudgePermission,
  getlineageSubGraghData,
  getDataDictionaryPlatformList,
  modifyTdwFieldDes,
  modifyTdwTableDes,
  modifyResultTableDes,
  modifyRawTableDes,
  modifyFieldInfo,
  getDictionaryStorageList,
  getRangeTrendData,
  getHeatTrendData,
  getDictScore,
  getDictSimilarity,
  getRecentQuery,
  getSearchHistory,
  getRecentSearchRecord,
  getDataSetSensitivity,
  updateDataSetSensitivity,
  getDataQualitySummary,
  getDataValueCost,
  getValueRateTrend,
  getValueRate,
  getImportanceIndex,
  getImportanceRateTrend,
  getLifeCycleApplication,
  getLifeCycleStorage,
  getDataChangeRecord,
  getFootPointData,
  getDataTraces,
};

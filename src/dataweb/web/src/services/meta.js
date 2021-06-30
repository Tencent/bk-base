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

import Meta from '../controller/meta';
import IDE from '../controller/ide';

const meta = new Meta();

/** 获取表结构相关接口基础接口地址 */
const getResultTablesBase = {
  url: '/v3/meta/result_tables/:rtid/',
};

/** 【原始接口】获取当前RtId下面的字段信息和配置信息
 * @param (params) rtid
 * @param (query) flag=all
 */
const getSchemaAndSqlBase = {
  url: '/v3/storekit/result_tables/:rtid/schema_and_sql/',
};

/** 【逻辑特殊，仅限数据入库使用！！！】获取当前RtId下面的字段信息和配置信息
 * @param (params) rtid
 * @param (params) clusterType:所属类型
 * @param (query) flag=all
 */
const getSchemaAndSqlByClusterType = {
  url: getSchemaAndSqlBase.url,
  callback: (res, option) => {
    const result = meta.formatSqlSchemaDataByClusterType(res.data, option);
    // console.log('getSchemaAndSqlByClusterType', result)
    return result;
  },
};

/** 【数据开发】获取当前RtId下面的字段信息和配置信息
 * @param (params) rtid
 * @param (params) clusterType:所属类型
 * @param (query) flag=all
 */
const getSchemaAndSqlFilterCluster = {
  url: [getSchemaAndSqlBase],
  callback(res, option) {
    const resp = res[0];
    return Object.assign({}, resp, { data: meta.formatFlowFields(resp.data, option) });
  },
};

/** 检查ip状态
 * @param biz_id
 * @param data POST Data
 */
const checkIpStatus = {
  url: '/bizs/:biz_id/check_ips_status/',
  method: 'POST',
};
// const postData = option.params.data
/** 模块IP选择器，选择模块获取当前模块下面的IP数量
 * @param (params) biz_id
 * @param (query) bk_obj_id
 * @param (query) bk_inst_id
 */
const getHostIpCounts = {
  url: '/bizs/:biz_id/module_host_count/',
};

/**
 * 业务列表
 */
const getAllBizs = {
  url: '/bizs/all_bizs/', // 'bizs/',
  callback(res, options) {
    return Object.assign(res, { data: meta.formatBizsList(res.data, options) });
  },
};

/**
 * 根据业务查询结果表列表
 */
const getBizResultTableList = {
  url: 'v3/meta/result_tables/',
};

/**
 * 合流功能里根据父节点的result_table_id来获取父节点的表结构
 */
const getResultTables = {
  url: '/v3/meta/result_tables/:rtid/fields/',
};


const getMultiResultTables = (service, options) => {
  const { rtids } = options.params;
  return {
    url: rtids.map(id => ({
      url: getResultTables.url.replace(':rtid', id),
    })),
    callback(resonse, options) {
      const ide = new IDE(options);
      return ide.formatMultiTableSchemaResponse(resonse, options);
    },
  };
};

/** 模块选择器中获取模块树 */
const getModuleTree = {
  url: '/bizs/:bizId/get_instance_topo/',
};

/** 获取集群过期时间配置[全量]
 * @param (query) cluster_type: 类型
 */
const getStorageClusterExpiresConfigs = {
  url: '/v3/storekit/clusters/:cluster_type/',
};

/** 获取集群过期时间配置[返回指定群集类型的过期时间]
 * @param (query) cluster_type: 群集类型
 * @param (param) cluster_name: 群集名称
 */
const getStorageClusterExpiresConfigsByType = {
  url: '/v3/storekit/clusters/:cluster_type/:cluster_name/',
};

/** 根据RTID获取FlowID */
const getStorageInfo = {
  url: '/result_tables/:rtid/get_storage_info/',
};

/** 获取Result Table信息，包含字段信息 */
const getResultTablesWithFields = {
  url: `${getResultTablesBase.url}?related=fields`,
  callback(res) {
    const filterFn = item => item.field_type !== 'timestamp' && item.field_type !== 'offset';
    if (res.data.fields && Array.isArray(res.data.fields)) {
      res.data.fields = res.data.fields.filter(item => filterFn(item));
    }
    return res;
  },
};

/** 获取Result Table信息，包含字段信息、清洗数据、存储信息 */
const getResultTablesStorages = {
  url: `${getResultTablesBase.url}?related=fields&related=data_processing`,
};

/**  获取单个数据处理实例的信息 */
const getProcessingInstance = {
  url: '/v3/meta/data_processings/:processing_id/',
};

/** 功能开关|全量 不做权限控制 */
const getOperationConfig = {
  url: '/v3/meta/operation_configs/',
};

/** 获取区域标签
 * 区分不同地域
 */
const getAvailableGeog = {
  url: '/v3/meta/tag/geog_tags/',
  callback(res, option) {
    return Object.assign(res, { data: meta.formatGeogArea(res.data, option) });
  },
};

/** 返回全量可用
 *  弃用
 */
const getGeogTags = {
  url: '/v3/meta/tag/geog/',
};

/**
 * 获取项目对应的区域代码
 */
const modifyProjectInfo = {
  url: '/v3/meta/projects/:project_id/',
};

/**
 * 修改项目信息
 */
const modifyProjectInfoPut = {
  url: '/v3/meta/projects/:project_id/',
  method: 'PUT',
};

/**
 * 创建新项目
 */
const creatProject = {
  url: '/v3/meta/projects/',
  method: 'POST',
};

/**
 * 恢复项目
 * @param (param) project_id
 */
const restoreProject = {
  url: '/v3/meta/projects/:project_id/enabled/',
  method: 'patch',
};

/**
 * 删除项目
 * @param (param) project_id
 */
const deleteProject = {
  url: 'v3/meta/projects/:project_id/disabled/',
  method: 'patch',
};

/** 我有权限的结果数据表 */
const getMineResultTables = {
  url: 'result_tables/mine/?is_clean=true&bk_biz_id=:bk_biz_id',
};

/** 自动补齐SQL */
const getSQLFields = {
  url: 'result_tables/:rtId/list_field/?filter_time=0',
  callback(res) {
    return Object.assign(res, { data: meta.formatField(res.data) });
  },
};
/** udf获取结果数据表 */
const getUDFResultTables = {
  url: '/v3/meta/result_tables/mine/?action_id=result_table.query_data',
};

/** 获取当前有权限的业务列表 */
const getMineBizs = {
  url: 'v3/auth/users/scope_dimensions/?action_id=:action_id&dimension=:dimension',
  callback(res, options) {
    return Object.assign(res, { data: meta.formatBizsList(res.data, options) });
  },
};

/** 自动获取相关字段中文名称 */
const zhNameSuggest = {
  url: '/v3/meta/analyse/similarity/suggest/',
  method: 'post',
};

// 获取所有用户名单
const getProjectMember = {
  url: 'projects/list_all_user/',
};

/** 获取指定项目下面有权限的rt表
 * @query bk_username
 * @query bk_biz_id
 * @query tdw_filter 检索内容(用于tdw右侧边栏条件过滤)
 * @query page
 * @query page_size
 * @query is_query =1为只返回可查询存储关联的rt
 * @query is_clean 新接口暂时无效
 */
const getMineResultTablesNew = {
  url: '/v3/meta/result_tables/mine/',
};

/** 获取我的项目信息 */
const getProjectList = {
  url: '/v3/meta/projects/mine/',
};

/** 拉取标签 */
const getRecommendsTags = {
  url: '/v3/meta/tag/recommends/',
};

export {
  getSchemaAndSqlByClusterType,
  getSchemaAndSqlFilterCluster,
  getStorageClusterExpiresConfigs,
  getStorageClusterExpiresConfigsByType,
  checkIpStatus,
  getHostIpCounts,
  getAllBizs,
  getBizResultTableList,
  getResultTables,
  getMultiResultTables,
  getModuleTree,
  getResultTablesWithFields,
  getResultTablesStorages,
  getProcessingInstance,
  getStorageInfo,
  getOperationConfig,
  modifyProjectInfo,
  getGeogTags,
  getAvailableGeog,
  getResultTablesBase,
  getSchemaAndSqlBase,
  getMineBizs,
  getMineResultTables,
  zhNameSuggest,
  getUDFResultTables,
  getSQLFields,
  modifyProjectInfoPut,
  getProjectMember,
  creatProject,
  restoreProject,
  deleteProject,
  getMineResultTablesNew,
  getProjectList,
  getRecommendsTags,
};

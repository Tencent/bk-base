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

import { getResultTablesBase } from './meta.js';

/**
 * rt => result_table简称
 */
/**
 * 获取单个结果表实例的信息
 */
const getResultTables = {
  url: '/meta/result_tables/:result_table_id/',
  method: 'GET',
};

/**
 * 新增tdw
 */
/**
 * 新增TDW表-获取集群数据
 */
const getClusterListForTdw = {
  url: '/v3/auth/tdw/user/get_clusters_by_user/',
};
/**
 * 新增TDW表-获取BD数据
 */
const getDbListForTdw = {
  url: '/v3/auth/tdw/user/get_db_with_select_priv/',
  // return ajax.get(`v3/auth/tdw/user/get_db_with_select_priv/?cluster_id=${clusterId}`)
};
/**
 * 新增TDW表-获取BD数据
 */
const getDbListWithCreate = {
  url: '/v3/auth/tdw/user/get_db_with_create_priv/',
  // return ajax.get(`v3/auth/tdw/user/get_db_with_create_priv/?cluster_id=${clusterId}`)
};
/**
 * 新增TDW表-获取tdw表
 */
const getTdwTableList = {
  url: '/v3/auth/tdw/user/get_tables_by_db_name/',
  // return ajax.get(`v3/auth/tdw/user/get_tables_by_db_name/?db_name=${params}`)
};
/**
 * 上传TDW-jar文件
 */
const upLoadFileJar = {
  url: '/v3/dataflow/flow/flows/:flowId/upload/',
  method: 'POST',
  // return ajax.post(`/v3/dataflow/flow/flows/${flowId}/upload/`, params)
};
/**
 * 创建 tdw 存量表对应的RT
 */
const createTdwRt = {
  url: '/v3/dataflow/flow/flows/:fid/create_tdw_source/',
  method: 'POST',
};
/**
 * 更新 tdw 存量表对应的RT
 */
const updateTdwRt = {
  url: '/v3/dataflow/flow/flows/:fid/update_tdw_source/',
  method: 'PATCH',
};
/**
 * 删除 tdw 存量表对应的RT
 */
const deleteTdwRt = {
  url: '/v3/dataflow/flow/flows/:fid/remove_tdw_source/',
  method: 'POST',
};
/**
 * 编辑 - 获取标准化之后的RT
 */
const getInfoTdw = getResultTablesBase;
/**
 * 编辑 - 查询洛子ID是否可以手动修改
 */
const getCheckLzId = {
  url: '/v3/meta/tdw/tables/access_existed_lz_id/',
};

/**
 * 回填 - 根据应用组获取tdbank的BID数据
 * @param (query) {
        cluster_type: 'tdbank',
        cluster_group: xxx
    }
 */
const getBIDTdw = {
  url: '/v3/storekit/clusters/',
};
/**
 * 获取 TDW-JAR Field的字段类型

 */
const getTypeListJar = {
  url: '/v3/meta/field_type_configs/',
};

export {
  getTypeListJar,
  getBIDTdw,
  getClusterListForTdw,
  getDbListForTdw,
  getDbListWithCreate,
  getTdwTableList,
  upLoadFileJar,
  createTdwRt,
  updateTdwRt,
  deleteTdwRt,
  getInfoTdw,
  getCheckLzId,
  getResultTables,
};

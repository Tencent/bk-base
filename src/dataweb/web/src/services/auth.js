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

import Auth from '../controller/auth';
import Meta from '../controller/meta';

const auth = new Auth();
const meta = new Meta();

/** 列举项目有权限的集群组 */
const getClusterGroup = {
  url: '/v3/auth/projects/:id/cluster_group/',
};

/**
 * 获取数据入库集群配置 | 列举 raw_data 有权限的集群组
 *  */
const getClusterGroupByRawId = {
  url: '/v3/auth/raw_data/:raw_data_id/cluster_group/',
};

/** 获取集群配置 | 获取所有类型为cluster_type的存储集群
 * @param clusterType: 类型
 * @param geogArea: 区域
 * @param role: usr
 */
const getStorageClusterConfigs = {
  url: '/v3/storekit/clusters/:clusterType/',
};

/**
 * 获取集群组列表
 */
const getClusterGroupConfig = {
  url: '/v3/meta/cluster_group_configs/',
};

const getRoleManager = {
  url: '/bizs/:bizId/list_role_member/',
};

/** 【数据入库】获取集群配置，包含权限
 * @param (params) id: raw_data_id
 * @param (query) clusterType: 集群类型
 */
const getClusterWithAccess = {
  url: [getClusterGroupByRawId, getStorageClusterConfigs, getClusterGroupConfig],
  callback(res, option) {
    return auth.formatClusterResultData(res, option.params.clusterType);
  },
};

/** 【数据开发-流程节点】获取集群配置，包含权限
 * @param (params) id: raw_data_id
 * @param (query) clusterType: 集群类型
 */
const getFlowClusterWithAccess = {
  url: [getClusterGroup, getStorageClusterConfigs, getClusterGroupConfig],
  callback(res, option) {
    return auth.formatClusterResultData(res, option.params.clusterType);
  },
};

/**
 * 获取某个业务下的结果表数量
 */
const getBizTicketsCount = {
  url: 'v3/auth/projects/:project_id/biz_tickets_count/',
  callback(res, options) {
    return Object.assign(res, { data: meta.formatBizsList(res.data, options) });
  },
};

/**
 * 获取某个业务下所有申请过的结果表
 */
const getAccessResultTable = {
  url: 'v3/auth/projects/:project_id/data_tickets/',
};

/**
 *  申请业务数据获取通用的业务列表(post方式)
 */
const getTicketsList = {
  url: 'v3/auth/tickets/',
  method: 'POST',
};

/**
 *  申请业务数据获取通用的业务列表(get方式)
 */
const getMethodTicketsList = {
  url: 'v3/auth/tickets/',
};

/** 权限续期
 * @param (params) id: raw_data_id
 * @param (query) expire: 续期时间
 */
const setPermissionRenewal = {
  url: '/v3/auth/tokens/:id/renewal/',
  method: 'POST',
};

/** 权限交接
 * @param (params) receiver
 */
const setAuthTransfer = {
  url: '/v3/auth/users/handover/',
  method: 'POST',
};

/** 获取数据集成业务列表
 * @param (params) bkUser: 用户名
 * @param (query) action_id: biz.job_access/biz.common_access
 */

const getAccessBizList = {
  url: '/v3/auth/users/:bkUser/bizs/',
};

/** 获取平台级管理员列表 */
const getAdminList = {
  url: '/v3/auth/roles/bkdata.resource_manager/users/',
};

/** 获取敏感度级别列表
 * @param (query) has_biz_role
 * @param (query) bk_biz_id
 */

const getSecretLevelList = {
  url: '/v3/auth/sensitivity/',
};

/** 校验用户与对象权限
 * @param (query) action_id
 * @param (query) object_id
 */

const checkUserAuth = {
  url: '/v3/auth/users/:user_id/check/',
  method: 'POST',
};

/** 添加项目下人员
 * @param (params) project_id
 * 参数示例：
 * {
        role_users: [
            { role_id: 'project.manager', user_ids: params.admin },
            {
                role_id: 'project.flow_member',
                user_ids: params.member
            }
        ]
    }
 */
const addProjectMember = {
  url: 'v3/auth/projects/:project_id/role_users/',
  method: 'PUT',
};

/** 申请业务数据
 * @param (params) action
 * @param (params) reason
 * @param (params) subject_class
 * @param (params) subject_id
 * @param (params) object_class
 * @param (params) scope
 */
const applyBiz = {
  url: 'v3/auth/ticket/',
  method: 'POST',
};
/** 获取发布状态 */
const getPublishStatus = {
  url: '/tools/get_functions/',
};

/** 获取发布状态 */
const getAuthResultTablesByProject = {
  url: '/v3/auth/projects/:project_id/data/',
};

export {
  getClusterGroup,
  getStorageClusterConfigs,
  getClusterGroupConfig,
  getRoleManager,
  getClusterWithAccess,
  getFlowClusterWithAccess,
  getBizTicketsCount,
  getAccessResultTable,
  getTicketsList,
  getMethodTicketsList,
  setPermissionRenewal,
  setAuthTransfer,
  getAccessBizList,
  getAdminList,
  getSecretLevelList,
  checkUserAuth,
  addProjectMember,
  applyBiz,
  getPublishStatus,
  getAuthResultTablesByProject,
};

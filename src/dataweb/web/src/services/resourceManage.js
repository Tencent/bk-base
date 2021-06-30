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
 * 获取资源组列表
 * listId   - all_list 所有列表
 *          - my_list 我的列表
 */
const getGourpList = {
  url: '/v3/resourcecenter/resource_groups/:listId/',
};

/** 获取资源组列表(无指标) */
const getGroupListWithoutOption = {
  url: '/v3/resourcecenter/resource_groups/',
};

/**
 * 创建资源组
 */

const createGroup = {
  url: '/v3/resourcecenter/resource_groups/',
  method: 'POST',
};

/**
 * 删除资源组
 */
const deleteGroup = {
  url: '/v3/resourcecenter/resource_groups/:resource_group_id/',
  method: 'DELETE',
};

/**
 * 查看资源组
 */
const viewGroup = {
  url: '/v3/resourcecenter/resource_groups/:resource_group_id/',
};

/**
 * 更新资源组
 */
const updateGroup = {
  url: '/v3/resourcecenter/resource_groups/:resource_group_id/',
  method: 'PUT',
};

/**
 * 获取资源类型
 */

const getResourceTypeList = {
  url: 'v3/resourcecenter/service_configs/',
};

/**
 * 获取套餐配置
 */

const getResourceUnitList = {
  url: 'v3/resourcecenter/resource_units/',
};

/**
 * 提交扩容申请
 */

const createGroupCapacityApply = {
  url: 'v3/resourcecenter/group_capacity_applys/',
  method: 'POST',
};

/** 查询服务列表 */
const getServiceList = {
  url: 'v3/resourcecenter/:resourceType/:resource_group_id/query_service_type/',
};

/** 查询集群列表 */
const getClusterList = {
  url: 'v3/resourcecenter/:resourceType/:resource_group_id/query_cluster/',
};

/** 获取计算资源cpu指标 */
const getCpuHistory = {
  url: 'v3/resourcecenter/:resourceType/:resource_group_id/cpu_history/',
};

/** 获取计算资源内存指标 */
const getMemoryHistory = {
  url: 'v3/resourcecenter/:resourceType/:resource_group_id/memory_history/',
};

/** 获取计算资源磁盘指标 */
const getDiskHistory = {
  url: 'v3/resourcecenter/:resourceType/:resource_group_id/disk_history/',
};

/** 获取app指标 */
const getAppHistory = {
  url: 'v3/resourcecenter/:resourceType/:resource_group_id/app_history/',
};

/** 获取计算资源任务数指标 */
const getTaskHistory = {
  url: 'v3/resourcecenter/:resourceType/:resource_group_id/project_tasks/',
};

/** 获取项目的资源组授权 */
const resourceViewList = {
  url: 'v3/resourcecenter/project_auth/:project_id/resource_groups/',
};

/** 项目申请资源组 */
const projectApplyResource = {
  url: 'v3/resourcecenter/project_auth/:project_id/add_resource_group',
  method: 'POST',
};

/** 授权列表 */
const getAuthList = {
  url: 'v3/resourcecenter/resource_groups/:resource_group_id/auth_list/',
};
export {
  getGourpList,
  createGroup,
  deleteGroup,
  viewGroup,
  updateGroup,
  getResourceTypeList,
  getResourceUnitList,
  createGroupCapacityApply,
  getServiceList,
  getClusterList,
  getCpuHistory,
  getMemoryHistory,
  getDiskHistory,
  getAppHistory,
  getTaskHistory,
  getGroupListWithoutOption,
  resourceViewList,
  projectApplyResource,
  getAuthList,
};

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
 * 获取我的告警列表
 * @param start_time {query} 最早告警时间
 * @param end_time {query} 最晚告警时间
 * @param alert_type {query} 告警类型, data_monitor, task_monitor
 * @param alert_target {query} 告警对象
 * @param alert_level {query} 告警级别, warning, danger
 * @param alert_status {query} 告警状态, alerting, shielded, converged, summarizing
 */
const getMineDmonitorAlert = {
  url: 'v3/datamanage/dmonitor/alert_details/mine/',
  method: 'GET',
};

/**
 * 获取告警详情
 */
const getAlertDetail = {
  url: 'v3/datamanage/dmonitor/alert_details/:alert_id/',
  method: 'GET',
};

/**
 * 获取告警对象列表
 */
const getMineAlertTargetList = {
  url: 'v3/datamanage/dmonitor/alert_targets/mine/',
};

/**
 * 获取告警配置列表
 * @param project_id {query} 项目ID
 * @param bk_biz_id {query} 业务ID
 * @param active {query} 告警是否启用
 * @param notify_way {query} 告警通知方式
 * @param scope {query} 只包含于用户相关的告警配置, received, managed
 */
const getMineDmonitorAlertConfig = {
  url: 'v3/datamanage/dmonitor/alert_configs/mine/',
  method: 'GET',
};

/**
 * 获取单个告警配置
 * @param alert_config_id {params} 告警配置ID
 */
const getAlertConfig = {
  url: 'v3/datamanage/dmonitor/alert_configs/:alert_config_id/',
  method: 'GET',
};

/**
 * 获取单个告警配置
 * @param alert_target_type {params} 告警对象类型
 * @param alert_target_id {params} 告警对象ID
 */
const getAlertConfigByTarget = {
  url: 'v3/datamanage/dmonitor/alert_configs/:alert_target_type/:alert_target_id/',
  method: 'GET',
};

/**
 * 更新单个告警配置
 */
const updateAlertConfig = {
  url: 'v3/datamanage/dmonitor/alert_configs/:alert_config_id/',
  method: 'PATCH',
};

/**
 * 告警屏蔽Restful接口
 * @param start_time {params} 屏蔽开始时间
 * @param end_time {params} 屏蔽结束时间
 * @param alert_level {params} 屏蔽告警级别
 * @param alert_code {params} 屏蔽告警策略
 * @param dimensions {params} 屏蔽告警维度
 */
const dmonitorAlertShield = {
  url: 'v3/datamanage/dmonitor/alert_shields/',
  method: 'POST',
};

/**
 * 获取当前有权限的实体
 */
const getMineAuthScopeDimension = {
  url: 'v3/auth/users/scope_dimensions/?action_id=:action_id&dimension=:dimension',
  method: 'GET',
};

/**
 * 获取有权向的sceope
 */
const getMineAuthScope = {
  url: 'v3/auth/users/:user_id/scopes/?action_id=:action_id&show_display=:show_display',
  method: 'GET',
};

/**
 * 获取数据平台质量告警支持的通知方式
 */
const getNotifyWayList = {
  url: 'v3/datamanage/dmonitor/notify_ways/',
  method: 'GET',
};

/**
 * 获取接收群组列表
 */
const getReceiveGroupList = {
  url: 'v3/datamanage/dmonitor/receive_groups/',
  method: 'GET',
};

/**
 * 获取数据质量指标
 */
const getDmonitorMetrics = {
  url: 'v3/datamanage/dmonitor/metrics/:measurement/',
  method: 'GET',
};

/** 数据开发模块的告警配置弹窗里获取alert_config_ids参数的接口
 * @param flow_id
 */
const getAlertConfigByFlowID = {
  url: '/v3/datamanage/dmonitor/alert_configs/dataflow/:flow_id/',
};

/** 数据开发模块的告警配置弹窗里提交告警配置的接口
 * @param alert_config_id
 */
const updateAlertConfigByFlowID = {
  url: '/v3/datamanage/dmonitor/alert_configs/:alert_config_id/',
  method: 'PATCH',
};

const getJobStatusConfigs = {
  url: '/v3/meta/job_status_configs/',
};

/**
 * 获取离线执行记录
 * @param processing_ids
 * @param period
 */
const getBatchExecutions = {
  url: '/v3/datamanage/dmonitor/batch_executions/',
  method: 'GET',
};

/**
 * 获取离线调度信息
 * @param processing_ids
 */
const getBatchSchedules = {
  url: '/v3/datamanage/dmonitor/batch_schedules/',
  method: 'GET',
};

/**
 * 画布任务获取告警列表信息
 * @query alert_config_id
 */
const getListAlert = {
  url: 'flows/:flowId/list_alert/',
};

export {
  getMineDmonitorAlert,
  getAlertConfig,
  getAlertConfigByTarget,
  getAlertDetail,
  getMineDmonitorAlertConfig,
  getMineAlertTargetList,
  getMineAuthScope,
  getMineAuthScopeDimension,
  getNotifyWayList,
  getReceiveGroupList,
  getDmonitorMetrics,
  dmonitorAlertShield,
  updateAlertConfig,
  getAlertConfigByFlowID,
  updateAlertConfigByFlowID,
  getJobStatusConfigs,
  getBatchExecutions,
  getBatchSchedules,
  getListAlert,
};

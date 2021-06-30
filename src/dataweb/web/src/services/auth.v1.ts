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

/** **********************************************
 * 该模块主要用于封装 API 调用，屏蔽基本配置信息，并支持 MOCK 拦截。
 * 默认返回内容为 Promise，注意处理回调逻辑
 ************************************************/
export const getObjectClass = {
  url: 'v3/auth/objects/objects_scope_action/',
};

export const getRoles = {
  url: '/v3/auth/roles/',
};

export const getRole = {
  url: '/v3/auth/roles/:roleId/',
};

export const queryUserRoles = {
  url: 'v3/auth/roles/user_role/',
};

export const queryRoleUsers = {
  url: '/v3/auth/roles/multi_users/',
};

export const updateRoleUsers = {
  url: '/v3/auth/roles/multi_update_roles/',
  method: 'PUT',
};

export const ticketStatus = {
  url: 'v3/auth/tickets/ticket_status/',
};

export const listTicket = {
  url: 'v3/auth/tickets/',
};

export const queryAuthScope = {
  url: '/v3/auth/objects/',
};

export const submitApplication = {
  url: '/v3/auth/tickets/',
  method: 'POST',
};

export const ticketState = {
  url: 'v3/auth/ticket_states/',
};

export const ticketDetail = {
  url: 'v3/auth/ticket_states/:stateId/',
};

export const ticketTypes = {
  url: 'v3/auth/tickets/ticket_types/',
};

export const submitApprove = {
  url: 'v3/auth/ticket_states/:id/approve/',
  method: 'POST',
};

export const queryTokens = {
  url: '/v3/auth/tokens/',
};

export const userPermScopes = {
  url: '/auth/user_perm_scopes/',
};

export const retrieveTokenDetail = {
  url: '/v3/auth/tokens/:tokenId/',
};

export const updateToken = {
  url: '/v3/auth/tokens/:tokenId/',
  method: 'PUT',
};

export const submitNewToken = {
  url: 'v3/auth/tokens/',
  method: 'POST',
};

export const todoCount = {
  url: 'v3/auth/tokens/',
  method: 'POST',
};

export const getTodoCount = {
  url: 'v3/auth/ticket_states/todo_count/',
};

/**
 * 获取授权码明文
 * @param {Number} tokenId
 */
export const getClearPermissionCode = {
  url: 'v3/auth/tokens/:tokenId/plaintext/',
};

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
/** **********************************************
 * 该模块主要用于封装 API 调用，屏蔽基本配置信息，并支持 MOCK 拦截。
 * 默认返回内容为 Promise，注意处理回调逻辑
 ************************************************/
export function getObjectClass() {
  return bkRequest.httpRequest('authV1/getObjectClass');
}

export function getTodoCount() {
  return bkRequest.httpRequest('authV1/getTodoCount');
}

export function queryRoleUsers(params) {
  return bkRequest.httpRequest('authV1/queryRoleUsers', { query: params });
}

export function updateRoleUsers(params) {
  return bkRequest.httpRequest('authV1/updateRoleUsers', { params });
}

export function ticketStatus(params) {
  return bkRequest.httpRequest('authV1/ticketStatus', { params });
}

export function listTicket(params) {
  return bkRequest.httpRequest('authV1/listTicket', { query: params });
}

export function queryAuthScope(params) {
  return bkRequest.httpRequest('authV1/queryAuthScope', { query: params });
}

export function submitApplication(params) {
  return bkRequest.httpRequest('authV1/submitApplication', { params });
}

export function ticketState(queryParam) {
  return bkRequest.httpRequest('authV1/ticketState', { query: queryParam });
}

export function ticketDetail(stateId) {
  return bkRequest.httpRequest('authV1/ticketDetail', { params: { stateId } });
}

export function ticketTypes() {
  return bkRequest.httpRequest('authV1/ticketTypes');
}

export function submitApprove(id, params) {
  return bkRequest.httpRequest('authV1/submitApprove', {
    params: {
      id,
      process_message: params.process_message,
      status: params.status,
    },
  });
}

export function queryTokens(params) {
  return bkRequest.httpRequest('authV1/queryTokens', { query: params });
}

export function userPermScopes(params) {
  return bkRequest.httpRequest('authV1/userPermScopes', { query: params });
}

export function retrieveTokenDetail(tokenId) {
  return bkRequest.httpRequest('authV1/retrieveTokenDetail', { params: { tokenId } });
}

export function updateToken(tokenId, params) {
  return bkRequest.httpRequest('authV1/updateToken', { params: { ...params, tokenId } });
}

export function submitNewToken(params) {
  return bkRequest.httpRequest('authV1/submitNewToken', { params });
}

export function todoCount(params) {
  return bkRequest.httpRequest('authV1/todoCount', { params });
}

/**
 * 获取授权码明文
 * @param {Number} tokenId
 */
export function getClearPermissionCode(tokenId) {
  return bkRequest.httpRequest('authV1/getClearPermissionCode', { params: { tokenId } });
}

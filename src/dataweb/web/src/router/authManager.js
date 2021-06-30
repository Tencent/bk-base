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

export default [
  // 权限管理页
  {
    name: 'authManage',
    path: '/auth-manage/',
    meta: { alias: '权限管理页' },
    component: () => import('@/pages/powerManage/powerManage.vue'),
  },
  // 申请权限详情页面
  {
    name: 'Auth',
    path: '/auth-manage/detail/:applyid',
    meta: { alias: '申请权限详情页面' },
    component: () => import('@/pages/powerManage/authDetail.vue'),
  },
  // 申请权限审核页面
  {
    path: '/applyauth-check',
    name: 'applyauth_check',
    meta: { alias: '申请权限审核页面' },
    component: () => import('@/pages/powerManage/applyAuthCheck.vue'),
  },
  // // 申请权限审核页面详情
  {
    path: '/applyauth-check/detail/:applyid',
    name: 'applyauth_check_detail',
    meta: { alias: '申请权限审核页面详情' },
    component: () => import('@/pages/powerManage/applyAuthCheckDetail.vue'),
  },
  {
    path: '/auth-center',
    redirect: '/auth-center/permissions',
  },
  {
    path: '/auth-center/permissions',
    name: 'MyPermissions',
    meta: { alias: '权限管理-我的权限' },
    component: () => import('@/pages/authCenter/permissions/index.vue'),
  },
  {
    path: '/auth-center/token-management',
    name: 'TokenManagement',
    meta: { alias: '权限管理-授权管理' },
    component: () => import('@/pages/authCenter/token/TokenManagement.vue'),
  },
  {
    path: '/auth-center/token-management/new',
    name: 'TokenNew',
    meta: { alias: '权限管理-授权管理' },
    component: () => import('@/pages/authCenter/token/TokenNew.vue'),
  },
  {
    path: '/auth-center/token-management/edit/:token_id',
    name: 'TokenEdit',
    meta: { alias: '权限管理-授权管理' },
    component: () => import('@/pages/authCenter/token/TokenNew.vue'),
  },
  {
    path: '/auth-center/records',
    name: 'Records',
    meta: { alias: '权限管理-我的申请' },
    component: () => import('@/pages/authCenter/ticket/Records.vue'),
  },
  {
    path: '/auth-center/todos',
    name: 'Todos',
    meta: { alias: '权限管理-待办事项' },
    component: () => import('@/pages/authCenter/ticket/Todos.vue'),
  },
];

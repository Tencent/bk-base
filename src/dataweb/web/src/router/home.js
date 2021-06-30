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
function getHomeRoute(option) {
  return option.isActive('dataflow')
    ? import('@/pages/userCenter/userCenter.vue')
    : import('@/pages/dataManage/dataManage.vue');
}

export default option => [
  {
    path: '/login/success',
    name: 'login-success',
    component: () => import('@/pages/Login/Success.vue'),
  },
  {
    path: '/',
    name: 'home',
    meta: { alias: '首页' },
    component: () => getHomeRoute(option),
  },

  // 用户中心
  {
    path: '/user-center',
    name: 'user_center',
    meta: { alias: '用户中心' },
    component: () => import('@/pages/userCenter/userCenter.vue'),
  },
  {
    path: '/no-project',
    name: 'no_project',
    component: () => import('@/pages/noProject/noProject.vue'),
  },
  {
    path: '/resource-detail/:resourceId/:type/:resourceGroup',
    name: 'resource-detail',
    meta: { alias: '资源组详情' },
    component: () => import('@/pages/userCenter/ResourceManage/ResourceDetaile.vue'),
  },
  // 路由跳转服务，根据RTID解析自动跳转到目的地址
  {
    path: '/routehub/:rtid?',
    name: 'routeHub',
    meta: { alias: '路由跳转服务' },
    component: () => import('@/pages/RouterHub/index.vue'),
  },
];

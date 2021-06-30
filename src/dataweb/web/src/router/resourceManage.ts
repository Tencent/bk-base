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

// 数据管理列表页
export default [
  // 数据管理列表页
  {
    path: '/resource-manage',
    name: 'resource-manage',
    meta: { alias: '资源管理-Index' },
    component: () => import('@/pages/ResourceManage/ResourceIndex'),
    redirect: '/resource-manage/list',
    children: [
      {
        path: 'list',
        name: 'resource-list',
        meta: { alias: '资源管理-资源列表' },
        component: () => import('@/pages/ResourceManage/List/Index'),
        children: [
          {
            path: '/',
            name: 'resource-index',
            meta: { alias: '资源管理-我使用的资源' },
            component: () => import('@/pages/ResourceManage/List/TabViews/InUse'),
          },
          {
            path: 'owns/',
            name: 'resource-owns',
            meta: { alias: '资源管理-我管理的资源' },
            component: () => import('@/pages/ResourceManage/List/TabViews/Owns'),
          },
          {
            path: 'avaliable/',
            name: 'resource-avaliable',
            meta: { alias: '资源管理-可申请的资源' },
            component: () => import('@/pages/ResourceManage/List/TabViews/Avaliable'),
          },
        ],
      },
      {
        path: 'pool',
        name: 'resource-pool',
        meta: { alias: '资源管理-业务资源池' },
        component: () => import('@/pages/ResourceManage/Pool/Index'),
      },
    ],
  },
];

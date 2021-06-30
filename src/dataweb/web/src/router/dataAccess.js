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
    path: '/data-access/',
    name: 'data',
    meta: { alias: '数据集成-数据源列表' },
    component: () => import('@/pages/RouterHub/SubRuter.vue'),
    children: [
      {
        path: '',
        name: 'data-index',
        meta: { alias: '数据集成-首页' },
        component: () => import('@/pages/dataManage/dataManage.vue'),
      },
      // 数据管理详情页V1.1
      {
        path: 'data-detail/:did/:tabid?',
        name: 'data_detail',
        meta: { alias: '数据集成-数据源详情' },
        component: () => import('@/pages/dataManage/dataDetailV1.1.vue'),
      },
      // 数据清洗
      {
        path: 'new-clean/:rawDataId',
        name: 'create_clean',
        meta: { alias: '数据集成-新增清洗' },
        component: () => import('@/pages/dataManage/cleanChild/dataClean.vue'),
      },
      {
        path: 'edit-clean/:rawDataId/:rtid',
        meta: { alias: '数据集成-编辑清洗' },
        name: 'edit_clean',
        component: () => import('@/pages/dataManage/cleanChild/dataClean.vue'),
      },

      // 结果数据查询
      {
        path: 'query/:defaultBizId?/:defaultResultId?/:clusterType?',
        name: 'dataQuery',
        meta: { alias: '数据查询' },
        component: () => import('@/pages/DataQuery/DataQueryIndex.vue'),
      },
      {
        path: 'createdataid',
        name: 'createDataid',
        meta: { alias: '数据集成-数据接入列表' },
        component: () => import('@/pages/DataAccess/NewForm/AccessIndex.vue'),
      },
      {
        path: 'newform',
        name: 'newform',
        meta: { alias: '数据集成-新建数据接入' },
        component: () => import('@/pages/DataAccess/NewForm/AccessIndex.vue'),
      },
      {
        path: 'updatedataid/:did',
        name: 'updateDataid',
        meta: { alias: '数据集成-编辑数据源' },
        component: () => import('@/pages/DataAccess/NewForm/AccessIndex.vue'),
      },
      {
        path: 'success/:rawDataId',
        name: 'success',
        meta: { alias: '数据集成-数据接入成功页' },
        component: () => import('@/pages/DataAccess/NewForm/AccessSuccess.vue'),
      },
      // 数据清洗成功页
      {
        path: 'success/:rawDataId',
        name: 'clean-success',
        meta: { alias: '数据集成-数据清洗成功页' },
        component: () => import('@/pages/DataAccess/NewForm/FormItems/Components/AccessClearSuccess.vue'),
      },
    ],
  },
];

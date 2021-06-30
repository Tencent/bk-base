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
  // 数据集市
  {
    path: '/data-mart/',
    name: 'dataMart',
    component: () => import('@/pages/datamart/DataMart.index.vue'),
    children: [
      {
        path: 'data-map',
        name: 'DataMap',
        meta: { alias: '数据集市-数据地图', keepAlive: true },
        component: () => import('@/pages/datamart/dataMap.vue'),
      },
      // 数据字典新改造页
      {
        path: 'data-dictionary',
        name: 'DataDictionary',
        meta: { alias: '数据集市-数据字典新改造页', keepAlive: true },
        component: () => import('@/pages/datamart/dataDictionaryNew.vue'),
      },
      // 数据字典详情
      {
        path: 'data-dictionary/detail',
        name: 'DataDetail',
        meta: { alias: '数据集市-数据字典详情' },
        component: () => import('@/pages/datamart/DataDict/DataDetail.vue'),
      },
      // 数据字典搜索结果页
      {
        path: 'data-dictionary/search-result',
        name: 'DataSearchResult',
        meta: { alias: '数据集市-数据字典搜索结果页' },
        component: () => import('@/pages/datamart/dataDictionarySearch.vue'),
      },
      // 数据盘点
      {
        path: 'data-dictionary/data-inventory',
        name: 'DataInventory',
        meta: { alias: '数据集市-数据盘点' },
        component: () => import('@/pages/datamart/dataDictionaryInventory.vue'),
      },
      // 数据标准
      {
        path: 'data-standard',
        name: 'DataStandard',
        meta: { alias: '数据集市-数据标准' },
        component: () => import('@/pages/datamart/dataDictionaryStandard.vue'),
      },
      // 数据模型
      {
        path: 'data-model/',
        name: 'dataModel',
        meta: { alias: '数据集市-数据模型' },
        redirect: 'data-model/:modelId?/view',
        component: () => import('@/pages/DataModelManage/DMMIndex.vue'),
        children: [
          {
            path: ':modelId?/view',
            name: 'dataModelView',
            meta: { alias: '数据模型-查看模式' },
            component: () => import('@/pages/DataModelManage/Components/ModelRightView.vue'),
          },
          {
            path: ':modelId/edit',
            name: 'dataModelEdit',
            meta: { alias: '数据模型-编辑模式' },
            component: () => import('@/pages/DataModelManage/Components/ModeRightBody.vue'),
          },
          // 操作记录
          {
            path: ':modelId/operation',
            name: 'dataModelOperation',
            meta: { alias: '数据模型-操作记录' },
            component: () => import('@/pages/DataModelManage/Components/ModelOperation.vue'),
          },
        ],
      },
      // 数据标准详情页
      {
        path: 'data-standard-detail',
        name: 'DataStandardDetail',
        meta: { alias: '数据集市-数据标准详情页' },
        component: () => import('@/pages/datamart/DataStandardComp/StandardDetail.vue'),
      },
    ],
  },
];

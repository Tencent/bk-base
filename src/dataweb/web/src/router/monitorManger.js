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
  // 监控告警中心告警配置列表
  {
    path: '/dmonitor-center/alert-config/',
    name: 'AlertConfigList',
    meta: { alias: '数据质量-告警配置列表' },
    component: () => import('@/pages/dmonitorCenter/alertConfigList.vue'),
  },
  // 创建监控告警中心告警配置
  {
    path: '/dmonitor-center/alert-config/add/',
    name: 'AlertConfigAdd',
    meta: { alias: '数据质量-新建告警配置' },
    component: () => import('@/pages/dmonitorCenter/alertConfigDetail.vue'),
  },
  // 监控告警中心告警配置详情
  {
    path: '/dmonitor-center/alert-config/:alertConfigId/',
    name: 'AlertConfigDetail',
    meta: { alias: '数据质量-告警配置详情' },
    component: () => import('@/pages/dmonitorCenter/alertConfigDetail.vue'),
  },
  // 监控告警中心告警详情
  {
    path: '/dmonitor-center/alert/:alertId/:tabid?',
    name: 'AlertDetail',
    meta: { alias: '数据质量-新建告警配置' },
    component: () => import('@/pages/dmonitorCenter/alertDetail.vue'),
  },
];

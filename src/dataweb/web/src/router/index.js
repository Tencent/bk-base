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

import Vue from 'vue';
import VueRouter from 'vue-router';
import dataAccess from './dataAccess';
import authManager from './authManager';
import monitorManger from './monitorManger';
import dataManager from './dataManager';
import getHomeManger from './home';
import dataDev from './dataDev';
import httpCode from './httpCode';
import dataExplore from './dataExplore';
import dataReports from './dataReports';
import datamodelmanager from './datamodelmanager';
import resourceManage from './resourceManage';
import extend from '@/extends/index';

Vue.use(VueRouter);

const getRouter = (option) => {
  const router = new VueRouter({
    linkActiveClass: 'active',
    routes: [
      ...httpCode,
      ...getHomeManger(option),
      ...dataDev,
      ...authManager,
      ...monitorManger,
      ...dataManager,
      ...dataAccess,
      ...dataExplore,
      ...dataReports,
      ...datamodelmanager,
      ...resourceManage,
      ...extend.router,
    ],
  });

  router.beforeEach((to, from, next) => {
    /** flowIframe 判断当前是否被iframe嵌入 被嵌入时跳页面需要带上该参数防止导航出现 */
    const { flowIframe } = from.query;
    if (flowIframe && !to.query.flowIframe) {
      Object.assign(to.query, { flowIframe });
    }
    window.BKMonitor && window.BKMonitor.routeDwellTimeStart(from, to);
    next();
  });

  router.afterEach((to, from) => {
    try {
      window.BKMonitor && window.BKMonitor.routeDwellTimeEnd(to, from);
      const toWindowLocation = `${window.location.href.split('#')[0]}#${to.fullPath}`;
      window.top.postMessage(
        JSON.stringify({
          operation: 'save_current_app_url',
          window_name: window.name,
          app_current_url: toWindowLocation,
        }),
        '*',
      );
    } catch (e) {
      console.info('postMessage err:', e);
    }
  });

  return router;
};

export default getRouter;

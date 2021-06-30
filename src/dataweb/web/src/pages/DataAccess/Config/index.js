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

import extend from '@/extends/index';
async function getCurrentComponent(type, mode = 'new') {
  const configExt = await extend.callJsFragmentFn('AccessConfig') || {};

  /** 查看详情和新建接入共用组件 */
  const ALL_ACCESS_OBJ_COMPONENTS_BASE = {
    http: () => import('@/pages/DataAccess/NewForm/FormItems/Components/AccessHttp'),
    file: () => import('@/pages/DataAccess/NewForm/FormItems/Components/AccessFile'),
    offlinefile: () => import('@/pages/DataAccess/NewForm/FormItems/Components/AccessOfflineFile'),
    kafka: () => import('@/pages/DataAccess/NewForm/FormItems/Components/AccessKafka'),
    ...(configExt.ALL_ACCESS_OBJ_COMPONENTS_BASE || {})
  };

  const EMPTY_COMPONENT = () => import('@/pages/DataAccess/NewForm/FormItems/Components/AccessEmpty');

  /** 查看详情区别于新建需要的加载组件 */
  const ALL_ACCESS_OBJ_COMPONENTS_DETAILS = {
    log: () => import('@/pages/DataAccess/Details/LogAccessObject'),
    script: () => import('@/pages/DataAccess/Details/ScriptAccessObject'),
    db: () => import('@/pages/DataAccess/Details/DBAccessObject'),
    ...(configExt.ALL_ACCESS_OBJ_COMPONENTS_DETAILS || {})
  };

  /** 新建接入需要组件，区别查看详情组件 */
  const ALL_ACCESS_OBJ_COMPONENTS_NEW = {
    log: () => import('@/pages/DataAccess/NewForm/FormItems/Components/AccessLog'),
    script: () => import('@/pages/DataAccess/NewForm/FormItems/Components/AccessScript'),
    db: () => import('@/pages/DataAccess/NewForm/FormItems/Components/AccessDB'),
    ...(configExt.ALL_ACCESS_OBJ_COMPONENTS_NEW || {})
  };

  const ALL_COMPONENTS = {
    new: Object.assign({}, ALL_ACCESS_OBJ_COMPONENTS_BASE, ALL_ACCESS_OBJ_COMPONENTS_NEW),
    detail: Object.assign({}, ALL_ACCESS_OBJ_COMPONENTS_BASE, ALL_ACCESS_OBJ_COMPONENTS_DETAILS),
  };

  /** 节点相关配置 */
  const CONFIG = [
    { id: 'log', name: window.$t('日志文件') },
    { id: 'db', name: window.$t('数据库') },
    { id: 'queue', name: window.$t('消息队列') },
    { id: 'http', name: 'HTTP' },
    { id: 'file', name: window.$t('文件上传_SE') },
    { id: 'script', name: window.$t('脚本上报') },
    { id: 'sdk', name: 'SDK' },
    { id: 'custom', name: window.$t('自定义') },
    { id: 'kafka', name: 'KAFKA' },
    { id: 'offlinefile', name: window.$t('离线文件上传') },
    ...(configExt.BASECONFIG || {})
  ];

  const config = CONFIG.find(conf => conf.id === type) || { name: 'Not Found' };
  const compoent = (ALL_COMPONENTS[mode] && ALL_COMPONENTS[mode][type]) || EMPTY_COMPONENT;
  return Object.assign({}, config, { component: compoent });
}

export { getCurrentComponent };

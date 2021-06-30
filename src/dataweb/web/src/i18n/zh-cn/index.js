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

import common from './common.json';
import dataClean from './dataclean.json';
import dataAccess from './dataAccess.json';
import dataManage from './dataManage.json';
import dataRemoval from './dataRemoval.json';
import sdkNode from './sdkNode.json';
import dataMap from './dataMap.json';
import dataExplore from './dataExplore.json';
import nodeForm from './nodeForm.json';
import dataInventory from './dataInventory.json';
import resourceManage from './resourceManage.json';
import authManage from './authManage.json';
import dataAlert from './dataAlert.json';
import dataLog from './dataLog.json';
import dataView from './dataView.json';
import dataDict from './dataDict.json';
import extend from '@/extends/index';
const i18nExt = extend.callJsFragmentFn('i18n') || {};

export default Object.assign(
  {},
  common,
  dataMap,
  dataClean,
  dataManage,
  dataRemoval,
  sdkNode,
  dataAccess,
  dataExplore,
  nodeForm,
  dataInventory,
  authManage,
  dataAlert,
  dataView,
  resourceManage,
  dataLog,
  dataDict,
  { ...(i18nExt.langZh || {}) }
);

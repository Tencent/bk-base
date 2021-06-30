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

import * as dataAccess from './dataAccess';
import * as auth from './auth';
import * as dataStorage from './dataStorage';
import * as meta from './meta';
import * as dataClear from './dataClear';
import * as modelFlow from './modelFlow';
import * as dataFlow from './dataFlow';
import * as tdw from './tdw';
import * as dataDict from './dataDict';
import * as udf from './udf';
import * as dataExplore from './dataExplore';
import * as dataRemoval from './dataRemoval';
import * as dmonitorCenter from './dmonitorCenter';
import * as dataInventory from './dataInventory';
import * as monitor from './monitor';
import * as dataStandard from './dataStandard';
import * as resourceManage from './resourceManage';
import * as dataLog from './dataLog';
import * as dataMart from './dataMart';
import * as dataModelManage from './dataModelManage';
import * as dataGraph from './dataGraph';
import * as authV1 from './auth.v1';
import * as common from './common';
import extend from '@/extends/index';

export default {
  dataAccess,
  auth,
  dataStorage,
  meta,
  dataClear,
  modelFlow,
  dataFlow,
  tdw,
  dataDict,
  udf,
  dataExplore,
  dmonitorCenter,
  dataRemoval,
  dataInventory,
  monitor,
  dataStandard,
  dataLog,
  resourceManage,
  dataMart,
  dataModelManage,
  dataGraph,
  authV1,
  common,
  ...extend.services,
};

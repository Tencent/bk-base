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

/**
 *  Vuex配置文件
 */

import Vue from 'vue';
import Vuex from 'vuex';
import common from './modules/common';
import appEnv from './modules/env';
import createDataid from './modules/createDataid';
import api from './modules/api';
import ide from './modules/ide';
import auth from './modules/auth';
import accessDetail from './modules/dataAccess/details';
import dataQuery from './modules/dataQuery/index';
import tdw from './modules/tdw';
import docs from './modules/docs';
import udf from './modules/udf';
import dataTag from './modules/dataTag';
import global from './modules/global';
import modeling from './modules/modeling';
Vue.use(Vuex);

export default new Vuex.Store({
  modules: {
    global,
    common,
    appEnv,
    createDataid,
    api,
    ide,
    auth,
    accessDetail,
    dataQuery,
    tdw,
    docs,
    udf,
    dataTag,
    modeling,
  },
});

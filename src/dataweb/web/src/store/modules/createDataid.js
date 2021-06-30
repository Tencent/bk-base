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
 *  数据接入状态管理
 */
import { ajax, bkRequest } from '@/common/js/ajax';

const state = {
  dataDefine: {},
  dataAuth: {},
  dataType: {},
  accessObj: {},
  marks: '',
  method: {},
  filter: {},
  type: '',
  info: {},
};

const getters = {
  getDefine: state => state.dataDefine,
  getAuth: state => state.dataAuth,
  getType: state => state.dataType,
  getAccessObj: state => state.accessObj,
  getMarks: state => state.marks,
  getAccessType: state => state.type,
  getAccessMethod: state => state.method,
  getFilter: state => state.filter,
  getInfo: state => state.info,
};

const actions = {
  actionDataDefine({ commit }, dataDefine) {
    commit('mutateDataDefine', dataDefine);
  },
  actionDataAuth({ commit }, dataAuth) {
    commit('mutateDataAuth', dataAuth);
  },
  actionAccessObj({ commit }, accessObj) {
    commit('mutateAccessObj', accessObj);
  },
  actionMarks({ commit }, marks) {
    commit('mutateMarks', marks);
  },
  getDataSrcCtg() {
    return ajax.get('/v3/access/category/');
  },

  /**  查询部署状态统计 */
  getDeploySummary({}, did) {
    // return ajax.get(`/v3/collectorhub/deploy_plan/${did}/status/summary/`)
    return bkRequest.httpRequest('dataAccess/getAccessSummary', {
      mock: false,
      params: { raw_data_id: did },
    });
  },

  /** 查询部署状态 */
  getDeployPlanStatus({}, did) {
    // return ajax.get(`/v3/collectorhub/deploy_plan/${did}/status/`)
    return bkRequest.httpRequest('dataAccess/queryAccessStatus', {
      params: {
        raw_data_id: did,
        show_display: 1,
      },
    });
  },

  /** 查询部署计划, 部署计划详情
     * {
            raw_data_id: did,
            show_display: 1
        }
     */
  getDelpoyedList({}, option) {
    // return ajax.get(`/v3/collector/deploy_plan/${did}/?show_display=1`)
    return bkRequest.httpRequest('dataAccess/getAccessDetails', {
      mock: false,
      params: option.params,
      query: option.query,
    });
  },

  /** 接入变更历史 */
  getDeployHistory({}, did) {
    return bkRequest.httpRequest('dataAccess/getDeployHistory', {
      mock: false,
      params: {
        raw_data_id: did,
      },
    });
  },

  /** 查询历史部署状态 */
  getDeployStatusHistory({}, pa) {
    return bkRequest.httpRequest('dataAccess/getDeployStatusHistory', {
      mock: false,
      params: pa.params,
      query: pa.query,
    });
  },
};

const mutations = {
  mutateDataDefine(state, dataDefine) {
    state.dataDefine = dataDefine;
  },
  mutateDataAuth(state, dataAuth) {
    state.dataAuth = dataAuth;
  },
  mutateDataType(state, dataType) {
    state.dataType = dataType;
  },
  mutateAccessObj(state, accessObj) {
    state.accessObj = accessObj;
  },
  mutateMarks(state, marks) {
    state.marks = marks;
  },
  mutateAccessMethod(state, method) {
    state.method = method;
  },
  mutateFilter(state, filter) {
    state.filter = filter;
  },
  mutateDataInfo(state, info) {
    state.info = info;
  },
  selectDataAccessType(state, type) {
    state.type = type;
  },
};

export default {
  state,
  getters,
  actions,
  mutations,
};

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
 *  common - 数据查询
 */
import { ajax } from '@/common/js/ajax';
const state = {
  account: {
    tdw: {
      username: '',
      tdw_username: '',
      tdw_password: '',
    },
  },
};

const getters = {
  applicationGroupList: state => state.applicationGroupList,
};

const mutations = {
  setHistoryLoading(state, isLoading) {
    state.hsitory.isLoading = isLoading;
  },

  setHistoryList(state, list) {
    state.hsitory.list = list;
  },
};

const actions = {
  /*
   * 获取tdw账号
   */
  getTdwAccount() {
    return ajax.get('v3/auth/tdw/user/');
  },
  /*
   * 设置tdw账号
   */
  setTdwAccount({}, params) {
    return ajax.post('v3/auth/tdw/user/', params);
  },
  /*
   * 获取应用组
   */
  getApplicationGroup() {
    return ajax.get('v3/meta/tdw/app_groups/mine/');
  },
  /**
   * 根据应用组获取server、version、computing
   */
  getApplicationDetail({}, { appGroupName }) {
    return ajax.get(`v3/meta/tdw/app_groups/${appGroupName}/`);
  },
  /**
   * 新增TDW表-获取集群数据
   */
  getClusterListForTdw() {
    return ajax.get('v3/auth/tdw/user/get_clusters_by_user/');
  },
  /**
   * 新增TDW表-获取BD数据
   */
  getDbListForTdw({}, clusterId) {
    return ajax.get(`v3/auth/tdw/user/get_db_with_select_priv/?cluster_id=${clusterId}`);
  },
  /**
   * 新增TDW表-获取tdw表
   */
  getTdwTableList({}, params) {
    return ajax.get(`v3/auth/tdw/user/get_tables_by_db_name/?db_name=${params}`);
  },
  /**
   * 上传TDW-jar文件
   */
  upLoadFileJar({}, { params, flowId }) {
    const req = ajax.post(`/v3/dataflow/flow/flows/${flowId}/upload/`, params);
    // setTimeout(() => {
    //     cancelHttRequest.active(`/v3/dataflow/flow/flows/${flowId}/upload/`)
    // })
    return req;
  },
};

export default {
  namespaced: true,
  state,
  getters,
  actions,
  mutations,
};

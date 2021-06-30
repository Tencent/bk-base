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
 *  common - 公用状态管理
 */

const state = {
  /** 接入详情当前active tab */
  activeTabName: '2',
  accessSummaryStatus: {},
  alertData: {}, // alert_config_ids参数,
  // 丢弃数据
  discardData: [],
};

const getters = {
  discardData: state => state.discardData,
  getPid: state => state.pid,
};

const actions = {
  setDiscardData({ commit }, data) {
    commit('setDiscardData', data);
  },
  setActiveTabName({ commit }, tabName) {
    commit('setActiveTabName', tabName);
  },
  setAccessSummaryStatus({ commit }, accessSummaryStatus) {
    commit('setAccessSummaryStatus', accessSummaryStatus);
  },
  setAlertData({ commit }, alertData) {
    commit('setAlertData', alertData);
  },
};

const mutations = {
  setDiscardData(state, data) {
    state.discardData = data;
  },
  setActiveTabName(state, tabName) {
    state.activeTabName = tabName;
  },
  setAccessSummaryStatus(state, accessSummaryStatus) {
    state.accessSummaryStatus = accessSummaryStatus;
  },
  setAlertData(state, alertData) {
    state.alertData = alertData;
  },
};

export default {
  namespaced: true,
  state,
  getters,
  actions,
  mutations,
};

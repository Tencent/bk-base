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
import { getMethodWarning } from '@/common/js/util.js';
const state = {
  resultTable: '',
  activeBizId: 0,
  hsitory: {
    isLoading: false,
    list: [],
    viewOrders: [],
    isEsView: false,
    isSqlView: false,
    activeItem: null,
    activeTabName: '',
    clusterType: '',
  },

  search: {
    isLoading: false,
    historyLoading: false,
  },
};

const getters = {
  hsitory: state => state.hsitory,
};

const actions = {
  /** 获取个人在项目中最近十条选择结果表历史 */
  getMinResultHistory({ commit }) {
    commit('setHistoryLoading', true);
    return ajax
      .get('result_tables/list_selected_history/')
      .then((res) => {
        if (res.result) {
          const historyList = (res.data || []).map(item => ({ name: item, show: false, isloading: false }));
          commit('setHistoryList', historyList);
        } else {
          getMethodWarning(res.message, res.code);
        }
      })
      .finally(() => {
        commit('setHistoryLoading', false);
      });
  },

  /** c查询历史重新排序 */
  reOrderHistoryList({ commit, state }, { item, index }) {
    const historyList = state.hsitory.list;
    historyList.unshift(item);
    historyList.splice(index + 1, 1);
    commit('setHistoryList', historyList);
  },

  /** 设置当前可选择存储类型，对应api返回order */
  setViewOrders({ commit }, { orders }) {
    commit('setViewOrders', orders);
  },

  /** 设置当前选中历史数据Item，默认ActiveType */
  setActiveHistoryItem({ commit }, { item }) {
    commit('setActiveHistoryItem', item);
  },
};

const mutations = {
  setHistoryLoading(state, isLoading) {
    state.hsitory.isLoading = isLoading;
  },

  setHistoryList(state, list) {
    state.hsitory.list = list;
  },

  /**  设置当前查询记录的Orders，用于保存当前查询的存储类型 */
  setViewOrders(state, orders = []) {
    state.hsitory.viewOrders = orders;
    state.hsitory.isEsView = orders.some(order => /^es/i.test(order));
    state.hsitory.isSqlView = orders.some(order => !/^es/i.test(order));
    state.hsitory.activeTabName = (state.hsitory.isSqlView && 'sqlQuery') || 'esQuery';
  },

  setActiveHistoryItem(state, item) {
    state.hsitory.activeItem = item;
  },

  setResultTable(state, tableName) {
    state.resultTable = tableName;
  },

  /** 查询Loading设置 */
  setSearchLoading(state, isLoading) {
    state.search.isLoading = isLoading;
  },

  /** 查询历史Loading设置 */
  setSearchHistoryLoading(state, isLoading) {
    state.search.historyLoading = isLoading;
  },

  /** 设置查询Tab当前选中 */
  setActiveTabName(state, name) {
    state.hsitory.activeTabName = name;
  },

  setClusterType(state, clusterType) {
    state.hsitory.clusterType = clusterType;
  },

  setActiveBizid(state, id) {
    state.activeBizId = id;
  },
};

export default {
  namespaced: true,
  state,
  getters,
  actions,
  mutations,
};

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

const defaultParams = {
  func_language: 'java',
  func_udf_type: 'udf',
  func_name: '',
  func_alias: '',
  calculateType: [],
  input_type: [
    {
      index: 0,
      content: '',
    },
  ],
  return_type: [
    {
      index: 0,
      content: '',
    },
  ],
  explain: '',
  example: '',
  example_return_value: '',
  code_config: {
    dependency_config: [],
    code: '',
  },
  support_framework: [],
};
const state = {
  initStatus: false,
  developParams: defaultParams,
};

const getters = {
  initStatus: state => state.initStatus,
  devlopParams: state => state.developParams,
};

const mutations = {
  changeInitStatus(state, status) {
    state.initStatus = status;
  },
  saveDevParams(state, params) {
    state.developParams = params === undefined ? defaultParams : params;
  },
};
const actions = {
  editUDFStatusSet({ commit, state }, data) {
    commit('saveDevParams', Object.assign(state.developParams, data));
    commit('changeInitStatus', true); // 改变初始化状态
  },
};
export default {
  namespaced: true,
  state,
  getters,
  mutations,
  actions,
};

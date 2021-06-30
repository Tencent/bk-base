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
 *  流程化建模相关数据管理
 */

const state = {
  modelInfo: {},
  sampleSetsInfo: {},
  modelInfoForm: {
    modelName: '',
    modelAlias: '',
    sceneName: '',
    isPublic: false,
    description: '',
    modelingType: 'aiops',
    sampleType: '',
    runEnv: '',
    selectedProjectId: '',
    processingClusterId: '',
    storageClusterId: '',
  },
  /** 字段类型列表 */
  typeList: [],
};

const getters = {
  modelInfo: state => state.modelInfo,
  sampleSetsInfo: state => state.sampleSetsInfo,
  typeList: state => state.typeList,
};

const actions = {
  setModelInfo({ commit }, info) {
    commit('setModelInfo', info);
  },
  setSampleSetsInfo({ commit }, info) {
    commit('setSampleSetsInfo', info);
  },
  setTypeList({ commit }, info) {
    commit('setTypeList', info);
  },
};

const mutations = {
  setSampleSetsInfo(state, info) {
    state.sampleSetsInfo = info;
  },
  setModelInfo(state, info) {
    state.modelInfo = info;
    const {
      modelName,
      description,
      sensitivity,
      projectId,
      runEnv,
      modelingType,
      modelAlias,
      sceneName,
      sampleType,
    } = info;
    const { processingClusterId, storageClusterId } = info.properties || {};
    state.modelInfoForm = {
      model_name: modelName,
      modelName: modelName || '',
      modelAlias: modelAlias || '',
      modelingType: modelingType || 'aiops',
      sceneName: sceneName || '',
      sampleType: sampleType || '',
      description: description || '',
      runEnv: runEnv || '',
      isPublic: sensitivity !== 'private',
      selectedProjectId: Number(projectId),
      processingClusterId: processingClusterId || '',
      storageClusterId: storageClusterId || '',
    };
  },
  setTypeList(state, info) {
    state.typeList = info;
  },
};

export default {
  namespaced: true,
  state,
  getters,
  actions,
  mutations,
};

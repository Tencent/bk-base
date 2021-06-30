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

/* eslint-disable no-param-reassign */
/**
 *  数据接入状态管理
 */
import { getObjectClass, userPermScopes, retrieveTokenDetail, getTodoCount } from '@/common/api/auth';
import { showMsg } from '@/common/js/util';
import { bkRequest } from '@/common/js/ajax';

const state = {
  permissions: [],
  scopes: [],
  selectedScopes: [],
  allRoleList: [],
  objectScopes: {},
  isEdit: {},
  adminList: [],
  todoCount: 0,
};

const getters = {
  getAdminList: state => state.adminList,
  getTodoCount: state => state.todoCount,
};

const actions = {
  getAdminList({ commit }) {
    bkRequest.httpRequest('auth/getAdminList').then((res) => {
      if (res.result) {
        commit('setAdminList', res.data);
      } else {
        showMsg(res.message, 'error');
      }
    });
  },
  async actionSetTodoCount({ commit }, todoCount) {
    if (todoCount === undefined) {
      await getTodoCount().then((res) => {
        if (res.result) {
          todoCount = res.data;
        } else {
          showMsg(res.message, 'error');
        }
      });
    }
    commit('mutationSetTodoCount', todoCount);
    return todoCount;
  },
  actionResetSelection({ commit }) {
    commit('mutationSetPermissions', []);
    commit('mutationSetSelectedScopes', []);
  },
  actionSetPermissions({ commit }, permissions) {
    commit('mutationSetPermissions', permissions);
  },
  actionSetSelectedScopes({ commit }, selectedScopes) {
    commit('mutationSetSelectedScopes', selectedScopes);
  },
  actionSetEditState({ commit }, isEdit) {
    commit('mutationSetEditState', isEdit);
  },
  async actionUpdateObjectTypeList({ commit }) {
    if (state.scopes.length === 0) {
      await getObjectClass().then((res) => {
        if (res.result) {
          commit('mutationSetObjectTypeList', res.data);
        } else {
          showMsg(res.message, 'error');
        }
      });
    }

    return state.scopes;
  },
  async actionUpdateObjectScopes({ commit }, currentScope) {
    return await userPermScopes({
      show_display: true,
      action_id: currentScope.action_id,
    }).then((res) => {
      if (res.result) {
        commit('mutationSetObjectScopes', {
          objectScopes: res.data,
          currentScope,
        });
        return state.objectScopes;
      }
      window.gVue.getMethodWarning(res.message, res.code);
    });
  },
  actionLoadToken({}, tokenId) {
    return retrieveTokenDetail(tokenId).then((resp) => {
      if (resp.result) {
        return state.objectScopes;
      }
      window.gVue.getMethodWarning(resp.message, resp.code);
    });
  },
  actionSetObjectScopes({ commit }, { objectScopes, currentScope }) {
    commit('mutationSetObjectScopes', {
      objectScopes,
      currentScope,
    });
    return state.objectScopes;
  },
};

const mutations = {
  setAdminList(state, list) {
    state.adminList = list;
  },
  mutationSetTodoCount(state, todoCount) {
    state.todoCount = todoCount;
  },
  mutationSetPermissions(state, permissions) {
    state.permissions = permissions;
  },
  mutationSetSelectedScopes(state, selectedScopes) {
    state.selectedScopes = selectedScopes;
  },
  mutationSetObjectTypeList(state, scopes) {
    state.scopes = scopes;
  },
  mutationSetEditState(state, isEdit) {
    state.isEdit = isEdit;
  },
  async mutationSetObjectScopes(state, data) {
    const Map = {};
    const children = data.currentScope.scope_object_classes;
    for (const child of children) {
      Map[child.scope_object_class] = {
        scope_id_key: child.scope_id_key,
        scope_name_key: child.scope_name_key,
        data: [],
      };
      for (const objectScope of data.objectScopes) {
        // eslint-disable-next-line no-underscore-dangle
        const _data = {};
        _data.scope_id_key = String(objectScope.id);
        _data.scope_name_key = objectScope.name || objectScope.id;
        Map[child.scope_object_class].data.push(_data);
      }
    }
    state.objectScopes.key = data.currentScope.scope_id_key;
    state.objectScopes.value = data.currentScope.scope_name_key;
    state.objectScopes.Map = Map;
    state.objectScopes.object_class = data.currentScope.object_class;
  },
};

export default {
  namespaced: true,
  state,
  getters,
  actions,
  mutations,
};

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
import { listBkApps } from '@/common/api/base';

const state = {
  // 区域
  region: '',
  // 标识当前页面选中的项目ID，全局可访问
  pid: 0,

  // 当前pid是否合法，废弃
  pidIsValid: true,

  // 代码全文搜索，发现改属性被废弃，未被使用
  // @todo remove
  projectList: {
    loaded: false,
    list: [],
  },
  nowTime: 0,
  userName: '',
  isAdmin: false,
  bizId: 0,
  validDataScenario: ['log', 'custome', 'db', 'http', 'tdw', 'tglog', 'tlog', 'tqos', 'beacon', 'related_data'],
  dataScenarioHasAccessDetail: ['log', 'db', 'http', 'tdw', 'tglog', 'tlog', 'tqos', 'beacon', 'related_data'],
  accessOverView: {
    showDetails: ['tlog', 'log', 'tqos'],
    noDetails: ['db', 'http', 'tdw', 'tglog', 'beacon', 'related_data'],
  },

  blueKingAppList: [],

  // 部署状态的统计信息
  accessSummaryInfo: {},
  // 源数据最近日志状态
  recentLogInfo: {},
};

const getters = {
  getRegion: state => state.region,
  getPid: state => state.pid,
  getProjectLists: state => state.projectList,
  getPidIsValid: state => state.pidIsValid,
  getNow: state => state.nowTime,
  getUserName: state => state.userName,
  getBizId: state => state.bizId,
  getValidDataScenario: state => state.validDataScenario,
  getDataScenarioHasAccessDetail: state => state.dataScenarioHasAccessDetail,
  getAccessOverView: state => state.accessOverView,
  getAccessSummaryInfo: state => state.accessSummaryInfo,
  getProjectById: state => id => state.projectList.list.find(item => item.project_id === id),
};

const actions = {
  updateRegion({ commit }, region) {
    commit('setRegion', region);
  },
  updateUserName({ commit }, username) {
    commit('setUserName', username);
  },
  updateIsAdmin({ commit }, isAdmin) {
    commit('setIsAdmin', isAdmin);
  },
  updatePid({ commit }, pid) {
    commit('setPid', pid);
  },
  updateProjectList({ commit }, list) {
    commit('setProjectList', list);
  },
  updatePidIsValid({ commit }, status) {
    commit('setPidIsValid', status);
  },
  updateTime({ commit }, time) {
    commit('setTime', time);
  },
  updateBizId({ commit }, biz) {
    commit('setBizId', biz);
  },
  updateBlueKingAppList({ commit }) {
    if (state.blueKingAppList.length === 0) {
      return listBkApps().then((resp) => {
        if (resp.result) {
          const blueKingAppList = [];
          for (const item of resp.data) {
            blueKingAppList.push({
              bk_app_code: item.app_code,
              bk_app_name: `${item.app_name}(${item.app_code})`,
            });
          }
          commit('setBlueKingAppList', blueKingAppList);
          return blueKingAppList;
        }
        window.gVue.getMethodWarning(resp.message, resp.code);
      });
    }
    return state.blueKingAppList;
  },
};

const mutations = {
  setRegion(state, region) {
    state.region = region;
  },
  setGlobalOption(state, option) {
    state.globalOption = option;
  },
  setUserName(state, username) {
    state.userName = username;
  },
  setIsAdmin(state, isAdmin) {
    state.isAdmin = isAdmin;
  },
  setPid(state, pid) {
    state.pid = pid;
  },
  setProjectList(state, list) {
    state.projectList.loaded = true;
    state.projectList.list = list;
  },
  setPidIsValid(state, status) {
    state.pidIsValid = status;
  },
  setTime(state, time) {
    state.nowTime = time;
  },
  setBizId(state, biz) {
    state.bizId = biz;
  },
  setBlueKingAppList(state, blueKingAppList) {
    state.blueKingAppList = blueKingAppList;
  },
  /**
   * @description 更新公共存储的state变量
   * @param {*} state
   * @param {*} value
   * eg: this.$store.commit('updateCommonState', {
   *      accessSummaryInfo: {
   *          summary: {}
   *      }
   * })
   */
  updateCommonState(state, value) {
    Object.keys(value).forEach((key) => {
      state[key] = value[key];
    });
  },
};

export default {
  state,
  getters,
  actions,
  mutations,
};

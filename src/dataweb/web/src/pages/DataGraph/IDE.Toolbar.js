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

import { OPERATION_ACTION } from './Graph/Config/Toolbar.Config.js';
import { postMethodWarning, showMsg } from '@/common/js/util';
class IDEToolbar {
  constructor(store, flowId, bkRequest) {
    this.actions = OPERATION_ACTION;
    this.bkRequest = bkRequest;
    this.$store = store;
    this.debugId = 0;
    this.flowId = flowId;
    this.debugInfo = {};
  }

  updateFlowid(flowid) {
    this.flowId = flowid;
    this.debugId = 0;
    this.debugInfo = {};
  }

  /** 画布工具栏事件监听 */
  handleToolbarAction(action, params) {
    let result = null;
    switch (action) {
      case this.actions.START_DEBUG:
        result = this.startDebugHandler(params);
        break;
      case this.actions.STOP_DEBUG:
        result = this.stopDebugHandler(params);
        break;
      case this.actions.MAGIC_POSITION:
        result = this.magicPosition(params);
        break;
      case this.actions.START_INSTANCE:
        result = this.startInstance(params);
        break;
      case this.actions.STOP_INSTANCE:
        result = this.stopInstance(params);
        break;
      case this.actions.RESTART_INSTANCE:
        result = this.restartInstance(params);
        break;
      case this.actions.START_COMPLEMENT:
        result = this.complementSuccess(params);
        break;
      case this.actions.APPLY_COMPLEMENT:
        result = this.applyComplement(params);
        break;
      case this.actions.STOP_COMPLEMENT:
        result = this.stopComplement(params);
        break;
      case this.actions.CANCEL_COMPLEMENT:
        result = this.cancelComplementSuccess(params);
        break;
      case this.actions.UPDATE_COMPLEMENT_ICON:
        result = this.updateComplementIcon(params);
        break;
      case this.actions.START_MONITORING:
        result = this.getMonitoringData(params);
        break;
      case this.actions.STOP_MONITORING:
        result = this.stopMonitor(params);
        break;
    }

    return result;
  }

  /** 开始调试 */
  startDebugHandler(params) {
    return this.$store
      .dispatch('api/flows/startDebug', { flowId: this.flowId })
      .then((resp) => {
        if (resp.result) {
          this.debugId = resp.data;
          this.callbackFuncResolve(params, this.actions.START_DEBUG, resp);
          return Promise.resolve(resp);
        }
        postMethodWarning(resp.message, 'error');
        return Promise.reject(resp);
      })
      .catch(error => Promise.reject(error));
  }

  /** 停止调试 */
  stopDebugHandler(params) {
    return this.$store
      .dispatch('api/flows/stopDebug', { flowId: this.flowId, debugId: this.debugId })
      .then((resp) => {
        if (resp.result) {
          this.callbackFuncResolve(params, this.actions.STOP_DEBUG, resp);
          return Promise.resolve(resp);
        }
        postMethodWarning(resp.message, 'error');
        return Promise.reject(resp);
      })
      .catch(error => Promise.reject(error));
  }

  /** 查询调试状态 */
  queryDebugInfo() {
    return this.$store
      .dispatch('api/flows/queryDebug', { flowId: this.flowId, debugId: this.debugId })
      .then((resp) => {
        if (resp.result) {
          this.debugInfo = resp.data;
          return Promise.resolve(resp.data);
        }
        return Promise.reject(resp);
      })
      .catch(error => Promise.reject(error));
  }

  /**
   * 获取执行结果
   */
  queryDeployInfo() {
    return this.$store
      .dispatch('api/flows/requestLatestDeployInfo', { flowId: this.flowId })
      .then((resp) => {
        if (resp.result && resp.data) {
          const isDone = !(
            resp.data.status !== 'success'
            && resp.data.status !== 'failure'
            && resp.data.status !== 'finished'
            && resp.data.status !== 'terminated'
          );
          Object.assign(resp.data, { is_done: isDone });
          return Promise.resolve(resp.data);
        }
        return Promise.reject(resp);
      })
      .catch(error => Promise.reject(error));
  }

  /** 自动排版 */
  magicPosition(params) {
    return this.commonCallback(this.actions.MAGIC_POSITION, params);
  }

  /**
   * 启动流程
   * @param {*} params ：启动参数
   */
  startInstance(params) {
    const options = {
      flowId: this.flowId,
      consumingMode: params.consuming_mode,
      clusterGroup: params.cluster_group,
    };
    return this.$store.dispatch('api/flows/startDataflow', options).then((resp) => {
      if (resp.result) {
        this.callbackFuncResolve(params, this.actions.START_INSTANCE, resp.data);
        return Promise.resolve(resp);
      }
      if (resp.code === '1574007') {
        postMethodWarning(this.getWarningTpl(resp.message), 'error');
      } else {
        postMethodWarning(resp.message, 'error');
      }
      postMethodWarning(resp.message, 'error');
      return Promise.reject(resp);
    });
  }

  restartInstance(params) {
    const options = {
      flowId: this.flowId,
      consumingMode: params.consuming_mode,
      clusterGroup: params.cluster_group,
    };
    return this.$store.dispatch('api/flows/restartDataflow', options).then((resp) => {
      if (resp.result) {
        this.callbackFuncResolve(params, this.actions.START_INSTANCE, resp.data);
        return Promise.resolve(resp);
      }
      if (resp.code === '1574007') {
        postMethodWarning(this.getWarningTpl(resp.message), 'error');
      } else {
        postMethodWarning(resp.message, 'error');
      }
      postMethodWarning(resp.message, 'error');
      return Promise.reject(resp);
    });
  }

  /**
   * 停止流程
   * @param {*} params ：参数
   */
  stopInstance(params) {
    return this.$store.dispatch('api/flows/stopDataflow', { flowId: this.flowId }).then((resp) => {
      if (resp.result) {
        this.callbackFuncResolve(params, this.actions.STOP_INSTANCE, resp.data);
        return Promise.resolve(resp);
      }
      postMethodWarning(resp.message, 'error');
      return Promise.reject(resp);
    });
  }

  commonCallback(action, params) {
    this.callbackFuncResolve(params, action, params);
    return Promise.resolve({ result: true });
  }

  /** 补算启动成功 */
  complementSuccess(params) {
    return this.commonCallback(this.actions.START_COMPLEMENT, params);
  }
  /** 申请补算 */
  applyComplement(params) {
    return this.commonCallback(this.actions.APPLY_COMPLEMENT, params);
  }
  updateComplementIcon(params) {
    return this.commonCallback(this.actions.UPDATE_COMPLEMENT_ICON, params);
  }
  cancelComplementSuccess(params) {
    return this.commonCallback(this.actions.CANCEL_COMPLEMENT, params);
  }

  /** 停止补算 */
  stopComplement(params) {
    return this.bkRequest
      .httpRequest('dataFlow/stopComplementTask', {
        // 发送停止补算的请求
        params: {
          fid: this.flowId,
        },
      })
      .then((res) => {
        if (res.result) {
          this.$store.commit('ide/setComplementStatus', 'none');
          this.callbackFuncResolve(params, this.actions.STOP_COMPLEMENT, 1);
          showMsg(window.$t('停止成功'), 'success');
          return Promise.resolve(res);
        }
        showMsg(res.message, 'error');
        return Promise.reject(res);
      })
      .catch((err) => {
        showMsg(err.message, 'error');
        return Promise.reject(err);
      });
  }

  getWarningTpl(msg) {
    const tpl = (
      <span>
        {msg}，
        <a style="cursor:pointer;color: #3a84ff;" onclick="window.location.reload(true)">
          请刷新
        </a>
      </span>
    );
    return tpl;
  }

  callbackFuncResolve(params, action, response) {
    typeof params.callback === 'function' && params.callback(action, response);
  }

  /**
   * 打点监控时获取输入和输出
   */
  getMonitoringData(params) {
    return this.$store.dispatch('api/flows/dataMonitor', { flowId: this.flowId }).then((resp) => {
      if (resp.result) {
        if (resp.data) {
          this.callbackFuncResolve(params, this.actions.START_MONITORING, resp.data);
          return Promise.resolve(resp.data);
        }
      } else {
        showMsg(resp.message, 'error');
        return Promise.reject(resp);
      }
    });
  }

  stopMonitor(params) {
    this.callbackFuncResolve(params, this.actions.STOP_MONITORING, 1);
    return Promise.resolve(1);
  }
}

export default IDEToolbar;

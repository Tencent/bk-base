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
 * API Module
 */
// import {ajax} from '@/common/js/ajax'
import { ajax, bkRequest } from '@/common/js/ajax';
import flows from './api/flows';

export default {
  namespaced: true,
  modules: {
    flows,
  },
  state: {},
  mutations: {},
  actions: {
    // /**
    //  * 接入模块（DataId接口）
    //  */
    // getDataId ({commit, state, dispatch}, {dataId, params}) {
    //     return ajax.get(`/v3/collector/deploy_plan/${dataId}/`, {
    //         params: params
    //     })
    // },
    // getSummary ({commit, state, dispatch}, {dataId, params}) {
    //     return ajax.get(`/v3/collectorhub/deploy_plan/${dataId}/status/summary/`, {
    //         params: params
    //     })
    // },

    /**
     *  ETL模块
     */
    getEtl({}, { rid }) {
      return bkRequest.httpRequest('dataClear/getCleanRules', { params: { rtid: rid } });
    },
    createEtl({}, params) {
      return ajax.post('etls/', params);
    },
    updateEtl({}, { rid, params }) {
      return ajax.put(`/etls/${rid}/`, params);
    },
    startEtl({}, { rid }) {
      return ajax.post(`/etls/${rid}/start/`);
    },
    stopEtl({}, { rid }) {
      return ajax.post(`/etls/${rid}/stop/`);
    },
    getEtlTemplate({}, { param }) {
      return ajax.get(`/etls/get_etl_template/?data_id=${param}`);
    },
    recommendationParam({}, param) {
      return ajax.post('/etls/get_etl_hint/', param);
    },
    requestDataIdSet({}) {
      return ajax.get('v3/access/field_type/');
    },
    /**
     * 工具模块（Tools接口）
     */
    listTimeFormat({}) {
      return ajax.get('v3/meta/time_format_configs/');
      // return ajax.get(`v3/access/time_format/`)
    },
    /**
     *etl节点
     */
    listEtlTimeFormat({}) {
      return ajax.get('v3/databus/cleans/time_formats/');
      // return ajax.get(`v3/access/time_format/`)
    },
    getDataIdEtl({}, { rid }) {
      return ajax.get(`/etls/${rid}/`);
    },
    // 获取云区域列表
    getAreaLists({}) {
      return ajax.get('/bizs/list_cloud_area/');
    },
    /**
     * dataflow的api
     */
    /**
     * 获取全部dataflow
     */
    getAllDataflows({}) {
      return ajax.get('flows/');
    },
    /**
     * 获取单个dataflow
     */
    getDataflowInfo({}, { flowId }) {
      return ajax.get(`flows/${flowId}/`);
    },
    /**
     * 修改dataflow信息
     */
    updateDataflowInfo({}, { flowId, param }) {
      return ajax.patch(`v3/dataflow/flow/flows/${flowId}/`, param);
    },
    /**
     * 获取告警列表信息
     */
    getListAlert({}, { flowId }) {
      return ajax.get(`flows/${flowId}/list_alert/`);
    },
  },
};

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

import Cookies from 'js-cookie';

const lan = /zh/i.test(Cookies.get('blueking_language') || 'zh-cn') ? 'zh' : 'en';
const BASE_URL = window.BKBASE_Global.helpDocUrl;
const RUN_VERSION = window.BKBASE_Global.runVersion;
const getDocHelperUrl = (path) => {
  let docBaseUrl;
  switch (RUN_VERSION) {
    case 'tgdp':
      docBaseUrl = `${window.location.protocol}//${window.location.hostname}/o/bk_docs_center/markdown/`;
      break;
    case 'opensource':
      docBaseUrl = 'https://bk.tencent.com/docs/markdown/';
  }
  return `${docBaseUrl}${path}`;
};

/**
 *  文档链接
 */

const state = {
  ieod: {
    helpDocRoot: `${BASE_URL}zh/`,
    cleanRule: `${BASE_URL + lan}/user-guide/datahub/data-clean/detail.html`,
    esSearchRule: `${BASE_URL + lan}/user-guide/datalab/queryengine/es-query/full-text-retrieval.html`,
    realtimeSqlRule: `${BASE_URL + lan}/user-guide/dataflow/bksql-function/stream-processing.html`,
    offlineSqlRule: `${BASE_URL + lan}/user-guide/dataflow/bksql-function/batch-processing.html`,
    querySqlRule: `${BASE_URL + lan}/user-guide/datalab/queryengine/sql-query/concepts.html`,
    customAccessReporting: `${BASE_URL + lan}/user-guide/datahub/data-access/custom/gsecmdline.html`,
    applyBizPermission: `${BASE_URL}zh/user-guide/auth-management/permission.html#如何申请业务权限`,
    notebookMlsql: `${BASE_URL + lan}/user-guide/datalab/notebook/mlsql/mlsql.html`,
    customModel: `${BASE_URL}zh/user-guide/dataflow/components/modeling/process-model.html`,
    notebookHelp: `${BASE_URL}zh/user-guide/datalab/notebook/concepts.html`,
    dataMarket: `${BASE_URL}/zh/user-guide/datamarket/data-model/concepts.html`,
    dataMartFeedback: `${window.BKBASE_Global.ceUrl}/products/view/2282?&tab_id=all_issues_btn#create-feedback`,
  },
  // 出海版 && 企业版统一
  tgdp: {
    helpDocRoot: getDocHelperUrl('数据平台/产品白皮书/intro/intro.md'),
    cleanRule: getDocHelperUrl('数据平台/产品白皮书/user-guide/datahub/data-clean.md'),
    esSearchRule: getDocHelperUrl('数据平台/产品白皮书/user-guide/datalab/es-query/full-text-retrieval.md'),
    realtimeSqlRule: getDocHelperUrl('数据平台/产品白皮书/user-guide/dataflow/bksql-function/stream-processing.md'),
    offlineSqlRule: getDocHelperUrl('数据平台/产品白皮书/user-guide/dataflow/bksql-function/batch-processing.md'),
    querySqlRule: getDocHelperUrl('数据平台/产品白皮书/user-guide/datalab/sql-query/concepts.md'),
    customAccessReporting: getDocHelperUrl('数据平台/产品白皮书/user-guide/datahub/data-access/custom/gsecmdline.md'),
    applyBizPermission: getDocHelperUrl('数据平台/产品白皮书/user-guide/auth-management/permission.md'),
  },
  // 开源版
  opensource: {
    helpDocRoot: getDocHelperUrl('数据平台/产品白皮书/intro/intro.md'),
    cleanRule: getDocHelperUrl('数据平台/产品白皮书/user-guide/datahub/data-clean.md'),
    esSearchRule: getDocHelperUrl('数据平台/产品白皮书/user-guide/datalab/es-query/full-text-retrieval.md'),
    realtimeSqlRule: getDocHelperUrl('数据平台/产品白皮书/user-guide/dataflow/bksql-function/stream-processing.md'),
    offlineSqlRule: getDocHelperUrl('数据平台/产品白皮书/user-guide/dataflow/bksql-function/batch-processing.md'),
    querySqlRule: getDocHelperUrl('数据平台/产品白皮书/user-guide/datalab/sql-query/concepts.md'),
    customAccessReporting: getDocHelperUrl('数据平台/产品白皮书/user-guide/datahub/data-access/custom/gsecmdline.md'),
    applyBizPermission: getDocHelperUrl('数据平台/产品白皮书/user-guide/auth-management/permission.md'),
  },
  graphNode: {
    flink: `${BASE_URL}zh/user-guide/dataflow/code/flink-streaming-code.html`,
    spark: `${BASE_URL}zh/user-guide/dataflow/code/spark-structured-streaming-code.html`,
    realtimeDelay: `${BASE_URL}zh/user-guide/dataflow/stream-processing/stream-guide/tumbling-window.html`,
    udf: `${BASE_URL}zh/user-guide/dataflow/udf.html`,
  },
  docsAddress: {
    dataPermission: `${BASE_URL}zh/user-guide/auth-management/sensitivity.html`,
    dataLog: `${BASE_URL}zh/user-guide/dataflow/tasklog.html`,
    dataRecalculate: `${BASE_URL}zh/user-guide/dataflow/batch-processing/rerun.html?h=补算`,
    sqlFunc: `${BASE_URL}zh/user-guide/datalab/queryengine/sql-query/bksql.html`,
  },
};

const getters = {
  getPaths: state => state[window.BKBASE_Global.runVersion],
  getNodeDocs: state => state.graphNode,
  getDocsUrl: state => state.docsAddress,
};

export default {
  namespaced: true,
  state,
  getters,
};

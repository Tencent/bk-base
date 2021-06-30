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

import CoreRequest from './CoreRequest';
import HttpRequst from './_httpRequest';

export default class APIController extends CoreRequest {
  constructor(options) {
    // eslint-disable-next-line no-underscore-dangle
    const _options = Object.assign({}, options);
    const responseConfig = {
      ignoreInherit: false,
    };
    const defaultOpt = {
      axios: _options.axios.basic,
      interceptors: _options.axios.interceptors,
      methodExtends: _options.axios.methodExtends,
      customAttr: _options.axios.customAttr,
    };
    const requestConfig = _options.axios ? defaultOpt : {};
    super(requestConfig, responseConfig);
    this.cache = {};
    this.options = _options;
    // eslint-disable-next-line no-underscore-dangle
    this.HttpRequst = new HttpRequst(this.__axios.bind(this), this.options);
    this.CancelToken = this.axiosInstance.CancelToken;
  }

  // 接收用户请求
  request(url, options = {}) {
    const opts = Object.assign(
      {},
      {
        method: 'GET',
        query: {},
        ext: {},
      },
      options,
    );
    // eslint-disable-next-line no-underscore-dangle
    return this.__axios(url, opts.method, opts.params, opts.query, opts.ext, opts.baseURL);
  }

  ajax() {
    return this.axiosInstance;
  }

  /**
   *
   * @param {String} service: 服务名称
   * @param {Object} options: 配置项
   */
  async httpRequest(service, options = {}) {
    return this.HttpRequst.request(service, options);
  }
}

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

import axios from 'axios';

const qs = require('qs');

class HttpRequest {
  constructor(axiosConfig) {
    this.axiosInstance = axios.create(Object.assign({}, axiosConfig.axios));
    this.initInterceptors(axiosConfig.interceptors);
    this.initAttribute(axiosConfig.customAttr);
    this.initExtendMethods(axiosConfig.methodExtends);
    this.initInstanceExtends();
    this.axiosInstance.origin = axios;
  }

  initInstanceExtends() {
    this.axiosInstance.CancelToken = axios.CancelToken;
  }

  initExtendMethods(methodExtends) {
    if (methodExtends && methodExtends.length) {
      if (Array.isArray(methodExtends)) {
        methodExtends.forEach((method) => {
          this.methodExtend(method);
        });
      } else {
        if (typeof methodExtends === 'object') {
          this.methodExtend(methodExtends);
        }
      }
    }
  }

  initAttribute(customAttr) {
    if (customAttr) {
      this.formatAttrToBindAxiosInstance(customAttr);
      customAttr.constructor.bind(this.axiosInstance);
      Object.assign(this.axiosInstance, customAttr);
    }
  }

  initInterceptors(interceptors) {
    if (interceptors) {
      // eslint-disable-next-line no-param-reassign
      interceptors.response = Object.assign(
        {},
        {
          resolve: () => {},
          reject: () => {},
        },
        interceptors.response,
      );
      // eslint-disable-next-line no-param-reassign
      interceptors.request = Object.assign(
        {},
        {
          resolve: () => {},
          reject: () => {},
        },
        interceptors.request,
      );
      this.axiosInstance.interceptors.response.use(interceptors.response.resolve, interceptors.response.reject);
      this.axiosInstance.interceptors.request.use(interceptors.request.resolve, interceptors.request.reject);
    }
  }

  formatAttrToBindAxiosInstance(customAttr) {
    Object.keys(customAttr).forEach((key) => {
      const instance = customAttr[key];
      if (typeof instance === 'function') {
        // eslint-disable-next-line no-param-reassign
        customAttr[key] = instance.bind(this.axiosInstance);
      } else {
        if (typeof instance === 'object') {
          if (Array.isArray(instance)) {
            Array.prototype.forEach((ins) => {
              this.formatAttrToBindAxiosInstance(ins);
            });
          } else {
            this.formatAttrToBindAxiosInstance(customAttr[key]);
          }
        }
      }
    });
  }

  methodExtend(method) {
    const sourceEvt = this.axiosInstance[method.name];
    this.axiosInstance[method.name] = (...args) => {
      const output = method.exec.bind(this.axiosInstance);
      return output(sourceEvt, ...args);
    };
  }

  __axios(url, method, params, query, ext, baseURL) {
    const config = Object.assign(
      {},
      {
        url,
        method,
        data: params,
        params: query,
        paramsSerializer(params) {
          return qs.stringify(params, {
            arrayFormat: 'repeat',
          });
        },
      },
      ext || {},
    );
    if (baseURL) {
      Object.assign(config, {
        baseURL,
      });
    }
    return this.axiosInstance(config);
  }
}

export default HttpRequest;

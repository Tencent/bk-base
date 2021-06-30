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
 * AJAX Module
 */

import { IBKResponse } from '@/@types/types';
import bkStorage from '@/common/js/bkStorage.js';
import Bus from '@/common/js/bus.js';
import serviceList from '@/services/index.js';
import ApiController from '../api-controller/index';
import { BKHttpResponse } from './BKHttpResponse';

const httpRequestConfig = {
  mockList: [],
  serviceList,
  apiConfig: {},
  mock: false,
  axios: {
    basic: {
      xsrfCookieName: 'data_csrftoken',
      xsrfHeaderName: 'X-CSRFToken',
      withCredentials: true,
      headers: { 'X-Requested-With': 'XMLHttpRequest' },
      responseType: 'json',
      baseURL: window.BKBASE_Global.siteUrl,
    },
    customAttr: {},
    methodExtends: [],
    interceptors: {
      response: {
        resolve: (response: any) => {
          /** 当处于发布状态时，拦截一切接口请求，防止出现弹框 */
          const publishStatus = bkStorage.get('publishing');
          if (publishStatus === 1 && response.data && response.data.code !== '0000') {
            return {
              result: true,
              data: [],
            };
          }

          return response.data;
        },
        reject: (error: any) => {
          let url = '';
          const postData = { login_url: url };

          if (/Network Error\b/gi.test(error) || /status code 401/gi.test(error)) {
            const responseData = Object.prototype.hasOwnProperty.call(error.response || {}, 'data')
              ? error.response.data
              : error.response;
            if (responseData && responseData.login_url) {
              try {
                window.top.BLUEKING.corefunc.open_login_dialog(responseData.login_url);
              } catch (err) {
                Bus.$emit('show-login-modal', responseData || postData);
              }
            } else {
              Bus.$emit('show-login-modal', postData);
            }

            return Promise.resolve({
              result: false,
              message: error,
              code: '-1111',
            });
          }
          if (error.response) {
            let isRsolve = false;
            switch (error.response.status) {
              case 401:
                try {
                  window.top.BLUEKING.corefunc.open_login_dialog(error.response.login_url);
                } catch (err) {
                  const data = error.response && error.response.login_url ? error.response : postData;
                  Bus.$emit('show-login-modal', data);
                }
                isRsolve = true;
                break;
              case 500:
                alert(window.gVue.$t('系统出现异常_请记录下错误场景并与开发人员联系_谢谢'));
                break;
            }

            if (isRsolve) {
              return Promise.resolve({
                result: false,
                message: error,
                code: '401',
              });
            }
          }
          return Promise.reject(error);
        },
      },
      request: {
        resolve: (config: any) => {
          if (/delete|patch|put/i.test(config.method)) {
            Object.assign(config.headers, { 'X-HTTP-Method-Override': config.method });
            config.method = 'post';
          }
          return config;
        },
        reject: (error: any) => {
          return Promise.resolve(error);
        },
      },
    },
  },
};
const bkRequest = new ApiController(httpRequestConfig);
const ajax = bkRequest.ajax();
const CancelToken = bkRequest.CancelToken;

const bkHttpRequest = (mService: string, mOptions: any): Promise<IBKResponse<any>> => {
  const service = mService;
  const options = mOptions || {};
  const formatToWebSchema = !!options.ext.format;
  return bkRequest
    .httpRequest(service, options)
    .then(res => {
      return Promise.resolve(new BKHttpResponse<any>(res, formatToWebSchema));
    })
    .catch(err => {
      const msg = typeof err === 'string' ? err : err.message;
      window.gVue.$bkMessage(
        Object.assign(
          {
            message: msg,
            delay: 0,
            theme: 'error',
            offsetY: 80,
            ellipsisLine: 5,
            limit: 5,
          },
          options || {}
        )
      );
      return Promise.reject(err);
    });
};

export { ajax, bkRequest, ajax as axios, CancelToken, bkHttpRequest };

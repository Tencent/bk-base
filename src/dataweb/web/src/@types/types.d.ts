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

import VueRouter, { Route } from 'vue-router';

declare interface IObj<T> {
  [key: string]: T;
}

declare type IHttpRequest<T> = (url: string, method?: string, params?: IObj<any>, query?: IObj<any>) => T;
declare interface IHttpRequestOptions {
  params?: any;
  query?: any;
  ext?: any;
}
declare type IBKHttpRequest<T> = (url: string, options: IHttpRequestOptions) => Promise<IBKResponse<any>>;
declare module 'vue/types/vue' {
  // eslint-disable-next-line
  interface Vue {
    $bus: Vue;
    bkRequest: { httpRequest: IBKHttpRequest<any> };
    $route: Route;
    $router: VueRouter;
    CancelToken: any;
    $modules: any;
    $bkMessage: (options: any) => void;
    getMethodWarning: (message: string, code: string) => void;
    clearStatus: () => void;
  }
}

/**
 * 接口返回数据格式定义
 */
declare interface IResponse<T> {
  /**
   * 是否成功返回
   */
  result: boolean;

  /**
   * 返回数据
   */
  data: T;

  /**
   * 消息提示（常用于请求失败时错误提示）
   */
  message: string;

  /**
   * 请求返回状态码
   */
  code: string;

  /** 多个错误 */
  errors: any[];
}

declare interface IBKResponse<T> extends IResponse<T> {
  /**
   * 将前端对象格式化为服务器端数据格式
   * @param data 待格式化对象，如果为null则自动格式化实例中的data
   */
  getServeFormatData: (data: any) => any;

  /**
   * 格式化数据为前端格式
   * @param data
   */
  getWebFormatData: (data: any) => any;

  /**
   * 设置API返回数据Data到指定对象
   * @param scope 当前作用域 this
   * @param target 赋值key
   * @param validateSuccess 是否校验错误，默认 true
   * @param warning 是否自动提示错误，默认true
   */
  setData: (scope: any, target: string, validateSuccess: boolean, warning: boolean) => Promise<IResponse<T>>;

  /**
   * 校验返回结果是否包含错误
   * @param customValidate 自定义校验 | 会优先校验返回result是否为true，然后才会校验 customValidate
   * @param messageFun 获取错误消息的函数
   * @param warning 是否自动提示错误，默认true
   */
  validateResult: (customValidate: () => {}, messageFun: () => {} | null, warning: boolean) => boolean;

  /**
   * 需要对返回结果进行自定义处理
   * @param fn 自定义函数
   * @param warning 是否自动提示错误
   */
  setDataFn: (fn: (data: T) => {}, warning?: boolean) => void;
}

/**
 * Http 请求格式
 */
declare interface IRequestParams {
  /** 是否启用Mock */
  mock?: boolean;

  /**
   * Mock数据是否已经手动包含response结构
   * 默认：false，会自动追加结构 { data: {}, code:'0', result: true }
   * 如果设置为 true，则原封不动返回Mock数据
   * */
  manualSchema?: boolean;

  /**
   * 查询参数：用于路径参数
   * 例如： /api/user/:id, 参数用于替换:id
   */
  params?: any;

  /**
   * 查询参数：用于Url Query String
   * 例如：/api/user/1?id=1&type=2
   */
  query?: any;

  /**
   * 其他扩展
   * 例如：axios的CancelToken
   */
  ext?: any;
}

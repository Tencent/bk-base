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
const configuration: any = {};
const packages = require('./packages/index');

/** 获取配置项并赋值 **/
packages && Object.assign(configuration, packages.default || packages);

class ExtendConfiguration {
  router = [];
  bizComponments = [];
  services: any = {};
  jsFragment: any = {};
  vueFragment: any = {};

  constructor(configuration: any) {
    Object.assign(this, configuration || {});
  }

  public getBizComponent(componmentName: string) {
    console.log('getComponent', componmentName, this);
  }

  public getVueFragment(componmentName: string, vueScope: any) {
    const componment = this.vueFragment[componmentName];
    if (typeof componment === 'function') {
      return componment(vueScope);
    }

    return componment;
  }

  /**
     * 执行JsFragment返回的函数
     * @param methodName 方法名 或者 对象名
     * @param thisAgs
     * @param params
     * @returns
     */
  public callJsFragmentFn(methodName: string, thisAgs: any, params: any[]) {
    const componment = this.jsFragment[methodName];
    if (typeof componment === 'function') {
      return componment.apply(thisAgs, params);
    }

    return componment;
  }
}

export default new ExtendConfiguration(configuration);

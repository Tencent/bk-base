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

/* eslint-disable no-underscore-dangle */
// import * as monaco from 'monaco-editor'
class MonacoConfLoader {
  constructor() {
    if (window.__monaco_conf) {
      const { serveConf, providerConfigs } = window.__monaco_conf;
      this.serveConf = serveConf;
      this.providerConfigs = providerConfigs;
    } else {
      this.serveConf = this.getMonacoServerConf();
      this.providerConfigs = this.getProviderConfigs(this.serveConf);
      window.__monaco_conf = { serveConf: this.serveConf, providerConfigs: this.providerConfigs };
    }
  }

  init() {
    console.log('init', this);
  }

  // 获取服务器端语言配置
  // TO-DO:获取服务器端配置API
  getMonacoServerConf() {
    const conf = require('./monaco_conf.json');
    return conf;
  }

  // 格式化服务器端数据，返回自定义语言关键字、方法提示、补全
  getProviderConfigs(conf) {
    const confs = { tokensProviders: new Set(), provideCompletionItems: [] };
    Object.keys(conf).forEach((key) => {
      Array.prototype.reduce.call(
        conf[key],
        (pre, curr) => {
          pre.tokensProviders.add(curr.name);
          const item = {
            label: curr.name,
            kind: window.monaco.languages.CompletionItemKind[key],
            documentation: curr.documentation || curr.name,
            insertText: (curr.insertText && curr.insertText.value) || curr.name,
          };

          pre.provideCompletionItems.push(item);
          return pre;
        },
        confs,
      );
    });

    return confs;
  }

  getBuiltinFunctions() {
    return this.serveConf.Method.map(s => s.name);
  }

  getKeywords() {
    return this.serveConf.Keyword.map(s => s.name);
  }
}
const monacoConfLoader = new MonacoConfLoader();
export { monacoConfLoader };

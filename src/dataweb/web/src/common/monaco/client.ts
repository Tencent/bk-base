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
import * as rpc from 'vscode-ws-jsonrpc';
import { MonacoConfig } from './monaco-mode';
import bkSqlConfig from './monaco_conf';
import {
  MonacoLanguageClient, CloseAction, ErrorAction,
  MonacoServices, createConnection
} from 'monaco-languageclient';
import normalizeUrl = require('normalize-url');
const ReconnectingWebSocket = require('reconnecting-websocket');

interface IBKClient {
  editor: string,
  clientOptions: any
}

class MonacoClient implements IBKClient {
  editor: any
  clientOptions: any
  bkSqlLanguage: string
  serveConf: MonacoConfig
  webSocket: any
  service: any
  connInstance: any
  editoMode: any
  constructor(clientOptions: any, language = 'json', public socketUri = '') {
    this.editor = null;
    this.clientOptions = clientOptions;
    this.bkSqlLanguage = language;
    this.serveConf = bkSqlConfig;
    this.socketUri = this.createUrl(socketUri, false);
    this.webSocket = this.createWebSocket(this.socketUri);
    this.service = null;
    this.connInstance = null;
    this.editoMode = null;
  }

  registerBkSql(languageId: string) {
    // 关键字提示配置
    const that = this;
    function getTokensProviderConf() {
      const t = require('./tokenizer');
      return t.default.getTokenizer(
        getKeywords(),
        getBuiltinFunctions()
      );
    }

    function getBuiltinFunctions() {
      return that.serveConf.Method.map(config => config.name);
    }

    function getKeywords() {
      return that.serveConf.Keyword.map(config => config.name);
    }

    // Register a tokens provider for the language
    monaco.languages.setMonarchTokensProvider(
      languageId,
      getTokensProviderConf()
    );
  }

  initEditor(container: string, options: any) {
    const that = this;
    that.registerBkSql(that.bkSqlLanguage);
    // create Monaco editor
    const value = '';
    that.editoMode = monaco.editor.createModel(value, that.bkSqlLanguage, that.getModeUri());
    this.editor = monaco.editor.create(document.getElementById(container)!, Object.assign({}, {
      model: that.editoMode,
      glyphMargin: true,
      lightbulb: {
        enabled: true
      }
    }, options));
  }

  getModeUri() {
    return monaco.Uri.parse('inmemory://model.json');
  }

  init(container: string, options: any) {
    const that = this;
    // register Monaco languages
    monaco.languages.register({
      id: that.bkSqlLanguage,
      extensions: ['.json', '.bowerrc', '.jshintrc', '.jscsrc', '.eslintrc', '.babelrc'],
      aliases: ['JSON', 'json'],
      mimetypes: ['application/json'],
    });


    that.initEditor(container, options);
    // install Monaco language client services
    MonacoServices.install(this.editor);
    that.listenRpc(options.initializationOptions || {});
  }

  listenRpc(initializationOptions: any) {
    rpc.listen({
      webSocket: this.webSocket,
      onConnection: connection => {
        // create and start the language client
        const languageClient = this.createLanguageClient(connection, initializationOptions);
        const disposable = languageClient.start();
        connection.onClose(() => {
          disposable.dispose();
        });
      }
    });
  }

  createLanguageClient(connection: rpc.MessageConnection, initializationOptions: any): MonacoLanguageClient {
    const self = this;
    return new MonacoLanguageClient({
      name: 'BKSql_Language_Client',
      clientOptions: Object.assign({
        // use a language id as a document selector
        documentSelector: [self.bkSqlLanguage],
        initializationOptions: initializationOptions,
        // disable the default error handler
        errorHandler: {
          error: () => ErrorAction.Continue,
          closed: () => CloseAction.DoNotRestart
        }
      }, this.clientOptions),
      // create a language client connection from the JSON RPC connection on demand
      connectionProvider: {
        get: (errorHandler, closeHandler) => {
          self.connInstance = createConnection(connection, errorHandler, closeHandler);
          return Promise.resolve(self.connInstance);
        }
      }
    });
  }

  createUrl(path: string, resolveProtocol: false): string {
    if (resolveProtocol) {
      const protocol = location.protocol === 'https:' ? 'wss' : 'ws';
      return normalizeUrl(`${protocol}://${location.host}${location.pathname}${path}`);
    } else {
      return path;
    }
  }

  createWebSocket(url: string): WebSocket {
    const socketOptions = {
      maxReconnectionDelay: 10000,
      minReconnectionDelay: 1000,
      reconnectionDelayGrowFactor: 1.3,
      connectionTimeout: 10000,
      maxRetries: Infinity,
      debug: false
    };
    return new ReconnectingWebSocket(url, [], socketOptions);
  }

  // 获取服务器端语言配置
  // TO-DO:获取服务器端配置API
  getMonacoServerConf(): void {
    // const conf = require('./monaco_conf')
    // return conf
  }

  dispose() {
    console.log('client dispose');
    rpc.listen({
      webSocket: this.webSocket,
      onConnection: () => { }
    });
    this.editoMode && this.editoMode.dispose();
    this.editoMode = null;
    this.editor.dispose();
    this.webSocket.close(3001, 'dispose instance');
    this.webSocket = null;
    this.connInstance = null;
  }
}
declare global {
  interface Window {
    MonacoClient: any;
  }
}
window.MonacoClient = MonacoClient;

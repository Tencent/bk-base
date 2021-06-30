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

export class QueryableWorker {
    url = '';
    defaultListener = null;
    worker = null;
    listeners = {};
    constructor(url, defaultListener, onError) {
      this.url = url;
      this.defaultListener = defaultListener || function () { };
      this.worker = new Worker(url);
      if (onError) {
        this.worker.onerror = onError;
      }

      this.worker.onmessage = (event) => {
        if (
          event.data instanceof Object
                && Object.prototype.hasOwnProperty.call(event.data, 'queryMethodListener')
                && Object.prototype.hasOwnProperty.call(event.data, 'queryMethodArguments')
        ) {
          this.listeners[event.data.queryMethodListener].apply(this, event.data.queryMethodArguments);
        } else {
          this.defaultListener.call(this, event.data);
        }
      };
    }

    postMessage(message) {
      this.worker.postMessage(message);
    }

    terminate() {
      this.worker.terminate();
    }

    addListeners(name, listener) {
      this.listeners[name] = listener;
    }

    removeListeners(name) {
      delete this.listeners[name];
    }

    sendQuery(method, params, transfers = []) {
      if (arguments.length < 1) {
        throw new TypeError('QueryableWorker.sendQuery takes at least one argument');
        return;
      }
      this.worker.postMessage(
        {
          queryMethod: method,
          queryArguments: params,
        },
        transfers,
      );
    }
}

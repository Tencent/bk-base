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

/* eslint-disable no-undef */
/* eslint-disable no-underscore-dangle */
'use strict';
// Allow for running under nodejs/requirejs in tests
const _monaco = typeof monaco === 'undefined' ? self.monaco : monaco;
const languageDefinitions = {};
const lazyLanguageLoaders = {};
const LazyLanguageLoader = /** @class */ ((function () {
  function LazyLanguageLoader(languageId) {
    const _this = this;
    this._languageId = languageId;
    this._loadingTriggered = false;
    this._lazyLoadPromise = new Promise((resolve, reject) => {
      _this._lazyLoadPromiseResolve = resolve;
      _this._lazyLoadPromiseReject = reject;
    });
  }
  LazyLanguageLoader.getOrCreate = function (languageId) {
    if (!lazyLanguageLoaders[languageId]) {
      lazyLanguageLoaders[languageId] = new LazyLanguageLoader(languageId);
    }
    return lazyLanguageLoaders[languageId];
  };
  LazyLanguageLoader.prototype.whenLoaded = function () {
    return this._lazyLoadPromise;
  };
  LazyLanguageLoader.prototype.load = function () {
    const _this = this;
    if (!this._loadingTriggered) {
      this._loadingTriggered = true;
      languageDefinitions[this._languageId].loader().then(
        mod => _this._lazyLoadPromiseResolve(mod),
        err => _this._lazyLoadPromiseReject(err),
      );
    }
    return this._lazyLoadPromise;
  };
  return LazyLanguageLoader;
})());
export function loadLanguage(languageId) {
  return LazyLanguageLoader.getOrCreate(languageId).load();
}
export function registerLanguage(def) {
  const languageId = def.id;
  languageDefinitions[languageId] = def;
  _monaco.languages.register(def);
  const lazyLanguageLoader = LazyLanguageLoader.getOrCreate(languageId);
  _monaco.languages.setMonarchTokensProvider(
    languageId,
    lazyLanguageLoader.whenLoaded().then(mod => mod.language),
  );
  _monaco.languages.onLanguage(languageId, () => {
    lazyLanguageLoader.load().then((mod) => {
      _monaco.languages.setLanguageConfiguration(languageId, mod.conf);
    });
  });
}

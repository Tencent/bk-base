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

const path = require('path');
const { assetsPath } = require('./webpackConfig/utils');
const globalTimestamp = new Date().getTime();
const defaultConfig = require('./config')();
const baseConfig = require('./webpackConfig/index');

function resolve(dir) {
  return path.join(__dirname, '..', dir);
}

const createWebConfig = envConfig => {
  const config = (
    envConfig.WEBPACK_BUNDLE || envConfig.WEBPACK_SERVE ? Object.assign(envConfig, defaultConfig) : envConfig
  );

  const { scriptLoader } = baseConfig(true, config);

  const webpackConfig = {
    mode: 'production',
    devtool: false,
    entry: {
      modeling: [resolve('src/webWorker/labels.worker.ts')],
      dataExplore: [resolve('src/webWorker/dataExplore.worker.js')],
      editorWorker: 'monaco-editor-core/esm/vs/editor/editor.worker.js',
    },
    output: {
      path: config.assetsRoot,
      filename: assetsPath(`web_worker/bundle.worker.[name].js?t=${globalTimestamp}`),
      clean: {
        keep(asset) {
          return !asset.includes('web_worker');
        },
      },
    },
    resolve: {
      extensions: ['.js', '.ts'],
      alias: {
        '@': resolve('src'),
        nm: resolve('node_modules'),
      },
    },
    module: {
      rules: [...scriptLoader],
    },
    plugins: [
    ],
  };
  return webpackConfig;
};

module.exports = createWebConfig;



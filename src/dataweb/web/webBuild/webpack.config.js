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
const baseConfig = require('./webpackConfig/index');
const defaultConfig = require('./config')();
const extendConfig = require('../src/extends/packages/webpack.config');
const { mergeWithCustomize, default: merge } = require('webpack-merge');

function resolve(dir) {
  return path.join(__dirname, '..', dir);
}

const isProd = process.env.NODE_ENV === 'production';

const createWebConfig = envConfig => {
  const config = (envConfig.WEBPACK_BUNDLE
        || envConfig.WEBPACK_SERVE ? Object.assign(envConfig, defaultConfig) : envConfig);

  const { loaders, plugin, optimize, devServer, output } = baseConfig(isProd, config);
  const webpackConfig = {
    mode: isProd ? 'production' : 'development',
    entry: {
      app: ['babel-polyfill', './src/main.ts'],
    },
    output,
    resolve: {
      mainFields: ['module', 'main'],
      extensions: ['.js', '.tsx', '.vue', '.ts', '.scss', '.json'],
      symlinks: false,
      mainFiles: ['index'],
      alias: {
        vue$: resolve('node_modules/vue/dist/vue.esm.js'),
        vscode: require.resolve('monaco-languageclient/lib/vscode-compatibility'),
        'vscode-languageserver-protocol/lib/main': 'vscode-languageserver-protocol/lib/browser/main',
        'vscode-languageserver-protocol/lib/utils/is': 'vscode-languageserver-protocol/lib/common/utils/is',
        '@': resolve('src'),
        plugins: resolve('plugins'),
        jquery: 'jquery',
        nm: resolve('node_modules'),
        components: resolve('src/components'),
        'bkdata-ui': resolve('src/bkdata-ui'),
        dataweb: resolve('src'),
        '@GraphChild': resolve('src/pages/DataGraph/Graph/Children/components'),
        '@Graph': resolve('src/pages/DataGraph/Graph'),
        '@node_modules': resolve('node_modules'),
        '/@tencent/bkcharts-panel/img': resolve('node_modules/@tencent/bkcharts-panel/dist/img'),
      },
      fallback: {
        fs: false,
        net: false,
        timers: require.resolve('timers-browserify'),
        path: require.resolve('path-browserify'),
        http: require.resolve('stream-http'),
        stream: require.resolve('stream-browserify'),
        crypto: require.resolve('crypto-browserify'),
        os: require.resolve('os-browserify/browser'),
      },
    },
    experiments: {
      topLevelAwait: true,
    },
    module: {
      noParse: /^(jquery)$/,
      exprContextCritical: false,
      unknownContextCritical: false,
      unsafeCache: true,
      rules: [
        {
          test: /\.vue$/,
          loader: 'vue-loader',
          options: {
            hotReload: !isProd,
          },
        },
        {
          oneOf: [...loaders],
        },
      ],
    },
    target: isProd ? 'browserslist' : 'web',
    optimization: optimize,
    plugins: plugin.filter(Boolean),
    watchOptions: {
      ignored: /node_modules/,
    },
    node: {
      global: true,
      __filename: false,
      __dirname: false,
    },
    devtool: isProd ? false : 'eval-source-map',
    stats: {
      children: false,
      warningsFilter: /export .* was not found in/,
      source: false,
    },
  };


  if (!isProd) {
    webpackConfig.module.noParse = (
      /^(vue|vue-router|vuex|vue-highlightjs|jquery|vuedraggable|@blueking\/bkchart\.js)$/
    );
    webpackConfig.devServer = devServer;
    webpackConfig.cache = {
      type: 'filesystem',
    };
  }

  const targetConfig = mergeWithCustomize({
    customizeArray(a, b, key) {
      if (key === 'resolve.modules') {
        return [...new Set([...a, ...(b || []).map(val => {
          if (/^src\/extends\/packages/.test(val)) {
            return resolve(val);
          }

          return val;
        })])];
      }

      return undefined;
    },
    customizeObject(a, b, key) {
      if (key === 'resolve.alias' && Object.entries(b || {}).length) {
        // Custom merging
        return merge({}, a,
          Object.entries(b)
            .reduce((output, current) => {
              let target = current[1];
              if (/^src\/extends\/packages/.test(current[1])) {
                target = resolve(current[1]);
              }
              return { ...output, [current[0]]: target };
            }, {}));
      }
      // Fall back to default merging
      return undefined;
    }
  })(webpackConfig, extendConfig);
  return targetConfig;
};

module.exports = createWebConfig;

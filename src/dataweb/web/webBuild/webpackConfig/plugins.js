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

/* eslint-disable prettier/prettier */
const MiniCssExtractPlugin = require('mini-css-extract-plugin');
const path = require('path');
const webpack = require('webpack');
const { VueLoaderPlugin } = require('vue-loader');
const ProgressBarPlugin = require('progress-bar-webpack-plugin');
const CaseSensitivePathsPlugin = require('case-sensitive-paths-webpack-plugin');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const CopyWebpackPlugin = require('copy-webpack-plugin');
const BundleAnalyzerPlugin = require('webpack-bundle-analyzer');
let globalConfig = require('../../src/extends/packages/.global.dev.config.json');
const { globalTimestamp } = require('./utils');
const chalk = require('chalk');
const { assetsPath } = require('./utils');

/** 根据不同环境设置 */
const getEnvHtmlWebpackPluginOptions = (isProd, config) => {
  const encodeMeta = encodeURIComponent(`${config.assetsPublicPath}${globalTimestamp}`);

  const isDev = /^dev/gi.test(config.env.BUILD_ENV);
  const isLocalDev = /^dev$/gi.test(config.env.BUILD_ENV);

  if (isProd || (globalConfig === null || Object.keys(globalConfig).length === 0)) {
    globalConfig = require(`../../.global.${isDev ? 'dev' : 'prod'}.config.json`);
  }
  const scriptContent = `window.BKBASE_Global = ${JSON.stringify(globalConfig.BKBASE_Global)}`;
  const bodyScriptFunction = `<script type="text/javascript">
        function beforeBodyContent() {
            ${scriptContent}
        }
        beforeBodyContent();
        </script>`;
  const staticUrl = /\/$/.test(globalConfig.BKBASE_Global.staticUrl)
    ? globalConfig.BKBASE_Global.staticUrl.replace(/\/$/, '') : `${globalConfig.BKBASE_Global.staticUrl}`;

  const templateContent = ({ htmlWebpackPlugin }) => `
    <!DOCTYPE html>
    <html>
      <head>
      <meta charset="utf-8">
      <title>${globalConfig.BKBASE_Global.webTitle}</title>
      <link rel="shortcut icon" href="${staticUrl}/favicon.ico" type="image/x-icon">
        ${htmlWebpackPlugin.tags.headTags}
        ${bodyScriptFunction}
      </head>
      <body>
        ${htmlWebpackPlugin.tags.bodyTags}
      </body>
    </html>
  `;
  const options = {
    meta: isLocalDev ? {} : { unihash: encodeMeta },
    publicPath: isLocalDev ? '/' : config.assetsPublicPath,
    env: config.env,
    filename: config.index,
    minify: {
      removeComments: true,
      collapseWhitespace: true,
      removeAttributeQuotes: true,
    },
    templateContent,
    inject: false
  };
  const minify = {
    removeComments: true,
    collapseWhitespace: true,
    removeAttributeQuotes: true,
  };

  return isProd
    ? { ...options, ...minify }
    : { filename: 'index.html', templateContent, inject: false };
};

/** webpack 插件 */
const plugins = (isProd, config) => [
  new webpack.ProvidePlugin({
    process: 'process/browser',
    Buffer: ['buffer', 'Buffer'],
    $: 'jquery',
    jquery: 'jquery',
    'window.jQuery': 'jquery',
    jQuery: 'jquery',
  }),

  new HtmlWebpackPlugin(getEnvHtmlWebpackPluginOptions(isProd, config)),
  new CaseSensitivePathsPlugin({ debug: false }),
  isProd ? undefined : new webpack.HotModuleReplacementPlugin(),
  new ProgressBarPlugin({
    format: `  build [:bar] ${chalk.green.bold(':percent')} (:elapsed seconds)`,
  }),
  new VueLoaderPlugin(),
  isProd
    ? new MiniCssExtractPlugin({
      filename: assetsPath(`css/[name].[contenthash].css?t=${globalTimestamp}`, config, isProd),
      chunkFilename: assetsPath(`css/chunk.[id].[contenthash].css?t=${globalTimestamp}`, config, isProd),
      // 忽略 Conflicting order between警告
      ignoreOrder: true,
    })
    : undefined,

  isProd
    ? new webpack.optimize.MinChunkSizePlugin({
      minChunkSize: 10000,
    })
    : undefined,

  new webpack.IgnorePlugin(/^\.\/locale$/, /moment$/),

  new CopyWebpackPlugin({
    patterns: [
      {
        from: path.resolve(__dirname, '../../static'),
        to: config.assetsSubDirectory,
        globOptions: { ignore: ['**/dist', '.*', '**/*.txt'] },
      },
      {
        from: path.resolve(__dirname, '../../src/common/images/logo'),
        to: `${config.staticRoot}/img/logo`,
        globOptions: { ignore: ['.*'] },
      },
    ].concat(
      isProd
        ? [
          /** 用于检测服务器端脚本是否改变 */
          {
            from: path.resolve(__dirname, '../../html'),
            to: `${globalTimestamp}.vO7JpshJEESM7o97ohlLdw`,
          },
        ] : []
    ),
    options: {
      concurrency: 300,
    },
  }),

  config.analyze
    ? new BundleAnalyzerPlugin({
      analyzerMode: 'server',
      analyzerHost: '127.0.0.1',
      analyzerPort: 5000,
    })
    : undefined,
];

/** 打包出口 */
const output = (isProd, config) => {
  const isLocalDev = /^dev$/gi.test(config.env.BUILD_ENV);
  if (isLocalDev) {
    return { filename: '[name].bundle.js' };
  }

  return {
    path: config.assetsRoot,
    filename: assetsPath(`js/bundle.[name].[chunkhash].js?t=${globalTimestamp}`),
    chunkFilename: assetsPath(`js/bundle.[name].[chunkhash].js?t=${globalTimestamp}`),
    publicPath: config.assetsPublicPath,
    clean: {
      keep: /web_worker/,
    },
  };
};

module.exports = {
  plugins,
  output,
};

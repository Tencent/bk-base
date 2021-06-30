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

const getEnv = (buildEnv, appCode, buildVer) => {
  const BUILD_VER = buildVer || process.env.BUILD_VER || 'openpaas'; // 运行（平台）版本
  const BUILD_ENV = buildEnv || process.env.BUILD_ENV || 'dev'; // 运行环境
  const APP_CODE = appCode || process.env.APP_CODE;

  // config.index.js 有关打包生成路径需要用到的变量
  const assetsRootCore = 'dist/';
  const assetsPublicPath = `/static/${assetsRootCore}`;
  const indexPagePath = `${assetsRootCore}index.html`;

  return {
    BUILD_VER, // openpaas|ieod
    BUILD_ENV, // develop|test|product
    APP_CODE,
    assetsRootCore,
    assetsPublicPath,
    indexPagePath,
  };
};

const getEnvConfig = configEnv => ({
  env: configEnv,
  staticRoot: path.resolve(__dirname, `../static/${configEnv.assetsRootCore}`),
  index: path.resolve(__dirname, `../static/${configEnv.indexPagePath}`),
  assetsRoot: path.resolve(__dirname, `../static/${configEnv.assetsRootCore}`),
  assetsSubDirectory: '',
  assetsPublicPath: configEnv.assetsPublicPath,
  productionSourceMap: configEnv.BUILD_ENV === 'product',
  bundleAnalyzerReport: false,
});

module.exports = (buildEnv, appCode, buildVer) => getEnvConfig(getEnv(buildEnv, appCode, buildVer));

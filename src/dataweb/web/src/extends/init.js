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

const fs = require('fs');
const path = require('path');
const packagePath = path.resolve(__dirname, 'packages');
const configFile = path.resolve(__dirname, 'extend.config');
let extendPro = fs.readFileSync(configFile, 'utf8');
extendPro = extendPro.replace(/^(\s*\n*)|(\s*\n*)$/igm, '');

const execProcess = require('./exec_process.js');
/** 初始化，将extends和配置文件设置为 --skip-worktree， 防止开发时提交 */
execProcess.result('sh ./src/extends/bash/ignore.sh', (err, response) => {
  if (!err) {
    const resp = `当前设置为 --skip-worktree 的文件为: \n${response}`;
    console.log(resp);
  } else {
    console.log(err);
  }
});

if (extendPro && fs.existsSync(extendPro)) {
  if (fs.existsSync(packagePath)) {
    fs.renameSync(packagePath, `${packagePath}.old`);
  }

  setTimeout(() => {
    fs.symlinkSync(extendPro, packagePath, 'dir');
  });
} else {
  console.error(`ERROR: extends 所在路径（'${extendPro}'）配置错误，请在 'extend.config' 配置正确路径，路径为绝对地址;`);
}

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

const accessTypes = [
  { id: 'log', name: window.$t('日志文件') },
  { id: 'db', name: window.$t('数据库') },
  { id: 'queue', name: window.$t('消息队列') },
  { id: 'http', name: 'HTTP' },
  { id: 'file', name: window.$t('文件上传_SE') },
  { id: 'script', name: window.$t('脚本上报') },
  { id: 'sdk', name: 'SDK' },
  { id: 'beacon', name: window.$t('灯塔') },
  { id: 'tglog', name: 'TGLOG' },
  { id: 'tlog', name: 'TLOG' },
  { id: 'tqos', name: 'TQOS' },
  { id: 'tdw', name: 'TDW' },
  { id: 'custom', name: window.$t('自定义') },
  { id: 'tube', name: 'TDBANK' },
  { id: 'tdm', name: 'TDM' },
];

export { accessTypes };

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

// import { MonacoMethod, MonacoConfig, MonacoKeyword } from './monaco-mode'

export default {
  'Method': [
    {
      'name': 'MAX',
      'documentation': 'number MAX(Array)',
      'insertText': { 'value': '' },
      'signatureHelp': true,
      'parameters': [
        {
          'label': 'Array numbers',
          'documentation': '需要计算最大值的数组'
        }
      ]
    },
    {
      'name': 'AVG',
      'documentation': 'number AVG(Array)',
      'insertText': { 'value': '' },
      'signatureHelp': true,
      'parameters': [
        {
          'label': 'Array numbers',
          'documentation': '需要计算平均值的数组'
        }
      ]
    },
    {
      'name': 'SUM',
      'documentation': 'number SUM(Array)',
      'insertText': { 'value': '' },
      'signatureHelp': true,
      'parameters': [
        {
          'label': 'Array numbers',
          'documentation': '需要统计个数的数组'
        }
      ]
    },
    {
      'name': 'NEWID',
      'documentation': 'number NEWID(Array)',
      'insertText': { 'value': '' },
      'signatureHelp': true,
      'parameters': [{ 'label': 'String', 'documentation': '新的ID' }]
    },
    {
      'name': 'COUNT',
      'documentation': 'number COUNT(Array)',
      'insertText': { 'value': '' },
      'signatureHelp': true,
      'parameters': [
        { 'label': 'Array numbers', 'documentation': '需要统计的数组' }
      ]
    }
  ],
  'Keyword': [
    { 'name': 'SELECT', 'documentation': '' },
    { 'name': 'FROM', 'documentation': '' },
    { 'name': 'WHERE', 'documentation': '' },
    { 'name': 'GROUP', 'documentation': '' },
    { 'name': 'BY', 'documentation': '' },
    { 'name': 'LEFT', 'documentation': '' },
    { 'name': 'FULL', 'documentation': '' },
    { 'name': 'JOIN', 'documentation': '' },
    { 'name': 'NOT', 'documentation': '' },
    { 'name': 'IS', 'documentation': '' },
    { 'name': 'LIKE', 'documentation': '' },
    { 'name': 'IN', 'documentation': '' },
    { 'name': 'CASE', 'documentation': '' },
    { 'name': 'WHEN', 'documentation': '' },
    { 'name': 'END', 'documentation': '' },
    { 'name': 'ELSE', 'documentation': '' },
    { 'name': 'AS', 'documentation': '' },
    { 'name': 'IF', 'documentation': '' },
    { 'name': 'ORDER', 'documentation': '' },
    { 'name': 'LIMIT', 'documentation': '' },
    { 'name': 'DESC', 'documentation': '' },
    { 'name': 'ASC', 'documentation': '' }
  ]
};

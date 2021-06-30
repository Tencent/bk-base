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

import defaultFunction from './defaultFunction.json';
import defaultKeywords from './defaultKeywords.json';
import operators from './operators.json';

/** 自定义格式化 */
class BKTokenizer {
  constructor(keywords, builtinFunctions) {
    this.keywords = keywords;
    this.builtinFunctions = builtinFunctions;
  }

  /** getTokenizer */
  getTokenizer() {
    return {
      defaultToken: '',
      tokenPostfix: '.sql',
      ignoreCase: true,

      brackets: [
        { open: '[', close: ']', token: 'delimiter.square' },
        { open: '(', close: ')', token: 'delimiter.parenthesis' },
      ],

      keywords: this.keywords || defaultKeywords,
      operators,
      builtinFunctions: this.builtinFunctions || defaultFunction,
      builtinVariables: [
        // NOT SUPPORTED
      ],
      tokenizer: {
        root: [
          { include: '@comments' },
          { include: '@whitespace' },
          { include: '@numbers' },
          { include: '@strings' },
          { include: '@complexIdentifiers' },
          { include: '@scopes' },
          [/[;,.]/, 'delimiter'],
          [/[()]/, '@brackets'],
          [
            /[\w@]+/,
            {
              cases: {
                '@keywords': 'keyword',
                '@operators': 'operator',
                '@builtinVariables': 'predefined',
                '@builtinFunctions': 'predefined',
                '@default': 'identifier',
              },
            },
          ],
          [/[<>=!%&+\-*/|~^]/, 'operator'],
        ],
        whitespace: [[/\s+/, 'white']],
        comments: [
          [/--+.*/, 'comment'],
          [/#+.*/, 'comment'],
          [/\/\*/, { token: 'comment.quote', next: '@comment' }],
        ],
        comment: [
          [/[^*/]+/, 'comment'],
          // Not supporting nested comments, as nested comments seem to not be standard?
          // [/\/\*/, { token: 'comment.quote', next: '@push' }],    // nested comment not allowed :-(
          [/\*\//, { token: 'comment.quote', next: '@pop' }],
          [/./, 'comment'],
        ],
        numbers: [
          [/0[xX][0-9a-fA-F]*/, 'number'],
          [/[$][+-]*\d*(\.\d*)?/, 'number'],
          // eslint-disable-next-line no-useless-escape
          [/((\d+(\.\d*)?)|(\.\d+))([eE][\-+]?\d+)?/, 'number'],
        ],
        strings: [
          [/'/, { token: 'string', next: '@string' }],
          [/"/, { token: 'string.double', next: '@stringDouble' }],
        ],
        string: [
          [/[^']+/, 'string'],
          [/''/, 'string'],
          [/'/, { token: 'string', next: '@pop' }],
        ],
        stringDouble: [
          [/[^"]+/, 'string.double'],
          [/""/, 'string.double'],
          [/"/, { token: 'string.double', next: '@pop' }],
        ],
        complexIdentifiers: [[/`/, { token: 'identifier.quote', next: '@quotedIdentifier' }]],
        quotedIdentifier: [
          [/[^`]+/, 'identifier'],
          [/``/, 'identifier'],
          [/`/, { token: 'identifier.quote', next: '@pop' }],
        ],
        scopes: [
          // NOT SUPPORTED
        ],
      },
    };
  }
}

export default {
  getTokenizer: (keywords, builtinFunctions) => new BKTokenizer(keywords, builtinFunctions).getTokenizer(),
};

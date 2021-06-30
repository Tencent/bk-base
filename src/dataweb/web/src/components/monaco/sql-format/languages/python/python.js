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

/* eslint-disable no-useless-escape */
/* eslint-disable no-underscore-dangle */
/* eslint-disable no-undef */
'use strict';
// Allow for running under nodejs/requirejs in tests
const _monaco = typeof monaco === 'undefined' ? self.monaco : monaco;
export const conf = {
  comments: {
    lineComment: '#',
    blockComment: ['\'\'\'', '\'\'\''],
  },
  brackets: [
    ['{', '}'],
    ['[', ']'],
    ['(', ')'],
  ],
  autoClosingPairs: [
    { open: '{', close: '}' },
    { open: '[', close: ']' },
    { open: '(', close: ')' },
    { open: '"', close: '"', notIn: ['string'] },
    { open: '\'', close: '\'', notIn: ['string', 'comment'] },
  ],
  surroundingPairs: [
    { open: '{', close: '}' },
    { open: '[', close: ']' },
    { open: '(', close: ')' },
    { open: '"', close: '"' },
    { open: '\'', close: '\'' },
  ],
  onEnterRules: [
    {
      beforeText: new RegExp('^\\s*(?:def|class|for|if|elif|else|while|try|with|finally|except|async).*?:\\s*$'),
      action: { indentAction: _monaco.languages.IndentAction.Indent },
    },
  ],
  folding: {
    offSide: true,
    markers: {
      start: new RegExp('^\\s*#region\\b'),
      end: new RegExp('^\\s*#endregion\\b'),
    },
  },
};
export const language = {
  defaultToken: '',
  tokenPostfix: '.python',
  keywords: [
    'and',
    'as',
    'assert',
    'break',
    'class',
    'continue',
    'def',
    'del',
    'elif',
    'else',
    'except',
    'exec',
    'finally',
    'for',
    'from',
    'global',
    'if',
    'import',
    'in',
    'is',
    'lambda',
    'None',
    'not',
    'or',
    'pass',
    'print',
    'raise',
    'return',
    'self',
    'try',
    'while',
    'with',
    'yield',
    'int',
    'float',
    'long',
    'complex',
    'hex',
    'abs',
    'all',
    'any',
    'apply',
    'basestring',
    'bin',
    'bool',
    'buffer',
    'bytearray',
    'callable',
    'chr',
    'classmethod',
    'cmp',
    'coerce',
    'compile',
    'complex',
    'delattr',
    'dict',
    'dir',
    'divmod',
    'enumerate',
    'eval',
    'execfile',
    'file',
    'filter',
    'format',
    'frozenset',
    'getattr',
    'globals',
    'hasattr',
    'hash',
    'help',
    'id',
    'input',
    'intern',
    'isinstance',
    'issubclass',
    'iter',
    'len',
    'locals',
    'list',
    'map',
    'max',
    'memoryview',
    'min',
    'next',
    'object',
    'oct',
    'open',
    'ord',
    'pow',
    'print',
    'property',
    'reversed',
    'range',
    'raw_input',
    'reduce',
    'reload',
    'repr',
    'reversed',
    'round',
    'set',
    'setattr',
    'slice',
    'sorted',
    'staticmethod',
    'str',
    'sum',
    'super',
    'tuple',
    'type',
    'unichr',
    'unicode',
    'vars',
    'xrange',
    'zip',
    'True',
    'False',
    '__dict__',
    '__methods__',
    '__members__',
    '__class__',
    '__bases__',
    '__name__',
    '__mro__',
    '__subclasses__',
    '__init__',
    '__import__',
  ],
  brackets: [
    { open: '{', close: '}', token: 'delimiter.curly' },
    { open: '[', close: ']', token: 'delimiter.bracket' },
    { open: '(', close: ')', token: 'delimiter.parenthesis' },
  ],
  tokenizer: {
    root: [
      { include: '@whitespace' },
      { include: '@numbers' },
      { include: '@strings' },
      [/[,:;]/, 'delimiter'],
      [/[{}\[\]()]/, '@brackets'],
      [/@[a-zA-Z]\w*/, 'tag'],
      [
        /[a-zA-Z]\w*/,
        {
          cases: {
            '@keywords': 'keyword',
            '@default': 'identifier',
          },
        },
      ],
    ],
    // Deal with white space, including single and multi-line comments
    whitespace: [
      [/\s+/, 'white'],
      [/(^#.*$)/, 'comment'],
      [/'''/, 'string', '@endDocString'],
      [/"""/, 'string', '@endDblDocString'],
    ],
    endDocString: [
      [/[^']+/, 'string'],
      [/\\'/, 'string'],
      [/'''/, 'string', '@popall'],
      [/'/, 'string'],
    ],
    endDblDocString: [
      [/[^"]+/, 'string'],
      [/\\"/, 'string'],
      [/"""/, 'string', '@popall'],
      [/"/, 'string'],
    ],
    // Recognize hex, negatives, decimals, imaginaries, longs, and scientific notation
    numbers: [
      [/-?0x([abcdef]|[ABCDEF]|\d)+[lL]?/, 'number.hex'],
      [/-?(\d*\.)?\d+([eE][+\-]?\d+)?[jJ]?[lL]?/, 'number'],
    ],
    // Recognize strings, including those broken across lines with \ (but not without)
    strings: [
      [/'$/, 'string.escape', '@popall'],
      [/'/, 'string.escape', '@stringBody'],
      [/"$/, 'string.escape', '@popall'],
      [/"/, 'string.escape', '@dblStringBody'],
    ],
    stringBody: [
      [/[^\\']+$/, 'string', '@popall'],
      [/[^\\']+/, 'string'],
      [/\\./, 'string'],
      [/'/, 'string.escape', '@popall'],
      [/\\$/, 'string'],
    ],
    dblStringBody: [
      [/[^\\"]+$/, 'string', '@popall'],
      [/[^\\"]+/, 'string'],
      [/\\./, 'string'],
      [/"/, 'string.escape', '@popall'],
      [/\\$/, 'string'],
    ],
  },
};

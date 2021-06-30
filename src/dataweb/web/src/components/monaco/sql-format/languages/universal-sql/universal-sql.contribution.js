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

/* eslint-disable no-param-reassign */
/* eslint-disable no-underscore-dangle */
'use strict';
import { registerLanguage } from '../_.contribution';

((function () {
  class Adapter {
    static positionToOffset(uri, position) {
      const model = window.monaco.editor.getModel(uri);
      return model.getOffsetAt(position);
    }

    static offsetToPosition(uri, offset) {
      const model = window.monaco.editor.getModel(uri);
      return model.getPositionAt(offset);
    }

    static textSpanToRange(uri, span) {
      const p1 = this._offsetToPosition(uri, span.start);
      const p2 = this._offsetToPosition(uri, span.start + span.length);
      const { lineNumber: startLineNumber, column: startColumn } = p1;
      const { lineNumber: endLineNumber, column: endColumn } = p2;
      return { startLineNumber, startColumn, endLineNumber, endColumn };
    }
  }

  console.log('---register---');
  const language = 'universal-sql';
  const sqlFormatter = require('sql-formatter');
  const _conf = require('../../../monacoConfLoader');
  const { monacoConfLoader } = _conf;
  registerLanguage({
    id: language,
    extensions: ['.sql'],
    aliases: ['SQL'],
    loader: () => import('./universal-sql'),
  });

  // 自动补全
  function getProvideCompletionItems(wordInfo) {
    return (
      monacoConfLoader.providerConfigs.provideCompletionItems
        .filter(item => item && new RegExp(wordInfo.word, 'iy').test(item.label)) || null
    );
  }

  // 关键字提示配置
  function getTokensProviderConf() {
    const t = require('../../../tokenizer');
    return t.default.getTokenizer(monacoConfLoader.getKeywords(), monacoConfLoader.getBuiltinFunctions());
  }

  // Register a tokens provider for the language
  window.monaco.languages.setMonarchTokensProvider(language, getTokensProviderConf());

  // Register a completion item provider for the new language
  window.monaco.languages.registerCompletionItemProvider(language, {
    provideCompletionItems: (model, position) => {
      const wordInfo = model.getWordUntilPosition(position);
      const items = getProvideCompletionItems(wordInfo);
      return {
        suggestions: items,
      };
    },
  });

  window.monaco.languages.registerSignatureHelpProvider(language, {
    signatureHelpTriggerCharacters: ['(', ','],
    provideSignatureHelp: (model, position) => {
      // 获取当前所在行数据
      const textUntilPosition = model.getValueInRange({
        startLineNumber: position.lineNumber,
        startColumn: 1,
        endLineNumber: position.lineNumber,
        endColumn: position.column,
      });
      const wordInfo = model.getWordUntilPosition(position);
      const resource = model.uri;
      const offset = Adapter.positionToOffset(resource, position);
      const word = model.getValueInRange({
        startLineNumber: position.lineNumber,
        startColumn: wordInfo.endColumn - offset,
        endLineNumber: position.lineNumber,
        endColumn: position.column,
      });
      // 正则：移除末尾的空格
      if (/\)\s*$/g.test(textUntilPosition)) {
        return [];
      }

      // 正则：匹配当前获取到的字符串，以括号包（方法参数）起来的部分
      const regLastFunc = /\(([^)]*)\)?[^(]*$/g;

      // 正则：匹配获取到的参数内部的字符串类型参数
      const regStrParams = /("([^"]|"")*")/g;

      // 正则：匹配获取方法名称
      const methodNameReg = /\s?[0-9a-zA-Z_]{1,}\s*\(/gi;

      const methodNames = word.match(methodNameReg);
      let methodName = '';

      // 正则匹配获取方法名称
      if (methodNames && methodNames.length) {
        methodName = methodNames[methodNames.length - 1];
        methodName = methodName.replace(/\s/, '');
        methodName = methodName.replace(/\(/, '');
      }

      // 获取当前参数，根据逗号的个数判断当前是第几个参数
      const lastFunc = textUntilPosition.match(regLastFunc);
      if (lastFunc && lastFunc.length) {
        const lastFuncParams = lastFunc[lastFunc.length - 1];

        // 替换字符串参数为：$$$，方便后面统计参数个数
        const formatParams = lastFuncParams.replace(regStrParams, '$$$');
        const paramsLen = formatParams.split(/,/);
        const activePosition = (paramsLen && paramsLen.length) || 0;

        return getActiveSignatures(activePosition - 1, methodName);
      }
      return [];
    },
  });

  // 方法参数提示
  function getSignatureHelpSignatures(methodName) {
    const signatures = (monacoConfLoader.serveConf.Method || [])
      .filter(s => new RegExp(methodName, 'ig').test(s.name));
    const signature = (signatures && signatures[0]) || {};
    return [
      {
        label: signature.documentation || signature.name,
        parameters: signature.parameters,
      },
    ];
  }

  // 参数当前提示序号
  function getActiveSignatures(index, methodName) {
    const _signatures = getSignatureHelpSignatures(methodName);
    const _params = (_signatures && _signatures[0] && _signatures[0].parameters) || [];
    const _paramsLen = _params.length || 0;
    if (_paramsLen < index + 1) {
      index = _paramsLen - 1;
    }

    return {
      activeParameter: index,
      activeSignature: 0,
      signatures: _signatures,
    };
  }

  // 格式化SQL代码
  window.monaco.languages.registerDocumentFormattingEditProvider(language, {
    async provideDocumentFormattingEdits(model) {
      const formatted = sqlFormatter.format(model.getValue());
      return [
        {
          range: model.getFullModelRange(),
          text: formatted,
        },
      ];
    },
  });
})());

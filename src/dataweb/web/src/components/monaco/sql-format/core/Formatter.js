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
/* eslint-disable no-plusplus */
/* eslint-disable no-underscore-dangle */
import { trimEnd } from './utils';
import tokenTypes from './tokenTypes';
import Indentation from './Indentation';
import InlineBlock from './InlineBlock';
import Params from './Params';

export default class Formatter {
  /**
     * @param {Object} cfg
     *   @param {Object} cfg.indent
     *   @param {Object} cfg.params
     * @param {Tokenizer} tokenizer
     */
  constructor(cfg, tokenizer) {
    this.cfg = cfg || {};
    this.indentation = new Indentation(this.cfg.indent);
    this.inlineBlock = new InlineBlock();
    this.params = new Params(this.cfg.params);
    this.tokenizer = tokenizer;
    this.previousReservedWord = {};
  }

  /**
     * Formats whitespaces in a SQL string to make it easier to read.
     *
     * @param {String} query The SQL query string
     * @return {String} formatted query
     */
  format(query) {
    const tokens = this.tokenizer.tokenize(query);
    const formatTokens = this.formatTokens(tokens);
    const formattedQuery = this.getFormattedQueryFromTokens(formatTokens);

    return formattedQuery.trim();
  }

  /**
     *  根据配置每行字段个数格式化换行
     *  针对Select Update 等设置字段时
     */
  formatTokens(tokens) {
    const tokenLen = tokens.length;
    let positoin = 0;
    const _tokens = [];

    while (positoin < tokenLen) {
      const token = tokens[positoin];
      _tokens.push(token, tokens[positoin + 1]);
      if (token.type === tokenTypes.RESERVED_TOPLEVEL) {
        const __tokens = this.getNextLimitToken(tokens, positoin);
        _tokens.push(...__tokens.tokens);
        positoin = __tokens.positoin;
      } else {
        positoin++;
      }
    }

    return _tokens;
  }

  getNextLimitToken(tokens, positoin) {
    const wordLimit = this.cfg.wordLimit || 3;
    let stop = false;
    const _tokens = [];
    const returnTokens = [];
    while (!stop) {
      positoin++;
      const _token = tokens[positoin];
      stop = !_token || _token.type === tokenTypes.RESERVED_TOPLEVEL;
      !stop && _tokens.push(_token);
    }

    const wordstring = _tokens.reduce(
      (pre, next) => ((next.value === ',' && pre.trimEnd()) || pre) + next.value,
      ' ',
    );
    const wordlist = wordstring.trim().split(/\s+|\n+/);
    let start = 0;
    let end = wordLimit;

    do {
      returnTokens.push(
        { type: tokenTypes.RESERVED_NEWLINE_AFTER, value: wordlist.slice(start, end).join(' ') },
        { type: tokenTypes.WHITESPACE, value: '' },
      );
      end = end + wordLimit;
      start = start + wordLimit;
    } while (start < wordlist.length);

    return { tokens: returnTokens, positoin };
  }

  getFormattedQueryFromTokens(tokens) {
    let formattedQuery = '';

    tokens.forEach((token, index) => {
      if (!token) {
        return;
      }
      if (token.type === tokenTypes.WHITESPACE) {
        return;
      }
      if (token.type === tokenTypes.LINE_COMMENT) {
        formattedQuery = this.formatLineComment(token, formattedQuery);
      } else if (token.type === tokenTypes.BLOCK_COMMENT) {
        formattedQuery = this.formatBlockComment(token, formattedQuery);
      } else if (token.type === tokenTypes.RESERVED_TOPLEVEL) {
        formattedQuery = this.formatToplevelReservedWord(token, formattedQuery);
        this.previousReservedWord = token;
      } else if (token.type === tokenTypes.RESERVED_NEWLINE) {
        formattedQuery = this.formatNewlineReservedWord(token, formattedQuery);
        this.previousReservedWord = token;
      } else if (token.type === tokenTypes.RESERVED_NEWLINE_AFTER) {
        formattedQuery = this.formatNewlineAfterReservedWord(token, formattedQuery);
        this.previousReservedWord = token;
      } else if (token.type === tokenTypes.RESERVED) {
        formattedQuery = this.formatWithSpaces(token, formattedQuery);
        this.previousReservedWord = token;
      } else if (token.type === tokenTypes.OPEN_PAREN) {
        formattedQuery = this.formatOpeningParentheses(tokens, index, formattedQuery);
      } else if (token.type === tokenTypes.CLOSE_PAREN) {
        formattedQuery = this.formatClosingParentheses(token, formattedQuery);
      } else if (token.type === tokenTypes.PLACEHOLDER) {
        formattedQuery = this.formatPlaceholder(token, formattedQuery);
      } else if (token.value === ',') {
        formattedQuery = this.formatComma(token, formattedQuery);
      } else if (token.value === ':') {
        formattedQuery = this.formatWithSpaceAfter(token, formattedQuery);
      } else if (token.value === '.' || token.value === ';') {
        formattedQuery = this.formatWithoutSpaces(token, formattedQuery);
      } else {
        formattedQuery = this.formatWithSpaces(token, formattedQuery);
      }
    });
    return formattedQuery;
  }

  formatLineComment(token, query) {
    return this.addNewline(query + token.value);
  }

  formatBlockComment(token, query) {
    return this.addNewline(this.addNewline(query) + this.indentComment(token.value));
  }

  indentComment(comment) {
    return comment.replace(/\n/g, `\n${this.indentation.getIndent()}`);
  }

  formatToplevelReservedWord(token, query) {
    this.indentation.decreaseTopLevel();
    query = this.addNewline(query);
    this.indentation.increaseToplevel();
    query += this.equalizeWhitespace(token.value.toUpperCase());
    return `${query} `; // this.addNewline(query)
  }

  formatNewlineReservedWord(token, query) {
    return `${this.addNewline(query) + this.equalizeWhitespace(token.value)} `;
  }

  formatNewlineAfterReservedWord(token, query) {
    const isTypeEquals = this.previousReservedWord.type === tokenTypes.RESERVED_TOPLEVEL;
    const isAfterTopLevel = this.previousReservedWord && isTypeEquals;
    return `${(isAfterTopLevel ? query : this.addNewline(query)) + token.value} `;
  }

  // Replace any sequence of whitespace characters with single space
  equalizeWhitespace(string) {
    return string.replace(/\s+/g, ' ');
  }

  // Opening parentheses increase the block indent level and start a new line
  formatOpeningParentheses(tokens, index, query) {
    /*
         * Take out the preceding space unless there was whitespace there in the original query
         * or another opening parens
         **/
    const previousToken = tokens[index - 1];
    if (
      previousToken
            && previousToken.type !== tokenTypes.WHITESPACE
            && previousToken.type !== tokenTypes.OPEN_PAREN
    ) {
      query = trimEnd(query);
    }
    query += tokens[index].value;

    this.inlineBlock.beginIfPossible(tokens, index);

    if (!this.inlineBlock.isActive()) {
      this.indentation.increaseBlockLevel();
      query = this.addNewline(query);
    }
    return query;
  }

  // Closing parentheses decrease the block indent level
  formatClosingParentheses(token, query) {
    if (this.inlineBlock.isActive()) {
      this.inlineBlock.end();
      return this.formatWithSpaceAfter(token, query);
    }
    this.indentation.decreaseBlockLevel();
    return this.formatWithSpaces(token, this.addNewline(query));
  }

  formatPlaceholder(token, query) {
    return `${query + this.params.get(token)} `;
  }

  // Commas start a new line (unless within inline parentheses or SQL "LIMIT" clause)
  formatComma(token, query) {
    query = `${trimEnd(query) + token.value} `;

    if (this.inlineBlock.isActive()) {
      return query;
    }
    if (/^LIMIT$/i.test(this.previousReservedWord.value)) {
      return query;
    }
    return this.addNewline(query);
  }

  formatWithSpaceAfter(token, query) {
    return `${trimEnd(query) + token.value} `;
  }

  formatWithoutSpaces(token, query) {
    return trimEnd(query) + token.value;
  }

  formatWithSpaces(token, query) {
    return `${query + token.value} `;
  }

  addNewline(query) {
    return `${trimEnd(query)}\n${this.indentation.getIndent()}`;
  }
}

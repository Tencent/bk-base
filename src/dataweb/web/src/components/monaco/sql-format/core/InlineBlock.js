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

/* eslint-disable no-plusplus */
import tokenTypes from './tokenTypes';

const INLINE_MAX_LENGTH = 50;

/**
 * Bookkeeper for inline blocks.
 *
 * Inline blocks are parenthized expressions that are shorter than INLINE_MAX_LENGTH.
 * These blocks are formatted on a single line, unlike longer parenthized
 * expressions where open-parenthesis causes newline and increase of indentation.
 */
export default class InlineBlock {
  constructor() {
    this.level = 0;
  }

  /**
   * Begins inline block when lookahead through upcoming tokens determines
   * that the block would be smaller than INLINE_MAX_LENGTH.
   * @param  {Object[]} tokens Array of all tokens
   * @param  {Number} index Current token position
   */
  beginIfPossible(tokens, index) {
    if (this.level === 0 && this.isInlineBlock(tokens, index)) {
      this.level = 1;
    } else if (this.level > 0) {
      this.level++;
    } else {
      this.level = 0;
    }
  }

  /**
   * Finishes current inline block.
   * There might be several nested ones.
   */
  end() {
    this.level--;
  }

  /**
   * True when inside an inline block
   * @return {Boolean}
   */
  isActive() {
    return this.level > 0;
  }

  // Check if this should be an inline parentheses block
  // Examples are "NOW()", "COUNT(*)", "int(10)", key(`somecolumn`), DECIMAL(7,2)
  isInlineBlock(tokens, index) {
    let length = 0;
    let level = 0;

    for (let i = index; i < tokens.length; i++) {
      const token = tokens[i];
      length += token.value.length;

      // Overran max length
      if (length > INLINE_MAX_LENGTH) {
        return false;
      }

      if (token.type === tokenTypes.OPEN_PAREN) {
        level++;
      } else if (token.type === tokenTypes.CLOSE_PAREN) {
        level--;
        if (level === 0) {
          return true;
        }
      }

      if (this.isForbiddenToken(token)) {
        return false;
      }
    }
    return false;
  }

  // Reserved words that cause newlines, comments and semicolons
  // are not allowed inside inline parentheses block
  isForbiddenToken({ type, value }) {
    return (
      type === tokenTypes.RESERVED_TOPLEVEL
            || type === tokenTypes.RESERVED_NEWLINE
            || type === tokenTypes.COMMENT
            || type === tokenTypes.BLOCK_COMMENT
            || value === ';'
    );
  }
}

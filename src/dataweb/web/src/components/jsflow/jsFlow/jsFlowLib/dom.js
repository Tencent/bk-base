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
export function nodeContains(root, el) {
  if (root.compareDocumentPosition) {
    return root === el || !!(root.compareDocumentPosition(el) & 16);
  }
  if (root.contains && el.nodeType === 1) {
    return root.contains(el) && root !== el;
  }
  while ((el = el.parentNode)) {
    if (el === root) return true;
  }
  return false;
}

/**
 * 节点及祖先节点是否包含传入的 class
 * 该方法暂支持 class 匹配
 *
 * @param {String} el 当前节点
 * @param {String} selector 选择器
 *
 * @return {NodeObject, Null} 节点对象或者null
 */
export function matchSelector(el, selector) {
  if (el.nodeType === 1 && el.classList.contains(selector)) {
    return el;
  }
  if (el.parentNode.nodeName !== 'HTML') {
    return matchSelector(el.parentNode, selector);
  }
  return null;
}
/**
 * 获取PC、移动端兼容的点击或触摸事件对象
 * @param {Object} event 事件对象
 */
export function getPolyfillEvent(event) {
  if ('touches' in event) {
    return event.touches[0];
  }
  return event;
}

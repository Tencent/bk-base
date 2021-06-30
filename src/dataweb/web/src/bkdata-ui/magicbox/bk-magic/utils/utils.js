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
/* eslint-disable no-prototype-builtins */
export function isVNode(node) {
  return typeof node === 'object' && node.hasOwnProperty('componentOptions');
}

export function isInArray(ele, array) {
  for (const item of array) {
    if (item === ele) {
      return true;
    }
  }

  return false;
}

export function isInlineElment(node) {
  const inlineElements = [
    'a',
    'abbr',
    'acronym',
    'b',
    'bdo',
    'big',
    'br',
    'cite',
    'code',
    'dfn',
    'em',
    'font',
    'i',
    'img',
    'input',
    'kbd',
    'label',
    'q',
    's',
    'samp',
    'select',
    'small',
    'span',
    'strike',
    'strong',
    'sub',
    'sup',
    'textarea',
    'tt',
    'u',
    'var',
  ];
  const tag = node.tagName.toLowerCase();
  const { display } = getComputedStyle(node);

  if ((isInArray(tag, inlineElements) && display === 'index') || display === 'inline') {
    console.warn('Binding node is displayed as inline element. To avoid some unexpected rendering error,'
            + 'please set binding node displayed as block element.');

    return true;
  }

  return false;
}

/**
 *  获取元素相对于页面的高度
 *  @param node {NodeElement} 指定的DOM元素
 */
export function getActualTop(node) {
  let actualTop = node.offsetTop;
  let current = node.offsetParent;

  while (current !== null) {
    actualTop += current.offsetTop;
    current = current.offsetParent;
  }

  return actualTop;
}

/**
 *  获取元素相对于页面左侧的宽度
 *  @param node {NodeElement} 指定的DOM元素
 */
export function getActualLeft(node) {
  let actualLeft = node.offsetLeft;
  let current = node.offsetParent;

  while (current !== null) {
    actualLeft += current.offsetLeft;
    current = current.offsetParent;
  }

  return actualLeft;
}

/**
 *  对元素添加样式类
 *  @param node {NodeElement} 指定的DOM元素
 *  @param className {String} 类名
 */
export function addClass(node, className) {
  const classNames = className.split(' ');
  if (node.nodeType === 1) {
    if (!node.className && classNames.length === 1) {
      node.className = className;
    } else {
      let setClass = ` ${node.className} `;
      classNames.forEach((cl) => {
        if (setClass.indexOf(` ${cl} `) < 0) {
          setClass += `${cl} `;
        }
      });
      const rtrim = /^\s+|\s+$/;
      node.className = setClass.replace(rtrim, '');
    }
  }
}

/**
 *  对元素删除样式类
 *  @param node {NodeElement} 指定的DOM元素
 *  @param className {String} 类名
 */
export function removeClass(node, className) {
  const classNames = className.split(' ');
  if (node.nodeType === 1) {
    let setClass = ` ${node.className} `;
    classNames.forEach((cl) => {
      setClass = setClass.replace(` ${cl} `, ' ');
    });
    const rtrim = /^\s+|\s+$/;
    node.className = setClass.replace(rtrim, '');
  }
}

/**
 *  在指定的对象中根据路径找到value
 *  @param obj {Object} - 指定的对象
 *  @param keys {String} - 路径，支持a.b.c的形式
 */
export function findValueInObj(obj, keys = '') {
  const keyArr = keys.split('.');
  let o = obj;
  let result = null;
  const { length } = keyArr;

  for (const [index, key] of keyArr.entries()) {
    if (!o) break;

    if (index === length - 1) {
      result = o[key];
      break;
    }

    o = o[key];
  }

  return result;
}

function convertToNum(el) {
  return Number(el) || el;
}

/**
 *  在项为对象的数组中，根据某一组{ key: value }，找到其所在对象中指定key的value
 *  @param arr {Array} - 指定的数组
 *  @param originItem {Object} - 依据的一组{ key: value }
 *  @param targetKey {String} - 指定的key值
 */
export function findValueInArrByRecord(arr, originItem, targetKey) {
  let result;
  const key = Object.keys(originItem)[0];
  const item = originItem[key];

  // eslint-disable-next-line no-unused-vars
  for (const [index, _arr] of arr.entries()) {
    if (convertToNum(_arr[key]) === convertToNum(item)) {
      result = _arr[targetKey];
      break;
    }
  }

  return result;
}

export function isObject(argv) {
  return Object.prototype.toString.call(argv).toLowerCase() === '[object object]';
}

/**
 *  判断某个值在指定的数组中的位置
 *  @param arr {Array} - 指定的数组
 *  @param target {String/Number/Object} - 指定的值
 *  @return result {Object} - 返回的对象，若为找到，该对象中key为result的值是false；若找到，key为result的值为true，
 *              同时key为index的值为当前项在数组中的索引值
 */
export function getIndexInArray(arr, target) {
  let result = {
    result: false,
  };
  const { stringify } = JSON;

  if (!arr.length) return result;

  for (const [index, item] of arr.entries()) {
    if (item) {
      const $item = isObject(item) ? stringify(item) : item.toString();
      const $target = isObject(target) ? stringify(target) : target.toString();

      if ($item === $target) {
        result = {
          result: true,
          index,
        };
        break;
      }
    }
  }

  return result;
}

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
export default {
  // 过期时间，默认5天
  age: 5 * 24 * 60 * 60 * 1000,
  /**
   * 设置过期时间
   * @param age
   * @returns {exports}
   */
  setAge(age) {
    this.age = age;
    return this;
  },
  /**
   * 设置 localStorage
   * @param key
   * @param value
   */
  set(key, value) {
    localStorage.removeItem(key);
    const isObject = value instanceof Object;
    const _time = new Date().getTime();
    const _age = this.age;

    // 如果不是对象，新建一个对象把 value 存起来
    if (!isObject) {
      const b = value;
      value = {};
      value._value = b;
    }
    // 加入时间
    value._time = _time;
    // 过期时间
    value._age = _time + _age;
    // 是否一个对象
    value._isObject = isObject;
    localStorage.setItem(key, JSON.stringify(value));
    return this;
  },
  /**
   * 判断一个 localStorage 是否过期
   * @param key
   * @returns {boolean}
   */
  isExpire(key) {
    let isExpire = true;
    let value = localStorage.getItem(key);
    const now = new Date().getTime();

    if (value) {
      value = JSON.parse(value);
      // 当前时间是否大于过期时间
      isExpire = now > value._age;
    } else {
      // 没有值也是过期
    }
    return isExpire;
  },
  /**
   * 获取某个 localStorage 值
   * @param key
   * @returns {*}
   */
  get(key) {
    const isExpire = this.isExpire(key);
    let value = null;
    if (!isExpire) {
      value = localStorage.getItem(key);
      value = JSON.parse(value);
      if (!value._isObject) {
        value = value._value;
      }
    }
    return value;
  },
};

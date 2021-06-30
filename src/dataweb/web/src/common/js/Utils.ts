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

/**
 * 前端定义接口格式化为服务器端格式
 * @param data ： 前端数据
 */
export function getServeFormData(data: any): any {
  let output = initOutput(data, {});
  if (typeof data === 'object' && data !== null) {
    if (Array.isArray(data)) {
      data.forEach(item => {
        output.push(getServeFormData(item));
      });
    } else {
      for (const key in data) {
        const serveKey = webFormatToServeFormat(key);
        output[serveKey] = getServeFormData(data[key]);
      }
    }
  } else {
    output = data;
  }

  return output;
}

/**
 * 根据已有数据初始化变量
 * @param data：已有数据
 * @param output：输出变量
 */
export function initOutput(data: any, output: any) {
  switch (typeof data) {
    case 'string':
      output = '';
      break;
    case 'number':
      output = 0;
      break;
    case 'object':
      if (Array.isArray(data)) {
        output = [];
      } else {
        output = {};
      }
      break;
    case 'boolean':
      output = true;
      break;
    case 'function':
      output = () => { };
      break;
    default:
      output = {};
      break;
  }

  return output;
}

/**
 * 前端格式转换为服务器格式
 * @param fieldName ：字段名称
 */
export function webFormatToServeFormat(fieldName: string): string {
  if (fieldName === undefined || fieldName === null) {
    fieldName = '';
  }
  function upperToServeLower(match: string) {
    return '_' + match.toLowerCase();
  }
  return (fieldName.startsWith('_') && fieldName) || fieldName.replace(/[A-Z]/g, upperToServeLower);
}

/**
 * 格式化服务器端接口返回数据
 * 前端数据命名规范：驼峰式
 * 服务器端命名规范：下划线
 * @param data ：服务器接口返回数据
 * @returns ： 前端驼峰式
 */
export function getWebData<T>(data: any) {
  let output = initOutput(data, {});
  if (typeof data === 'object' && data !== null) {
    if (Array.isArray(data)) {
      data.forEach(item => {
        output.push(getWebData(item));
      });
    } else {
      for (const key in data) {
        const webKey = serveFormatToWebFormat(key);
        output[webKey] = getWebData(data[key]);
      }
    }
  } else {
    output = data;
  }

  return output;
}

/**
 * 服务器格式转换为前端格式
 * @param fieldName ：字段名称
 */
export function serveFormatToWebFormat(fieldName: string): string {
  function serveToWebUpper(match: string) {
    return match.substring(match.lastIndexOf('_') + 1, match.length).toUpperCase();
  }
  return fieldName.replace(/_\w/g, serveToWebUpper);
}

/**
 * 深克隆
 */

export function deepClone(obj: any, cache: any[] = []) {
  if (obj == null || typeof obj !== 'object') {
    return obj;
  }
  const hit = cache.filter(c => c.original === obj)[0];
  if (hit) {
    return hit.copy;
  }
  const copy = Array.isArray(obj) ? [] : {};
  cache.push({
    original: obj,
    copy,
  });
  Object.keys(obj).forEach(key => {
    Object.assign(copy, { [key]: deepClone(obj[key], cache) });
  });
  return copy;
}

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

/* eslint-disable */
import $ from 'jquery';
import Vue from 'vue';
import { bkRequest } from './ajax';
import JSONBigNumber from 'json-bignumber';
import throttle from './throttle';

// 画布内的工具函数
export function hiddenSth() {
  $('.more').css('display', 'none');
  $('.delete-node').css('opacity', 0);
  $('.jtk-endpoint').css('opacity', 0);
  $('.jtk-overlay').css('opacity', 0);
}

export function showSth() {
  $('.more').css('display', 'block');
  $('.delete-node').css('opacity', 1);
  $('.jtk-endpoint').css('opacity', 1);
  $('.jtk-overlay').css('opacity', 1);
}

/**
 * @description 是否已经存在另一个方向的连线，禁止正反连线
 * @param source,target 父子节点
 * @param lines 线条的集合
 */
export function hasTheSameLine(source, target, data) {
  const sourceId = source.id;
  const targetId = target.id;
  const temp = data.lines.filter(l => (l.source.id === sourceId && l.target.id === targetId) || (l.source.id === targetId && l.target.id === sourceId));
  if (temp.length > 0) {
    return false;
  }
  return true;
}

/**
 * @description 顶层简易提示
 * @param {message} 提示信息
 * @param {level} 提示级别，'success, warning, error', 'primary' 四种状态
 */
export function showMsg(message, level = 'primary', options = {}) {
  new Vue().$bkMessage(
    Object.assign(
      {
        message,
        theme: level,
        delay: 0,
        limit: 1,
        ellipsisLine: 3,
        onClose() {
          $('.bk-message').remove();
        },
      },
      options
    )
  );
}

/**
 * 显示简易确认/取消弹窗
 * @param {title} title 弹窗的名称
 * @param {*} content 弹窗的具体内容
 * @param {*} onConfirm 确认时的回调
 * @param {*} onCancel 取消时的回调
 */
export function confirmMsg(title, content, onConfirm, onCancel, options = {}) {
  new Vue().$bkInfo(
    Object.assign(
      {
        title: title ? title.toString() : '',
        subTitle: content ? content.toString() : '',
        type: options.level || options.type || 'options',
        confirmFn() {
          if (onConfirm) {
            onConfirm();
          }
        },
        cancelFn() {
          if (onCancel) {
            onCancel();
          }
        },
      },
      options
    )
  );
}

/**
 * @description 弹框提示
 * @param {message} 提示信息
 * @param {level} 提示级别，'success, warning, error' 四种状态
 */
export function alertMsg(message, level, subtitle = '') {
  new Vue().$bkInfo({
    title: message,
    subTitle: subtitle,
    type: level,
  });
}

/**
 * @description post请求的错误提示
 * @param {message} 错误提示信息
 * @param {level} 错误级别，'success, warning, error'三种状态
 */
export function postMethodWarning(message, level, options = {}) {
  showMsg(message, level, options);
}

/**
 * @description get请求的错误提示
 * @param {message} 错误提示信息
 * @param {code} 错误码
 */
export function getMethodWarning(message, code) {
  showMsg(/string|number|boolean/.test(typeof message) ? `${code} : ${message}` : message, 'error');
}

export function placeholder(data, str) {
  if (typeof str !== 'string') {
    str = '请选择';
  }
  if (data.length > 0) {
    return window.gVue.$t(str);
  }
  return window.gVue.$t('暂无数据');
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
      classNames.forEach(cl => {
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
    classNames.forEach(cl => {
      setClass = setClass.replace(` ${cl} `, ' ');
    });
    const rtrim = /^\s+|\s+$/;
    node.className = setClass.replace(rtrim, '');
  }
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
 * 生成随机ID
 * @param {String} prefix 统一前缀
 * @param {Int} idLength 随机ID长度
 */
export function generateId(prefix = '') {
  let d = new Date().getTime();
  const uuid = 'xxxxxx'.replace(/[xy]/g, c => {
    const r = (d + Math.random() * 16) % 16 | 0;
    d = Math.floor(d / 16);
    return (c === 'x' ? r : (r & 0x7) | 0x8).toString(16);
  });
  return `${prefix}${uuid}`;
}

/**
 * 判断是否为对象，KV结构
 */
export function isObject(o) {
  if (o instanceof Array) {
    return false;
  }
  if (o instanceof Object) {
    return true;
  }
  return false;
}

/**
 *tooltip提示
 * @argument
 * id （string）目标对象的id
 * content （string）提示语内容
 * direction （string） 可选为‘top’，‘bottom’， ‘left’
 * time （number）消失时间，默认为3000ms
 * isInput (boolean) 是否是用户自己输入的文本
 * disapper (boolean) 是否自动消失
 */
export function toolTip(id, content, direction, isInput, disapper, time) {
  if (isInput) {
    if (content.indexOf('<') > -1) {
      try {
        content = content.replace(/</g, '&lt;');
      } catch (error) {
        console.log(error);
      }
    }
    if (content.indexOf('>') > -1) {
      try {
        content = content.replace(/>/g, '&gt;');
      } catch (error) {
        console.log(error);
      }
    }
  }
  if (!direction) {
    direction = 'top';
  }
  if (!time) {
    time = 3000;
  }
  if (disapper === undefined) {
    disapper = true;
  }
  const target = document.getElementById(id);
  const template = document.createElement('div');
  template.innerHTML = content;
  const cssStyle = 'position: absolute;padding: 7px 10px;position: absolute;border-radius:2px;max-width: 500px;word-break: break-all;background: #212232;color: #fff; z-index: 50000;';
  template.style.cssText = 'padding: 7px 10px;position: absolute;border-radius:2px;left: 500px; top:0; opacity: 0';
  template.style.cssText += cssStyle;
  document.getElementsByTagName('body')[0].appendChild(template);
  const timer = setTimeout(e => {
    const span = document.createElement('span');
    const position = target.getBoundingClientRect();
    const leftCenter = position.left + position.width / 2; // 垂直居中
    const left = position.left - template.offsetWidth - 7;
    const top = position.top - template.offsetHeight - 15;
    const right = position.left + position.width + 12;
    const bottom = position.top + position.height + 12;
    const topCenter = position.top + position.height / 2; // 水平居中
    const bodyHeight = document.getElementsByTagName('body')[0].offsetHeight;
    const templateY = position.y + template.offsetHeight;
    if (template.offsetHeight > 0) {
      clearTimeout(timer);
      if (bodyHeight <= templateY) {
        direction = 'top';
      }
      if (direction === 'top') {
        template.style.cssText += `top: ${top}px;transform: translate(-50%, 0%);opacity: 1;` + `left: ${leftCenter}px;`;
        span.style.cssText = 'position: absolute;z-index: 200;top: 100%;left: 50%;border: 5px solid #212232;border-left-color: transparent;' + 'border-bottom-color: transparent;border-right-color: transparent;transform: translate(-50%, 0%)';
      } else if (direction === 'bottom') {
        template.style.cssText += `top: ${bottom}px;transform: translate(-50%, 0%);opacity: 1;left: ${leftCenter}px;`;
        span.style.cssText = 'position: absolute;z-index: 200;top: -10px;left: 50%;border: 5px solid #212232;' + 'border-left-color: transparent;border-top-color: transparent;border-right-color: transparent;transform: translate(-50%, 0%)';
      } else if (direction === 'left') {
        template.style.cssText += `top: ${topCenter}px;transform: translate(0%, -50%);opacity: 1;left: ${left}px;`;
        span.style.cssText = 'position: absolute;z-index: 200;top: 50%;left: 100%;border: 5px solid #212232;border-right-color: transparent;' + 'border-bottom-color: transparent;border-top-color: transparent;transform: translate(0%, -50%)';
      } else if (direction === 'right') {
        template.style.cssText += `top: ${topCenter}px;transform: translate(0%, -50%);opacity: 1;left: ${right}px;`;
        span.style.cssText = 'position: absolute;z-index: 200;top: 50%;left: -10px;border: 5px solid #212232;' + 'border-left-color: transparent;border-bottom-color: transparent;border-top-color: transparent;transform: translate(0%, -50%)';
      }
    }
    $('.def-tooltip').remove();
    template.classList.add('def-tooltip');
    template.appendChild(span);
  });

  if (disapper) {
    setTimeout(() => {
      $('.def-tooltip').remove();
    }, time);
  }
}

/**
 * [getValueFromObj 从数组中找出指定key的值]
 * @arr {Array} - 原数组
 * @fromKey {String} - 已知key
 * @toKey {String} - 要找出value的key值
 * @value {String/Number} - 已知的value
 */
export function getValueFromObj(arr, toKey, fromKey, value) {
  let result;
  for (const item of arr) {
    if (item[fromKey] === value) {
      result = item[toKey];
      break;
    }
  }

  return result;
}

/*
 * 页面自动滚动
 */
export function scrollView(parents, top) {
  parents.animate(
    {
      scrollTop: top,
    },
    300
  );
}

/**
 * 比较工具函数 (传-1表示逆序 不传或1表示顺序排列)
 */
export function compare(propertyName, num) {
  if (!num) {
    num = 1;
  }
  return function (object1, object2) {
    const value1 = object1[propertyName];
    const value2 = object2[propertyName];
    if (value2 < value1) {
      return num;
    }
    if (value2 > value1) {
      return -num;
    }
    return 0;
  };
}

/**
 * 拷贝对象
 * @param obj
 * @returns {any}
 */
export function copyObj(obj) {
  if (obj !== undefined) {
    return JSON.parse(JSON.stringify(obj));
  }
  return obj;
}

/**
 * 深拷贝扩展对象
 * @param target
 * @param { Array } sources
 * @returns {object}
 */
export function deepAssign(target, sources) {
  const sourcesArray = Array.isArray(sources) ? [...sources] : [sources];
  const { length } = sourcesArray;
  if (typeof target !== 'object' && typeof target !== 'function') {
    target = {};
  }
  if (length === 0) {
    target = this;
  }

  sourcesArray.forEach(source => {
    // eslint-disable-next-line no-restricted-syntax
    for (const key in source) {
      if (Object.prototype.hasOwnProperty.call(source, key)) {
        const targetValue = target[key];
        if (Array.isArray(targetValue)) {
          target[key].push(...(source[key] || []));
        } else if (typeof targetValue === 'object') {
          // eslint-disable-next-line no-param-reassign
          target[key] = deepAssign.call(this, targetValue, source[key]);
        } else {
          // eslint-disable-next-line no-param-reassign
          target[key] = source[key];
        }
      }
    }
  });

  return target;
}

export function linkToCC(isShowConfirm = false) {
  function goOut() {
    try {
      window.top.BLUEKING.api.open_app_by_other('bk_cc');
    } catch (err) {
      try {
        window.top.BLUEKING.api.open_app_by_other('bk_cmdb');
      } catch (err2) {
        window.open(window.BKBASE_Global.ccUrl);
      }
    }
  }
  if (!isShowConfirm) {
    goOut();
  } else {
    confirmMsg(
      window.gVue.$t('即将离开此页面'),
      window.gVue.$t('离开此页面_已经填写的数据将会丢失'),
      () => {
        goOut();
      },
      () => { },
      {
        quickClose: false,
        theme: 'warning',
        hasHeader: true,
        closeIcon: false,
      }
    );
  }
}

export function isEnglishChar(str) {
  return /[A-Za-z]+/.test(str);
}

export function isChineseChar(str) {
  return /[\u4E00-\u9FA5]+/.test(str);
}

export function openWith404Validate(url, noUrl) {
  bkRequest
    .request(url, {
      method: 'get',
    })
    .then(response => {
      if (response.status === 404) {
        window.open(noUrl, '_blank');
      } else {
        window.open(url, '_blank');
      }
    })
    .catch(e => {
      window.open(noUrl, '_blank');
    });
}

/**
 *  给定任一节点父节点名称，从该节点回溯至数据结构树的根节点
 *  @param root 构造的树
 *  @param need_find 任一节点父节点
 *  @param stack [root]
 */
export function findParent(root, needFind, stack) {
  // 给定任一节点名称，从该节点回溯至数据结构树的根节点
  if (!('children' in root)) {
    if (needFind === root.value) {
      return true;
    }
    stack.pop();
    return false;
  }
  for (const child of root.children) {
    stack.push(child);
    if (needFind === child.value) {
      return true;
    }
    // 递归
    const _needFind = findParent(child, needFind, stack);
    if (_needFind) {
      return true;
    }
  }
  stack.pop();
  return false;
}

export function debounce(func, wait, immediate) {
  let timeout;
  let result;
  const debounced = function () {
    const context = this;
    const args = arguments;

    if (timeout) {
      clearTimeout(timeout);
    }
    if (immediate) {
      // 如果已经执行过，不再执行
      const callNow = !timeout;
      timeout = setTimeout(() => {
        timeout = null;
      }, wait);
      if (callNow) {
        result = func.apply(context, args);
      }
    } else {
      timeout = setTimeout(() => {
        func.apply(context, args);
      }, wait);
    }
    return result;
  };

  debounced.cancel = () => {
    clearTimeout(timeout);
    timeout = null;
  };

  return debounced;
}

/** 根据key值的数组去重方法 */
export function deduplicate(arr, key) {
  const recordObj = {};
  const deduplicateArray = arr.reduce((cur, next) => {
    recordObj[next.key] ? '' : (recordObj[next.id] = true && cur.push(next));
    return cur;
  }, []);

  return deduplicateArray;
}

const requestAnimFrame = (function () {
  if (typeof window === 'undefined') {
    return function (callback) {
      callback();
    };
  }
  return (
    window.requestAnimationFrame ||
    window.webkitRequestAnimationFrame ||
    window.mozRequestAnimationFrame ||
    window.oRequestAnimationFrame ||
    window.msRequestAnimationFrame ||
    function (callback) {
      return window.setTimeout(callback, 1000 / 60);
    }
  );
})();

export function throttled(fn) {
  return throttle(fn, 60);
}

/**
 * 获取日志的高亮状态
 * @param {String} search 搜索关键字, 支持多字符，以空格分隔
 * @param {Array} logData 日志列表，数组类型
 * @param {Array} logKeys 日志所对应的key值，logData可能包含多个字段，该字段是日志内容字段的key值
 */
export function highLightContent(search, logData, logKeys) {
  const searchKey = search.trim().split(' ');
  return logData.map(log => {
    const copyLog = JSON.parse(JSON.stringify(log)); // 不影响原数据
    logKeys.forEach(key => {
      copyLog[key] = searchKey[0] ? log[key].replace(new RegExp(`(${searchKey.join('|')})`, 'gi'), `<span class="highlight">$1</span>`) : log[key];
    });
    return copyLog;
  });
}

/**
 * scrollTo回掉方法，promise封装，支持scrollTo之后处理相关逻辑
 * @param {DOM} container 滚动的容器,
 * @param {Number} position 滚动的位置
 */
export async function scrollToPosition(container, position) {
  position = Math.round(position);

  if (container.scrollTop === position) {
    return;
  }

  let resolveFn;
  let scrollListener;
  let timeoutId;

  const promise = new Promise(resolve => {
    resolveFn = resolve;
  });

  const finished = () => {
    container.removeEventListener('scroll', scrollListener);
    resolveFn();
  };

  scrollListener = () => {
    clearTimeout(timeoutId);

    // 到达位置或自上一次滚动事件起经过100毫秒后，滚动结束
    if (container.scrollTop === position) {
      finished();
    } else {
      timeoutId = setTimeout(finished, 100);
    }
  };

  container.addEventListener('scroll', scrollListener);

  container.scrollTo({
    top: position,
    behavior: 'smooth',
  });

  return promise;
}
export function fullScreen(ele) {
  if (ele.requestFullscreen) {
    ele.requestFullscreen();
  } else if (ele.mozRequestFullScreen) {
    ele.mozRequestFullScreen();
  } else if (ele.webkitRequestFullscreen) {
    ele.webkitRequestFullscreen();
  } else if (ele.msRequestFullscreen) {
    ele.msRequestFullscreen();
  }
}

export function exitFullscreen(element) {
  if (document.exitFullScreen) {
    document.exitFullScreen();
  } else if (document.mozCancelFullScreen) {
    document.mozCancelFullScreen();
  } else if (document.webkitExitFullscreen) {
    document.webkitExitFullscreen();
  } else if (element.msExitFullscreen) {
    element.msExitFullscreen();
  }
}

export function isFullScreen() {
  return !!(document.fullscreen || document.mozFullScreen || document.webkitIsFullScreen || document.webkitFullScreen || document.msFullScreen);
}

/** 数字格式化
 * kFormatter 格式化成 xxk 的形式，适用于英文
 * abbreviateNumber 根据数字格式化单位为k,m,b,t
 * cnFormatter 格式化为中文单位，万、万亿
 */
export const numberFormat = {
  kFormatter(num) {
    return Math.abs(num) > 999 ? `${Math.sign(num) * (Math.abs(num) / 1000).toFixed(1)}k` : Math.sign(num) * Math.abs(num);
  },
  abbreviateNumber(value) {
    let newValue = value;
    if (value >= 1000) {
      const suffixes = ['', 'k', 'm', 'b', 't'];
      const suffixNum = Math.floor(`${value}`.length / 3);
      let shortValue = '';
      for (let precision = 2; precision >= 1; precision--) {
        shortValue = parseFloat((suffixNum !== 0 ? value / Math.pow(1000, suffixNum) : value).toPrecision(precision));
        const dotLessShortValue = `${shortValue}`.replace(/[^a-zA-Z 0-9]+/g, '');
        if (dotLessShortValue.length <= 2) break;
      }
      if (shortValue % 1 !== 0) shortValue = shortValue.toFixed(1);
      newValue = shortValue + suffixes[suffixNum];
    }
    return newValue;
  },
  cnFormatter(value) {
    let num = '';
    let k;
    let sizes;
    let i;
    k = 10000;
    sizes = ['', '万', '亿', '万亿'];
    if (value < k) {
      num = value;
    } else {
      i = Math.floor(Math.log(value) / Math.log(k));
      num = value / Math.pow(k, i) + sizes[i];
    }

    return num;
  },
};

/*
 * 秒数转时分秒
 * value  秒
 * */
export const formatSeconds = value => {
  let theTime = value;
  if (theTime === 0) {
    return '--';
  }
  if (theTime < 1) {
    theTime = theTime.toFixed(2);
  } else {
    theTime = Math.round(value);
  }
  let theTime1 = 0;
  let theTime2 = 0;
  if (theTime > 60) {
    theTime1 = parseInt(theTime / 60);
    theTime = parseInt(theTime % 60);
  }
  if (theTime1 > 60) {
    theTime2 = parseInt(theTime1 / 60);
    theTime1 = parseInt(theTime1 % 60);
  }
  return `${theTime2}h ${theTime1}m ${theTime}s`;
};
export function clipboardCopy(content, callback) {
  const transfer = document.createElement('textarea');
  document.body.appendChild(transfer);
  transfer.value = content; // 这里表示想要复制的内容
  transfer.focus();
  transfer.select();
  if (document.execCommand('copy')) {
    document.execCommand('copy');
  }
  transfer.blur();
  callback && callback();
  document.body.removeChild(transfer);
}

export function translateUnit(num) {
  const minute = 60;
  const hour = 60 * minute;
  const day = 24 * hour;
  const week = 7 * day;
  const month = 4 * week;
  let result = 0;
  let unit = '';
  if (num < 60) {
    result = num;
    unit = 's';
  } else if (num >= minute && num < hour) {
    result = num / minute;
    unit = 'min';
  } else if (num >= hour && num < day) {
    result = num / hour;
    unit = 'h';
  } else if (num >= day && num < week) {
    result = num / day;
    unit = 'd';
  } else if (num >= week && num < month) {
    result = num / week;
    unit = 'w';
  } else {
    result = num / month;
    unit = 'm';
  }
  return result + unit;
}

/**
 * 读取Blob格式返回数据
 * @param {*} response
 */
export function readBlobResponse(response) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = function () {
      resolve(reader.result);
    };

    reader.onerror = function () {
      reject(reader.error);
    };

    reader.readAsText(response);
  });
}

/**
 * 读取Blob格式返回Json数据
 * @param {*} resp
 */
export function readBlobRespToJson(resp) {
  return readBlobResponse(resp).then(resText => Promise.resolve(JSONBigNumber.parse(resText)));
}

/**
 * Deep merge.
 * @param target
 * @param ...sources
 */
export function mergeDeep(target, ...sources) {
  if (!sources.length) return target;
  const source = sources.shift();

  if (isObject(target) && isObject(source)) {
    for (const key in source) {
      if (isObject(source[key]) && key !== 'component') { // GraphNode配置合并
        if (!target[key] && key) Object.assign(target, { [key]: {} });
        mergeDeep(target[key], source[key]);
      } else {
        Object.assign(target, { [key]: source[key] });
      }
    }
  }

  return mergeDeep(target, ...sources);
}
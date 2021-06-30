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
export function calcChartParams(data) {
  let projCountDtick = null; // 轴间隔
  let applyNodeDtick = null; // 轴间隔
  let maxApplyNode = Math.max(...data); // 轴上最大值
  if (maxApplyNode / 10 >= 2) {
    maxApplyNode = (maxApplyNode / 10 + 1) * 10;
    projCountDtick = 15;
  } else if (maxApplyNode / 5 >= 2) {
    maxApplyNode = (maxApplyNode / 5 + 1) * 5;
    projCountDtick = 5;
  } else if (maxApplyNode / 2 >= 2) {
    maxApplyNode = (maxApplyNode / 2 + 1) * 2;
    projCountDtick = 2;
  } else {
    maxApplyNode = maxApplyNode + 1;
    projCountDtick = 1;
  }
  // if (maxApplyNode > 0) {
  const intervals = maxApplyNode / projCountDtick; // 间隔数量
  maxApplyNode = Math.ceil((maxApplyNode * (intervals + 1.5)) / intervals);
  // 为了图的最大值不被legend遮挡，Y轴最大值需要手动加1.5/intervals的高度
  applyNodeDtick = maxApplyNode / intervals; // 重新计算每个间隔的高度
  if (`${applyNodeDtick}.0` !== applyNodeDtick) {
    // 如果间隔是浮点数，就取整，再重新计算最大高度
    applyNodeDtick = Math.ceil(applyNodeDtick);
    maxApplyNode = applyNodeDtick * intervals;
  }
  return {
    maxApplyNode,
    applyNodeDtick,
  };
  // }
}

export function getExplorer() {
  const explorer = window.navigator.userAgent;

  if (explorer.indexOf('Safari') > -1 && explorer.indexOf('Chrome') === -1) {
    return 'Safari';
  }
  if (explorer.indexOf('Chrome') > -1 && explorer.indexOf('Safari') > -1) {
    return 'Chrome';
  }
}

export function handlerPercentText(num, digit = 6) {
  const numStr = num.toString();
  const index = numStr.indexOf('.');
  let percent = Number(numStr.slice(0, index + digit)) * 100;
  const strPercent = percent.toString();
  if (strPercent.length > 4) {
    // js浮点数计算精度问题
    const index = strPercent.indexOf('.');
    percent = strPercent.slice(0, index + 3); // 取小数点后两位数字
  }
  const percentIndex = strPercent.indexOf('.'); // 整数位大于0并且不等于1时，保留小数点后2位数字
  if (strPercent.slice(0, percentIndex) > 0 && num !== 1) {
    percent = strPercent.slice(0, percentIndex) + strPercent.slice(percentIndex, percentIndex + 3);
  }
  // 百分比如果小于0.01的话，默认赋值为0.01
  if (percent < 0.01) {
    percent = 0.01;
  }
  return Number(percent);
}

export function isNumber(value) {
  return !Number.isNaN(Number(value));
}

export function getAxixParams(data) {
  const maxXaxis = data.length ? Math.max(...data) : 0;

  let dtick = maxXaxis > 1 ? calcChartParams(data).applyNodeDtick : 1;
  // 轴上最大值大于500
  if (calcChartParams(data).maxApplyNode > 500) {
    dtick = null;
  }

  // 默认最大值为 10
  const rangeMax = calcChartParams(data).maxApplyNode || 10;
  return {
    dtick,
    range: [0, rangeMax],
  };
}
export function tranNumber(num, point = 2) {
  // 1000 = 1k 1000000 = 1m 10亿=1bn
  // 将数字转换为字符串,然后通过split方法用.分隔,取到第0个
  const numStr = num.toString().split('.')[0];
  num = Number(num);
  const len = numStr.length;
  if (len <= 3) {
    return Number(num.toFixed(point));
  }
  if (len > 3 && len <= 6) {
    return `${(num / 1000).toFixed(point)}K`;
  }
  if (len > 6 && len <= 8) {
    return `${(num / 1000000).toFixed(point)}Mil`;
  }
  return `${(num / 100000000).toFixed(point)}Bil`;
}

/**
 * @param { string} 12:22
 */
export function getNowTime(time) {
  const hour = time.split(':')[0];
  let minutes = time.split(':')[1];
  let dayTime = '';
  if (hour < 12) {
    dayTime = window.$t('上午');
  } else if (hour === 12) {
    dayTime = window.$t('中午');
  } else {
    dayTime = window.$t('下午');
  }
  minutes = minutes.toString().length === 1 ? `0${minutes}` : minutes;
  return `${dayTime} ${hour}:${minutes}`;
}

/**
 * 高级搜索参数校验
 */
export function checkParams(val) {
  if (
    val.length >= 3
        || (val.match(/^[\u4e00-\u9fa5]/) || []).length >= 3
        || (val.match(/^[a-zA-Z]/) || []).length >= 3
        || val === ''
  ) {
    return true;
  }
  return false;
}

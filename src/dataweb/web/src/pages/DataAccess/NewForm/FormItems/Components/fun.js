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

import { bkRequest } from '@/common/js/ajax';
import { postMethodWarning } from '@/common/js/util.js';

function request(url, options = {}) {
  return bkRequest
    .httpRequest(url, options)
    .then((res) => {
      if (res.result) {
        return Promise.resolve(res);
      }
      postMethodWarning(res.message, 'error');
      return Promise.reject(res);
    })
    .catch((e) => {
      postMethodWarning(e, 'error');
    });
}

/** ODM 根据bk_biz_id获取数据结构列表 */
function getDataStructByBizId(bizId) {
  return bkRequest.httpRequest('dataAccess/getDataStruct', {
    params: {
      bk_biz_id: bizId,
    },
  });
}

/** TGLOG 根据bk_biz_id获取数据结构详情 */
function getDataStructDetailByBizId(bizId, odm) {
  return bkRequest.httpRequest('dataAccess/getDataStructDetail', {
    params: {
      bk_biz_id: bizId,
    },
    query: {
      name: odm,
    },
  });
}

/** 获取时间格式 */
function getDateFormat() {
  return bkRequest.httpRequest('dataAccess/getTimeFormat').then((res) => {
    if (res.result) {
      const data = res.data.map((d) => {
        const formatArray = d.time_format_alias
          .split(' ')
          .map(item => item.replace(/[y{2,4}d{2}]/g, match => match.toLocaleUpperCase()));
        // let formatArray = d.time_format_alias.split(' ')
        // formatArray[0] = formatArray[0].toLocaleUpperCase()
        // if (formatArray.length === 2) {
        //     formatArray[1] = formatArray[1].toLocaleLowerCase()
        // }
        const formatStr = formatArray.join(' ');
        return Object.assign({}, d, {
          id: d.time_format_name,
          name: formatStr,
          time_format: d.time_format_name,
        });
      });

      Object.assign(res, { data });
    }

    return res;
  });
}

/** 灯塔模块：获取事件名称 */
function getEventAppKeyBizId(bizId) {
  return bkRequest.httpRequest('dataAccess/getEventList', {
    query: {
      bk_biz_id: bizId,
    },
  });
}

/** TQOS 获取所有ID列表 */
function getTQOSLists(bizId) {
  return bkRequest.httpRequest('dataAccess/getTSODList', {
    query: {
      bk_biz_id: bizId,
    },
  });
}

/** 获取分隔符列表 */
function getDelimiter() {
  return request('dataAccess/getDelimiterList');
}

/** 获取区域标签 */
function getGeogTags() {
  return request('meta/getAvailableGeog');
}

export {
  getDataStructByBizId,
  getDateFormat,
  getDataStructDetailByBizId,
  getEventAppKeyBizId,
  getTQOSLists,
  getDelimiter,
  getGeogTags,
};

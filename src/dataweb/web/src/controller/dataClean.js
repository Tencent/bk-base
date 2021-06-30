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

/* eslint-disable no-underscore-dangle */
import * as moment from 'moment';
class DataClean {
  /** 格式化清洗数据列表
   * 格式化时间格式
   */
  formatDataCleanList(data) {
    const getFormatData = () => data.map((d) => {
      const obj = {
        created_at: this.__formatTimeString(d.created_at),
        updated_at: this.__formatTimeString(d.updated_at),
      };
      return Object.assign(d, obj);
    },
      // Object.assign(d, {
      //     created_at: this.__formatTimeString(d.created_at),
      //     updated_at: this.__formatTimeString(d.updated_at),
      // })
    );
    return (data && getFormatData()) || [];
  }

  __formatTimeString(timeStr) {
    try {
      return (timeStr && moment(timeStr).format('YYYY-MM-DD HH:mm:ss')) || timeStr;
    } catch (e) {
      return timeStr;
    }
  }
}

export default DataClean;

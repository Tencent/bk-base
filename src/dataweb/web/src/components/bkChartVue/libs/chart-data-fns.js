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

import {
  parse,
  parseISO,
  toDate,
  isValid,
  format,
  startOfSecond,
  startOfMinute,
  startOfHour,
  startOfDay,
  startOfWeek,
  startOfMonth,
  startOfQuarter,
  startOfYear,
  addMilliseconds,
  addSeconds,
  addMinutes,
  addHours,
  addDays,
  addWeeks,
  addMonths,
  addQuarters,
  addYears,
  differenceInMilliseconds,
  differenceInSeconds,
  differenceInMinutes,
  differenceInHours,
  differenceInDays,
  differenceInWeeks,
  differenceInMonths,
  differenceInQuarters,
  differenceInYears,
  endOfSecond,
  endOfMinute,
  endOfHour,
  endOfDay,
  endOfWeek,
  endOfMonth,
  endOfQuarter,
  endOfYear,
} from 'date-fns';

const FORMATS = {
  datetime: 'MMM d, yyyy, HH:mm:ss',
  // millisecond: 'HH:mm:ss',
  second: 'HH:mm:ss',
  minute: 'HH:mm',
  hour: 'HH:mm',
  day: 'd',
  week: 'PP',
  month: 'MMM yyyy',
  quarter: 'qqq - yyyy',
  year: 'yyyy',
};

export default function dataFns(Chart) {
  // eslint-disable-next-line no-underscore-dangle
  Chart._adapters._date.override({
    _id: 'date-fns', // DEBUG

    formats() {
      return FORMATS;
    },

    parse(value, fmt) {
      let target = value;
      if (target === null || typeof target === 'undefined') {
        return null;
      }
      const type = typeof value;
      if (type === 'number' || value instanceof Date) {
        target = toDate(value);
      } else if (type === 'string') {
        if (typeof fmt === 'string') {
          target = parse(value, fmt, new Date(), this.options);
        } else {
          target = parseISO(value, this.options);
        }
      }
      return isValid(target) ? target.getTime() : null;
    },

    format(time, fmt, opts = {}) {
      let target = time;
      let targetFmt = fmt;
      const { ticks = {} } = opts;
      if (typeof ticks.formatTime === 'function') {
        target = ticks.formatTime.call(this, time);
      }

      if (fmt === undefined || fmt === 'undefined' || fmt === 'null' || fmt === null) {
        console.log('fmt is null or undefined');
        targetFmt = 'yyyy-MM-dd h:mm:ss.SSS a';
      }
      return format(target, targetFmt, this.options);
    },

    add(time, amount, unit) {
      switch (unit) {
        case 'millisecond':
          return addMilliseconds(time, amount);
        case 'second':
          return addSeconds(time, amount);
        case 'minute':
          return addMinutes(time, amount);
        case 'hour':
          return addHours(time, amount);
        case 'day':
          return addDays(time, amount);
        case 'week':
          return addWeeks(time, amount);
        case 'month':
          return addMonths(time, amount);
        case 'quarter':
          return addQuarters(time, amount);
        case 'year':
          return addYears(time, amount);
        default:
          return time;
      }
    },

    diff(max, min, unit) {
      switch (unit) {
        case 'millisecond':
          return differenceInMilliseconds(max, min);
        case 'second':
          return differenceInSeconds(max, min);
        case 'minute':
          return differenceInMinutes(max, min);
        case 'hour':
          return differenceInHours(max, min);
        case 'day':
          return differenceInDays(max, min);
        case 'week':
          return differenceInWeeks(max, min);
        case 'month':
          return differenceInMonths(max, min);
        case 'quarter':
          return differenceInQuarters(max, min);
        case 'year':
          return differenceInYears(max, min);
        default:
          return 0;
      }
    },

    startOf(time, unit, weekday) {
      switch (unit) {
        case 'second':
          return startOfSecond(time);
        case 'minute':
          return startOfMinute(time);
        case 'hour':
          return startOfHour(time);
        case 'day':
          return startOfDay(time);
        case 'week':
          return startOfWeek(time);
        case 'isoWeek':
          return startOfWeek(time, {
            weekStartsOn: +weekday,
          });
        case 'month':
          return startOfMonth(time);
        case 'quarter':
          return startOfQuarter(time);
        case 'year':
          return startOfYear(time);
        default:
          return time;
      }
    },

    endOf(time, unit) {
      switch (unit) {
        case 'second':
          return endOfSecond(time);
        case 'minute':
          return endOfMinute(time);
        case 'hour':
          return endOfHour(time);
        case 'day':
          return endOfDay(time);
        case 'week':
          return endOfWeek(time);
        case 'month':
          return endOfMonth(time);
        case 'quarter':
          return endOfQuarter(time);
        case 'year':
          return endOfYear(time);
        default:
          return time;
      }
    },
  });
}

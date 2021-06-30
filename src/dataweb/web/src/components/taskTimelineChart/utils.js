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
import { timeHour, timeDay, timeMonday, timeMonth, timeYear } from 'd3-time';

const timeUnitMap = {
  hour: timeHour,
  day: timeDay,
  week: timeMonday,
  month: timeMonth,
  year: timeYear,
};

/** 每个周期所展示的条目数 */
export const count = 5;

export const removeAxisPath = selection => {
  selection.select('path.domain').remove();
}

export const addAxis = (context, offset) => {
  return context.append('g').attr('transform', `translate(0, ${offset})`);
}

export const setLineColor = (selection, color) => {
  selection.selectAll('.tick line').attr('stroke', color);
}

export const getVisableTick = (unit, domain) => {
  const timeInterval = timeUnitMap[unit];
  let time = timeInterval.floor(domain[0]);
  const end = timeInterval.floor(domain[1]);
  const range = [time];
  while (time < end) {
    time = timeInterval.offset(time, 1);
    range.push(time);
  }

  return range;
}

/** 获取5个周期内的启动时间 */
export const getAllStartTime = (startTime, schedulingPeriod) => {
  const times = [new Date(startTime)];
  const cycle = timeUnitMap[schedulingPeriod.schedule_period];
  let len = 1;
  do {
    const lastTime = times[len - 1];
    const nextTime = cycle.offset(lastTime, schedulingPeriod.count_freq);
    times.push(nextTime);
    len = times.length;
  } while (len < count);
  return times;
}

export const getAccChartData = (data, scheduling) => {
  let accCalculateParams = {
    start: new Date(scheduling.start_time),
    cycleCount: 0, // 启动时间的调度周期个数
  };
  const accChartDates = calculateOneAccCycleData(data, accCalculateParams, scheduling);
  return accChartDates;
}

/** 计算距离指定时间（开始时间）最近的累加范围开始时间 */
export const getAccInitTime = (accParams, data, range) => {
  const startTime = accParams.start;
  const count = accParams.cycleCount;
  const { window_size, window_size_unit, window_offset_unit, window_offset, window_start_offset_unit, window_start_offset } = data;
  const accumulate_start_time = new Date(data.accumulate_start_time);
  const windowSizeCycle = timeUnitMap[window_size_unit];
  const windowOffsetUnit = timeUnitMap[window_offset_unit];
  const windowStartUnit = timeUnitMap[window_start_offset_unit];

  let initTime = accumulate_start_time;

  /** 如果累加基准值比较大，向前减去累加范围的步长，直到第一次小于开始时间（考虑偏移影响） */
  if (startTime.getTime() <= accumulate_start_time.getTime()) {
    do {
      initTime = windowSizeCycle.offset(initTime, -window_size);
    } while (windowOffsetUnit(initTime, window_offset) >= startTime);
    return initTime;
  }

  /** 如果累加基准值比较小，向后加上累加范围的步长，在满足窗口偏移条件下的时间计委initTime */
  while (windowSizeCycle.offset(initTime, window_size) < startTime) {
    initTime = windowSizeCycle.offset(initTime, window_size);
  }
  if (windowOffsetUnit.offset(initTime, window_offset) < startTime) {
    return initTime;
  }

  /** 不满足窗口偏移的时间，将计入上一个调度周期的累加，返回null，进行下一个调度周期的计算 */
  const curAccStartTime = windowSizeCycle.offset(initTime, -window_size);
  range.push({
    start: windowStartUnit.offset(curAccStartTime, window_start_offset),
    end: initTime,
    count: count - 1,
  });

  return null;
}

export const calculateOneAccCycleData = (data, accCalcParams, scheduling) => {
  const { window_start_offset, window_start_offset_unit, window_end_offset, window_end_offset_unit, window_offset_unit, window_offset } = data;
  const windowStartUnit = timeUnitMap[window_start_offset_unit];
  const windowEndUnit = timeUnitMap[window_end_offset_unit];
  const schedulingCycle = timeUnitMap[scheduling.schedule_period];
  const windowOffsetUnit = timeUnitMap[window_offset_unit];
  const range = [];

  let accStartTime;

  /** 在周期Count下，进行相关时间的计算 */
  while (accCalcParams.cycleCount < count) {
    accStartTime = getAccInitTime(accCalcParams, data, range); // 根据当前调度时间，获取距离最近的开始累加时间
    if (accStartTime !== null && windowStartUnit.offset(accStartTime, window_start_offset) < accCalcParams.start && accCalcParams.start <= windowEndUnit.offset(accStartTime, window_end_offset)) {
      // 开始累加时间需要满足 起始偏移 && 结束偏移 约束
      while (accCalcParams.start <= windowEndUnit.offset(accStartTime, window_end_offset) && accCalcParams.cycleCount < count) {
        // 在窗口长度内，进行当前周期累加窗口的计算
        range.push({
          start: windowStartUnit.offset(accStartTime, window_start_offset),
          end: windowOffsetUnit.offset(accCalcParams.start, -window_offset),
          count: accCalcParams.cycleCount,
        });
        accCalcParams.start = schedulingCycle.offset(accCalcParams.start, scheduling.count_freq);
        accCalcParams.cycleCount++;
      }
    } else {
      accCalcParams.start = schedulingCycle.offset(accCalcParams.start, scheduling.count_freq);
      accCalcParams.cycleCount++;
    }
  }
  return range;
}

export const getChartDataExtend = (data, schedulingPeriod) => {
  return data.window_type === 'accumulate' ? getAccChartData(data, schedulingPeriod) : getAllSliderChartData(data, schedulingPeriod);
}

export const getAllSliderChartData = (data, schedulingPeriod) => {
  const { window_offset, window_offset_unit, window_size_unit, window_size } = data;
  const count = 5;
  const schedulingUnit = timeUnitMap[schedulingPeriod.schedule_period];
  const windowUnit = timeUnitMap[window_size_unit];
  const offsetUnit = timeUnitMap[window_offset_unit];

  let rectXEnd = offsetUnit.offset(new Date(schedulingPeriod.start_time), -window_offset);
  let rectXStart = windowUnit.offset(rectXEnd, -window_size);
  const range = [
    {
      start: rectXStart,
      end: rectXEnd,
    },
  ];

  console.log('first', rectXEnd, rectXStart, schedulingPeriod.start_time);

  for (let i = 0; i < count - 1; i++) {
    rectXEnd = schedulingUnit.offset(rectXEnd, schedulingPeriod.count_freq);
    rectXStart = windowUnit.offset(rectXEnd, -window_size);
    range.push({
      start: rectXStart,
      end: rectXEnd,
    });
    continue;
  }

  return range;
}

/** 将100以内数字转为中文，1 => 一， 12 => 十二 */
export const toChNum = num => {
  const cn = ['零', '一', '二', '三', '四', '五', '六', '七', '八', '九'];
  if (num < 10) {
    return cn[num];
  }
  const tensPlace = parseInt((num % 100) / 10) === 1 ? '' : cn[parseInt((num % 100) / 10)];
  const onesPlace = num % 10 === 0 ? '' : cn[num % 10];
  return `${tensPlace}十${onesPlace}`;
}

/** 获取无限循环模式下，可视区滚动窗口数据 */
export const getScrollChartData = (domain, startTime, offset, schedulingPeriod) => {
  const { val, unit } = offset;
  const limit = domain[0];
  let lastStartTime = startTime;
  while (lastStartTime < domain[1]) {
    lastStartTime = timeUnitMap[schedulingPeriod.unit].offset(lastStartTime, schedulingPeriod.count_freq);
  }

  let start = timeUnitMap[unit].offset(lastStartTime, -val);
  const range = [start];
  while (start > limit) {
    start = timeUnitMap[schedulingPeriod.unit].offset(start, -schedulingPeriod.count_freq);
    range.push(start);
  }

  return range;
}

/** 无限循环模式下，获取可见区域内的启动时间 */
export const getVisableStartTime = (domain, time, schedulingPeriod) => {
  const cycle = timeUnitMap[schedulingPeriod.schedule_period];
  let startTime = time;
  let endTime = cycle.offset(startTime, schedulingPeriod.count_freq);
  const range = [startTime];
  while (startTime > domain[0]) {
    startTime = cycle.offset(startTime, -schedulingPeriod.count_freq);
    range.push(startTime);
  }

  while (endTime < domain[1]) {
    range.push(endTime);
    endTime = cycle.offset(endTime, schedulingPeriod.count_freq);
  }

  return range;
}


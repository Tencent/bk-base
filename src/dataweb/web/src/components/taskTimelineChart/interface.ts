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

export interface IConfig {
  layout: Layout;
  container: string;
  schedulingPeriod?: SchedulingPeriod;
  domain: Date[];
  rectHeight?: number;
}

export interface IPreviewConfig extends IConfig {
  schedulingPeriod: SchedulingPeriod,
  data: Data,
  rectHeight: number,
}

export interface Layout {
  height: number;
  width: number;
  top: number;
  left: number;
  right: number;
  bottom: number;
  firstAxisTop: number;
  secondAxisTop: number;
}
export interface SchedulingPeriod {
  schedule_period: string;
  count_freq: string;
  start_time: string;
}

export interface Data {
  window_type: string;
  window_end_offset: string;
  window_size: string;
  window_start_offset_unit: string;
  window_size_unit: string;
  window_offset: string;
  window_end_offset_unit: string;
  accumulate_start_time: string;
  window_offset_unit: string;
  dependency_rule: string;
  result_table_id: string;
  window_start_offset: string;
  color: string;
}


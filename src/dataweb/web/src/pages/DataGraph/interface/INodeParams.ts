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

export interface IChartPanel {
  isInit: boolean;
  render: (element: string, data: WindowInfoEntity, ScheduleConfig: ScheduleConfig) => void;
  updateChart: (data: WindowInfoEntity, ScheduleConfig?: ScheduleConfig) => void;
}
export interface INodeParams {
  config: Config;
  frontend_info?: FrontendInfo;
  from_links?: FromLinksEntity[] | null;
}
export interface Config {
  outputs?: OutputsEntity[];
  inputs?: InputsEntity[] | null;
  name: string;
  bk_biz_id: number | string;
  dedicated_config: DedicatedConfig;
  window_info?: WindowInfoEntity[] | null;
  node_type?: string;
}
export interface OutputsEntity {
  bk_biz_id: number;
  fields?: null[] | null;
  table_name: string;
  table_custom_name: string;
  validate: Validate;
  output_name: string;
}
export interface Validate {
  table_name: TableNameOrOutputNameOrFieldConfig;
  output_name: TableNameOrOutputNameOrFieldConfig;
  field_config: TableNameOrOutputNameOrFieldConfig;
}
export interface TableNameOrOutputNameOrFieldConfig {
  status: boolean;
  errorMsg: string;
}
export interface InputsEntity {
  id: number;
  from_result_table_ids?: string[] | null;
}
export interface DedicatedConfig {
  output_config: OutputConfig;
  sql: string;
  recovery_config: RecoveryConfig;
  self_dependence: SelfDependence;
  schedule_config: ScheduleConfig;
}
export interface OutputConfig {
  enable_customize_output: boolean;
  output_baseline_location: string;
  output_baseline: string;
  output_offset_unit: string;
  output_offset: string;
  output_baseline_type?: string;
}
export interface RecoveryConfig {
  recovery_enable: boolean;
  recovery_times: string;
  recovery_interval: string;
}
export interface SelfDependence {
  self_dependency: boolean;
  self_dependency_config: SelfDependencyConfig;
}
export interface SelfDependencyConfig {
  fields?: null[] | null;
  dependency_rule: string;
}
export interface ScheduleConfig {
  schedule_period: string;
  count_freq: string;
  start_time: string;
}
export interface WindowInfoEntity {
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
  color?: string;
}
export interface FrontendInfo {
  y: number;
  x: number;
}
export interface FromLinksEntity {
  to_node_id: number;
  from_node_id: number;
  source: SourceOrTarget;
  target: SourceOrTarget;
}
export interface SourceOrTarget {
  node_id: number;
  id: string;
  arrow: string;
  x: number;
  y: number;
}

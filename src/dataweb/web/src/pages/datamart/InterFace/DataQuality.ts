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
 * 数据质量相关接口
 * Data Quality Interface Define
 * Author: jiejieli
 */

// 信息总览质量事件数据
export interface IQualityInfoOverview {
  errors: any;
  message: string;
  code: string;
  data: IOverviewData;
  result: boolean;
}

export interface IOverviewData {
  eventNotifiedCount: number;
  eventCount: number;
}

// 信息总览审核规则数据
export interface IQualityAuditRule {
  errors: any;
  message: string;
  code: string;
  data: IAuditRuleData;
  result: boolean;
}

// 规则数据
export interface IAuditRuleData {
  runningRuleCount: number;
  ruleCount: number;
  waitingRuleCount: number;
  failedRuleCount: number;
}

// 审核规则表格数据
export interface IQualityAuditRuleTable {
  errors: any;
  message: string;
  code: string;
  data: Array<IAuditRuleTableData>;
  result: boolean;
}

// 规则表格数据
export interface IAuditRuleTableData {
  updatedBy: string;
  ruleTemplate: string;
  updatedAt: string;
  ruleName: string;
  event: IEvent;
  ruleConfig: IRuleConfig;
  auditTaskStatus: string;
  createdAt: string;
  createdBy: string;
  ruleConfigAlias: string;
  ruleId: number;
}

// 规则配置
export interface IRuleConfig {
  input: Array<IInput>;
  rule: IRule;
  output: IOutput;
}

// 规则配置输出接口
export interface IOutput {
  eventSubType: string;
  eventType: string;
  tags: Array<string>;
  eventId: string;
  eventName: string;
  eventAlias: string;
  eventCurrency: number;
}

export interface IRule {
  function: IFunction;
}

// 规则函数接口
export interface IFunction {
  functionType: string;
  functionParams: Array<IFunctionParams>;
  functionName: string;
}

// 函数参数接口
export interface IFunctionParams {
  eventId: string;
  paramType: string;
}

// 函数输入接口
export interface IInput {
  eventId: string;
  inputType: string;
}

export interface IEvent {
  eventName: string;
  eventAlias: string;
}

// 质量事件统计信息
export interface IQualityEventInfo {
  errors: any;
  message: string;
  code: string;
  data: IEventData;
  result: boolean;
}

// Event Data Interface
export interface IEventData {
  trend: ITrend;
  totalEvents: number;
  summary: ISummary;
}

export interface ISummary {
  invalidData: IInvalidData;
  customEvent1: ICustomEvent1;
  dataInterrupt: IDataInterrupt;
  dataLoss: IDataLoss;
}

// Trend Data Interface
export interface ITrend {
  invalidData: IInvalidData;
  customEvent1: ICustomEvent1;
  dataInterrupt: IDataInterrupt;
  dataLoss: IDataLoss;
}

export interface IDataLoss {
  count: Array<number>;
  alias: string;
  times: Array<string>;
}

export interface IDataInterrupt {
  count: Array<number>;
  alias: string;
  times: Array<string>;
}

export interface ICustomEvent1 {
  count: Array<number>;
  alias: string;
  times: Array<string>;
}

export interface IInvalidData {
  count: Array<number>;
  alias: string;
  times: Array<string>;
}

// 质量事件表格信息
export interface IQualityEventTableData {
  errors: any;
  message: string;
  code: string;
  data: Array<IData>;
  result: boolean;
}

// 质量事件表格的数据接口
export interface IData {
  timer: any;
  isExpand: boolean;
  notifyWays: Array<string>;
  inEffect: boolean;
  eventSubType: string;
  description: string;
  receivers: string;
  eventId: string;
  sensitivity: string;
  eventPolarity: string;
  rule: IEventRule;
  updatedAt: string;
  createdBy: string;
  eventAlias: string;
  updatedBy: string;
  eventCurrency: number;
  createdAt: string;
  eventInstances: Array<IEventInstances>;
  eventType: string;
  expandTable: Array<IEventInstances>;
}

// Event Instances Interface
export interface IEventInstances {
  eventDetail: string;
  eventStatusAlias: string;
  eventTime: string;
}

export interface IEventRule {
  ruleName: string;
  ruleConfigAlias: string;
  ruleId: string;
}

// 告警通知方式
export interface INotifyWayList {
  errors: any;
  message: string;
  code: string;
  data: Array<INotifyWayData>;
  result: boolean;
}

export interface INotifyWayData {
  description: string;
  notifyWayAlias: string;
  active: boolean;
  notifyWay: string;
  notifyWayName: string;
  icon: string;
}

// 规则配置函数列表
export interface IQualityRuleFunc {
  errors: any;
  message: string;
  code: string;
  data: Array<IRuleFuncData>;
  result: boolean;
}

export interface IRuleFuncData {
  functionAlias: string;
  updatedBy: any;
  createdAt: string;
  description: string;
  updatedAt: any;
  createdBy: string;
  functionType: string;
  functionConfigs: any;
  functionLogic: any;
  active: boolean;
  id: number;
  functionName: string;
}

// 规则配置指标
export interface IQualityRuleIndex {
  errors: any;
  message: string;
  code: string;
  data: Array<IRuleIndexData>;
  result: boolean;
}

export interface IRuleIndexData {
  id: number;
  metricName: string;
  metricAlias: string;
  metricType: string;
  metricUnit: string;
  sensitivity: string;
  metricOrigin: string;
  metricConfig: string;
  active: boolean;
  createdBy: string;
  createdAt: string;
  updatedBy: any;
  updatedAt: any;
  description: string;
}

// 规则配置模板
export interface IQualityRuleTemplate {
  errors: any;
  message: string;
  code: string;
  data: Array<IRuleTemplateData>;
  result: boolean;
}

export interface IRuleTemplateData {
  updatedBy: any;
  templateName: string;
  createdAt: string;
  templateAlias: string;
  createdBy: string;
  updatedAt: any;
  templateConfig: ITemplateConfig;
  active: boolean;
  id: number;
  description: string;
}

export interface ITemplateConfig {
  rule: ITemplateRule;
}

export interface ITemplateRule {
  function: ITemplateFunction;
}

export interface ITemplateFunction {
  functionType: string;
  functionParams: Array<ITemplateFunctionParams>;
  functionName: string;
}

export interface ITemplateFunctionParams {
  paramType: string;
  metricName: string;
  metricField: string;
}

// 新建规则
export interface IQualityCreateRule {
  errors: any;
  message: string;
  code: string;
  data: Array<ICreateRuleData>;
  result: boolean;
}

export interface ICreateRuleData {
  updatedBy: string;
  ruleTemplate: string;
  updatedAt: string;
  ruleName: string;
  event: ICreateEvent;
  ruleConfig: ICreateRuleConfig;
  auditTaskStatus: string;
  createdAt: string;
  createdBy: string;
  ruleConfigAlias: string;
  ruleId: number;
}

export interface ICreateRuleConfig {
  input: Array<ICreateInput>;
  rule: ICreateRule;
  output: ICreateOutput;
}

export interface ICreateOutput {
  eventSubType: string;
  eventType: string;
  tags: Array<string>;
  eventId: string;
  eventName: string;
  eventAlias: string;
  eventCurrency: number;
}

export interface ICreateRule {
  function: ICreateFunction;
}

export interface ICreateFunction {
  functionType: string;
  functionParams: Array<ICreateFunctionParams>;
  functionName: string;
}

export interface ICreateFunctionParams {
  eventId: string;
  paramType: string;
}

export interface ICreateInput {
  eventId: string;
  inputType: string;
}

export interface ICreateEvent {
  eventName: string;
  eventAlias: string;
}

// 结果表字段
export interface IRtFieldsList {
  message: string;
  code: string;
  data: Array<IRtFieldsData>;
  result: boolean;
}

export interface IRtFieldsData {
  fieldType: string;
  fieldAlias: string;
  description: any;
  roles: IRoles;
  createdAt: string;
  isDimension: boolean;
  createdBy: string;
  updatedAt: string;
  origins: any;
  fieldName: string;
  id: number;
  fieldIndex: number;
  updatedBy: string;
}

export interface IRoles {
  eventTime: boolean;
}

// 新建规则配置字段
export interface IRuleConfigFields {
  data_set_id: string;
  rule_name: string;
  rule_description: string;
  rule_template_id: string;
  rule_config: Array<ICreateRuleConfigField>;
  event_name: string;
  event_alias: string;
  event_description: string;
  event_type: string;
  event_sub_type: string;
  event_polarity: string;
  event_currency: string;
  sensitivity: string;
  event_detail_template: string;
  notify_ways: Array<any>;
  receivers: string[];
  convergence_config: IConvergenceConfig;
  rule_id: string;
  audit_task_status: string;
  rule_config_alias: string;
}

export interface IConvergenceConfig {
  duration: number;
  alert_threshold: number;
  mask_time: number;
}

export interface ICreateRuleConfigField {
  metric: IMetric;
  function: string;
  constant: IConstant;
  operation: string;
}

export interface IConstant {
  constant_type: string;
  constant_value: number;
}

export interface IMetric {
  metric_type: string;
  metric_name: string;
}

// 质量事件查看指标接口
export interface IQualityIndex {
  message: string;
  code: string;
  data: IQualityIndexData;
  result: boolean;
}

export interface IQualityIndexData {
  F1NullRate: IF1NullRate;
}

export interface IF1NullRate {
  metricField: string;
  lastDay: Array<ILastDay>;
  lastWeek: Array<ILastWeek>;
  metricType: string;
  metricAlias: string;
  today: Array<IToday>;
  metricName: string;
}

export interface IToday {
  nullRate: number;
  time: number;
}

export interface ILastWeek {
  nullRate: number;
  time: number;
}

export interface ILastDay {
  nullRate: number;
  time: number;
}

// 数据审核事件模板变量列表
export interface IAuditEventTemp {
  errors: any;
  message: string;
  code: string;
  data: Array<IAuditEventTempData>;
  result: boolean;
}

export interface IAuditEventTempData {
  id: number;
  varName: string;
  varAlias: string;
  varExample: string;
  active: boolean;
  createdBy: string;
  createdAt: string;
  updatedBy: any;
  updatedAt: any;
  description: string;
}

// 数据质量事件子类型字典表
export interface IQualityEventTypes {
  errors: any;
  message: string;
  code: string;
  data: Array<IQualityEventTypesData>;
  result: boolean;
}

export interface IQualityEventTypesData {
  eventTypeName: string;
  id: number;
  eventTypeAlias: string;
  seqIndex: number;
  parentTypeName: string;
  active: boolean;
  createdBy: string;
  createdAt: string;
  updatedBy: any;
  updatedAt: any;
  description: string;
}

// 查询修正规则配置
export interface IQualityFixRuleConfig {
  errors: any;
  message: string;
  code: string;
  result: boolean;
  data: Array<IQualityFixRuleConfigData>;
}

export interface IQualityFixRuleConfigData {
  correctConfigId: number;
  dataSetId: string;
  bkBizId: number;
  flowId: number;
  nodeId: number;
  sourceSql: string;
  correctSql: string;
  generateType: string;
  correctConfigs: Array<ICorrectConfigs>;
  createdBy: string;
  createdAt: string;
  updatedBy: string;
  updatedAt: string;
  description: string;
}

export interface ICorrectConfigs {
  correctConfigItemId: number;
  field: string;
  correctConfigDetail: ICorrectConfigDetail;
  correctConfigAlias: string;
  createdBy: string;
  createdAt: string;
  updatedBy: string;
  updatedAt: string;
}

export interface ICorrectConfigDetail {
  rules: Array<IRules>;
  output: IQualityFixRuleConfigOutput;
}

export interface IQualityFixRuleConfigOutput {
  generateNewField: boolean;
  newField: string;
}

export interface IRules {
  condition: ICondition;
  filling: IFilling;
}

export interface IFilling {
  fillingName: string;
  fillingType: string;
  fillingValueType: string;
  fillingValue: number;
}

export interface ICondition {
  conditionName: string;
  conditionType: string;
  conditionValue: string;
}

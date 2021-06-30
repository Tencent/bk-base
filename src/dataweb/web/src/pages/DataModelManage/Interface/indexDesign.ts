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

export interface ICalculationAtomsList {
  errors: any;
  message: string;
  code: string;
  result: boolean;
  data: ICalculationAtomsData;
}

export interface ICalculationAtomsData {
  results: Array<ICalculationAtomsResults>;
  stepId: number;
}

export interface ICalculationAtomsResults {
  modelId: number;
  projectId: number;
  calculationAtomName: string;
  calculationAtomAlias: string;
  calculationAtomType: string;
  description: string;
  fieldType: string;
  calculationContent: ICalculationContent;
  calculationFormula: string;
  indicatorCount: number;
  createdBy: string;
  createdAt: string;
  updatedBy: string;
  updatedAt: string;
  indicators: Array<IIndicators>;
  isChecked: boolean;
  displayName: string;
}

export interface IIndicators {
  indicatorName: string;
  indicatorAlias: string;
  subIndicators: Array<ISubIndicators>;
}

export interface ISubIndicators {
  indicatorName: string;
  indicatorAlias: string;
}

export interface ICalculationContent {
  option: string;
  content: ICalculationSubContent;
}

export interface ICalculationSubContent {
  calculationField: string;
  calculationFunction: string;
}

// 引用集市统计口径
export interface IQuoteCalculationAtom {
  errors: any;
  message: string;
  code: string;
  result: boolean;
  data: IQuoteCalculationAtomData;
}

export interface IQuoteCalculationAtomData {
  stepId: number;
}

// 字段类型列表
export interface IFieldTypeListRes {
  message: string;
  code: string;
  data: Array<IFieldTypeListData>;
  result: boolean;
}

export interface IFieldTypeListData {
  fieldType: string;
  fieldTypeName: string;
  updatedBy: any;
  createdAt: string;
  updatedAt: string;
  createdBy: string;
  fieldTypeAlias: string;
  active: boolean;
  description: string;
}

// Sql统计函数列表
export interface ISqlFuncs {
  errors: any;
  message: string;
  code: string;
  result: boolean;
  data: Array<ISqlFuncsData>;
}

export interface ISqlFuncsData {
  functionName: string;
  outputType: string;
  allowFieldType: Array<string>;
}

// 统计口径详情
export interface ICalculationAtomDetail {
  message: string;
  code: string;
  data: ICalculationAtomDetailData;
  result: boolean;
}

export interface ICalculationAtomDetailData {
  modelId: number;
  calculationAtomName: string;
  description: string;
  fieldType: string;
  createdAt: string;
  updatedAt: string;
  createdBy: string;
  calculationAtomAlias: string;
  stepId: number;
  calculationContent: ICalculationDetailContent;
  projectId: number;
  calculationFormula: string;
  updatedBy: string;
  displayName: string;
}

export interface ICalculationDetailContent {
  content: ICalculationContent;
  option: string;
}

export interface ICalculationContent {
  calculationField: string;
  calculationFunction: string;
}

// 创建指标的参数对象的接口
export interface ICreateIndexParams {
  model_id: string | number;
  indicator_name: string;
  indicator_alias: string;
  description: string;
  calculation_atom_name: string;
  aggregation_fields: string[];
  filter_formula: string;
  scheduling_content: object;
  parent_indicator_name: string | null;
  scheduling_type: string;
}

// 指标详情接口
export interface IIndicatorDetail {
  errors: any;
  message: string;
  code: string;
  result: boolean;
  data: IIndicatorDetailData;
}

export interface IIndicatorDetailData {
  modelId: number;
  projectId: number;
  indicatorName: string;
  indicatorAlias: string;
  description: string;
  calculationAtomName: string;
  aggregationFields: Array<string>;
  filterFormula: string;
  schedulingContent: ISchedulingContent;
  parentIndicatorName: any;
  createdBy: string;
  createdAt: string;
  updatedBy: string;
  updatedAt: string;
  stepId: number;
  filterFormulaStrippedComment: string;
}

export interface ISchedulingContent {
  windowType: string;
  countFreq: number;
  schedulePeriod: string;
  fixedDelay: number;
  dependencyConfigType: string;
  unifiedConfig: IUnifiedConfig;
  advanced: IAdvanced;
}

export interface IAdvanced {
  recoveryTimes: number;
  recoveryEnable: boolean;
  recoveryInterval: string;
}

export interface IUnifiedConfig {
  windowSize: number;
  windowSizePeriod: string;
  dependencyRule: string;
}

// 可以被引用的统计口径列表
export interface ICalculationAtomsCanBeQuoted {
  errors: any;
  message: string;
  code: string;
  result: boolean;
  data: ICalculationAtomsCanBeQuotedData;
}

export interface ICalculationAtomsCanBeQuotedData {
  results: Array<ICalculationAtomsCanBeQuoteResult>;
}

export interface ICalculationAtomsCanBeQuoteResult {
  modelId: number;
  projectId: number;
  calculationAtomName: string;
  calculationAtomAlias: string;
  calculationAtomType: string;
  description: string;
  fieldType: string;
  calculationContent: ICalculationAtomsCanBeQuoteContent;
  calculationFormula: string;
  createdBy: string;
  createdAt: string;
  updatedBy: string;
  updatedAt: string;
  isChecked: boolean;
}

export interface ICalculationAtomsCanBeQuoteContent {
  option: string;
  content: IContent;
}

export interface IContent {
  calculationField: string;
  calculationFunction: string;
}

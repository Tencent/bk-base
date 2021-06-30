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

// 单个结果表实例的信息
export interface IResultInfo {
  message: string;
  code: string;
  data: Array<IResultInfoData>;
  result: boolean;
}

export interface IResultInfoData {
  fieldType: string;
  fieldAlias: string;
  description: string;
  roles: IResultInfoRoles;
  createdAt: string;
  isDimension: boolean;
  createdBy: string;
  updatedAt: string;
  origins: string;
  fieldName: string;
  id: number;
  fieldIndex: number;
  updatedBy: string;
}

export interface IResultInfoRoles {
  eventTime: boolean;
}

// 数据修正判断模板列表
export interface IQualityFixCorrectConditions {
  message: string;
  code: string;
  data: Array<IQualityFixCorrectConditionsData>;
  result: boolean;
}

export interface IQualityFixCorrectConditionsData {
  conditionTemplateType: string;
  updatedBy: any;
  updatedAt: any;
  createdAt: string;
  conditionTemplateConfig: string;
  conditionTemplateName: string;
  createdBy: string;
  id: number;
  active: boolean;
  conditionTemplateAlias: string;
  description: string;
}

// 数据修正填充模板列表
export interface IQualityFixFillTemp {
  message: string;
  code: string;
  data: Array<IQualityFixFillTempData>;
  result: boolean;
}

export interface IQualityFixFillTempData {
  handlerTemplateAlias: string;
  updatedBy: any;
  updatedAt: any;
  createdAt: string;
  handlerTemplateConfig: string;
  createdBy: string;
  handlerTemplateType: string;
  handlerTemplateName: string;
  active: boolean;
  id: number;
  description: string;
}

// 质量修正信息总览
export interface IQualityOverviewInfo {
  message: string;
  code: string;
  data: Array<IQualityOverviewData>;
  result: boolean;
}
export interface IQualityOverviewData {
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
  output: IOutput;
}

export interface IOutput {
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

// 提交修正SQL调试任务
export interface ISubmitCorrectSql {
  errors: any;
  message: string;
  code: string;
  result: boolean;
  data: ISubmitCorrectSqlData;
}

export interface ISubmitCorrectSqlData {
  debugRequestId: string;
}

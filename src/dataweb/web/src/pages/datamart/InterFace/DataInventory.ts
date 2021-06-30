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

export interface IScoreDistribution {
  errors: any;
  message: string;
  code: string;
  data: IScoreDistributionData;
  result: boolean;
}

export interface IScoreDistributionData {
  perc: Array<any>;
  cumPerc: Array<any>;
  cnt: Array<any>;
  y: Array<any>;
  x: Array<any>;
  z: Array<any>;
}

// 价值、重要度等评分趋势
export interface IQualityScoreTrend {
  errors: any;
  message: string;
  code: string;
  data: Array<IQualityScoreTrendData>;
  result: boolean;
}

export interface IQualityScoreTrendData {
  index: number;
  num: Array<number>;
  scoreLevel: string;
  time: Array<string>;
}

// 价值、重要度等等级分布
export interface IValueLevelDistribution {
  errors: any;
  message: string;
  code: string;
  data: IValueLevelDistributionData;
  result: boolean;
}

export interface IValueLevelDistributionData {
  y: Array<number>;
  x: Array<string>;
  sumCount: number;
  z: Array<number>;
}

// 总存储成本分布
export interface IStorageCapacityDistribution {
  errors: any;
  message: string;
  code: string;
  data: IStorageCapacityDistributionData;
  result: boolean;
}

export interface IStorageCapacityDistributionData {
  sumTspider: number;
  sumHdfs: number;
  sumCapacity: number;
}

// 存储成本趋势
export interface IStorageCapacityTrend {
  errors: any;
  message: string;
  code: string;
  data: IStorageCapacityTrendData;
  result: boolean;
}

export interface IStorageCapacityTrendData {
  tspiderCapacity: Array<any>;
  hdfsCapacity: Array<any>;
  totalCapacity: Array<any>;
  time: Array<any>;
}

// 数据查询次数分布
export interface IDataHeatQuery {
  message: string;
  code: string;
  data: IHeatQueryData;
  result: boolean;
}

export interface IHeatQueryData {
  y: Array<number>;
  x: Array<string>;
  z: Array<number>;
}

// 数据开发后继依赖节点个数
export interface IRelyonNodeData {
  message: string;
  code: string;
  data: IRelyonData;
  result: boolean;
}

export interface IRelyonData {
  y: Array<number>;
  x: Array<string>;
  z: Array<number>;
}

// 应用业务个数分布
export interface IApplyNodeData {
  message: string;
  code: string;
  data: IApplyNode;
  result: boolean;
}

export interface IApplyNode {
  y: Array<number>;
  x: Array<number>;
  z: Array<number>;
}

// 应用项目个数分布
export interface IApplyProjData {
  message: string;
  code: string;
  data: IApplyProj;
  result: boolean;
}

export interface IApplyProj {
  y: Array<number>;
  x: Array<number>;
  z: Array<number>;
}

// 应用APP个数分布
export interface IApplyAppData {
  message: string;
  code: string;
  data: IApplyApp;
  result: boolean;
}

export interface IApplyApp {
  y: Array<number>;
  x: Array<number>;
  z: Array<number>;
}

// 业务重要度、关联BIP、项目运营状态、数据敏感度分布、数据生成类型
export interface IImportanceDistribution {
  errors: any;
  message: string;
  code: string;
  data: IImportanceDistributionData;
  result: boolean;
}

export interface IImportanceDistributionData {
  datasetCount: Array<number>;
  metric: Array<boolean>;
  datasetPerct: Array<number>;
  bizCount: Array<number>;
  bizPerct: Array<number>;
}

// 业务星际&运营状态分布
export interface IBizImportanceDistribution {
  errors: any;
  message: string;
  code: string;
  data: IBizImportanceDistributionData;
  result: boolean;
}

export interface IBizImportanceDistributionData {
  datasetCount: Array<number>;
  operStateName: Array<string>;
  bizCount: Array<number>;
  bipGradeName: Array<string>;
}

// 价值、重要度等等级分布&评分趋势
export interface IScoreLevelTrend {
  errors: any;
  message: string;
  code: string;
  data: IScoreLevelTrendData;
  result: boolean;
}

export interface IScoreLevelTrendData {
  scoreTrend: Array<IScoreTrend>;
  levelDistribution: ILevelDistribution;
}

export interface ILevelDistribution {
  y: Array<number>;
  x: Array<string>;
  sumCount: number;
  z: Array<number>;
}

export interface IScoreTrend {
  index: number;
  num: Array<number>;
  scoreLevel: string;
  time: Array<string>;
}

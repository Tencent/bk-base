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

import { INodeParams } from './INodeParams';

export interface ItableData {
  field_type: string;
  field_alias: string;
  description: any;
  is_generated_field: boolean;
  field_index: number;
  field_name: string;
  input_field_name: string;
  is_primary_key: boolean;
  input_result_tableId: string;
  field_clean_content: any;
  relation: IRelation;
  is_extended_field: boolean;
  field_category: string;
  is_opened_application_clean: boolean;
  is_join_field: boolean;
  field_constraint_content: any;
  join_field_name: any;
  application_clean_content: IApplicationCleanContent;
  isDrag: boolean;
  isSetProcessField: boolean;
  input_table_field: string;
  extends: Array<any>;
}

export interface IApplicationCleanContent {
  clean_option: string;
  clean_content: string;
}

export interface IRelation {
  inputResultTableId: string;
  relatedModelId: number;
  inputFieldName: string;
}

/** 模型管理节点模型列表 */
export interface ImodelList {
  data: Array<IData>;
}

export interface IData {
  modelType: string;
  models: Array<IModels>;
  description: string;
  order: number;
  modelTypeAlias: string;
}

export interface IModels {
  modelType: string;
  modelId: number;
  modelName: string;
  modelAlias: string;
  latestVersionId: string;
}

export { INodeParams };

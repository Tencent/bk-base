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

import { ServiceModel } from './serviceModel';
/** 数据模型列表 */
export const getModelList = new ServiceModel('/datamanage/datamodel/models/');

/** 模型置顶 */
export const topModel = new ServiceModel('/datamanage/datamodel/models/:model_id/top/', 'POST');

/** 模型取消置顶 */
export const cancelTop = new ServiceModel('/datamanage/datamodel/models/:model_id/cancel_top/', 'POST');

/** 数据模型详情 */
export const getModelInfo = new ServiceModel('/datamanage/datamodel/models/:model_id/info/');

/** 创建数据模型 */
export const createModel = new ServiceModel('/datamanage/datamodel/models/', 'POST');

/** 修改数据模型 */
export const updateModel = new ServiceModel('/datamanage/datamodel/models/:model_id/', 'PUT');

/** 删除数据模型 */
export const deleteModel = new ServiceModel('/datamanage/datamodel/models/:model_id/', 'DELETE');

/** 拉取主表详情 */
export const getMasterTableInfo = new ServiceModel('/datamanage/datamodel/models/:model_id/master_tables/');

/** 修改主表 */
export const updateMasterTableInfo = new ServiceModel('/datamanage/datamodel/models/:model_id/master_tables/', 'POST');

/** 数据模型字段加工校验 */
export const verifyFieldProcessingLogic = new ServiceModel('/datamanage/datamodel/sql_verify/column_process/', 'POST');

/** 字段约束配置列表 */
export const getFieldContraintConfigs = new ServiceModel('/datamanage/datamodel/field_constraint_configs/');

/** 判断数据模型名称是否存在 */
export const validateModelName = new ServiceModel('/datamanage/datamodel/models/validate_model_name/');

/** 统计口径列表 */
export const getCalculationAtoms = new ServiceModel('/datamanage/datamodel/calculation_atoms/');

/** 可以被引用的统计口径列表 */
export const getCalculationAtomsCanBeQuoted = new ServiceModel(
  '/datamanage/datamodel/calculation_atoms/can_be_quoted/'
);

/** 创建统计口径 */
export const createCalculationAtom = new ServiceModel('/datamanage/datamodel/calculation_atoms/', 'POST');

/** 修改统计口径 */
export const editCalculationAtom = new ServiceModel(
  '/datamanage/datamodel/calculation_atoms/:calculation_atom_name/',
  'PUT'
);

/** 统计口径详情 */
export const calculationAtomDetail = new ServiceModel(
  '/datamanage/datamodel/calculation_atoms/:calculation_atom_name/info/'
);

/** 删除统计口径 */
export const deleteCalculationAtom = new ServiceModel(
  '/datamanage/datamodel/calculation_atoms/:calculation_atom_name/',
  'DELETE'
);

/** 引用集市统计口径 */
export const quoteCalculationAtom = new ServiceModel('/datamanage/datamodel/calculation_atoms/quote/', 'POST');

/** SQL统计函数列表 */
export const sqlFuncs = new ServiceModel('/datamanage/datamodel/calculation_functions/');

/** 指标列表 */
export const getIndicatorList = new ServiceModel('/datamanage/datamodel/indicators/');

/** 删除指标 */
export const deleteIndicator = new ServiceModel('/datamanage/datamodel/indicators/:indicator_name', 'DELETE');

/** 创建指标 */
export const createIndicator = new ServiceModel('/datamanage/datamodel/indicators/', 'POST');

/** 修改指标 */
export const editIndicator = new ServiceModel('/datamanage/datamodel/indicators/:indicator_name/', 'PUT');

/** 获取指标详情 */
export const getIndicatorDetail = new ServiceModel('/datamanage/datamodel/indicators/:indicator_name/info/');

/** 字段类型列表 */
export const getFieldTypeConfig = new ServiceModel('/datamanage/datamodel/field_type_configs/');

/** 结果数据表结构预览 */
export const getResultTablesFields = new ServiceModel('/datamanage/datamodel/result_tables/:rt_id/fields/');

/** 获取模型预览数据 */
export const getModelOverview = new ServiceModel('/datamanage/datamodel/models/:model_id/overview/');

/** 确认模型预览 */
export const confirmModelOverview = new ServiceModel(
  '/datamanage/datamodel/models/:model_id/confirm_overview/',
  'POST'
);

/** 确认指标 */
export const confirmModelIndicators = new ServiceModel(
  '/datamanage/datamodel/models/:model_id/confirm_indicators/',
  'POST'
);

/** 获取数据模型变更内容 */
export const getModelVersionDiff = new ServiceModel('/datamanage/datamodel/models/:model_id/diff/');

/** 数据模型发布 */
export const releaseModelVersion = new ServiceModel('/datamanage/datamodel/models/:model_id/release/', 'POST');

/** 获取关联维度表模型 */
export const getDimensionModelsCanBeRelated = new ServiceModel(
  '/datamanage/datamodel/models/:model_id/dimension_models/can_be_related/'
);

/** 数据模型操作记录 */
export const operationRecords = new ServiceModel('/datamanage/datamodel/models/:model_id/operation_log/', 'POST');

/** 数据模型操作者 */
export const getOperators = new ServiceModel('/datamanage/datamodel/models/:model_id/operators/');

/** 数据模型操作前后diff */
export const getOperationDiff = new ServiceModel(
  '/datamanage/datamodel/models/:model_id/operation_log/:operation_id/diff/'
);

/** 查看态数据 */
export const getModelViewData = new ServiceModel('/datamanage/datamodel/models/:model_id/latest_version/info/');

/** 发布列表 */
export const getReleaseList = new ServiceModel('/datamanage/datamodel/models/:model_id/release_list/');

/** 获取指标字段信息 */
export const getIndicatorFields = new ServiceModel('/datamanage/datamodel/indicators/:indicator_name/fields/');

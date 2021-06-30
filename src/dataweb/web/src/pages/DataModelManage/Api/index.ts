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

import { bkHttpRequest } from '@/common/js/ajax';
import { HttpRequestParams } from '@/controller/common';
import { IDataModelManage } from '../Interface/index';
import {
  ICalculationAtomsCanBeQuoted,
  ICreateIndexParams,
  IFieldTypeListRes,
  ISqlFuncs,
} from '../Interface/indexDesign';

const moduleName = 'dataModelManage';

/**
 *
 * @param methodName
 * @param requestParams
 * @param queryParams
 * @param exts
 */
const bkHttp = (methodName: string, requestParams = {}, queryParams = {}, ext = {}): Promise<IBKResponse<any>> => {
  return bkHttpRequest(
        `${moduleName}/${methodName}`,
        new HttpRequestParams(requestParams, queryParams, false, true, ext)
  );
};

/**
 * 获取数据模型列表
 * @param query IDataModelManageQuery.IModelListQuery
 */
export const getModelList = (
  query: IDataModelManageQuery.IModelListQuery = {}
): Promise<IBKResponse<IDataModelManage.IModelList[]>> => {
  return bkHttp('getModelList', {}, query);
};

/**
 * 模型置顶
 * @param model_id 模型ID
 * @param description 置顶描述
 */
export const topModel = (model_id: string, description?: string): Promise<IBKResponse<boolean>> => {
  return bkHttp('topModel', { model_id, description }, {});
};

/**
 * 取消模型置顶
 * @param model_id 模型ID
 * @param description 置顶描述
 */
export const cancelTop = (model_id: string, description?: string): Promise<IBKResponse<boolean>> => {
  return bkHttp('cancelTop', { model_id, description }, {});
};

/**
 * 数据模型详情
 * @param model_id 模型ID
 * @param with_details 展示模型主表字段、模型关联关系、统计口径、指标等详情, 取值['master_table', 'calculation_atoms', 'indicators']
 * @param ext 其他配置
 */
export const getModelInfo = (model_id: string, with_details?: string[], ext = {}): Promise<IBKResponse<any>> => {
  return bkHttp('getModelInfo', { model_id }, { with_details }, ext);
};

/**
 * 修改数据模型
 * @param model_id 模型ID
 * @param model_alias 模型别名
 * @param description 模型描述
 * @param tag_codes 标签
 */
export const updateModel = (model_id: string, model_alias?: string, description?: string, tags?: object[]) => {
  return bkHttp('updateModel', { model_id, model_alias, description, tags }, {});
};

/**
 * 删除数据模型
 * @param model_id 模型ID
 */
export const deleteModel = (model_id: string) => {
  return bkHttp('deleteModel', { model_id }, {});
};

/**
 * 创建数据模型
 * @param model_name 模型名称
 * @param model_alias 模型别名
 * @param model_type 模型类型
 * @param project_id 项目id
 * @param description 模型描述
 * @param tag_codes 标签
 */
export const createModel = (
  model_name: string,
  model_alias: string,
  model_type: string,
  project_id: number,
  description: string,
  tags: object[]
) => {
  return bkHttp('createModel', { model_name, model_alias, description, tags, model_type, project_id }, {});
};

/**
 * 拉取主表详情
 * @param model_id 模型id
 * @param with_time_field 是否带有time字段
 * @param allow_field_type 允许的字段类型 string | array
 * @param with_details 返回结果是否带有可删除/删除信息
 */
export const getMasterTableInfo = (
  model_id: string,
  with_time_field = false,
  allow_field_type?: string | Array<string>,
  with_details?: Array<string>,
  latest_version?: boolean
): Promise<IBKResponse<IDataModelManage.IMasterTableInfo>> => {
  return bkHttp(
    'getMasterTableInfo',
    { model_id },
    { with_time_field, allow_field_type, with_details, latest_version },
    { format: true }
  );
};

/**
 * 修改主表
 * @param model_id 模型id
 * @param fields 模型主表字段列表
 * @param model_relation 模型主表关联信息
 */
export const updateMasterTableInfo = (
  model_id: string,
  fields: IDataModelManage.IMasterTableField[],
  model_relation: IModelRelation[] = []
): Promise<IBKResponse<any>> => {
  return bkHttp('updateMasterTableInfo', { model_id, fields, model_relation }, {});
};

/**
 * 数据模型字段加工校验
 * @param table_name 表名称
 * @param verify_fields 需要执行校验的列表语句
 * @param scope_field_list 语句执行背景依赖表
 */
export const verifyFieldProcessingLogic = (table_name: string, verify_fields: [], scope_field_list: []) => {
  return bkHttp('verifyFieldProcessingLogic', { table_name, verify_fields, scope_field_list }, {});
};

/** 字段约束配置列表 */
export const getFieldContraintConfigs = (): Promise<IBKResponse<IDataModelManage.IFieldContraintConfig[]>> => {
  return bkHttp('getFieldContraintConfigs', {}, {}, { format: true });
};

/** 判断数据模型名称是否存在 */
export const validateModelName = (model_name: string): Promise<IBKResponse<any>> => {
  return bkHttp('validateModelName', {}, { model_name });
};

/**
 * 统计口径列表
 * @param model_id 模型id
 * @param with_indicators 是否返回指标信息,默认不返回
 */
export const getCalculationAtoms = (
  model_id?: string | number,
  with_indicators?: boolean
): Promise<IBKResponse<any[]>> => {
  return bkHttpRequest(
    'dataModelManage/getCalculationAtoms',
    new HttpRequestParams({}, { model_id, with_indicators }, false, true, {})
  );
};

/**
 * 可以被引用的统计口径列表
 * @param model_id 模型id
 */
export const getCalculationAtomsCanBeQuoted = (
  model_id?: string | number
): Promise<IBKResponse<ICalculationAtomsCanBeQuoted>> => {
  return bkHttpRequest(
    'dataModelManage/getCalculationAtomsCanBeQuoted',
    new HttpRequestParams({}, { model_id }, false, true, {})
  );
};

/**
 * 创建统计口径
 * @param model_id 模型id
 * @param calculation_atom_name 统计口径名称
 * @param calculation_atom_alias 统计口径别名
 * @param description 统计口径描述
 * @param field_type 数据类型
 * @param calculation_content 统计方式
 */
export const createCalculationAtom = (
  model_id: string | number,
  field_type: string,
  description: string,
  calculation_atom_name: string,
  calculation_atom_alias: string,
  calculation_content: object
): Promise<IBKResponse<any[]>> => {
  return bkHttpRequest(
    'dataModelManage/createCalculationAtom',
    new HttpRequestParams(
      { model_id, calculation_atom_name, calculation_atom_alias, description, field_type, calculation_content },
      {},
      false,
      true,
      {}
    )
  );
};

/**
 * 修改统计口径
 * @param model_id 模型id
 * @param calculation_atom_alias 统计口径别名
 * @param description 统计口径描述
 * @param field_type 数据类型
 * @param calculation_content 统计方式
 */
export const editCalculationAtom = (
  model_id: string | number,
  field_type: string,
  description: string,
  calculation_atom_name: string,
  calculation_atom_alias: string,
  calculation_content: object
): Promise<IBKResponse<any[]>> => {
  return bkHttpRequest(
    'dataModelManage/editCalculationAtom',
    new HttpRequestParams(
      { model_id, field_type, description, calculation_atom_name, calculation_atom_alias, calculation_content },
      {},
      false,
      true,
      {}
    )
  );
};

/**
 * 统计口径详情
 * @param calculation_atom_name 统计口径名称
 * @param with_indicators 是否返回指标信息,默认不返回
 */
export const calculationAtomDetail = (
  calculation_atom_name: string,
  with_indicators?: boolean
): Promise<IBKResponse<any[]>> => {
  return bkHttpRequest(
    'dataModelManage/calculationAtomDetail',
    new HttpRequestParams({ calculation_atom_name }, { with_indicators }, false, true, {})
  );
};

/**
 * 删除统计口径
 * @param calculation_atom_name 统计口径名称
 * @param model_id 模型id
 */
export const deleteCalculationAtom = (
  calculation_atom_name: string,
  model_id: string | number
): Promise<IBKResponse<any[]>> => {
  return bkHttpRequest(
    'dataModelManage/deleteCalculationAtom',
    new HttpRequestParams({ calculation_atom_name, model_id }, {}, false, true, {})
  );
};

/**
 * 引用集市统计口径
 * @param model_id 模型id
 * @param calculation_atom_names 引用的统计口径名称列表
 */
export const quoteCalculationAtom = (
  model_id: string | number,
  calculation_atom_names: string[]
): Promise<IBKResponse<any[]>> => {
  return bkHttpRequest(
    'dataModelManage/quoteCalculationAtom',
    new HttpRequestParams({ model_id, calculation_atom_names }, {}, false, true, {})
  );
};

/**
 * SQL统计函数列表
 * @param function_name 模型id
 * @param output_type 输出字段类型
 */
export const sqlFuncs = (function_name?: string | number, output_type?: string): Promise<IBKResponse<ISqlFuncs[]>> => {
  return bkHttpRequest(
    'dataModelManage/sqlFuncs',
    new HttpRequestParams({}, { function_name, output_type }, false, true, {})
  );
};

/**
 * 指标列表
 * @param calculation_atom_name 统计口径名称
 * @param model_id 模型id
 * @param parent_indicator_name 父指标名称
 * @param indicator_name 指标名称
 * @param with_sub_indicators 是否显示子指标
 */
export const getIndicatorList = (
  calculation_atom_name?: string,
  model_id?: string | number,
  parent_indicator_name?: string,
  indicator_name?: string,
  with_sub_indicators?: boolean
): Promise<IBKResponse<ISqlFuncs[]>> => {
  return bkHttpRequest(
    'dataModelManage/getIndicatorList',
    new HttpRequestParams(
      {},
      { calculation_atom_name, model_id, parent_indicator_name, indicator_name, with_sub_indicators },
      false,
      true,
      {}
    )
  );
};

/**
 * 删除指标
 * @param indicator_name 指标名称
 */
export const deleteIndicator = (indicator_name: string, model_id: number): Promise<IBKResponse<ISqlFuncs[]>> => {
  return bkHttpRequest(
    'dataModelManage/deleteIndicator',
    new HttpRequestParams({ indicator_name, model_id }, {}, false, true, {})
  );
};

/**
 * 创建指标
 * @param model_id 模型id
 * @param indicator_name 指标名称
 * @param indicator_alias 指标别名
 * @param description 指标描述
 * @param calculation_atom_name 统计口径名称
 * @param aggregation_fields 聚合字段
 * @param filter_formula 过滤SQL
 * @param scheduling_content 调度内容
 * @param parent_indicator_name 父指标名称
 */
export const createIndicator = (params: ICreateIndexParams): Promise<IBKResponse<ISqlFuncs[]>> => {
  return bkHttpRequest('dataModelManage/createIndicator', new HttpRequestParams(params, {}, false, true, {}));
};

/**
 * 修改指标
 * @param indicator_alias 指标别名
 * @param description 指标描述
 * @param calculation_atom_name 统计口径名称
 * @param aggregation_fields 聚合字段
 * @param filter_formula 过滤SQL
 * @param scheduling_content 调度内容
 */
export const editIndicator = (params: ICreateIndexParams): Promise<IBKResponse<ISqlFuncs[]>> => {
  return bkHttpRequest('dataModelManage/editIndicator', new HttpRequestParams(params, {}, false, true, {}));
};

/**
 * 获取指标详情
 * @param indicator_name 指标名称
 * @param with_sub_indicators 是否显示子指标
 */
export const getIndicatorDetail = (
  indicator_name: string,
  with_sub_indicators?: boolean,
  model_id?: string | number
): Promise<IBKResponse<any[]>> => {
  return bkHttpRequest(
    'dataModelManage/getIndicatorDetail',
    new HttpRequestParams(
      {
        indicator_name,
        with_sub_indicators,
      },
      {
        model_id,
      },
      false,
      true,
      {}
    )
  );
};

/**
 * 拉取数据类型列表
 * @param exclude_field_type string | Array
 */
export const getFieldTypeConfig = (
  exclude_field_type: string | Array<string>
): Promise<IBKResponse<IFieldTypeListRes>> => {
  return bkHttp('getFieldTypeConfig', {}, { exclude_field_type });
};

/**
 * 结果数据表结构预览
 * @param rt_id
 */
export const getResultTablesFields = (rt_id: string | number): Promise<IBKResponse<any>> => {
  return bkHttp('getResultTablesFields', { rt_id }, {});
};

/**
 * 获取模型预览数据
 * @param model_id
 */
export const getModelOverview = (model_id: string, latest_version: boolean | undefined): Promise<IBKResponse<any>> => {
  return bkHttp('getModelOverview', { model_id }, { latest_version });
};

/**
 * 确认模型预览
 * @param model_id
 */
export const confirmModelOverview = (model_id: string): Promise<IBKResponse<any>> => {
  return bkHttp('confirmModelOverview', { model_id }, {});
};

/**
 * 确认模型指标
 * @param model_id
 */
export const confirmModelIndicators = (model_id: string): Promise<IBKResponse<any>> => {
  return bkHttp('confirmModelIndicators', { model_id }, {});
};

/**
 * 获取数据模型变更内容
 * @param model_id
 */
export const getModelVersionDiff = (
  model_id: string | number,
  orig_version_id: string | undefined | null,
  new_version_id: string | undefined,
  ext = {}
): Promise<IBKResponse<any>> => {
  return bkHttp(
    'getModelVersionDiff',
    { model_id },
    { orig_version_id, new_version_id },
    Object.assign({ format: true }, ext)
  );
};

/**
 * 数据模型发布
 * @param model_id
 */
export const releaseModelVersion = (model_id: string, version_log: string): Promise<IBKResponse<any>> => {
  return bkHttp('releaseModelVersion', { model_id, version_log }, {});
};

/**
 * 获取可被关联的维度表模型
 * @param model_id
 */
export const getDimensionModelsCanBeRelated = (
  model_id: string,
  related_model_id: string | undefined,
  published: boolean | undefined
): Promise<IBKResponse<any>> => {
  return bkHttp('getDimensionModelsCanBeRelated', { model_id }, { related_model_id, published });
};

/**
 * 数据模型操作记录
 * @param model_id
 * @param conditions 搜索条件参数,object_operation操作类型，object_type操作对象类型，created_by操作者， 操作对象待定
 * @param start_time 启始时间
 * @param end_time 终止时间
 * @param page 页码
 * @param page_size 每页条数
 */
export const operationRecords = (
  model_id: number,
  page: string | number,
  page_size: string | number,
  conditions?: object,
  start_time?: string,
  end_time?: string,
  order_by_created_at?: string
): Promise<IBKResponse<any>> => {
  return bkHttp(
    'operationRecords',
    { model_id, page, page_size, conditions, start_time, end_time, order_by_created_at },
    {},
    { format: true }
  );
};

/**
 * 操作者列表
 * @param model_id
 */
export const getOperators = (model_id: number): Promise<IBKResponse<any>> => {
  return bkHttp('getOperators', { model_id }, {}, { format: true });
};

/**
 * 数据模型操作前后diff
 * @param model_id
 * @param operation_id
 */
export const getOperationDiff = (
  model_id: number,
  operation_id: string | number,
  ext = {}
): Promise<IBKResponse<any>> => {
  return bkHttp('getOperationDiff', { model_id, operation_id }, {}, Object.assign({ format: true }, ext));
};

/**
 * 获取模型查看态数据
 * @param model_id
 */
export const getModelViewData = (model_id: number, with_details?: string, ext = {}): Promise<IBKResponse<any>> => {
  return bkHttp('getModelViewData', { model_id }, { with_details }, Object.assign({ format: true }, ext));
};

/**
 * 获取版本列表
 * @param model_id
 */
export const getReleaseList = (model_id: number, ext = {}): Promise<IBKResponse<any>> => {
  return bkHttp('getReleaseList', { model_id }, {}, Object.assign({ format: true }, ext));
};

/**
 * 获取指标字段信息
 * @param indicator_name
 */
export const getIndicatorFields = (indicator_name: number, model_id: number, ext = {}): Promise<IBKResponse<any>> => {
  return bkHttp('getIndicatorFields', { indicator_name }, { model_id }, Object.assign({ format: true }, ext));
};

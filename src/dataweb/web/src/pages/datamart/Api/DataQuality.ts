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

import { bkRequest } from '@/common/js/ajax';
import { HttpRequestParams } from '@/controller/common';
import {
  IQualityInfoOverview,
  IQualityAuditRule,
  IQualityAuditRuleTable,
  IQualityEventInfo,
  IQualityEventTableData,
  IQualityRuleFunc,
  IQualityRuleIndex,
  IQualityRuleTemplate,
  IQualityCreateRule,
  IRuleConfigFields,
  IAuditEventTemp,
  IQualityEventTypes,
} from '@/pages/datamart/InterFace/DataQuality';
import { ISubmitCorrectSql, IQualityFixCorrectConditions } from '@/pages/datamart/InterFace/DataDict/DataQualityFix';

/**
 * 数据质量获取信息总览
 * @param (query) data_set_id 数据id
 * @param mock : 是否模拟数据
 */
export const getQualityInfoOverview = (data_set_id: string): Promise<IResponse<IQualityInfoOverview>> => {
  return bkRequest.httpRequest(
    'dataMart/getQualityInfoOverview',
    new HttpRequestParams({}, { data_set_id }, false, true, {})
  );
};

/**
 * 数据质量信息总览审核规则
 * @param (query) data_set_id 数据id
 * @param mock : 是否模拟数据
 */
export const getQualityInfoAuditRule = (data_set_id: string): Promise<IResponse<IQualityAuditRule>> => {
  return bkRequest.httpRequest(
    'dataMart/getQualityInfoAuditRule',
    new HttpRequestParams({}, { data_set_id }, false, true, {})
  );
};

/**
 * 数据质量审核规则表格数据
 * @param (query) data_set_id 数据id
 * @param mock : 是否模拟数据
 */
export const getQualityAuditRuleTable = (data_set_id: string): Promise<IResponse<IQualityAuditRuleTable>> => {
  return bkRequest.httpRequest(
    'dataMart/getQualityAuditRuleTable',
    new HttpRequestParams({}, { data_set_id }, false, true, {})
  );
};

/**
 * 查询质量事件统计信息
 * @param (query) data_set_id 数据id
 * @param mock : 是否模拟数据
 */
export const queryQualityEventInfo = (
  data_set_id: string,
  start_time?: number,
  end_time?: number
): Promise<IResponse<IQualityEventInfo>> => {
  return bkRequest.httpRequest(
    'dataMart/queryQualityEventInfo',
    new HttpRequestParams({}, { data_set_id, start_time, end_time }, false, true, {})
  );
};

/**
 * 查询质量事件表格信息
 * @param (query) data_set_id 数据id
 * @param mock : 是否模拟数据
 */
export const queryQualityEventTableData = (data_set_id: string): Promise<IResponse<IQualityEventTableData>> => {
  return bkRequest.httpRequest(
    'dataMart/queryQualityEventTableData',
    new HttpRequestParams({}, { data_set_id }, false, true, {})
  );
};

/**
 * 获取数据平台质量告警支持的通知方式
 * @param mock : 是否模拟数据
 */
export const getNotifyWayList = (): Promise<IResponse<IQualityEventTableData>> => {
  return bkRequest.httpRequest('dmonitorCenter/getNotifyWayList', new HttpRequestParams({}, {}, false, true, {}));
};

/**
 * 获取规则配置函数列表
 * @param mock : 是否模拟数据
 */
export const queryQualityRuleFunc = (): Promise<IResponse<IQualityRuleFunc>> => {
  return bkRequest.httpRequest('dataMart/queryQualityRuleFunc', new HttpRequestParams({}, {}, false, true, {}));
};

/**
 * 获取规则配置指标列表
 * @param mock : 是否模拟数据
 */
export const queryQualityRuleIndex = (): Promise<IResponse<IQualityRuleIndex>> => {
  return bkRequest.httpRequest('dataMart/queryQualityRuleIndex', new HttpRequestParams({}, {}, false, true, {}));
};

/**
 * 获取规则配置模板列表
 * @param mock : 是否模拟数据
 */
export const queryQualityRuleTemplates = (): Promise<IResponse<IQualityRuleTemplate>> => {
  return bkRequest.httpRequest('dataMart/queryQualityRuleTemplates', new HttpRequestParams({}, {}, false, true, {}));
};

/**
 *  新建规则
 *  @params (params): formData
 * @param mock : 是否模拟数据
 */
export const createQualityRules = (formData: IRuleConfigFields): Promise<IResponse<IQualityCreateRule>> => {
  return bkRequest.httpRequest(
    'dataMart/createQualityRules',
    new HttpRequestParams({ ...formData }, {}, false, true, {})
  );
};

/**
 *  保存规则
 *  @params (params): formData
 * @param mock : 是否模拟数据
 */
export const updateQualityRules = (formData: IRuleConfigFields): Promise<IResponse<IQualityCreateRule>> => {
  return bkRequest.httpRequest(
    'dataMart/updateQualityRules',
    new HttpRequestParams({ ...formData }, {}, false, true, {})
  );
};

/**
 *  质量审核规则配置里获取规则的二级菜单
 *  @params (params): data_set_id // 数据id
 * @param mock : 是否模拟数据
 */
export const getResultTables = (data_set_id: string): Promise<IResponse<IQualityCreateRule>> => {
  return bkRequest.httpRequest(
    'meta/getResultTables',
    new HttpRequestParams({ rtid: data_set_id }, {}, false, true, {})
  );
};

/**
 *  删除规则
 *  @params (params): rule_id // 规则id
 * @param mock : 是否模拟数据
 */
export const deleteQualityRules = (rule_id: string): Promise<IResponse<IQualityCreateRule>> => {
  return bkRequest.httpRequest(
    'dataMart/deleteQualityRules', new HttpRequestParams({ rule_id }, {}, false, true, {}));
};

/**
 *  停止规则审核任务
 *  @params (params): rule_id // 规则id
 * @param mock : 是否模拟数据
 */
export const stopQualityRulesTask = (rule_id: string): Promise<IResponse<IQualityCreateRule>> => {
  return bkRequest.httpRequest(
    'dataMart/stopQualityRulesTask',
    new HttpRequestParams({ rule_id }, {}, false, true, {})
  );
};

/**
 *  启动规则审核任务
 *  @params (params): rule_id // 规则id
 * @param mock : 是否模拟数据
 */
export const startQualityRulesTask = (rule_id: string): Promise<IResponse<IQualityCreateRule>> => {
  return bkRequest.httpRequest(
    'dataMart/startQualityRulesTask',
    new HttpRequestParams({ rule_id }, {}, false, true, {})
  );
};

/**
 *  质量事件查看指标接口
 *  @params (params): rule_id // 规则id
 *  @params (query): start_time // 开始时间
 *  @params (query): end_time //结束时间
 * @param mock : 是否模拟数据
 */
export const lookQualityIndex = (
  rule_id: string,
  start_time: string,
  end_time: string
): Promise<IResponse<IQualityCreateRule>> => {
  return bkRequest.httpRequest(
    'dataMart/lookQualityIndex',
    new HttpRequestParams({ rule_id }, { start_time, end_time }, false, true, {})
  );
};

/**
 *  质量事件查看指标接口
 *  @params (query): end_time //结束时间
 * @param mock : 是否模拟数据
 */
export const getAuditEventTemp = (): Promise<IResponse<IAuditEventTemp>> => {
  return bkRequest.httpRequest('dataMart/getAuditEventTemp', new HttpRequestParams({}, {}, false, true, {}));
};

/**
 *  数据质量事件子类型字典表
 *  @params (query): parent_event_type 默认为data_quality
 */
export const getDataQualityEventTypes = (type: string): Promise<IResponse<IQualityEventTypes>> => {
  return bkRequest.httpRequest(
    'dataMart/getDataQualityEventTypes',
    new HttpRequestParams({}, { type }, false, true, {})
  );
};

/**
 *  查询修正规则配置
 *  @params (query): data_set_id: string 数据集id
 */
export const getFixRuleConfig = (data_set_id: string): Promise<IResponse<IQualityEventTypes>> => {
  return bkRequest.httpRequest(
    'dataMart/getFixRuleConfig',
    new HttpRequestParams({}, { data_set_id }, false, true, {})
  );
};

/**
 *  数据修正填充模板列表
 */
interface FixTempParams {
  filling_template_name?: string;
  filling_template_alias?: string;
  filling_template_type?: string;
  filling_template_config?: string;
  description?: string;
  created_by?: string;
  created_at?: string;
  updated_by?: string;
  updated_at?: string;
}
export const getFixFillTempList = (fixTempParams: FixTempParams): Promise<IResponse<IQualityEventTypes>> => {
  return bkRequest.httpRequest(
    'dataMart/getFixFillTempList',
    new HttpRequestParams({}, { fixTempParams }, false, true, {})
  );
};

/**
 *  数据修正判断模板列表
 */
interface FixCorrectConditionsParams {
  condition_template_name?: string;
  condition_template_alias?: string;
  condition_template_type?: string;
  condition_template_config?: string;
  description?: string;
  created_by?: string;
  created_at?: string;
  updated_by?: string;
  updated_at?: string;
}
export const getFixCorrectConditions = (
  fixTempParams: FixCorrectConditionsParams
): Promise<IResponse<IQualityFixCorrectConditions>> => {
  return bkRequest.httpRequest(
    'dataMart/getFixCorrectConditions',
    new HttpRequestParams({}, { fixTempParams }, false, true, {})
  );
};

/**
 *  查询修正规则配置
 *  @params (query): data_set_id: string 数据集id
 *  @params (query): source_sql: string 原始SQL
 *  @params (query): correct_configs: Object 修正配置
 */
export const getQualityFixSql = (
  data_set_id: string,
  source_sql: string,
  correct_configs: object,
  ext = {}
): Promise<IResponse<any>> => {
  return bkRequest.httpRequest(
    'dataMart/getQualityFixSql',
    new HttpRequestParams({ data_set_id, source_sql, correct_configs }, {}, false, true, {})
  );
};

/**
 *  提交修正SQL调试任务
 *  @params (params): source_data_set_id: string 上游数据集ID
 *  @params (params): source_sql: string 原始SQL
 *  @params (correct_configs): Object 修正配置
 */
export const submitCorrectSql = (
  source_data_set_id: string,
  data_set_id: string,
  source_sql: string,
  correct_configs: Array<object>
): Promise<IResponse<ISubmitCorrectSql>> => {
  return bkRequest.httpRequest(
    'dataMart/submitCorrectSql',
    new HttpRequestParams({ source_data_set_id, data_set_id, source_sql, correct_configs }, {}, false, true, {})
  );
};

/**
 *  获取修正SQL调试任务
 *  @params (query): debug_request_id: string 调试请求ID
 */
export const getCorrectSqlTask = (debug_request_id: string): Promise<IResponse<any>> => {
  return bkRequest.httpRequest(
    'dataMart/getCorrectSqlTask',
    new HttpRequestParams({}, { debug_request_id }, false, true, {})
  );
};

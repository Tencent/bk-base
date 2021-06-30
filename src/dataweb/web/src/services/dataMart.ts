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
 * 数据质量获取信息总览
 *  @params (query): data_set_id // 数据id
 */
const getQualityInfoOverview = {
  url: 'v3/datamanage/dataquality/events/summary/',
};

/**
 * 数据质量信息总览审核规则
 *  @params (query): data_set_id // 数据id
 */
const getQualityInfoAuditRule = {
  url: 'v3/datamanage/dataquality/rules/summary/',
};

/**
 * 数据质量审核规则表格数据
 *  @params (query): data_set_id // 数据id
 */
const getQualityAuditRuleTable = {
  url: 'v3/datamanage/dataquality/rules/',
};

/**
 * 查询质量事件统计信息
 *  @params (query): data_set_id // 数据id
 */
const queryQualityEventInfo = {
  url: 'v3/datamanage/dataquality/events/stats/',
};

/**
 * 查询质量事件表格信息
 *  @params (query): data_set_id // 数据id
 */
const queryQualityEventTableData = {
  url: 'v3/datamanage/dataquality/events/',
};

/**
 * 规则配置函数列表
 */
const queryQualityRuleFunc = {
  url: 'v3/datamanage/dataquality/functions/',
};

/**
 * 规则配置指标列表
 */
const queryQualityRuleIndex = {
  url: 'v3/datamanage/dataquality/metrics/',
};

/**
 * 规则配置模板列表
 */
const queryQualityRuleTemplates = {
  url: 'v3/datamanage/dataquality/rule_templates/',
};

/**
 * 新建或者保存规则
 *  @params (query): data_set_id // 数据id
 */
const createQualityRules = {
  url: 'v3/datamanage/dataquality/rules/',
  method: 'POST',
};

/**
 * 保存规则
 *  @params (query): rule_id // 规则id
 */
const updateQualityRules = {
  url: 'v3/datamanage/dataquality/rules/:rule_id/',
  method: 'PUT',
};

/**
 * 删除规则
 *  @params (params): rule_id // 规则id
 */
const deleteQualityRules = {
  url: 'v3/datamanage/dataquality/rules/:rule_id/',
  method: 'DELETE',
};

/**
 * 停止规则审核任务
 *  @params (params): rule_id // 规则id
 */
const stopQualityRulesTask = {
  url: 'v3/datamanage/dataquality/rules/:rule_id/stop/',
  method: 'POST',
};

/**
 * 启动规则审核任务
 *  @params (params): rule_id // 规则id
 */
const startQualityRulesTask = {
  url: 'v3/datamanage/dataquality/rules/:rule_id/start/',
  method: 'POST',
};

/**
 * 质量事件查看指标接口
 *  @params (params): rule_id // 规则id
 *  @params (query): start_time // 开始时间
 *  @params (query): end_time //结束时间
 */
const lookQualityIndex = {
  url: 'v3/datamanage/dataquality/rules/:rule_id/metrics/',
};

/**
 * 数据审核事件模板变量列表
 */
const getAuditEventTemp = {
  url: 'v3/datamanage/dataquality/event_template_variables/',
};

/**
 * 价值、收益比、热度、广度、重要度等评分分布接口
 *  @params (params): score_type // 类型
 */
const getScoreDistribution = {
  url: 'v3/datamanage/datastocktake/score_distribution/:score_type/',
  method: 'POST',
};

/**
 * 价值、重要度等评分趋势
 *  @params (params): score_type // 类型
 */
const getScoreTrend = {
  url: 'v3/datamanage/datastocktake/score_distribution/trend/:score_type/',
  method: 'POST',
};

/**
 * 价值、重要度等等级分布
 *  @params (params): score_type // 类型
 */
const getLevelDistribution = {
  url: 'v3/datamanage/datastocktake/level_distribution/:score_type/',
  method: 'POST',
};

/**
 * 总存储成本分布
 *  @params (params): score_type // 类型
 */
const getStorageCapacityDistribution = {
  url: 'v3/datamanage/datastocktake/cost_distribution/storage_capacity/',
  method: 'POST',
};

/**
 * 存储成本趋势
 *  @params (params): score_type // 类型
 */
const getStorageCapacityTrend = {
  url: 'v3/datamanage/datastocktake/cost_distribution/storage_trend/',
  method: 'POST',
};

/**
 * 数据质量事件子类型字典表
 *  @params (query): parent_event_type 默认为data_quality
 */
const getDataQualityEventTypes = {
  url: 'v3/datamanage/dataquality/event_types/',
};

/**
 * 查询修正规则配置
 *  @params (query): data_set_id: string 数据集id
 */
const getFixRuleConfig = {
  url: 'v3/datamanage/dataquality/correct_configs/',
};

/**
 * 生成修正SQL
 *  @params (params): data_set_id: string 数据集id
 *  @params (params): source_sql: string 原始SQL
 *  @params (params): correct_configs: Object 修正配置
 */
const getQualityFixSql = {
  url: 'v3/datamanage/dataquality/correct_configs/correct_sql/',
  method: 'POST',
};

/**
 * 数据修正填充模板列表
 *  @params (query): filling_template_name: string 判断模板名称
 *  @params (query): filling_template_alias: string 判断模板别名
 *  @params (query): filling_template_type: string 判断模板类型
 *  @params (query): filling_template_config: string 判断模板配置
 *  @params (query): description: string 模板描述
 *  @params (query): created_by: string 创建人
 *  @params (query): created_at: string 创建时间
 *  @params (query): updated_by: string 更新人
 *  @params (query): updated_at: string 更新时间
 */
const getFixFillTempList = {
  url: 'v3/datamanage/dataquality/correct_handlers/',
};

/**
 * 数据修正判断模板列表
 *  @params (query): condition_template_name: string 判断模板名称
 *  @params (query): condition_template_alias: string 判断模板别名
 *  @params (query): condition_template_type: string 判断模板类型
 *  @params (query): condition_template_config: string 判断模板配置
 *  @params (query): description: string 模板描述
 *  @params (query): created_by: string 创建人
 *  @params (query): created_at: string 创建时间
 *  @params (query): updated_by: string 更新人
 *  @params (query): updated_at: string 更新时间
 */
const getFixCorrectConditions = {
  url: 'v3/datamanage/dataquality/correct_conditions/',
};

/**
 * 提交修正SQL调试任务
 *  @params (params): source_data_set_id: string 上游数据集ID
 *  @params (params): source_sql: string 原始SQL
 *  @params (correct_configs): Object 修正配置
 */
const submitCorrectSql = {
  url: 'v3/datamanage/dataquality/correct_configs/debug/submit/',
  method: 'POST',
};

/**
 * 获取修正SQL调试任务
 *  @params (query): debug_request_id: string 调试请求ID
 */
const getCorrectSqlTask = {
  url: 'v3/datamanage/dataquality/correct_configs/debug/result/',
};

export {
  getQualityInfoOverview,
  getQualityInfoAuditRule,
  getQualityAuditRuleTable,
  queryQualityEventInfo,
  queryQualityEventTableData,
  queryQualityRuleFunc,
  queryQualityRuleIndex,
  queryQualityRuleTemplates,
  createQualityRules,
  updateQualityRules,
  deleteQualityRules,
  stopQualityRulesTask,
  startQualityRulesTask,
  lookQualityIndex,
  getAuditEventTemp,
  getScoreDistribution,
  getScoreTrend,
  getLevelDistribution,
  getStorageCapacityDistribution,
  getStorageCapacityTrend,
  getDataQualityEventTypes,
  getFixRuleConfig,
  getQualityFixSql,
  getFixFillTempList,
  getFixCorrectConditions,
  submitCorrectSql,
  getCorrectSqlTask,
};

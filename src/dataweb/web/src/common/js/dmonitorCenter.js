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

/* eslint-disable */
const alertTypeMappings = {
  task_monitor: window.gVue.$t('任务监控'),
  data_monitor: window.gVue.$t('数据监控'),
};

const alertLevelMappings = {
  warning: window.gVue.$t('警告'),
  danger: window.gVue.$t('严重'),
};

const alertTargetTypeMappings = {
  rawdata: window.gVue.$t('数据源'),
  dataflow: window.gVue.$t('数据开发任务'),
};

const alertCodeMappings = {
  task: window.gVue.$t('任务异常'),
  no_data: window.gVue.$t('数据源无数据'),
  data_time_delay: window.gVue.$t('数据时间延迟'),
  process_time_delay: window.gVue.$t('处理时间延迟'),
  data_drop: window.gVue.$t('无效数据'),
  data_trend: window.gVue.$t('数据波动'),
  data_interrupt: window.gVue.$t('数据中断'),
  data_loss: window.gVue.$t('数据丢失'),
  batch_delay: window.gVue.$t('离线任务延迟'),
};

const alertStatusMappings = {
  alerting: window.gVue.$t('告警中'),
  shielded: window.gVue.$t('已屏蔽'),
  converged: window.gVue.$t('已收敛'),
  summarizing: window.gVue.$t('汇总中'),
  recovered: window.gVue.$t('已恢复'),
};

const alertSendStatusMappings = {
  success: window.gVue.$t('已发送'),
  error: window.gVue.$t('发送失败'),
  init: window.gVue.$t('准备发送'),
  partial_error: window.gVue.$t('部分发送成功'),
};

const diffTrendMappings = {
  decrease: window.gVue.$t('减少'),
  increase: window.gVue.$t('增长'),
  both: window.gVue.$t('变化'),
};

const alertRuleDisplay = {
  en: {
    task: ({}) => '-',
    no_data: ({ no_data_interval }) => `No data is reported in ${no_data_interval / 60} minute(s).`,
    data_time_delay: ({ lasted_time, delay_time }) => `Data Time has been delayed exceeding ${delay_time / 60} minute(s) for ${lasted_time / 60} minute(s).`,
    process_time_delay: ({ lasted_time, delay_time }) => `Process Time has been delayed exceeding ${delay_time / 60} minute(s) for ${lasted_time / 60} minute(s).`,
    data_drop: ({ drop_rate }) => `The rate of invalid data exceeds ${drop_rate}% during data processing.`,
    data_trend: ({ diff_period, diff_trend, diff_count }) => `Compared to the same time ${diff_period} hour(s) ago, the amount of data is ${diffTrendMappings[diff_trend]} by ${diff_count} %`,
    data_interrupt: ({}) => '-',
    data_loss: ({}) => '-',
    batch_delay: ({ schedule_delay, execute_delay }) => `Batch task has been delayed exceeding ${((schedule_delay + execute_delay) / 60).toFixed(2)} minutes`,
  },
  'zh-cn': {
    task: ({}) => '-',
    no_data: ({ no_data_interval }) => `数据源持续${no_data_interval / 60}分钟没有数据输入`,
    data_time_delay: ({ lasted_time, delay_time }) => `数据时间持续${lasted_time / 60}分钟延迟超过${delay_time / 60}分钟`,
    process_time_delay: ({ lasted_time, delay_time }) => `数据处理持续${lasted_time / 60}分钟延迟超过${delay_time / 60}分钟`,
    data_drop: ({ drop_rate }) => `数据处理过程中无效数据率超过${drop_rate}%`,
    data_trend: ({ diff_period, diff_trend, diff_count }) => `数据相比${diff_period}小时前同一时刻${diffTrendMappings[diff_trend]}${diff_count}%`,
    data_interrupt: ({}) => '-',
    data_loss: ({}) => '-',
    batch_delay: ({ schedule_delay, execute_delay }) => `离线任务延迟超过${((schedule_delay + execute_delay) / 60).toFixed(2)}分钟`,
  },
};

const alertRuleDescription = {
  task: window.gVue.$t('清洗_入库_流式计算任务在执行过程中发生异常'),
  no_data: window.gVue.$t('持续一段时间内_数据源没有任何数据产生'),
  data_time_delay: window.gVue.$t('持续一段时间内_数据时间相对于本地时间延迟超过阈值'),
  process_time_delay: window.gVue.$t('持续一段时间内_处理时间延迟超过阈值'),
  data_drop: window.gVue.$t('数据在流转或处理过程中不符合计算逻辑或格式错误等原因而主动丢弃'),
  data_trend: window.gVue.$t('数据量相对上一周期相同时刻波动超过阈值'),
  data_interrupt: window.gVue.$t('任务在执行过程中被中断或无法跟踪任务执行情况'),
  data_loss: window.gVue.$t('数据在传输过程中出现上下游不一致的情况'),
  batch_delay: window.gVue.$t('离线任务在执行过程中出现延迟_没有在预期时间内输出结果'),
};

const flowRecoverTips = {
  task: [],
  no_data: [window.gVue.$t('数据接入源本身无数据'), window.gVue.$t('数据平台变更影响数据量统计结果_详情请关注数据平台用户群公告'), window.gVue.$t('采集器故障_可企业微信联系蓝鲸助手排查问题')],
  data_time_delay: [window.gVue.$t('当前上报数据包含历史数据'), window.gVue.$t('上游结点数据处理延迟较大'), window.gVue.$t('任务资源不足_无法及时处理所有数据'), window.gVue.$t('数据平台变更_详情请关注数据平台用户群公告')],
  process_time_delay: [window.gVue.$t('任务资源不足_无法及时处理所有数据'), window.gVue.$t('数据平台变更_详情请关注数据平台用户群公告')],
  data_drop: [window.gVue.$t('具体数据无效原因请参考数据质量指标标签页')],
  data_trend: [],
  data_interrupt: [],
  data_loss: [],
  batch_delay: [],
};

export { alertTypeMappings, alertLevelMappings, alertCodeMappings, alertStatusMappings, alertSendStatusMappings, alertRuleDescription, alertRuleDisplay, alertTargetTypeMappings, flowRecoverTips };

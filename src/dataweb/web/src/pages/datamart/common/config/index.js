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

const dataTypeIds = {
  // key为数据类型，value为对应的id
  raw_data: 'data_id',
  result_table: 'result_table_id',
  tdw_table: 'tdw_table_id',
};

const sensitiveObj = {
  public: window.$t('公开'),
  private: window.$t('业务私有'),
  confidential: window.$t('业务机密'),
  topsecret: window.$t('业务绝密'),
};

const dataStatus = {
  init: window.$t('准备中'),
  running: window.$t('运行中'),
  finish: window.$t('完成'),
};

// 高级搜索评分相关字段
const dataValueOperateMap = {
  heat_operate: 'heat_score',
  asset_value_operate: 'asset_value_score',
  storage_capacity_operate: 'storage_capacity',
  range_operate: 'range_score',
  importance_operate: 'importance_score',
  assetvalue_to_cost_operate: 'assetvalue_to_cost',
};
// 'rgba(31,119,180,1.00)', // muted blue
// 'rgba(255,127,14,1.00)', // safety orange
// 'rgba(44,160,44,1.00)', // cooked asparagus green
// 'rgba(214,39,40,1.00)', // brick red
// 'rgba(148,103,189,1.00)', // muted purple
// 'rgba(140,86,75,1.00)', // chestnut brown
// 'rgba(227,119,194,1.00)', // raspberry yogurt pink
// 'rgba(127,127,127,1.00)', // middle gray
// 'rgba(188,189,34,1.00)', // curry yellow-green
// 'rgba(23,190,207,1.00)' // blue-teal
// plotly图表默认配色
const chartColors = [
  // '#1f77b4', // muted blue
  // '#ff7f0e', // safety orange
  // '#2ca02c', // cooked asparagus green
  // '#d62728', // brick red
  // '#9467bd', // muted purple
  // '#8c564b', // chestnut brown
  // '#e377c2', // raspberry yogurt pink
  // '#7f7f7f', // middle gray
  // '#bcbd22', // curry yellow-green
  // '#17becf' // blue-teal
  '#5B8FF9',
  '#5AD8A6',
  '#5D7092',
  '#F6BD16',
  '#E8684A',
  '#6DC8EC',
  '#9270CA',
  '#FF9D4D',
  '#269A99',
  '#FF99C3',
];

module.exports = {
  dataTypeIds,
  sensitiveObj,
  dataStatus,
  dataValueOperateMap,
  chartColors,
};

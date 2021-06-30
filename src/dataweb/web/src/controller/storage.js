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

import Meta from './meta';
class Storage {
  foramtStorageScenarioConfigs(data) {
    const result = {};
    (data
            && data.forEach((item) => {
              if (!result[item.storage_scenario_alias]) {
                result[item.storage_scenario_alias] = {
                  id: item.storage_scenario_name,
                  name: item.storage_scenario_alias,
                  storage_scenario_priority: item.storage_scenario_priority,
                  cluster_type_priority: item.cluster_type_priority,
                  active: item.active,
                  children: [
                    {
                      id: item.cluster_type,
                      name: item.formal_name,
                      description: item.description,
                      disabled: item.cluster_type === 'tredis',
                    },
                  ],
                };
              } else {
                result[item.storage_scenario_alias].children.push({
                  id: item.cluster_type,
                  name: item.formal_name,
                  description: item.description,
                  disabled: item.cluster_type === 'tredis',
                });
              }
            }))
            || [];

    return Object.keys(result)
      .map(key => result[key])
      .sort((a, b) => b.cluster_type_priority - a.cluster_type_priority)
      .sort((a, b) => b.storage_scenario_priority - a.storage_scenario_priority);
  }

  foramtDataSourceList(data) {
    const rawsourceMapList = {
      raw_data: window.$t('原始数据'),
      clean: window.$t('清洗数据'),
    };
    const result = (
      (data
                && data.map(item => ({
                  id: item.data_type,
                  name: rawsourceMapList[item.data_type],
                  children: item.sub_data.map(it => ({
                    id: it.id,
                    name: it.data_name,
                    name_alias: it.data_alias,
                    data_type: item.data_type,
                  })),
                })))
            || []
    ).filter(item => item.children && item.children.length);
    return result;
  }

  formatRawdataFields(data, option) {
    const meta = new Meta();
    const { clusterType } = option.params;
    return meta.extendSqlSchmaFields(data, clusterType);
  }
}

export default Storage;

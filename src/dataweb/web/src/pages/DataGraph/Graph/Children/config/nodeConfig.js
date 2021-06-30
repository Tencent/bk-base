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

import extend from '@/extends/index';

export const getNodeConfig = async () => {
  const { nodeConfigExtend } = await extend.callJsFragmentFn('NodeConfig', this) || { nodeConfigExtend: {} };
  const nodeConfig = Object.assign({
    offline: {
      tabConfig: [
        {
          label: window.$t('基础配置'),
          name: 'base-config',
          disabled: false,
          component: () => import('@GraphChild/Common/Basic.vue'),
        },
        {
          label: window.$t('高级配置'),
          name: 'advanced',
          disabled: false,
          component: () => import('@GraphChild/OfflineNode/Advanced.vue'),
        },
        {
          label: window.$t('执行记录'),
          name: 'execute-record',
          disabled: 'dynamic',
          component: () => import('@GraphChild/OfflineNode/ExecutionRecord.vue'),
        },
      ],
      config: {
        bk_biz_id: '',
        name: '',
        sql: '',
        from_nodes: [],
        outputs: [],
        advanced: {
          start_time: '',
          self_dependency: false,
          recovery_enable: false,
          recovery_times: 1,
          recovery_interval: '5m',
          max_running_task: -1,
          active: false,
          self_dependency_config: {
            dependency_rule: 'self_finished',
            fields: [],
          },
        },
        window_type: 'fixed', // 必须传以下参数
        dependency_config_type: 'unified',
        fallback_window: 1, // 窗口长度
        fixed_delay: 0, // 延迟时间
        // window_type=accumulate_by_hour 必须传以下参数
        data_start: 0, // 数据起点
        data_end: 23, // 数据终点
        delay: '0', // 延迟时间
        count_freq: 1, // 统计频率的值，固定，不能修改
        schedule_period: 'day', // 统计频率的单位，固定，不能修改
        delay_period: 'hour', // 统计延迟单位
        unified_config: {
          window_size: 1,
          window_size_period: 'day', // 窗口长度的单位
          window_delay: 0,
          dependency_rule: 'all_finished',
        },
        custom_config: {},
        data_correct: {
          is_open_correct: false,
          correct_configs: [
            {
              correct_config_item_id: null,
              field: 'log',
              correct_config_detail: {
                rules: [
                  {
                    condition: {
                      condition_name: 'custom_sql_condition',
                      condition_type: 'custom',
                      condition_value: '',
                    },
                    handler: {
                      handler_name: 'fixed_filling',
                      handler_type: 'fixed',
                      handler_value: 100,
                    },
                  },
                ],
                output: {
                  generate_new_field: false,
                  new_field: 'log',
                },
              },
              correct_config_alias: null,
              created_by: 'admin',
              created_at: '2020-07-27 10:30:00',
              updated_by: 'admin',
              updated_at: '2020-07-27 10:31:00',
              description: '',
            },
          ],
        },
      },
      output: 'singleOutput',
      hasProcessName: false,
      // 是否展示数据修正
      isShowDataFix: true,
    },
    realtime: {
      tabConfig: [
        {
          label: window.$t('节点配置'),
          name: 'base-config',
          disabled: false,
          component: () => import('@GraphChild/Realtime/Basic.vue'),
        },
      ],
      config: {
        outputs: [],
        from_nodes: [],
        output_name: '',
        table_name: '',
        name: '',
        bk_biz_id: 0,
        sql: '',
        window_type: 'none',
        count_freq: 30,
        window_time: 10,
        waiting_time: 0,
        session_gap: 0,
        expired_time: 0,
        window_lateness: {
          allowed_lateness: false,
          lateness_time: 1,
          lateness_count_freq: 60,
        },
        data_correct: {
          is_open_correct: false,
          correct_configs: [
            {
              correct_config_item_id: null,
              field: 'log',
              correct_config_detail: {
                rules: [
                  {
                    condition: {
                      condition_name: 'custom_sql_condition',
                      condition_type: 'custom',
                      condition_value: '',
                    },
                    handler: {
                      handler_name: 'fixed_filling',
                      handler_type: 'fixed',
                      handler_value: 100,
                    },
                  },
                ],
                output: {
                  generate_new_field: false,
                  new_field: 'log',
                },
              },
              correct_config_alias: null,
              created_by: 'admin',
              created_at: '2020-07-27 10:30:00',
              updated_by: 'admin',
              updated_at: '2020-07-27 10:31:00',
              description: '',
            },
          ],
        },
      },
      output: 'singleOutput',
      hasProcessName: false,
      // 是否展示数据修正
      isShowDataFix: true,
    },
    split: {
      tabConfig: [],
      config: {
        outputs: [],
        from_nodes: [],
        from_result_table_ids: [],
        output_name: '',
        table_name: '',
        name: '',
        bk_biz_id: 0,
        description: '',
        config: [
          {
            bk_biz_id: '',
            logic_exp: '',
            display_bk_biz_id: '',
          },
        ],
      },
      // 自定义 output 组件，定义了该参数无效化默认的 output
      customOutputComponent: () => import('@GraphChild/Split/SplitOutput.vue'),
      output: 'singleOutput',
      hasProcessName: false,
    },
    merge: {
      tabConfig: [],
      config: {
        outputs: [],
        from_nodes: [],
        from_result_table_ids: [],
        output_name: '',
        table_name: '',
        name: '',
        bk_biz_id: 0,
        description: '',
      },
      output: 'singleOutput',
      hasProcessName: false,
    },
  }, nodeConfigExtend);

  return nodeConfig;
};

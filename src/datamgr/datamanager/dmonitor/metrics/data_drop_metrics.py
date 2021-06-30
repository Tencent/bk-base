# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
License for BK-BASE 蓝鲸基础平台:
--------------------------------------------------------------------
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
import copy
import json
import logging
import time

from gevent import monkey

from dmonitor.base import BaseDmonitorTaskGreenlet
from dmonitor.settings import DMONITOR_TOPICS

monkey.patch_all()


def data_drop_metrics():
    logging.info('Start to execute generating data drop metrics task')

    task_configs = {
        'consumer_configs': {
            'type': 'kafka',
            'alias': 'op',
            'topic': 'dmonitor_drop_audit',
            'partition': False,
            'group_id': 'dmonitor',
            'batch_message_max_count': 10000,
            'batch_message_timeout': 5,
        },
        'task_pool_size': 50,
    }

    try:
        task = DataDropMetricsTaskGreenlet(configs=task_configs)
        task.start()
        task.join()
    except Exception as e:
        logging.error('Raise exception({error}) when init drop metrics task'.format(error=e), exc_info=True)


class DataDropMetricsTaskGreenlet(BaseDmonitorTaskGreenlet):
    GENERATE_INTERVAL = 60
    PENDING_TIME = 60

    def __init__(self, *args, **kwargs):
        """初始化生成数据丢弃指标的任务

        :param task_configs: 缓存同步任务配置
            {
                'consumer_configs': {
                    'type': 'kafka',
                    'alias': 'op',
                    'topic': 'bkdata_data_monitor_metrics591',
                    'partition': False,
                    'group_id': 'dmonitor',
                    'batch_message_max_count': 5000,
                    'batch_message_timeout': 0.1,
                },
                'task_pool_size': 100,
            }
        """
        configs = kwargs.pop('configs', {})

        super(DataDropMetricsTaskGreenlet, self).__init__(*args, **kwargs)

        self.init_consumer(configs.get('consumer_configs'))
        self.init_task_pool(configs.get('task_pool_size'))

        now = time.time()

        self._metric_cache = {
            'inputs': {},
            'outputs': {},
            'drops': {}
        }
        self._last_generate_time = now + self.PENDING_TIME

    def refresh_metadata_cache(self, now):
        """刷新埋点解析依赖的元数据信息

        :param now: 当前刷新缓存的时间
        """
        pass

    def handle_monitor_value(self, message, now):
        """处理各个模块上报的任务埋点

        :param message: 延迟原始指标
            {
                "time": 1542960360.000001,
                "database": "monitor_data_metrics",
                "data_loss_input_total": {
                    "data_cnt": 100,
                    "data_inc": 10,
                    "tags": {
                        "module": "stream",
                        "component": "flink",
                        "cluster": null,
                        "storage": "channel_11",
                        "logical_tag": "591_test1119str",
                        "physical_tag": "171_1fe25fadfef54a4899d781fc9d1e55d3|591_test1119str|0"
                    }
                }
            },
            {
                "time": 1542960360.000001,
                "database": "monitor_data_metrics",
                "data_loss_output_total": {
                    "data_cnt": 100,
                    "data_inc": 10,
                    "tags": {
                        "module": "stream",
                        "component": "flink",
                        "cluster": null,
                        "storage": "channel_11",
                        "logical_tag": "591_test1119str",
                        "physical_tag": "171_1fe25fadfef54a4899d781fc9d1e55d3|591_test1119str|0"
                    }
                }
            },
            {
                "time": 1542960360.000001,
                "database": "monitor_data_metrics",
                "data_loss_drop" {
                    "data_cnt": 100,
                    "reason": "xxx",
                    "tags": {
                        "module": "stream",
                        "component": "flink",
                        "cluster": null,
                        "storage": "channel_11",
                        "logical_tag": "591_test1119str",
                        "physical_tag": "171_1fe25fadfef54a4899d781fc9d1e55d3|591_test1119str|0"
                    }
                }
            }
        :param now: 当前处理数据的时间
        """
        try:
            if 'data_loss_input_total' in message:
                message_info = message['data_loss_input_total']
                point_key = self.generate_metric_key(message_info.get('tags', {}))

                if point_key not in self._metric_cache['inputs']:
                    self._metric_cache['inputs'][point_key] = 0
                self._metric_cache['inputs'][point_key] += message_info['data_inc']
            elif 'data_loss_output_total' in message:
                message_info = message['data_loss_output_total']
                point_key = self.generate_metric_key(message_info.get('tags', {}))

                if point_key not in self._metric_cache['outputs']:
                    self._metric_cache['outputs'][point_key] = 0
                self._metric_cache['outputs'][point_key] += message_info['data_inc']
            elif 'data_loss_drop' in message:
                message_info = message['data_loss_drop']
                tags = message_info.get('tags', {})
                point_key = self.generate_metric_key(tags)

                if point_key not in self._metric_cache['drops']:
                    self._metric_cache['drops'][point_key] = {
                        'time': message.get('time', time.time()),
                        'module': tags.get('module', ''),
                        'component': tags.get('component', ''),
                        'cluster': tags.get('cluster', ''),
                        'logical_tag': tags.get('logical_tag', ''),
                        'storage': tags.get('storage', ''),
                        'tags': tags,
                        'metrics': []
                    }
                self._metric_cache['drops'][point_key]['metrics'].append({
                    'drop_tag': tags.get('drop_tag', ''),
                    'reason': message_info.get('reason', ''),
                    'data_cnt': message_info.get('data_cnt', 0),
                })

        except Exception as e:
            logging.error('Combine data error: %s, message: %s' % (e, json.dumps(message)))

    def generate_metric_key(self, tags):
        """生成单个指标的标志key

        :param tags: 维度字典
        """
        module = tags.get('module', '')
        component = tags.get('component', '')
        cluster = tags.get('cluster', '')
        logical_tag = tags.get('logical_tag', '')
        storage = tags.get('storage', '')
        point_key = '{}_{}_{}_{}_{}'.format(module, component, cluster, logical_tag, storage)
        return point_key

    def do_monitor(self, now, task_pool):
        """执行衍生指标或者监控的逻辑

        :param now: 当前时间戳
        :param task_pool: 任务处理协程池
        """
        if now - self._last_generate_time > self.GENERATE_INTERVAL:
            self.generate_drop_metrics()
            self.clear_cache_metrics()
            self._last_generate_time = now

    def generate_drop_metrics(self):
        """生成延迟指标

        :param now: 当前时间戳
        """
        for drop_point, drop_info in self._metric_cache['drops'].items():
            drop_metrics = drop_info['metrics']
            all_drop_cnt = 0
            drop_codes = {}

            for drop_metric in drop_metrics:
                drop_code = drop_metric['drop_tag']
                if drop_code not in drop_codes:
                    drop_codes[drop_code] = {
                        'drop_code': drop_code,
                        'reason': drop_metric['reason'],
                        'drop_cnt': 0
                    }
                drop_codes[drop_code]['drop_cnt'] += drop_metric['data_cnt']
                all_drop_cnt += drop_metric['data_cnt']

            if all_drop_cnt < 1:
                continue

            input_cnt = self._metric_cache['inputs'].get(drop_point, 0)
            output_cnt = self._metric_cache['outputs'].get(drop_point, 0)
            target_cnt = input_cnt if input_cnt >= output_cnt else output_cnt
            if target_cnt < 1:
                continue

            if all_drop_cnt > target_cnt:
                # 丢弃量大于输入输出数据量
                target_cnt = all_drop_cnt

            drop_rate = int(all_drop_cnt * 100 / target_cnt)

            metric_info = {
                'time': drop_info['time'],
                'database': 'monitor_data_metrics',
                'data_loss_drop_rate': {
                    'drop_cnt': all_drop_cnt,
                    'message_cnt': target_cnt,
                    'drop_rate': drop_rate,
                    'drop_detail': json.dumps(drop_codes),
                    'tags': {
                        'module': drop_info['module'],
                        'component': drop_info['component'],
                        'cluster': drop_info['cluster'],
                        'storage': drop_info['storage'],
                        'logical_tag': drop_info['logical_tag'],
                        'data_set_id': drop_info['logical_tag'],
                    }
                }
            }
            metric_info['data_loss_drop_rate']['tags'].update(drop_info['tags'])

            for drop_code, drop_detail in drop_codes.items():
                tags = copy.deepcopy(metric_info['data_loss_drop_rate']['tags'])
                tags['reason'] = drop_detail['reason']
                tags['drop_code'] = drop_detail['drop_code']
                metric_detail_info = {
                    'time': drop_info['time'],
                    'database': 'monitor_data_metrics',
                    'data_loss_drop_detail': {
                        'drop_cnt': drop_detail['drop_cnt'],
                        'tags': tags
                    }
                }
                self.produce_metric(DMONITOR_TOPICS['data_cleaning'], json.dumps(metric_detail_info))

            message = json.dumps(metric_info)
            self.produce_metric(
                DMONITOR_TOPICS['data_cleaning'], message
            )
            self.produce_metric(
                DMONITOR_TOPICS['data_drop_metric'], message
            )

    def clear_cache_metrics(self):
        """清理缓存的指标
        """
        self._metric_cache = {
            'inputs': {},
            'outputs': {},
            'drops': {}
        }

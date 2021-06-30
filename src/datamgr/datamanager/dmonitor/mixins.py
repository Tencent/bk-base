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
import json
import logging

from gevent import pool, monkey

from api import datamanage_api
from common.mixins.meta_cache_mixin import MetaCacheMixin
from common.redis import connections
from utils.time import strtotime

from dmonitor.settings import META_CACHE_CONFIGS, GEOG_AREA_CODE

monkey.patch_all()


class DmonitorMetaCacheMixin(MetaCacheMixin):
    """数据监控元数据刷新Mixin，主要用于从Redis中同步最新的元数据信息
    """
    SYNC_POOL_SIZE = 100

    def __init__(self, *args, **kwargs):
        super(DmonitorMetaCacheMixin, self).__init__(*args, **kwargs)

    def fetch_from_redis(self, redis_conn, key, result_id, result):
        """从缓存中获取元数据信息，并写到结果集中

        :param redis_conn: 缓存连接
        :param key: 需要获取的缓存key
        :pararm result_id: 组装成字典后的主键
        :param result: 结果集
        """
        redis_content = redis_conn.get(key)

        if redis_content:
            try:
                redis_data = json.loads(redis_content)
                result[result_id] = redis_data
            except Exception as e:
                logging.error(e, exc_info=True)

    def fetch_data_set_infos_from_redis(self):
        data_sets = {}
        sync_pool = pool.Pool(self.SYNC_POOL_SIZE)
        redis_conn = connections['default']

        key_sets = META_CACHE_CONFIGS['data_set']['key_sets']
        key_template = META_CACHE_CONFIGS['data_set']['key_template']

        data_set_ids = redis_conn.smembers(key_sets)
        for data_set_id in data_set_ids:
            data_set_key = key_template.format(data_set_id)
            sync_pool.spawn(self.fetch_from_redis, redis_conn, data_set_key, data_set_id, data_sets)

        sync_pool.join()
        return data_sets

    def fetch_data_set_by_id_from_redis(self, data_set_id):
        redis_conn = connections['default']
        key_template = META_CACHE_CONFIGS['data_set']['key_template']

        data_set_key = key_template.format(data_set_id)

        try:
            return json.loads(redis_conn.get(data_set_key))
        except Exception as e:
            logging.error(e, exc_info=True)
            return {}

    def fetch_data_operations_from_redis(self):
        data_operations = {}
        sync_pool = pool.Pool(self.SYNC_POOL_SIZE)
        redis_conn = connections['default']

        key_sets = META_CACHE_CONFIGS['data_operation']['key_sets']
        key_template = META_CACHE_CONFIGS['data_operation']['key_template']

        data_operation_ids = redis_conn.smembers(key_sets)
        for data_operation_id in data_operation_ids:
            data_operation_key = key_template.format(data_operation_id)
            sync_pool.spawn(self.fetch_from_redis, redis_conn, data_operation_key, data_operation_id, data_operations)

        sync_pool.join()
        return data_operations

    def fetch_flow_infos_from_redis(self):
        flow_infos = {}
        sync_pool = pool.Pool(self.SYNC_POOL_SIZE)
        redis_conn = connections['default']

        key_sets = META_CACHE_CONFIGS['flow']['key_sets']
        key_template = META_CACHE_CONFIGS['flow']['key_template']

        flow_ids = redis_conn.smembers(key_sets)
        for flow_id in flow_ids:
            flow_key = key_template.format(flow_id)
            sync_pool.spawn(self.fetch_from_redis, redis_conn, flow_key, flow_id, flow_infos)

        sync_pool.join()
        return flow_infos

    def fetch_dataflow_infos_from_redis(self):
        dataflow_infos = {}
        sync_pool = pool.Pool(self.SYNC_POOL_SIZE)
        redis_conn = connections['default']

        key_sets = META_CACHE_CONFIGS['dataflow']['key_sets']
        key_template = META_CACHE_CONFIGS['dataflow']['key_template']

        dataflow_ids = redis_conn.smembers(key_sets)
        for dataflow_id in dataflow_ids:
            try:
                dataflow_id = int(dataflow_id)
            except ValueError:
                continue
            flow_key = key_template.format(dataflow_id)
            sync_pool.spawn(self.fetch_from_redis, redis_conn, flow_key, dataflow_id, dataflow_infos)

        sync_pool.join()
        return dataflow_infos

    def fetch_alert_configs(self):
        """获取数据监控告警配置
        """
        try:
            res = datamanage_api.alert_configs.list({
                'tags': [GEOG_AREA_CODE],
                'active': True
            })
            return res.data if res.is_success() else []
        except Exception as e:
            logging.error(e, exc_info=True)
            return []

    def fetch_disabled_alert_configs(self, recent_updated=300):
        try:
            res = datamanage_api.alert_configs.list({
                'tags': [GEOG_AREA_CODE],
                'active': False,
                'recent_updated': recent_updated,
            })
            return res.data if res.is_success() else []
        except Exception as e:
            logging.error(e, exc_info=True)
            return []

    def fetch_alert_shields(self):
        """获取告警屏蔽配置
        """
        try:
            res = datamanage_api.alert_shields.in_effect()
            alert_shields = res.data if res.is_success() else []
            for alert_shield in alert_shields:
                alert_shield['start_time'] = strtotime(alert_shield['start_time'])
                alert_shield['end_time'] = strtotime(alert_shield['end_time'])
            return alert_shields
        except Exception as e:
            logging.error(e, exc_info=True)
            return []

    def gen_logical_key(self, tags):
        """生成数据流逻辑标识的key

        :param tags: 维度字典

        :return: 数据流逻辑标识
        """
        return '{module}_{component}_{logical_tag}'.format(**tags)

    def gen_unique_key(self, tags):
        """用所有tags生成唯一key

        :param tags: 所有维度

        :return: 唯一key
        """
        keys = sorted(tags.keys())
        return '_'.join(map(lambda x: str(tags[x]), keys))

    def gen_receivers(self, alert_config):
        """从告警配置中的接收者配置生成告警接收者列表

        :param alert_config: 告警配置

        :return: 告警接收者列表
        """
        receivers = []
        for receiver_config in alert_config.get('receivers', []):
            receivers.append(self.gen_receiver(receiver_config))
        return receivers

    def gen_receiver(self, receiver_config):
        """根据接收者配置生成接收者列表

        :param receiver_config: 接收者配置

        :return: 接收者列表
        """
        if receiver_config['receiver_type'] == 'user':
            return receiver_config['username']
        elif receiver_config['receiver_type'] == 'list':
            return ','.join(receiver_config['userlist'])
        else:
            return json.dumps(receiver_config)

    def gen_notify_ways(self, alert_config):
        """根据告警配置中的通知配置生成通知方式列表

        :param notify_config: 通知方式配置

        :return: 通知方式列表
        """
        return alert_config.get('notify_config', [])

    def get_flow_node_by_target(self, target):
        """从告警配置的target配置中获取flow_id和node_id

        :param target: 告警配置的告警对象配置

        :return: 数据流ID和数据流节点ID
        """
        flow_id, node_id = None, None
        if target['target_type'] == 'dataflow':
            flow_id = int(target.get('flow_id') or 0) or None
            node_id = int(target.get('node_id') or 0) or None
        elif target['target_type'] == 'rawdata':
            flow_id = 'rawdata%s' % target.get('raw_data_id')
            node_id = target.get('data_set_id')
        return flow_id, node_id

    def get_flow_display(self, flow_info, language='zh-cn'):
        """根据数据流信息和语言配置生成数据流的展示方式

        :param flow_info: 数据流信息

        :return: 数据流展示文本
        """
        RAW_DATA_DISPLAY = {
            'zh-cn': '数据源',
            'en': 'Raw Data',
        }
        TASK_DISPLAY = {
            'zh-cn': '数据开发任务',
            'en': 'DataFlow Task',
        }
        if flow_info.get('flow_type') == 'rawdata':
            return RAW_DATA_DISPLAY[language], flow_info.get('raw_data_alias')
        else:
            return TASK_DISPLAY[language], flow_info.get('flow_name')

    def get_logical_tag_display(self, logical_tag, tags=None, flow_info={}):
        """生成数据流逻辑标识展示信息

        :param logical_tag: 数据流逻辑标识
        :param tags: 维度字典
        :param flow_info: 数据流信息

        :return: 逻辑标识展示文本
        """
        if logical_tag.isdigit():
            entity_display = '数据源'
            entity_display_en = 'RawData'

            if 'flow_name' in flow_info:
                entity_display = '{}[{}]'.format(entity_display, flow_info['flow_name'])
                entity_display_en = '{}[{}]'.format(entity_display_en, flow_info['flow_name'])
        else:
            flow_type_display, flow_name = self.get_flow_display(flow_info)
            flow_type_display_en, flow_name_en = self.get_flow_display(flow_info, 'en')
            flow_display = '{}[{}]'.format(flow_type_display, flow_name)
            flow_display_en = '{}[{}]'.format(flow_type_display_en, flow_name_en)

            TASK_DISPLAYS = {
                'clean': {
                    'zh-cn': '清洗任务',
                    'en': 'Clean task',
                },
                'stream': {
                    'zh-cn': '实时节点',
                    'en': 'Stream task',
                },
                'realtime': {
                    'zh-cn': '实时节点',
                    'en': 'Stream task',
                },
                'batch': {
                    'zh-cn': '离线节点',
                    'en': 'Batch task',
                },
                'shipper': {
                    'zh-cn': '入库任务',
                    'en': 'Shipper task',
                }
            }
            if flow_info.get('flow_type') == 'rawdata':
                node_info = flow_info.get('nodes', {}).get(logical_tag, {})
                node_name = node_info.get('clean_config_name')
            else:
                node_id = str(tags.get('node_id'))
                node_info = flow_info.get('nodes', {}).get(node_id, {})
                node_name = node_info.get('node_name')

            if tags is not None:
                module = tags.get('module')
                entity_display = '{}[{}]'.format(TASK_DISPLAYS.get(module, {}).get('zh-cn', '任务'), node_name)
                entity_display_en = '{}[{}]'.format(TASK_DISPLAYS.get(module, {}).get('en', 'Task'), node_name)
                if module == "shipper":
                    entity_display = "{}({})".format(
                        entity_display, tags.get("storage_cluster_type")
                    )
                    entity_display_en = "{}({})".format(
                        entity_display_en, tags.get("storage_cluster_type")
                    )
            else:
                entity_display = '任务[{}]'.format(node_name)
                entity_display_en = 'Task[{}]'.format(node_name)

            entity_display = '{}-{}'.format(flow_display, entity_display)
            entity_display_en = '{}-{}'.format(flow_display_en, entity_display_en)
        return entity_display, entity_display_en

    def get_flow_node_display(self, flow_info, node_info):
        """生成数据流节点的展示文本

        :param flow_info: 数据流信息
        :param node_info: 数据流节点信息

        :return: 数据流节点的展示文本
        """
        entity_display = '任务({flow_name})'.format(flow_name=flow_info.get('flow_name'))
        entity_display_en = 'Task({flow_name})'.format(flow_name=flow_info.get('flow_name'))

        if len(node_info.keys()) > 0:
            entity_display = '{flow}节点({node_name})'.format(flow=entity_display, node_name=node_info.get('node_name'))
            entity_display_en = '{flow} Node({node_name})'.format(
                flow=entity_display_en, node_name=node_info.get('node_name')
            )
        return entity_display, entity_display_en

    def get_data_operation_display(self, data_operation):
        """生成数据处理或者数据传输的展示文本

        :param data_operation: 数据处理或者数据传输配置

        :return: 展示文本
        """
        data_processing_display = '数据处理任务({processing_id})'
        data_processing_display_en = 'Data Processing Task({processing_id})'
        data_transferring_display = '数据传输任务({transferring_id})'
        data_transferring_display_en = 'Data Transferring Task({transferring_id})'

        if data_operation.get('data_operation_type') == 'data_processing':
            processing_id = data_operation.get('processing_id')
            return (data_processing_display.format(processing_id=processing_id),
                    data_processing_display_en.format(processing_id=processing_id))
        elif data_operation.get('data_operation_type') == 'data_transferring':
            transferring_id = data_operation.get('transferring_id')
            return (data_transferring_display.format(transferring_id=transferring_id),
                    data_transferring_display_en.format(transferring_id=transferring_id))

    def gen_target_id(self, target):
        """生成告警对象唯一ID

        :param target: 告警对象配置

        :return: 告警对象唯一ID
        """
        target_items = [target.get('target_type')]
        flow_id, node_id = self.get_flow_node_by_target(target)
        if flow_id:
            target_items.append('%s=%s' % ('flow_id', str(flow_id)))
        if 'dimensions' in target:
            for key, value in target['dimensions'].items():
                target_items.append('%s=%s' % (key, value))
        return '&'.join(target_items)

    def convert_display_time(self, seconds, target_unit=None, precision='minute'):
        """转换时间展示

        :param seconds: 秒数
        :param target_unit: 转换目标单位
        :param precision: 转换的精度

        :return: 时间展示文本
        """
        seconds = int(seconds)
        last_times = {
            'seconds': 0,
            'minutes': 0,
            'hours': 0,
        }
        if target_unit == 'second' or (target_unit is None and seconds < 60):
            time_display = '{seconds}秒'.format(seconds=seconds)
            time_display_en = '{seconds}s'.format(seconds=seconds)
            return time_display, time_display_en

        minutes = seconds // 60
        last_times['seconds'] = seconds - minutes * 60

        if target_unit == 'minute' or (target_unit is None and minutes < 60):
            return self.format_as_minute(minutes, precision, last_times)

        hours = minutes // 60
        last_times['minutes'] = minutes - hours * 60

        if target_unit == 'hour' or (target_unit is None and hours < 24):
            return self.format_as_hour(hours, precision, last_times)

        days = hours // 24
        last_times['hours'] = hours - days * 24

        return self.format_as_day(days, precision, last_times)

    def format_as_minute(self, minutes, precision, last_times):
        time_display = '{minutes}分钟'.format(minutes=minutes) if minutes != 0 else ''
        time_display_en = '{minutes}m'.format(minutes=minutes) if minutes != 0 else ''
        if precision == 'second':
            sec_display, sec_display_en = self.convert_display_time(last_times['seconds'], 'second', 'second')
            time_display = time_display + sec_display
            time_display_en = time_display_en + sec_display_en
        return time_display, time_display_en

    def format_as_hour(self, hours, precision, last_times):
        time_display = '{hours}小时'.format(hours=hours) if hours != 0 else ''
        time_display_en = '{hours}h'.format(hours=hours) if hours != 0 else ''
        if precision in ('minute', 'second'):
            min_display, min_display_en = self.convert_display_time(last_times['minutes'] * 60, 'minute', 'minute')
            time_display = time_display + min_display
            time_display_en = time_display_en + min_display_en
        if precision == 'second':
            sec_display, sec_display_en = self.convert_display_time(last_times['seconds'], 'second', 'second')
            time_display = time_display + sec_display
            time_display_en = time_display_en + sec_display_en
        return time_display, time_display_en

    def format_as_day(self, days, precision, last_times):
        time_display = '{days}天'.format(days=days) if days != 0 else ''
        time_display_en = '{days}d'.format(days=days) if days != 0 else ''
        if precision in ('second', 'minute', 'hour'):
            hour_display, hour_display_en = self.convert_display_time(last_times['hours'] * 3600, 'hour', 'hour')
            time_display = time_display + hour_display
            time_display_en = time_display_en + hour_display_en
        if precision in ('minute', 'second'):
            min_display, min_display_en = self.convert_display_time(last_times['minutes'] * 60, 'minute', 'minute')
            time_display = time_display + min_display
            time_display_en = time_display_en + min_display_en
        if precision == 'second':
            sec_display, sec_display_en = self.convert_display_time(last_times['seconds'], 'second', 'second')
            time_display = time_display + sec_display
            time_display_en = time_display_en + sec_display_en
        return time_display, time_display_en

    def get_data_set_hash_value(self, data_set_id):
        """获取数据集的hash值

        :param data_set_id 数据集ID
        """
        cnt = 0
        for char in data_set_id:
            cnt += ord(char)
        return cnt

    def fetch_batch_executions_by_time(self, recent_time=300):
        """获取最近一段时间范围内的离线执行记录

        :param recent_time: 最近一段时间
        """
        try:
            res = datamanage_api.dmonitor_batch_executions.by_time({'recent_time': recent_time}, raise_exception=True)
            return res.data if res.is_success() else {}
        except Exception as e:
            logging.error(e, exc_info=True)
            return {}

    def fetch_batch_latest_executions(self, processing_ids):
        try:
            res = datamanage_api.dmonitor_batch_executions.latest({
                'processing_ids': processing_ids}, raise_exception=True)
            return res.data if res.is_success() else {}
        except Exception as e:
            logging.error(e, exc_info=True)
            return {}

    def fetch_batch_schedules(self, processing_ids):
        try:
            res = datamanage_api.dmonitor_batch_schedules({
                'processing_ids': processing_ids}, raise_exception=True)
            return res.data if res.is_success() else {}
        except Exception as e:
            logging.error(e, exc_info=True)
            return {}

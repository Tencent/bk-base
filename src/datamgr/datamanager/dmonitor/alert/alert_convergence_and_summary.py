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
import time
from collections import Counter

import gevent
from gevent import monkey

from api import datamanage_api
from common.mixins.db_write_mixin import DbWriteMixin
from utils.time import timetostr

from dmonitor.alert.alert_codes import (
    AlertCode, AlertLevel, AlertStatus, AlertSendStatus,
    RECEIVER_TYPE_SUPPORT_MAPPINGS, RECEIVER_TYPE_NOT_SUPPORT_MAPPINGS
)
from dmonitor.metrics.base import DmonitorAlerts, MetricEncoder
from dmonitor.base import BaseDmonitorTaskGreenlet
from dmonitor.mixins import DmonitorMetaCacheMixin
from dmonitor.event.event_report import report_dmonitor_event

monkey.patch_all()


def alert_convergence_and_summary():
    logging.info('Start to execute alert convergence and summary task')

    task_configs = {
        'consumer_configs': {
            'type': 'kafka',
            'alias': 'op',
            'topic': 'dmonitor_alerts',
            'partition': False,
            'group_id': 'dmonitor',
            'batch_message_max_count': 5000,
            'batch_message_timeout': 0.1,
        },
        'task_pool_size': 100,
        'db_write_count': 4,
    }

    try:
        task = AlertConvergenceAndSummaryTaskGreenlet(configs=task_configs)
        task.start()
        task.join()
    except Exception as e:
        logging.error(
            'Raise exception({error}) when init alert convergence and summary task'.format(error=e), exc_info=True,
        )


class TrainType(object):
    DANGER = {'code': AlertLevel.DANGER.value, 'interval': 60}
    WARNING = {'code': AlertLevel.WARNING.value, 'interval': 10 * 60}


class AlertContent(object):
    TITLES = {
        'zh-cn': '《数据平台告警》',
        'en': '《Data System Alarm》',
    }
    BIZ_TITLES = {
        'zh-cn': '[告警业务]',
        'en': '[Alarm Business{plural}]',
    }
    BIZ_SAMPLES = {
        'zh-cn': '{sample_biz}等{count}个业务',
        'en': '{count} businesses such as {sample_biz}',
    }
    PROJECT_TITLES = {
        'zh-cn': '[告警项目]',
        'en': '[Alarm Project{plural}]',
    }
    PROJECT_SAMPLES = {
        'zh-cn': '{sample_project}等{count}个项目',
        'en': '{count} projects such as {sample_project}',
    }
    ALERT_CODE_TITLES = {
        AlertCode.TASK.value: {
            'zh-cn': '[任务异常]',
            'en': '[Task Exception]'
        },
        AlertCode.NO_DATA.value: {
            'zh-cn': '[数据断流]',
            'en': '[Data Dry Up]'
        },
        AlertCode.DATA_TREND.value: {
            'zh-cn': '[数据波动]',
            'en': '[Data Fluctuation]'
        },
        AlertCode.DATA_LOSS.value: {
            'zh-cn': '[数据丢失]',
            'en': '[Data Loss]'
        },
        AlertCode.DATA_TIME_DELAY.value: {
            'zh-cn': '[数据时间延迟]',
            'en': '[Data Time Delay]'
        },
        AlertCode.PROCESS_TIME_DELAY.value: {
            'zh-cn': '[处理时间延迟]',
            'en': '[Process Time Delay]'
        },
        AlertCode.DATA_INTERRUPT.value: {
            'zh-cn': '[数据中断]',
            'en': '[Data Interruption]'
        },
        AlertCode.DATA_DROP.value: {
            'zh-cn': '[无效数据]',
            'en': '[Data Invalid]'
        },
        AlertCode.BATCH_DELAY.value: {
            'zh-cn': '[离线任务延迟]',
            'en': '[Batch Delay]'
        },
        AlertCode.DELAY_TREND.value: {
            'zh-cn': '[处理延迟增长]',
            'en': '[Delay Increase]'
        }
    }
    FLOW_SAMPLES = {
        'zh-cn': '{flow_count}个{flow_type_display} ({sample_flow_name}{etc})',
        'en': '{flow_count} {flow_type_display}{plural} ({sample_flow_name}{etc})'
    }
    ETC = {
        'zh-cn': '等',
        'en': ', etc.'
    }


class AlertConvergenceAndSummaryTaskGreenlet(DbWriteMixin, BaseDmonitorTaskGreenlet):
    CACHE_REFRESH_INTERVAL = 60
    TICK_INTERVAL = 60
    SAVE_ALERT_INTERVAL = 5
    SAVE_ALERT_THREADHOLD = 100

    def __init__(self, *args, **kwargs):
        """初始化告警匹配策略的任务

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

        super(AlertConvergenceAndSummaryTaskGreenlet, self).__init__(*args, **kwargs)

        self.init_consumer(configs.get('consumer_configs'))
        self.init_task_pool(configs.get('task_pool_size'))

        self.station = AlertStation(self)

        now = time.time()

        self._alert_configs = []
        self._alert_shields = []
        self._alert_details = []
        self._flow_infos = {}
        self._biz_cache = {}
        self._project_cache = {}

        self._cache_last_refresh_time = None
        self._last_tick_time = now
        self._last_save_alert_time = now

        self.db_write_start(configs.get('db_write_count', 4))
        self.refresh_metadata_cache(now)

    def refresh_metadata_cache(self, now):
        """刷新告警匹配任务依赖的元数据信息

        :param now: 当前刷新缓存的时间
        """
        if self._cache_last_refresh_time and now - self._cache_last_refresh_time < self.CACHE_REFRESH_INTERVAL:
            return

        gevent.joinall([
            self._task_pool.spawn(self.refresh_metadata, self._alert_configs, self.fetch_alert_configs, update=False),
            self._task_pool.spawn(self.refresh_metadata, self._alert_shields, self.fetch_alert_shields, update=False),
        ])

        gevent.joinall([
            self._task_pool.spawn(
                self.refresh_metadata, self._flow_infos, self.fetch_flow_infos_from_redis, update=False),
            self._task_pool.spawn(self.refresh_metadata, self._biz_cache, self.fetch_biz_infos, update=False),
            self._task_pool.spawn(self.refresh_metadata, self._project_cache, self.fetch_project_infos, update=False)
        ])

        for alert_config in self._alert_configs:
            if alert_config.get('active', False):
                self.station.add_alert_config(alert_config)

        self._cache_last_refresh_time = now

    def handle_monitor_value(self, message, now):
        """处理原始告警信息

        :param message: 原始告警信息
        """
        alert = DmonitorAlerts.from_message(message)
        alert_detail = {
            'message': alert.get_metric('message'),
            'message_en': alert.get_metric('message_en'),
            'full_message': alert.get_metric('full_message'),
            'full_message_en': alert.get_metric('full_message_en'),
            'alert_config_id': alert.get_tag('alert_config_id'),
            'alert_code': alert.get_tag('alert_code'),
            'alert_type': alert.get_tag('alert_type'),
            'flow_id': alert.get_tag('flow_id'),
            'node_id': alert.get_tag('node_id'),
            'bk_biz_id': alert.get_tag('bk_biz_id'),
            'project_id': alert.get_tag('project_id'),
            'generate_type': alert.get_tag('generate_type'),
            'dimensions': json.dumps(alert.tags),
            'alert_id': alert.alert_id,
            'alert_status': alert.get_metric('alert_status'),
            'alert_time': alert.time_str,
            'alert_level': alert.get_tag('alert_level', TrainType.WARNING['code']),
        }
        # alert_status是altering，上报事件
        # todo: 后期可以判断屏蔽是用户屏蔽还是系统屏蔽的时候，把用户屏蔽的告警事件也上报
        if alert.get_metric('alert_status') == AlertStatus.ALERTING.value:
            report_dmonitor_event(alert_detail)
            logging.info(f'dmonitor report event, event_message:{alert_detail}')

        if alert.get_metric('alert_status') == AlertStatus.INIT.value:
            alert_status, alerting_info, reasons, alert_config = self.station.add_alert(alert)
            alert_code = alert.get_tag('alert_code')
            alert_detail.update({
                'alert_status': alert_status,
                'monitor_config': json.dumps(alert_config.get('monitor_config', {}).get(alert_code, {})),
                'receivers': json.dumps(self.gen_receivers(alert_config)),
                'notify_ways': json.dumps(self.gen_notify_ways(alert_config)),
                'alert_converged_info': json.dumps(reasons),
                'description': alerting_info,
            })

        self._alert_details.append(alert_detail)

    def do_monitor(self, now, task_pool):
        """执行衍生指标或者监控的逻辑

        :param now: 当前时间戳
        :param task_pool: 任务处理协程池
        """
        if now - self._last_tick_time > self.TICK_INTERVAL:
            self.station.clear_convergence_cache(now)
            self.station.tick(now)
            self._last_tick_time = now

        if (len(self._alert_details) > self.SAVE_ALERT_THREADHOLD
                or now - self._last_save_alert_time > self.SAVE_ALERT_INTERVAL):
            if len(self._alert_details) > 0:
                storage_obj = {
                    'store_type': 'insert',
                    'db_name': 'log',
                    'storage': 'mysql',
                    'table_name': 'datamonitor_alert_detail',
                    'data': self._alert_details,
                }
                self.push_task(storage_obj)
                self._alert_details = []
                self._last_save_alert_time = now


class AlertStation(DmonitorMetaCacheMixin):
    def __init__(self, task=False):
        """初始化告警Station

        :param task: 原始任务，以共享任务中的缓存数据
        """
        super(AlertStation, self).__init__()

        self.trains = {}
        self.alert_configs = {}
        self.convergences = {}
        self.alert_send_status = {}

        self._task = task
        # 记录每个alert_config与Train的反向引用， 在alert_config变更时更新train中的alert_config配置
        self.alert_configs_trains = {}

    def add_alert_config(self, alert_config):
        """新增告警配置

        :param alert_config: 告警配置
        """
        alert_config_id = alert_config.get('id', 0)
        self.alert_configs[alert_config_id] = alert_config

        # 生成收敛配置统计的槽位
        if alert_config_id not in self.convergences:
            self.convergences[alert_config_id] = {}
        for alert_code, alert_code_config in alert_config.get('monitor_config', {}).items():
            if alert_code_config.get('monitor_status', 'off') == 'off':
                continue
            if alert_code not in self.convergences[alert_config_id]:
                self.convergences[alert_config_id][alert_code] = {}

        # 首先根据配置生成列车
        alert_config_trains = []
        receivers = alert_config.get('receivers', [])
        notify_ways = self.gen_notify_ways(alert_config)
        targets = alert_config.get('monitor_target', [])

        for target in targets:
            target_id = self.gen_target_id(target)
            for receiver_config in receivers:
                for notify_way in notify_ways:
                    for train_type in [TrainType.DANGER, TrainType.WARNING]:
                        receiver_type = receiver_config.get('receiver_type')
                        if (receiver_type in RECEIVER_TYPE_SUPPORT_MAPPINGS
                                and notify_way not in RECEIVER_TYPE_SUPPORT_MAPPINGS[receiver_type]):
                            continue
                        if (receiver_type in RECEIVER_TYPE_NOT_SUPPORT_MAPPINGS
                                and notify_way in RECEIVER_TYPE_NOT_SUPPORT_MAPPINGS[receiver_type]):
                            continue
                        receiver = self.gen_receiver(receiver_config)
                        train_id = self.gen_train_id(target_id, receiver, notify_way, train_type['code'])
                        alert_config_trains.append(train_id)
                        # 如果当前没有此列车， 则新增
                        if train_id not in self.trains:
                            self.trains[train_id] = AlertTrain(
                                train_id,
                                target, target_id,
                                receiver,
                                notify_way, train_type,
                                station=self,
                                task=self._task
                            )
                        self.trains[train_id].add_alert_config(alert_config)

        # 如果此alert_config之前已经生成过train了， 则把此次没有生成的train都add一遍， 来去除旧train中的alert_config
        for train_id in self.alert_configs_trains.get(alert_config_id, []):
            if train_id in alert_config_trains:
                continue
            if train_id not in self.trains:
                continue
            self.trains[train_id].add_alert_config(alert_config)

        self.alert_configs_trains[alert_config_id] = alert_config_trains
        return True

    def gen_train_id(self, target_id, receiver, notify_way, alert_level):
        """生成告警列车ID

        :param target_id: 告警对象ID
        :param receiver: 接收者
        :param notify_way: 通知方式
        :param alert_level: 告警级别

        :return: 告警列车ID
        """
        return '&'.join([
            target_id,
            'receiver=%s' % receiver,
            'notify_way=%s' % notify_way,
            'alert_level=%s' % alert_level
        ])

    def add_alert(self, alert):
        """新增告警

        :param alert: 告警信息

        :return: 告警状态、告警收敛信息、告警触发时的各种信息、告警配置
        """
        alert_config_id = alert.get_tag('alert_config_id')
        alert_code = alert.get_tag('alert_code')

        if alert.get_tag('module'):
            unique_key = self.gen_logical_key(alert.tags)
        else:
            unique_key = alert.get_tag(
                'raw_data_id',
                alert.get_tag(
                    'result_table_id', self.gen_unique_key(alert.tags)
                )
            )

        if alert_config_id:
            if alert_config_id not in self.alert_configs or alert_config_id not in self.convergences:
                return AlertStatus.CONVERGED.value, '无法找到该告警配置', {'alert_config_id': alert_config_id}, {}
            alert_config = self.alert_configs[alert_config_id]

            alert_shield = self.check_alert_shields(alert)
            if alert_shield is not None:
                reason = alert_shield.get('reason')
                return AlertStatus.SHIELDED.value, '告警被屏蔽, 原因: %s' % reason, alert_shield, alert_config

            if (alert_code not in self.convergences[alert_config_id]
                    or alert_code not in self.alert_configs[alert_config_id].get('monitor_config', {})):
                return AlertStatus.CONVERGED.value, '无法找到该告警策略', {
                    'alert_config_id': alert_config_id,
                    'alert_code': alert_code,
                }, alert_config
            if unique_key not in self.convergences[alert_config_id][alert_code]:
                self.convergences[alert_config_id][alert_code][unique_key] = {
                    'alerts': [],
                    'last_alert_time': 0,
                }
            convergence = self.convergences[alert_config_id][alert_code][unique_key]

            # 判断告警是否满足触发条件
            trigger_config = self.alert_configs[alert_config_id].get('trigger_config', {})
            alert_threshold = int(trigger_config.get('alert_threshold') or 1)
            duration = int(trigger_config.get('duration') or 1)
            if self.insert_and_check_trigger_count(convergence['alerts'], alert, alert_threshold, duration):
                return AlertStatus.CONVERGED.value, '没有达到告警的触发条件', {
                    'alert_config_id': alert_config_id,
                    'alert_code': alert_code,
                    'alert_threshold': alert_threshold,
                    'duration': duration,
                }, alert_config

            # 判断告警是否应该按上次告警时间进行屏蔽
            convergence_config = self.alert_configs[alert_config_id].get('convergence_config', {})
            mask_time = int(convergence_config.get('mask_time') or 60) * 60
            if alert.timestamp - convergence['last_alert_time'] < mask_time:
                return AlertStatus.CONVERGED.value, '告警在收敛时间内', {
                    'alert_config_id': alert_config_id,
                    'alert_code': alert_code,
                    'last_alert_time': convergence['last_alert_time'],
                    'mask_time': mask_time,
                }, alert_config

            # 初始化通知信息
            self.init_notify_send_status(alert, alert_config)

            for train_id in self.alert_configs_trains.get(alert_config_id, []):
                self.trains[train_id].add_alert(alert)
            convergence['last_alert_time'] = alert.timestamp
            return AlertStatus.ALERTING.value, '', {}, alert_config
        else:
            return AlertStatus.CONVERGED.value, '该告警无告警策略ID', {}, {}

    def check_alert_shields(self, alert):
        """检查告警是否满足屏蔽策略

        :param alert: 告警信息
        """
        # 根据多个告警屏蔽规则屏蔽告警的汇总和发送
        # TODO 根据接收者和通知方式进行屏蔽
        for alert_shield in self._task._alert_shields:
            if alert_shield['alert_code'] and alert_shield['alert_code'] != alert.get_tag('alert_code'):
                continue

            if alert_shield['alert_level'] and alert_shield['alert_shield'] != alert.get_tag('alert_level'):
                continue

            if alert_shield['alert_config_id'] and alert_shield['alert_config_id'] != alert.get_tag('alert_config_id'):
                continue

            if alert_shield['dimensions']:
                satisfied = True
                for dim_key, dim_value in alert_shield['dimensions'].items():
                    if not isinstance(dim_value, list):
                        dim_value = [dim_value]
                    alert_tag_value = alert.get_tag(dim_key)
                    if alert_tag_value is None or str(alert_tag_value) not in dim_value:
                        satisfied = False
                    if not satisfied:
                        break
                if not satisfied:
                    continue

            if alert.timestamp >= alert_shield['start_time'] and alert.timestamp <= alert_shield['end_time']:
                return alert_shield
        return None

    def insert_and_check_trigger_count(self, alerts, alert, alert_threshold, duration):
        """插入告警，并检查告警是否满足触发条件

        :param alerts: 告警列表
        :param alert: 告警信息
        :param alert_threshold: 告警触发阈值
        :param duration: 触发阈值周期
        """
        # 如果槽位中无告警，则直接插入槽位中，并根据阈值来判断告警是否需要收敛，True：需要收敛，False：告警
        if len(alerts) == 0:
            alerts.append(alert.timestamp)
            if alert_threshold <= 1:
                return False
            else:
                return True

        # 找到当前告警所在位置，并插入告警
        index = len(alerts) - 1
        while index >= 0:
            if alert.timestamp > alerts[index]:
                alerts.insert(index + 1, alert.timestamp)
                break
            index -= 1

        # 根据是否达到阈值来决定是否需要收敛
        min_timestamp = alerts[-1] - duration * 60
        fit_alert_count = 0
        for alert_timestamp in alerts:
            if alert_timestamp >= min_timestamp:
                fit_alert_count += 1
        return fit_alert_count < alert_threshold

    def init_notify_send_status(self, alert, alert_config):
        notify_ways = self.gen_notify_ways(alert_config)
        receivers = alert_config.get('receivers', [])
        self.alert_send_status[alert.alert_id] = {
            'counter': Counter(),
            'total': 0,
            'info': {}
        }
        for receiver_config in receivers:
            receiver = self.gen_receiver(receiver_config)
            receiver_type = receiver_config.get('receiver_type')
            self.alert_send_status[alert.alert_id]['info'][receiver] = {}
            for notify_way in notify_ways:
                if (receiver_type in RECEIVER_TYPE_SUPPORT_MAPPINGS
                        and notify_way not in RECEIVER_TYPE_SUPPORT_MAPPINGS[receiver_type]):
                    continue
                if (receiver_type in RECEIVER_TYPE_NOT_SUPPORT_MAPPINGS
                        and notify_way in RECEIVER_TYPE_NOT_SUPPORT_MAPPINGS[receiver_type]):
                    continue
                self.alert_send_status[alert.alert_id]['info'][receiver][notify_way] = {
                    'status': AlertSendStatus.INIT.value,
                    'message': '',
                }
                self.alert_send_status[alert.alert_id]['total'] += 1

    def update_alert_send_status(self, alert_id, receiver, notify_way, status, message, send_time):
        """更新告警发送状态

        :param alert_id: 告警ID
        :param receiver: 告警接收者
        :param notify_way: 通知方式
        :param status: 告警状态
        :param message: 告警信息
        :param send_time: 告警发送时间
        """
        if alert_id in self.alert_send_status:
            if receiver in self.alert_send_status[alert_id]['info']:
                if notify_way in self.alert_send_status[alert_id]['info'][receiver]:
                    self.alert_send_status[alert_id]['info'][receiver][notify_way]['status'] = status
                    self.alert_send_status[alert_id]['info'][receiver][notify_way]['message'] = message
                    self.alert_send_status[alert_id]['counter'].update([status])

            if sum(self.alert_send_status[alert_id]['counter'].values()) == self.alert_send_status[alert_id]['total']:
                if self.alert_send_status[alert_id]['counter'][AlertSendStatus.SUCC.value] == 0:
                    status = AlertSendStatus.ERR.value
                elif self.alert_send_status[alert_id]['counter'][AlertSendStatus.ERR.value] == 0:
                    status = AlertSendStatus.SUCC.value
                else:
                    status = AlertSendStatus.PART_ERR.value

                logging.info(
                    'Update alert({alert_id}) send status({status})'.format(
                        alert_id=alert_id,
                        status=status
                    )
                )
                # 更新告警发送状态
                sql = (
                    'UPDATE datamonitor_alert_detail'
                    'SET alert_send_status=%s, alert_send_error=%s, alert_send_time=%s'
                    'WHERE alert_id=%s'
                )
                db_obj = {
                    'alert_send_status': status,
                    'alert_send_error': json.dumps(self.alert_send_status[alert_id]['info']),
                    'alert_send_time': send_time,
                    'alert_id': alert_id
                }
                update_storage_obj = {
                    'store_type': 'sql', 'db_name': 'log', 'storage': 'mysql',
                    'table_name': 'datamonitor_alert_detail', 'sql': sql, 'data': [db_obj],
                    'columns': ['alert_send_status', 'alert_send_error', 'alert_send_time', 'alert_id'],
                }
                self._task.push_task(update_storage_obj)
                del self.alert_send_status[alert_id]
        else:
            logging.info('Alert({alert_id}) not in send status cache'.format(alert_id=alert_id))

    def clear_convergence_cache(self, now):
        """清理告警收敛判断用的缓存

        :param now: 当前时间
        """
        for alert_config_id in self.convergences.keys():
            for alert_code in self.convergences[alert_config_id]:
                for unique_key, alert_info in self.convergences[alert_config_id][alert_code].items():
                    trigger_config = self.alert_configs[alert_config_id].get('trigger_config', {})
                    duration = int(trigger_config.get('duration') or 1) * 60
                    index = 0
                    for alert_timestamp in alert_info['alerts']:
                        # 增加5分钟等待时间
                        if now - duration - 300 <= alert_timestamp:
                            break
                        index += 1
                    alert_info['alerts'] = alert_info['alerts'][index:]

    def tick(self, now):
        """触发告警检查

        :param now: 当前时间
        """
        for train in self.trains.values():
            train.tick(now)
        return True


class AlertTrain(DmonitorMetaCacheMixin):
    """告警列车，以乘客的方式管理告警信息，当达到一定汇总时间范围时，推出列车（把告警汇总后推送出去）
    """
    def __init__(self, train_id, target, target_id, receiver,
                 notify_way, train_type=TrainType.WARNING,
                 station=False, task=False):
        """初始化告警列车配置信息

        :param train_id: 告警列车ID，由接收者、通知方式和告警级别组成
        :param target: 告警策略目标
        :param target_id: 告警策略目标ID
        :param receiver: 告警接收者
        :param notify_way: 告警通知方式
        :param train_type: 告警列车类型，由告警级别确定
        :param station: 告警station，用户一次性把汇总的告警通知出去
        :param task: 当前告警汇总任务
        """
        super(AlertTrain, self).__init__()

        self.train_id = train_id
        self.target = target
        self.target_id = target_id
        self.receiver = receiver
        self.notify_way = notify_way
        self.train_type = train_type['code']
        self.train_interval = train_type['interval']
        self.last_train_time = 0
        # cells结构 project_id[dict] -> flow_id[dict] -> alert_code[dict] -> alerts[list]
        self.cells = {}
        self.cells_ids = []
        self.cell_cnt = 0
        self.alert_configs = {}
        self.station = station
        self._task = task
        self.filter = {}

        if 'dimensions' in self.target:
            self.filter.update(self.target.get('dimensions', {}))

    def add_alert_config(self, alert_config):
        """在列车中增加告警策略

        :param alert_config: 告警策略
        """
        alert_config_id = alert_config.get('id', 0)

        # 判断这个监控配置是否与此列车匹配
        targets = alert_config.get('monitor_target', [])
        target_ids = map(self.gen_target_id, targets)
        if self.target_id not in target_ids:
            self.remove_alert_config(alert_config_id)
            return False

        receivers = self.gen_receivers(alert_config)
        if self.receiver not in receivers:
            # 如果当前列车中已有此alert_config则清除掉（alert_config update时）
            self.remove_alert_config(alert_config_id)
            return False

        notify_ways = self.gen_notify_ways(alert_config)
        if self.notify_way not in notify_ways:
            # 如果当前列车中已有此alert_config则清除掉（alert_config update时）
            self.remove_alert_config(alert_config_id)
            return False

        self.alert_configs[alert_config_id] = alert_config
        return True

    def remove_alert_config(self, alert_config_id):
        """移除告警策略

        :param alert_config_id: 告警策略ID
        """
        if alert_config_id in self.alert_configs:
            del self.alert_configs[alert_config_id]
        return True

    def alert_match(self, alert):
        """检车当前告警是否与列车的任意一个告警策略匹配

        :param alert: 当前告警
        """
        # 先看alert的级别与本列车是否符合
        alert_level = alert.get_tag('alert_level', AlertLevel.WARNING.value)
        if alert_level != self.train_type:
            return False

        if self.filter:
            for filter_key, filter_value in self.filter.items():
                if not isinstance(filter_value, list):
                    filter_value = [filter_value]
                alert_tag_value = alert.get_tag(filter_key)
                if alert_tag_value is None or str(alert_tag_value) not in filter_value:
                    return False

        # 如果是由该策略生成的告警，默认成功match
        alert_config_id = alert.get_tag('alert_config_id')
        if alert_config_id is not None:
            if alert_config_id in self.alert_configs:
                return True
            else:
                return False

        for alert_config in self.alert_configs.values():
            flow_id = alert.get_tag('flow_id', None)
            project_id = alert.get_tag('project_id', None)
            biz_id = alert.get_tag('bk_biz_id', None)
            alert_code = alert.get_tag('alert_code', None)

            # 告警类型的开关， 与本alert是否匹配
            monitors = alert_config.get('monitor_config', {})
            if alert_code not in AlertCode.get_enum_value_list():
                continue

            if alert_code not in monitors:
                continue

            if monitors[alert_code].get('monitor_status', 'off') == 'off':
                continue

            targets = alert_config.get('monitor_target', [])
            for target in targets:
                target_type = target.get('target_type')
                target_flow_id, target_node_id = self.get_flow_node_by_target(target)
                flow_info = self._task._flow_infos.get(str(target_flow_id), {})
                if target_type == 'dataflow':
                    target_project_id = flow_info.get('project_id')
                    if target_project_id != project_id:
                        # 目标项目不匹配
                        continue

                    if int(target_flow_id) != int(flow_id):
                        # 目标flow不匹配
                        continue
                    return True
                elif target_type == 'rawdata':
                    # 清洗任务的告警匹配
                    target_biz_id = flow_info.get('bk_biz_id')
                    if target_biz_id != biz_id:
                        continue

                    if str(target_flow_id) != str(flow_id):
                        continue
                    return True
        return False

    def add_alert(self, alert):
        """添加待通知的告警到当前列车

        :param alert: 告警
        """
        if not self.alert_match(alert):
            return False
        alert_code = alert.get_tag('alert_code', AlertCode.TASK.value)

        target_type = self.target.get('target_type')
        project_or_biz_id = None

        if target_type == 'dataflow' or target_type == 'platform':
            project_or_biz_id = alert.get_tag('project_id')
            flow_id = alert.get_tag('flow_id')
            # TODO: 补充project_name和flow_name等信息
        elif target_type == 'rawdata':
            project_or_biz_id = alert.get_tag('bk_biz_id')
            flow_id = alert.get_tag('flow_id')
            # TODO: 补充biz_name和raw_data_alias等信息

        if project_or_biz_id not in self.cells:
            self.cells[project_or_biz_id] = {}
        if flow_id not in self.cells[project_or_biz_id]:
            self.cells[project_or_biz_id][flow_id] = {}
        if alert_code not in self.cells[project_or_biz_id][flow_id]:
            self.cells[project_or_biz_id][flow_id][alert_code] = []

        self.cells[project_or_biz_id][flow_id][alert_code].append(alert)
        self.cells_ids.append(alert.alert_id)
        self.cell_cnt += 1
        return True

    def tick(self, now):
        """触发汇总告警的通知

        :param now: 当前时间
        """
        if now - self.last_train_time < self.train_interval:
            # 没到发车时间
            logging.info('[WAITING TRAIN] %s waited %s sec, interval %s sec' % (
                self.train_id, now - self.last_train_time, self.train_interval))
            return True
        if self.empty():
            # 空车不发
            return True
        # 更新发车时间
        self.last_train_time = now
        # 开车
        return self.send(now)

    def send(self, now):
        """组装并发送告警

        :param now: 当前时间
        """
        logging.info('sending Train %s' % self.train_id)
        # 根据车厢中的告警， 生成具体发出的告警信息
        message = self.get_alert_msg()
        message_en = self.get_alert_msg(language='en')
        # 发送告警
        self.send_alert(message, message_en, self.receiver, self.notify_way, self.train_type, now)
        # 清空车厢
        self.cells = {}
        self.cells_ids = []
        self.cell_cnt = 0
        return True

    def send_alert(self, message, message_en, receiver, notify_way, alert_level, now):
        """调用接口发送告警

        :param message: 告警信息
        :param message_en: 英文告警信息
        :param receiver: 告警接收者
        :param notify_way: 通知方式
        :param alert_level: 告警级别
        :param now: 当前时间
        """
        now_str = timetostr(now)
        response = datamanage_api.alerts.send({
            'notify_way': notify_way,
            'receiver': receiver,
            'message': message,
            'message_en': message_en,
        })

        # 所有收敛后的告警发送状态设置为success
        for alert_id in self.cells_ids:
            self.station.update_alert_send_status(
                alert_id, self.receiver, self.notify_way,
                AlertSendStatus.SUCC.value if response.is_success() else AlertSendStatus.ERR.value,
                response.message, now_str
            )

        if not response.is_success():
            logging.info('[Alert Send] ERROR [%s][%s]\n%s' % (notify_way, receiver, message), exc_info=True)
            return False

        # 记录告警log
        insert_storage_obj = {
            'store_type': 'insert', 'db_name': 'log', 'storage': 'mysql',
            'table_name': 'datamonitor_alert_log', 'data': [{
                'message': message,
                'message_en': message_en,
                'alert_time': now_str,
                'alert_level': alert_level,
                'receiver': receiver,
                'notify_way': notify_way,
                'dimensions': json.dumps(self.cells, cls=MetricEncoder)
            }],
        }
        self._task.push_task(insert_storage_obj)
        return True

    def get_alert_msg(self, language='zh-cn'):
        """生成告警信息

        :param language: 告警信息所属语言

        :return: 当前语言环境的告警信息
        """
        project_or_biz_ids = list(self.cells.keys())
        messages = []
        if self.target.get('target_type') == 'rawdata':
            # 业务
            sample_biz = self._task._biz_cache.get(str(project_or_biz_ids[0]), {}).get('bk_biz_name')
            biz_count = len(project_or_biz_ids)

            if biz_count > 1:
                sub_msg = AlertContent.BIZ_SAMPLES[language].format(sample_biz=sample_biz, count=biz_count)
                plural = 'es'
            else:
                sub_msg = sample_biz
                plural = ''

            message = '{biz_title}: {sub_msg}'.format(
                biz_title=AlertContent.BIZ_TITLES[language].format(plural=plural),
                sub_msg=sub_msg,
            )
        else:
            # 项目
            sample_project = self._task._project_cache.get(str(project_or_biz_ids[0]), {}).get('project_name')
            project_count = len(project_or_biz_ids)

            if project_count > 1:
                sub_msg = AlertContent.PROJECT_SAMPLES[language].format(
                    sample_project=sample_project, count=project_count)
                plural = 's'
            else:
                sub_msg = sample_project
                plural = ''

            message = '{project_title}: {sub_msg}'.format(
                project_title=AlertContent.PROJECT_TITLES[language].format(plural=plural),
                sub_msg=sub_msg,
            )
        messages.append(message)

        alert_code_cnt = {}
        for project_or_biz_id in self.cells.keys():
            for flow_id in self.cells[project_or_biz_id].keys():
                for alert_code in self.cells[project_or_biz_id][flow_id].keys():
                    if alert_code not in alert_code_cnt:
                        alert_code_cnt[alert_code] = {
                            'flows': {},
                            'project_or_bizs': {},
                            'alert_sample': False
                        }
                    alert_code_cnt[alert_code]['flows'][flow_id] = True
                    alert_code_cnt[alert_code]['project_or_bizs'][project_or_biz_id] = True
                    if not alert_code_cnt[alert_code]['alert_sample']:
                        alert_code_cnt[alert_code]['alert_sample'] = self.cells[
                            project_or_biz_id][flow_id][alert_code][0]

        # 任务异常
        if AlertCode.TASK.value in alert_code_cnt:
            messages.append(self.gen_sub_alert(AlertCode.TASK.value, alert_code_cnt, language))
        # 数据断流
        if AlertCode.NO_DATA.value in alert_code_cnt:
            messages.append(self.gen_sub_alert(AlertCode.NO_DATA.value, alert_code_cnt, language))
        # 数据波动
        if AlertCode.DATA_TREND.value in alert_code_cnt:
            messages.append(self.gen_sub_alert(AlertCode.DATA_TREND.value, alert_code_cnt, language))
        # 数据丢失
        if AlertCode.DATA_LOSS.value in alert_code_cnt:
            messages.append(self.gen_sub_alert(AlertCode.DATA_LOSS.value, alert_code_cnt, language))
        # 数据时间延迟
        if AlertCode.DATA_TIME_DELAY.value in alert_code_cnt:
            messages.append(self.gen_sub_alert(AlertCode.DATA_TIME_DELAY.value, alert_code_cnt, language))
        # 处理时间延迟
        if AlertCode.PROCESS_TIME_DELAY.value in alert_code_cnt:
            messages.append(self.gen_sub_alert(AlertCode.PROCESS_TIME_DELAY.value, alert_code_cnt, language))
        # 无效数据
        if AlertCode.DATA_DROP.value in alert_code_cnt:
            messages.append(self.gen_sub_alert(AlertCode.DATA_DROP.value, alert_code_cnt, language))
        # 数据流中断
        if AlertCode.DATA_INTERRUPT.value in alert_code_cnt:
            messages.append(self.gen_sub_alert(AlertCode.DATA_INTERRUPT.value, alert_code_cnt, language))
        # 离线任务延迟
        if AlertCode.BATCH_DELAY.value in alert_code_cnt:
            messages.append(self.gen_sub_alert(AlertCode.BATCH_DELAY.value, alert_code_cnt, language))
        # 数据流中断
        if AlertCode.DELAY_TREND.value in alert_code_cnt:
            messages.append(self.gen_sub_alert(AlertCode.DELAY_TREND.value, alert_code_cnt, language))
        return '\n'.join(messages)

    def gen_sub_alert(self, alert_code, alert_code_cnt, language):
        """生成子告警信息

        :param alert_code: 告警策略码
        :param alert_code_cnt: 当前告警策略码的告警数量
        :param language: 当前语言环境

        :return: 告警子信息
        """
        flows = alert_code_cnt[alert_code].get('flows', [])
        alert_sample = alert_code_cnt[alert_code].get('alert_sample')

        sample_flow_id = list(flows.keys())[0]
        flow_type_display, sample_flow_name = self.get_flow_display(
            self._task._flow_infos.get(str(sample_flow_id), {}), language
        )

        flow_count = len(flows)
        if flow_count > 0:
            plural = 's'
            etc = AlertContent.ETC[language]
        else:
            plural, etc = '', ''

        if flow_count == 1 and alert_sample is not None:
            if language == 'zh-cn':
                message = alert_sample.get_metric('full_message')
            else:
                message = alert_sample.get_metric('full_message_en')
            return '{title}: {message}'.format(
                title=AlertContent.ALERT_CODE_TITLES[alert_code][language],
                message=message
            )
        else:
            return '{title}: {sub_msg}'.format(
                title=AlertContent.ALERT_CODE_TITLES[alert_code][language],
                sub_msg=AlertContent.FLOW_SAMPLES[language].format(
                    flow_count=flow_count,
                    flow_type_display=flow_type_display,
                    plural=plural,
                    sample_flow_name=sample_flow_name,
                    etc=etc
                )
            )

    def empty(self):
        """当前告警列车是否为空
        """
        return self.cell_cnt < 1

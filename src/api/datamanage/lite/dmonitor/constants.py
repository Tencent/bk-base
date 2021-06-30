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

from common.bklanguage import BkLanguage
from django.utils.translation import ugettext as _


DEFAULT_MONITOR_CONFIG = {
    'no_data': {
        'no_data_interval': 600,  # 单位: 秒
        'monitor_status': 'on',
    },
    'data_trend': {
        'diff_period': 168,  # 波动比较周期, 单位: 小时
        'diff_count': 30,  # 波动数值, 与diff_unit共同作用
        'diff_unit': 'percent',  # 波动单位, 可选percent和number
        'diff_trend': 'decrease',  # 波动趋势, 可选increase, decrease和both
        'monitor_status': 'off',
    },
    'task': {
        'no_metrics_interval': 600,  # 无埋点时间, 单位: 秒
        'batch_exception_status': ['failed'],  # 离线异常状态
        'monitor_status': 'on',
    },
    'data_loss': {
        'monitor_status': 'off',
    },
    'data_time_delay': {
        'delay_time': 300,  # 延迟时间, 单位: 秒
        'lasted_time': 600,  # 持续时间, 单位: 秒
        'monitor_status': 'off',
    },
    'process_time_delay': {
        'delay_time': 300,
        'lasted_time': 600,
        'monitor_status': 'off',
    },
    'delay_trend': {'continued_increase_time': 600, 'monitor_status': 'off'},  # 持续增长时间, 单位: 秒
    'data_interrupt': {
        'monitor_status': 'off',
    },
    'data_drop': {
        'monitor_status': 'off',
        'drop_rate': 30,
    },
    'batch_delay': {
        'monitor_status': 'off',
        'schedule_delay': 3600,
        'execute_delay': 600,
    },
}
DATA_MONITOR_DEFAULT_CODES = [
    'no_data',
    'data_trend',
    'data_loss',
    'data_time_delay',
    'process_time_delay',
    'data_interrupt',
    'data_drop',
    'delay_trend',
]
TASK_MONITOR_DEFAULT_CODES = ['task', 'batch_delay']
DEFAULT_NOTIFY_CONFIG = {'mail': False, 'sms': False, 'weixin': False, 'work-weixin': False, 'voice': False}
DEFAULT_TRIGGER_CONFIG = {'duration': 1, 'alert_threshold': 1}  # 触发时间范围, 单位: 分钟  # 触发次数
DEFAULT_CONVERGENCE_CONFIG = {'mask_time': 60}

MSG_TYPE_MAPPSINGS = {'wechat': 'weixin', 'eewechat': 'work-weixin', 'phone': 'voice'}

GRAFANA_ALERT_STATUS = {
    'ok': _('恢复正常'),
    'paused': _('已暂停'),
    'alerting': _('告警中'),
    'pending': _('预警中'),
    'no_data': _('无指标'),
}

ALERT_TITLE_MAPPSINGS = {
    BkLanguage.CN: '《数据平台告警》',
    BkLanguage.EN: '《Data System Alarm》',
    BkLanguage.ALL: '《数据平台告警》',
}

ALERT_ENV_MAPPINGS = {
    'DEVELOP': {
        BkLanguage.CN: '开发环境',
        BkLanguage.EN: 'DEVELOP',
        BkLanguage.ALL: '开发环境',
    },
    'TEST': {
        BkLanguage.CN: '测试环境',
        BkLanguage.EN: 'TEST',
        BkLanguage.ALL: '测试环境',
    },
    'STAG': {
        BkLanguage.CN: '预发布环境',
        BkLanguage.EN: 'STAG',
        BkLanguage.ALL: '预发布环境',
    },
    'PRODUCT': {BkLanguage.CN: '正式环境', BkLanguage.EN: 'PRODUCT', BkLanguage.ALL: '正式环境'},
}

ALERT_VERSION_MAPPINGS = {
    'EE': {
        BkLanguage.CN: '企业版',
        BkLanguage.EN: 'ENTERPRISE',
        BkLanguage.ALL: '企业版',
    },
    'TGDP': {
        BkLanguage.CN: '海外版',
        BkLanguage.EN: 'TGDP',
        BkLanguage.ALL: '海外版',
    },
    'TENCENT': {
        BkLanguage.CN: '内部版',
        BkLanguage.EN: 'TENCENT',
        BkLanguage.ALL: '内部版',
    },
}

ALERT_CODES = [
    'data_trend',
    'no_data',
    'data_loss',
    'delay_trend',
    'data_time_delay',
    'process_time_delay',
    'data_interrupt',
    'data_drop',
    'task',
    'batch_delay',
]

ALERT_TYPES = [
    'task_monitor',
    'data_monitor',
]

ALERT_LEVELS = ['danger', 'warning', 'info']

CLUSTER_NODE_TYPES_MAPPINGS = {
    'mysql': 'mysql_storage',
    'hdfs': 'hdfs_storage',
    'es': 'elastic_storage',
    'tsdb': 'tsdb_storage',
    'treids': 'tredis_storage',
    'tspider': 'tspider_storage',
    'ignite': 'ignite',
    'hermes': 'hermes_storage',
    'queue': 'queue_storage',
    'druid': 'druid_storage',
    'postgresql': 'pgsql_storage',
    'tpg': 'tpg',
    'tdbank': 'tdbank',
    'queue_pulsar': 'queue_pulsar',
}

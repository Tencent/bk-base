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
import copy
from rest_framework import serializers
from django.utils.translation import ugettext_lazy as _

from common.base_utils import model_to_dict, custom_params_valid
from common.exceptions import ValidationError

from datamanage import exceptions as dm_errors
from datamanage.lite.dmonitor import constants
from datamanage.lite.dmonitor import models
from datamanage.utils.drf import GeneralSerializer


class NumberSerializerMixin(object):
    def valid_time_number(self, number, max_number):
        if not isinstance(number, int):
            return False

        # 最大监控时间为一周
        if number <= 0 or number > max_number:
            return False

        return True


class MonitorConfigBaseSerializer(serializers.Serializer, NumberSerializerMixin):
    monitor_status = serializers.ChoiceField(
        choices=(
            ('on', _('开')),
            ('off', _('关')),
        )
    )


class MonitorConfigNoDataSerializer(MonitorConfigBaseSerializer):
    no_data_interval = serializers.IntegerField(label=_('无数据时间'))

    def validate(self, attr):
        if not self.valid_time_number(attr['no_data_interval'], 10080 * 60):
            raise ValidationError(_('无数据时间必须是1~10080的正整数(分钟)'))
        return attr


class MonitorConfigDataTrendSerializer(MonitorConfigBaseSerializer):
    diff_period = serializers.IntegerField(label=_('波动比较周期'))
    diff_count = serializers.IntegerField(label=_('波动数值'))
    diff_unit = serializers.ChoiceField(choices=(('percent', _('百分比')), ('count', _('条数'))), label=_('波动单位'))
    diff_trend = serializers.ChoiceField(
        choices=(('both', _('变化')), ('increase', _('增长')), ('decrease', _('减少'))), label=_('波动趋势')
    )

    def validate(self, attr):
        if not self.valid_time_number(attr['diff_period'], 168):
            raise ValidationError(_('数据波动周期必须是1~168的正整数(小时)'))
        return attr


class MonitorConfigDataLossSerializer(MonitorConfigBaseSerializer):
    pass


class MonitorConfigDataDelaySerializer(MonitorConfigBaseSerializer):
    delay_time = serializers.IntegerField(label=_('延迟时间'))
    lasted_time = serializers.IntegerField(label=_('持续时间'))

    def validate(self, attr):
        if not self.valid_time_number(attr['delay_time'], 10080 * 60):
            raise ValidationError(_('数据延迟时间必须是1~10080的正整数(分钟)'))

        if not self.valid_time_number(attr['lasted_time'], 10080 * 60):
            raise ValidationError(_('数据延迟持续时间必须是1~10080的正整数(分钟)'))
        return attr


class MonitorConfigProcessDelaySerializer(MonitorConfigBaseSerializer):
    delay_time = serializers.IntegerField(label=_('延迟时间'))
    lasted_time = serializers.IntegerField(label=_('持续时间'))

    def validate(self, attr):
        if not self.valid_time_number(attr['delay_time'], 10080 * 60):
            raise ValidationError(_('数据延迟时间必须是1~10080的正整数(分钟)'))

        if not self.valid_time_number(attr['lasted_time'], 10080 * 60):
            raise ValidationError(_('数据延迟持续时间必须是1~10080的正整数(分钟)'))
        return attr


class MonitorConfigDelayTrendSerializer(MonitorConfigBaseSerializer):
    continued_increase_time = serializers.IntegerField(label=_('持续增长时间'))

    def validate(self, attr):
        if not self.valid_time_number(attr['continued_increase_time'], 10080 * 60):
            raise ValidationError(_('数据延迟持续时间必须是1~10080的正整数(分钟)'))
        return attr


class MonitorConfigTaskSerializer(MonitorConfigBaseSerializer):
    no_metrics_interval = serializers.IntegerField(label=_('无指标时间'))
    batch_exception_status = serializers.ListField(label=_('离线异常状态'))

    def validate(self, attr):
        if not self.valid_time_number(attr['no_metrics_interval'], 10080 * 60):
            raise ValidationError(_('任务持续停止时间必须是1~10080的正整数(分钟)'))

        if len(attr['batch_exception_status']) == 0:
            raise ValidationError(_('必须至少选择一种异常状态进行监控'))
        return attr


class MonitorConfigBatchDelaySerializer(MonitorConfigBaseSerializer):
    schedule_delay = serializers.IntegerField(label=_('调度延迟'))
    execute_delay = serializers.IntegerField(label=_('执行延迟'))

    def validate(self, attr):
        if not self.valid_time_number(attr['schedule_delay'], 10080 * 60):
            raise ValidationError(_('调度延迟时间必须是1~10080的正整数(分钟)'))

        if not self.valid_time_number(attr['execute_delay'], 10080 * 60):
            raise ValidationError(_('执行延迟时间必须是1~10080的正整数(分钟)'))
        return attr


class MonitorConfigDataInterruptSerializer(MonitorConfigBaseSerializer):
    pass


class MonitorConfigDataDropSerializer(MonitorConfigBaseSerializer):
    drop_rate = serializers.IntegerField(label=_('无效率'))

    def validate(self, attr):
        if attr['drop_rate'] <= 0 or attr['drop_rate'] > 100:
            raise ValidationError(_('无效率必须是0~100的正数'))
        return attr


class AlertConfigMineSerializer(serializers.Serializer):
    project_id = serializers.IntegerField(required=False, label=_('项目ID'))
    bk_biz_id = serializers.IntegerField(required=False, label=_('业务ID'))
    alert_target = serializers.CharField(required=False, label=_('告警对象'))
    alert_target_type = serializers.ChoiceField(
        required=False,
        label=_('告警对象类型'),
        choices=(
            ('dataflow', _('数据计算任务')),
            ('rawdata', _('数据集成任务')),
        ),
    )
    active = serializers.NullBooleanField(required=False, label=_('是否启用'))
    notify_way = serializers.CharField(required=False, label=_('通知方式'))
    scope = serializers.ChoiceField(
        default='received',
        label=('告警配置列表范围'),
        choices=(
            ('received', _('我作为接收人的告警配置')),
            ('managed', _('我管理的告警配置')),
            ('configured', _('我有权限且已配置的告警策略')),
        ),
    )


class AlertConfigMonitorConfigSerializer(serializers.Serializer):
    no_data = MonitorConfigNoDataSerializer(required=False, label=_('无数据告警配置'))
    data_trend = MonitorConfigDataTrendSerializer(required=False, label=_('数据波动告警配置'))
    data_loss = MonitorConfigDataLossSerializer(required=False, label=_('数据丢失告警配置'))
    data_time_delay = MonitorConfigDataDelaySerializer(required=False, label=_('数据时间延迟告警配置'))
    process_time_delay = MonitorConfigProcessDelaySerializer(required=False, label=_('处理时间延迟告警配置'))
    task = MonitorConfigTaskSerializer(required=False, label=_('任务告警配置'))
    batch_delay = MonitorConfigBatchDelaySerializer(required=False, label=_('离线任务延迟'))
    data_interrupt = MonitorConfigDataInterruptSerializer(required=False, label=_('数据中断告警'))
    data_drop = MonitorConfigDataDropSerializer(required=False, label=_('无效数据告警配置'))
    delay_trend = MonitorConfigDelayTrendSerializer(required=False, label=_('延迟增长告警配置'))


class AlertConfigMonitorTargetSerializer(serializers.Serializer):
    target_type = serializers.ChoiceField(
        choices=(
            ('dataflow', _('数据计算任务')),
            ('rawdata', _('数据集成任务')),
            ('platform', _('全平台任务')),
        )
    )
    flow_id = serializers.IntegerField(required=False, label=_('数据流ID'))
    node_id = serializers.IntegerField(required=False, allow_null=True, label=_('数据流节点ID'))
    raw_data_id = serializers.IntegerField(required=False, label=_('数据源ID'))
    data_set_id = serializers.IntegerField(required=False, allow_null=True, label=_('数据源任务ID'))
    dimensions = serializers.DictField(required=False, label=_('告警对象维度'))

    def validate(self, attr):
        if attr['target_type'] == 'dataflow' and 'flow_id' not in attr:
            raise ValidationError(_('数据计算类型告警目标必须有flow_id字段'))

        if attr['target_type'] == 'rawdata' and 'raw_data_id' not in attr:
            raise ValidationError(_('数据集成类型告警目标必须有raw_data_id字段'))
        return attr


class AlertConfigReceiversSerializer(serializers.Serializer):
    receiver_type = serializers.ChoiceField(
        choices=(
            ('user', _('用户')),
            ('list', _('用户列表')),  # 主要用于语音按顺序通知
            ('role', _('角色')),
        ),
        label=_('接收者类型'),
    )
    username = serializers.CharField(required=False, label=_('用户名'))
    userlist = serializers.ListField(required=False, label=_('用户列表'))
    role_id = serializers.IntegerField(required=False, label=_('角色ID'))
    scope_id = serializers.IntegerField(required=False, label=_('作用域ID'))

    def validate(self, attr):
        if attr['receiver_type'] == 'user' and 'username' not in attr:
            raise ValidationError(_('用户类型的接收者必须有username字段'))

        if attr['receiver_type'] == 'list' and 'userlist' not in attr:
            raise ValidationError(_('用户类型为接收列表必须有userlist字段'))

        if attr['receiver_type'] == 'role' and ('role_id' not in attr or 'scope_id' not in attr):
            raise ValidationError(_('角色类型的接收者必须有role_id和scope_id字段'))
        return attr


class AlertConfigTriggerConfigSerializer(serializers.Serializer, NumberSerializerMixin):
    duration = serializers.IntegerField(default=1, label=_('触发时间检测范围'))
    alert_threshold = serializers.IntegerField(default=1, label=_('触发次数阈值'))

    def validate(self, attr):
        if not self.valid_time_number(attr['duration'], 10080):
            raise ValidationError(_('触发周期必须是1~10080的正整数(分钟)'))

        if not self.valid_time_number(attr['alert_threshold'], 10000):
            raise ValidationError(_('触发次数必须是1~10000的正整数'))
        return attr


class AlertConfigConvergenceConfigSerializer(serializers.Serializer, NumberSerializerMixin):
    mask_time = serializers.IntegerField(default=60, label=_('告警屏蔽时间'))

    def validate(self, attr):
        if not self.valid_time_number(attr['mask_time'], 10080):
            raise ValidationError(_('告警屏蔽时间必须是1~10080的正整数(分钟)'))
        return attr


class AlertConfigSerializer(GeneralSerializer):
    bk_username = serializers.CharField(max_length=32, label=_('用户名'))
    monitor_target = serializers.ListField(label=_('告警目标'))
    monitor_config = serializers.DictField(label=_('告警配置'))
    notify_config = serializers.ListField(label=_('通知方式配置'))
    trigger_config = serializers.DictField(label=_('收敛配置(触发条件)'))
    convergence_config = serializers.DictField(label=_('收敛配置(告警屏蔽)'))
    receivers = serializers.ListField(label=_('告警接收者'))
    extra = serializers.JSONField()
    active = serializers.BooleanField(required=True, label=_('是否生效'))
    generate_type = serializers.ChoiceField(choices=(('user', _('用户')), ('admin', _('管理员')), ('system', _('系统'))))
    created_by = serializers.CharField(required=False)

    class Meta:
        model = models.DatamonitorAlertConfig

    def validate(self, attr):
        info = super(AlertConfigSerializer, self).validate(attr)
        if 'receivers' in info:
            if info['active']:
                receivers = custom_params_valid(AlertConfigReceiversSerializer, info['receivers'], many=True)
                if len(receivers) == 0:
                    raise ValidationError(_('告警接收者不能为空'))
            else:
                receivers = info['receivers']
            info['receivers'] = json.dumps(receivers)
        if 'monitor_target' in info:
            if info['active']:
                monitor_target = custom_params_valid(
                    AlertConfigMonitorTargetSerializer, info['monitor_target'], many=True
                )
            else:
                monitor_target = info['monitor_target']
            info['monitor_target'] = json.dumps(monitor_target)
        if 'monitor_config' in info:
            if info['active']:
                monitor_config = custom_params_valid(AlertConfigMonitorConfigSerializer, info['monitor_config'])
            else:
                monitor_config = info['monitor_config']
            info['monitor_config'] = json.dumps(monitor_config)
        if 'notify_config' in info:
            if info['active']:
                if isinstance(info['notify_config'], dict) and not any(info['notify_config'].values()):
                    raise ValidationError(_('必须有任意一种通知方式生效'))
                elif isinstance(info['notify_config'], list) and len(info['notify_config']) == 0:
                    raise ValidationError(_('必须有任意一种通知方式生效'))
            info['notify_config'] = json.dumps(info['notify_config'])
        if 'trigger_config' in info:
            if info['active']:
                trigger_config = custom_params_valid(AlertConfigTriggerConfigSerializer, info['trigger_config'])
            else:
                trigger_config = info['trigger_config']
            info['trigger_config'] = json.dumps(trigger_config)
        if 'convergence_config' in info:
            if info['active']:
                convergence_config = custom_params_valid(
                    AlertConfigConvergenceConfigSerializer, info['convergence_config']
                )
            else:
                convergence_config = info['convergence_config']
            info['convergence_config'] = json.dumps(convergence_config)
        if 'extra' in info:
            info['extra'] = json.dumps(info['extra'])

        if 'bk_username' in info:
            del info['bk_username']

        return info

    def to_representation(self, instance):
        info = model_to_dict(instance)
        try:
            info['receivers'] = json.loads(info['receivers'])
        except Exception:
            info['receivers'] = []
        try:
            info['monitor_target'] = json.loads(info['monitor_target'])
        except Exception:
            info['monitor_target'] = {}
        try:
            info['monitor_config'] = json.loads(info['monitor_config'])
        except Exception:
            info['monitor_config'] = {}
        try:
            info['notify_config'] = json.loads(info['notify_config'])
        except Exception:
            info['notify_config'] = {}
        try:
            info['trigger_config'] = json.loads(info['trigger_config'])
        except Exception:
            info['trigger_config'] = {}
        try:
            info['convergence_config'] = json.loads(info['convergence_config'])
        except Exception:
            info['convergence_config'] = {}
        try:
            info['extra'] = json.loads(info['extra'])
        except Exception:
            info['extra'] = {}
        try:
            info['created_at'] = info['created_at'].strftime('%Y-%m-%d %H:%M:%S')
            info['updated_at'] = info['updated_at'].strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            pass

        if hasattr(instance, 'datamonitoralertconfigrelation_set'):
            if instance.datamonitoralertconfigrelation_set.count() == 1:
                relation = instance.datamonitoralertconfigrelation_set.all()[0]
                info['alert_target_type'] = relation.target_type
                info['alert_target_id'] = relation.flow_id

        # 使所有配置都更新到最新的默认配置
        for alert_code in constants.DATA_MONITOR_DEFAULT_CODES:
            if alert_code in info['monitor_config']:
                last_config = info['monitor_config'][alert_code]
                info['monitor_config'][alert_code] = copy.copy(constants.DEFAULT_MONITOR_CONFIG[alert_code])
                info['monitor_config'][alert_code].update(last_config)
            else:
                info['monitor_config'][alert_code] = copy.copy(constants.DEFAULT_MONITOR_CONFIG[alert_code])
        for alert_code in constants.TASK_MONITOR_DEFAULT_CODES:
            if alert_code in info['monitor_config']:
                last_config = info['monitor_config'][alert_code]
                info['monitor_config'][alert_code] = copy.copy(constants.DEFAULT_MONITOR_CONFIG[alert_code])
                info['monitor_config'][alert_code].update(last_config)
            else:
                info['monitor_config'][alert_code] = copy.copy(constants.DEFAULT_MONITOR_CONFIG[alert_code])

        return info


class FlowAlertConfigSerializer(serializers.Serializer):
    bk_username = serializers.CharField(max_length=32, label=_('用户名'))
    node_id = serializers.CharField(required=False, max_length=512)


class FlowAlertConfigCreateSerializer(serializers.Serializer):
    bk_username = serializers.CharField(max_length=32, label=_('用户名'))
    flow_id = serializers.CharField(required=True, max_length=64)
    node_id = serializers.CharField(required=False, max_length=512)


class AlertLogSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.AlertLog

    def to_representation(self, instance):
        info = super(AlertLogSerializer, self).to_representation(instance)
        try:
            dimensions = json.loads(info['dimensions'])
            info['alert_details'] = []
            for biz_or_project_id in list(dimensions.keys()):
                for flow_id in list(dimensions[biz_or_project_id].keys()):
                    for alert_code in list(dimensions[biz_or_project_id][flow_id].keys()):
                        for alert_data in dimensions[biz_or_project_id][flow_id][alert_code]:
                            alert = json.loads(alert_data)
                            alert_metric = alert.get('dmonitor_alerts', {})
                            alert_tags = alert_metric.get('tags', {})
                            alert_detail = {
                                'message': alert_metric.get('message', ''),
                                'message_en': alert_metric.get('message_en', ''),
                                'full_message': alert_metric.get('full_message', ''),
                                'full_message_en': alert_metric.get('full_message_en', ''),
                                'alert_level': alert_tags.get('alert_level', 'warning'),
                                'alert_code': alert_tags.get('alert_code', ''),
                            }
                            alert_detail.update(alert_tags)
                            info['alert_details'].append(alert_detail)
            del info['dimensions']
        except Exception:
            pass
        return info


class AlertDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.AlertDetail

    def to_representation(self, instance):
        info = super(AlertDetailSerializer, self).to_representation(instance)
        try:
            info['receivers'] = json.loads(info['receivers'])
        except Exception:
            info['receivers'] = []
        try:
            info['notify_ways'] = json.loads(info['notify_ways'])
        except Exception:
            info['notify_ways'] = []
        try:
            info['monitor_config'] = json.loads(info['monitor_config'])
        except Exception:
            info['monitor_config'] = {}
        try:
            info['dimensions'] = json.loads(info['dimensions'])
        except Exception:
            info['dimensions'] = {}
        return info


class AlertDetailListSerializer(serializers.Serializer):
    flow_id = serializers.CharField(required=False, max_length=64, label=_('FlowID'))
    node_id = serializers.CharField(required=False, max_length=512, label=_('节点ID'))
    start_time = serializers.CharField(required=False, label=_('开始时间'))
    end_time = serializers.CharField(required=False, label=_('结束时间'))
    bk_biz_id = serializers.IntegerField(required=False, label=_('业务ID'))
    project_id = serializers.IntegerField(required=False, label=_('项目ID'))
    generate_type = serializers.ChoiceField(
        required=False, choices=(('user', _('用户')), ('admin', _('管理员')), ('system', _('系统')))
    )
    alert_config_ids = serializers.ListField(required=False, label=_('告警策略ID列表'))
    alert_status = serializers.CharField(required=False, max_length=32, label=_('告警状态'))
    dimensions = serializers.JSONField(required=False, label=_('按维度过滤'))
    group = serializers.CharField(required=False, label=_('分组维度'))


class AlertDetailMineSerializer(serializers.Serializer):
    start_time = serializers.CharField(required=False, label=_('开始时间'))
    end_time = serializers.CharField(required=False, label=_('结束时间'))
    alert_type = serializers.ChoiceField(
        required=False, label=_('告警类型'), choices=(('task_monitor', _('任务监控')), ('data_monitor', _('数据流监控')))
    )
    alert_target = serializers.CharField(required=False, label=_('告警对象'))
    alert_level = serializers.ChoiceField(
        required=False, label=_('告警级别'), choices=(('warning', _('警告')), ('danger', _('严重')))
    )
    alert_status = serializers.ListField(required=False, label=_('告警状态'))


class AlertTargetMineSerializer(serializers.Serializer):
    start_time = serializers.CharField(required=False, label=_('开始时间'))
    end_time = serializers.CharField(required=False, label=_('结束时间'))
    alert_type = serializers.ChoiceField(
        required=False, label=_('告警类型'), choices=(('task_monitor', _('任务监控')), ('data_monitor', _('数据流监控')))
    )
    alert_level = serializers.ChoiceField(
        required=False, label=_('告警级别'), choices=(('warning', _('警告')), ('danger', _('严重')))
    )
    alert_status = serializers.ListField(required=False, label=_('告警状态'))
    base = serializers.ChoiceField(
        required=False, label=_('依据'), choices=(('alert', _('告警')), ('alert_config', _('告警对象')))
    )
    project_id = serializers.IntegerField(required=False, label=_('项目ID'))
    bk_biz_id = serializers.IntegerField(required=False, label=_('业务ID'))
    alert_target_type = serializers.ChoiceField(
        required=False,
        label=_('告警对象类型'),
        choices=(
            ('dataflow', _('数据计算任务')),
            ('rawdata', _('数据集成任务')),
        ),
    )
    scope = serializers.ChoiceField(
        default='received', label=('告警配置列表范围'), choices=(('received', _('我作为接收人的告警配置')), ('managed', _('我管理的告警配置')))
    )


class AlertSendSerializer(serializers.Serializer):
    receiver = serializers.CharField(label=_('告警接收人'))
    notify_way = serializers.CharField(label=_('告警方式'))
    title = serializers.CharField(required=False, label=_('告警标题'))
    title_en = serializers.CharField(required=False, label=_('告警英文标题'))
    message = serializers.CharField(label=_('告警信息'))
    message_en = serializers.CharField(required=False, default='', label=_('英文告警信息'))


class AlertReportSerializer(serializers.Serializer):
    pass


class AlertShieldSerializer(serializers.ModelSerializer):
    start_time = serializers.DateTimeField(required=False, label=_('开始时间'))
    end_time = serializers.DateTimeField(required=True, label=_('结束时间'))
    reason = serializers.CharField(required=True, label=_('屏蔽原因'))
    alert_code = serializers.CharField(required=False, label=_('告警策略ID'))
    alert_level = serializers.CharField(required=False, label=_('告警级别'))
    alert_config_id = serializers.IntegerField(required=False, label=_('告警配置ID'))
    receivers = serializers.ListField(required=False, label=_('接收者'))
    notify_ways = serializers.ListField(required=False, label=_('通知方式'))
    dimensions = serializers.JSONField(required=False, label=_('屏蔽维度'))
    created_by = serializers.CharField(required=False, label=_('创建者'))

    class Meta:
        model = models.AlertShield

    def validate(self, attr):
        info = super(AlertShieldSerializer, self).validate(attr)

        if 'receivers' in info:
            info['receivers'] = json.dumps(info['receivers'])

        if 'notify_ways' in info:
            info['notify_ways'] = json.dumps(info['notify_ways'])

        if 'dimensions' in info:
            info['dimensions'] = json.dumps(info['dimensions'])

        return info

    def to_representation(self, instance):
        info = model_to_dict(instance)
        try:
            info['receivers'] = json.loads(info['receivers'])
        except Exception:
            info['receivers'] = []

        try:
            info['notify_ways'] = json.loads(info['notify_ways'])
        except Exception:
            info['notify_ways'] = []

        try:
            info['dimensions'] = json.loads(info['dimensions'])
        except Exception:
            info['dimensions'] = {}

        try:
            info['created_at'] = info['created_at'].strftime('%Y-%m-%d %H:%M:%S')
            info['updated_at'] = info['updated_at'].strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            pass

        return info


class DmonitorQuerySerializer(serializers.Serializer):
    database = serializers.ChoiceField(
        default='monitor_custom_metrics',
        label=_('时序数据库'),
        choices=(
            ('monitor_data_metrics', _('数据流监控指标')),
            ('monitor_custom_metrics', _('自定义监控指标')),
            ('monitor_performance_metrics', _('性能监控指标')),
        ),
    )
    sql = serializers.CharField(label=_('SQL'))
    tags = serializers.ListField(required=False, default=None, allow_null=True, label=_('地区标签'))

    def validate(self, attr):
        info = super(DmonitorQuerySerializer, self).validate(attr)
        info['tags'] = info['tags'] or []

        if len(info['tags']) > 1:
            raise dm_errors.OnlySupportOneGeogAreaError(_('只支持同时查询一个地区的TSDB指标'))

        return info


class DmonitorMetricRetrieveSerializer(serializers.Serializer):
    data_set_ids = serializers.ListField(required=False, label=_('数据集ID列表'))
    flow_ids = serializers.ListField(required=False, label=_('数据流ID列表'))
    node_ids = serializers.ListField(required=False, label=_('数据流节点ID列表'))
    storages = serializers.ListField(required=False, label=_('数据集所在存储列表'))
    conditions = serializers.JSONField(required=False, label=_('维度过滤条件'))
    dimensions = serializers.ListField(required=False, label=_('聚合维度'))
    format = serializers.ChoiceField(
        default='series',
        choices=(
            ('series', _('序列')),
            ('value', _('单值')),
            ('raw', _('原始指标')),
        ),
        label=_('指标格式'),
    )
    start_time = serializers.CharField(default='now()-1d', label=_('开始时间'))
    end_time = serializers.CharField(default='now()', label=_('结束时间'))
    time_grain = serializers.ChoiceField(
        default='1m',
        choices=(
            ('1m', _('1分钟')),
            ('10m', _('10分钟')),
            ('30m', _('30分钟')),
            ('1h', _('1小时')),
            ('1d', _('1天')),
        ),
    )
    fill = serializers.ChoiceField(
        default='null',
        choices=(('null', _('赋值为空')), ('0', _('赋值为0')), ('previous', _('赋值为上一个点的值')), ('none', _('不返回该时间点'))),
    )

    def validate(self, attr):
        info = super(DmonitorMetricRetrieveSerializer, self).validate(attr)

        if len(info['data_set_ids']) + len(info['flow_ids']) + len(info['node_ids']) == 0:
            raise dm_errors.MetricQueryParamError(_('参数中必须至少包含一项data_set_ids, flow_id或node_ids'))

        if len(info['data_set_ids']) > 100:
            raise dm_errors.MetricQueryParamError(_('不允许同时查询超过100个数据集'))

        if len(info['flow_ids']) > 10:
            raise dm_errors.MetricQueryParamError(_('不允许同时查询超过10个数据流'))

        if len(info['node_ids']) > 100:
            raise dm_errors.MetricQueryParamError(_('不允许同时查询超过100个数据流节点'))

        return info


class DmonitorReportSerializer(serializers.Serializer):
    message = serializers.JSONField(label=_('上报的数据'))
    kafka_cluster = serializers.CharField(required=False, default='kafka-op', label=_('kafka集群'))
    kafka_topic = serializers.CharField(required=False, default='bkdata_data_monitor_metrics591', label=_('上报数据的topic'))
    tags = serializers.ListField(required=False, default=None, allow_null=True, label=_('地区标签'))

    def validate(self, attr):
        info = super(DmonitorReportSerializer, self).validate(attr)
        info['tags'] = info['tags'] or []

        if len(info['tags']) > 1:
            raise dm_errors.OnlySupportOneGeogAreaError(_('只支持同时上报埋点到一个地区的kafka集群'))

        return info


class DmonitorMetricsDeleteSerializer(serializers.Serializer):
    data_set_id = serializers.CharField(required=True, label=_('数据集ID'))
    storage = serializers.CharField(required=False, label=_('数据集所在存储'))
    tags = serializers.ListField(required=False, default=None, allow_null=True, label=_('地区标签'))

    def validate(self, attr):
        info = super(DmonitorMetricsDeleteSerializer, self).validate(attr)
        info['tags'] = info['tags'] or []

        if len(info['tags']) > 1:
            raise dm_errors.OnlySupportOneGeogAreaError(_('只支持同时删除一个地区某个数据集的TSDB指标'))

        return info


class ResultTableListSerializer(serializers.Serializer):
    result_table_ids = serializers.ListField(label=_('结果表列表'))


class DmonitorBatchMonitorSerializer(serializers.Serializer):
    result_table_ids = serializers.ListField(label=_('结果表列表'))
    processing_ids = serializers.ListField(label=_('数据处理ID列表'))
    period = serializers.IntegerField(required=False, label=_('查询周期'))


class DmonitorBatchExecutionSerializer(serializers.Serializer):
    start_time = serializers.CharField(required=False, label=_('开始时间'))
    end_time = serializers.CharField(required=False, label=_('结束时间'))
    recent_time = serializers.IntegerField(required=False, label=_('最近时间范围'))


class DmonitorBatchScheduleSerializer(serializers.Serializer):
    processing_ids = serializers.ListField(label=_('数据处理ID列表'))


class DmonitorFlowSerializer(serializers.Serializer):
    with_nodes = serializers.BooleanField(default=False, label=_('是否包含节点信息'))
    update_duration = serializers.IntegerField(default=0, label=_('更新周期'))


class DmonitorDataOperationSerializer(serializers.Serializer):
    with_relations = serializers.BooleanField(default=False, label=_('是否包含关联关系'))
    with_status = serializers.BooleanField(default=False, label=_('是否包含状态信息'))
    with_node = serializers.BooleanField(default=False, label=_('是否包含节点或上下游节点信息'))


class DmonitorDataFlowSerializer(DmonitorFlowSerializer):
    with_nodes = serializers.BooleanField(default=False, label=_('是否包含节点信息'))
    only_running = serializers.BooleanField(default=False, label=_('是否只包含运行中的流'))


class GrafanaAlertSerilizer(serializers.Serializer):
    title = serializers.CharField(required=True, label=_('告警标题'))
    ruleName = serializers.CharField(required=True, label=_('告警规则名称'))
    ruleUrl = serializers.CharField(required=True, label=_('告警规则路径'))
    state = serializers.CharField(required=True, label=_('告警状态'))
    message = serializers.CharField(required=False, label=_('告警信息'))
    evalMatches = serializers.JSONField(required=False, label=_('告警参数'))

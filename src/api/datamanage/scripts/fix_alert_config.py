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

from common.transaction import auto_meta_sync

from datamanage.lite.dmonitor.constants import DEFAULT_MONITOR_CONFIG
from datamanage.lite.dmonitor.models import DatamonitorAlertConfigRelation


def fix_alert_config():
    old_raw_data_set, old_dataflow_set = get_old_alert_configs()
    new_raw_data_set, new_dataflow_set = get_new_alert_configs()

    combine_alert_config(new_dataflow_set, old_dataflow_set, 'dataflow')
    combine_alert_config(new_raw_data_set, old_raw_data_set, 'rawdata')


def get_old_alert_configs():
    # 准备所有历史数据
    relations = list(
        DatamonitorAlertConfigRelation.objects.exclude(alert_config_type='all_monitor')
        .select_related('alert_config')
        .all()
    )
    old_raw_data_set = {}
    old_dataflow_set = {}
    for item in relations:
        flow_id = str(item.flow_id)
        if item.target_type == 'dataflow':
            if flow_id not in old_dataflow_set:
                old_dataflow_set[flow_id] = []
            old_dataflow_set[flow_id].append(item)
        elif item.target_type == 'rawdata':
            if flow_id not in old_raw_data_set:
                old_raw_data_set[flow_id] = []
            old_raw_data_set[flow_id].append(item)

    return old_raw_data_set, old_dataflow_set


def get_new_alert_configs():
    # 准备所有历史数据
    relations = list(
        DatamonitorAlertConfigRelation.objects.filter(alert_config_type='all_monitor')
        .select_related('alert_config')
        .all()
    )
    new_raw_data_set = {}
    new_dataflow_set = {}
    exc = None
    err_flow_ids = []
    for item in relations:
        flow_id = str(item.flow_id)
        if item.target_type == 'dataflow':
            if flow_id in new_dataflow_set:
                exc = True
                err_flow_ids.append('dataflow%s' % flow_id)
            new_dataflow_set[flow_id] = item
        elif item.target_type == 'rawdata':
            if flow_id in new_raw_data_set:
                exc = True
                err_flow_ids.append('rawdata%s' % flow_id)
            new_raw_data_set[flow_id] = item

    if exc:
        print(err_flow_ids)
        raise
    return new_raw_data_set, new_dataflow_set


def del_alter_config(flow_id, new_flow_set, target_type, flow_alert_config_relations):
    if flow_id not in new_flow_set:
        print(
            'There is no new alert_config for {target_type}({flow_id})'.format(target_type=target_type, flow_id=flow_id)
        )
        with auto_meta_sync(using='bkdata_basic'):
            # 直接删除旧配置
            for relation in flow_alert_config_relations:
                alert_config = relation.alert_config
                relation.delete()
                alert_config.delete()
        return 'continue'
    new_flow_relation = new_flow_set[flow_id]

    if len(flow_alert_config_relations) != 2:
        print(
            'The {target_type}({flow_id}) doesn\'t have two alert_config. '
            'Its list of alert_config_relation_id is {relation_ids}'.format(
                target_type=target_type,
                flow_id=flow_id,
                relation_ids=json.dumps([x.id for x in flow_alert_config_relations]),
            )
        )
        return 'continue'
    if not check_correct_configs(flow_alert_config_relations[0], flow_alert_config_relations[1]):
        return 'continue'

    return new_flow_relation


def combine_alert_config(new_flow_set, old_flow_set, target_type):
    for flow_id, flow_alert_config_relations in list(old_flow_set.items()):
        new_flow_relation = del_alter_config(flow_id, new_flow_set, target_type, flow_alert_config_relations)
        if new_flow_relation == 'continue':
            continue
        try:
            with auto_meta_sync(using='bkdata_basic'):
                # 合并告警策略配置
                monitor_config = copy.copy(DEFAULT_MONITOR_CONFIG)
                notify_config, trigger_config_json, convergence_config_json, receivers_json = None, None, None, None
                alert_config = new_flow_relation.alert_config
                for relation in flow_alert_config_relations:
                    tmp_config = json.loads(relation.alert_config.monitor_config)
                    if not relation.alert_config.active:
                        for tmp_alert_code in list(monitor_config.keys()):
                            tmp_config[tmp_alert_code]['monitor_status'] = 'off'
                    monitor_config.update(tmp_config)
                    notify_config = json.loads(relation.alert_config.notify_config)
                    trigger_config_json = relation.alert_config.trigger_config
                    convergence_config_json = relation.alert_config.convergence_config
                    receivers_json = relation.alert_config.receivers
                    alert_config.active = alert_config.active or relation.alert_config.active
                alert_config.monitor_config = json.dumps(monitor_config)

                # 更新通知配置
                alert_config.notify_config = json.dumps(
                    list(
                        [
                            x[0]
                            for x in [y for y in zip(list(notify_config.keys()), list(notify_config.values())) if y[1]]
                        ]
                    )
                )

                # 更新告警收敛配置
                alert_config.trigger_config = trigger_config_json
                alert_config.convergence_config = convergence_config_json
                alert_config.receivers = receivers_json
                alert_config.save()

                # 删除旧配置
                for relation in flow_alert_config_relations:
                    alert_config = relation.alert_config
                    relation.delete()
                    alert_config.delete()

            print(
                'Success to update alert_config of {target_type}({flow_id})'.format(
                    target_type=target_type, flow_id=flow_id
                )
            )
        except Exception:
            print(
                'Failed to combine alert_config of {target_type}({flow_id})'.format(
                    target_type=target_type, flow_id=flow_id
                )
            )


def check_correct_configs(relation1, relation2):
    if not (relation1.alert_config.active and relation2.alert_config.active):
        return True

    if relation1.alert_config.notify_config != relation2.alert_config.notify_config:
        print(
            'The notify_config of {target_type}({flow_id}) '
            'included by config({config1_id}) and config({config2_id}) are not equal'.format(
                target_type=relation1.target_type,
                flow_id=relation1.flow_id,
                config1_id=relation1.alert_config.id,
                config2_id=relation2.alert_config.id,
            )
        )
        return False

    if relation1.alert_config.receivers != relation2.alert_config.receivers:
        print(
            'The receivers of {target_type}({flow_id}) '
            'included by config({config1_id}) and config({config2_id}) are not equal'.format(
                target_type=relation1.target_type,
                flow_id=relation1.flow_id,
                config1_id=relation1.alert_config.id,
                config2_id=relation2.alert_config.id,
            )
        )
        return False

    if relation1.alert_config.trigger_config != relation2.alert_config.trigger_config:
        print(
            'The trigger_config of {target_type}({flow_id}) '
            'included by config({config1_id}) and config({config2_id}) are not equal'.format(
                target_type=relation1.target_type,
                flow_id=relation1.flow_id,
                config1_id=relation1.alert_config.id,
                config2_id=relation2.alert_config.id,
            )
        )
        return False

    if relation1.alert_config.convergence_config != relation2.alert_config.convergence_config:
        print(
            'The convergence_config of {target_type}({flow_id}) '
            'included by config({config1_id}) and config({config2_id}) are not equal'.format(
                target_type=relation1.target_type,
                flow_id=relation1.flow_id,
                config1_id=relation1.alert_config.id,
                config2_id=relation2.alert_config.id,
            )
        )
        return False

    return True


if __name__ == '__main__':
    fix_alert_config()

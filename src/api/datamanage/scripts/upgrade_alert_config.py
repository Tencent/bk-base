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

from common.meta.common import create_tag_to_target
from common.transaction import auto_meta_sync

from datamanage.utils.api import AccessApi, MetaApi
from datamanage.lite.dmonitor.flow_models import Dataflow
from datamanage.lite.dmonitor.constants import (
    DEFAULT_MONITOR_CONFIG,
    DEFAULT_TRIGGER_CONFIG,
    DEFAULT_CONVERGENCE_CONFIG,
    MSG_TYPE_MAPPSINGS,
)
from datamanage.lite.dmonitor.models import DatamonitorAlertConfig, DatamonitorAlertConfigRelation
from datamanage.scripts.fix_alert_config import get_old_alert_configs, del_alter_config


def upgrade_alert_config():
    old_raw_data_set, old_dataflow_set = get_old_alert_configs()

    exist_new_relations = list(
        DatamonitorAlertConfigRelation.objects.filter(alert_config_type='all_monitor')
        .select_related('alert_config')
        .all()
    )
    exist_new_configs_flows = {}
    for item in exist_new_relations:
        flow_id = str(item.flow_id)
        exist_new_configs_flows[flow_id] = item

    new_raw_data_set, new_dataflow_set = create_all_flow_configs(exist_new_configs_flows)

    combine_alert_config(new_dataflow_set, old_dataflow_set, 'dataflow')
    combine_alert_config(new_raw_data_set, old_raw_data_set, 'rawdata')


def create_all_flow_configs(skip_flows):
    new_raw_data_set, new_dataflow_set = {}, {}
    alert_configs_geog_area = {}

    project_list = MetaApi.projects.list().data
    projects = {item['project_id']: item for item in project_list}

    raw_data_list = AccessApi.rawdata.list().data
    raw_datas = {item['id']: item for item in raw_data_list}
    dataflow_list = Dataflow.objects.values('flow_id', 'project_id')
    dataflows = {item['flow_id']: item for item in dataflow_list}

    raw_data_keys = list(raw_datas.keys())
    alert_config_index = 0
    alert_config_step = 100
    alert_config_length = len(raw_data_keys)

    # 创建所以未创建的rawdata的告警配置
    while alert_config_index < alert_config_length:
        with auto_meta_sync(using='bkdata_basic'):
            max_index = min(alert_config_index + alert_config_step, alert_config_length)
            for raw_data_id in raw_data_keys[alert_config_index:max_index]:
                flow_id = str(raw_data_id)
                if flow_id in skip_flows:
                    continue
                alert_config, relation, geog_area = create_rawdata_alert_config(raw_data_id, raw_datas)
                new_raw_data_set[str(raw_data_id)] = relation
                alert_configs_geog_area[alert_config.id] = geog_area
        alert_config_index += alert_config_step

    dataflow_keys = list(dataflows.keys())
    alert_config_index = 0
    alert_config_step = 100
    alert_config_length = len(dataflows)

    # 创建所有未创建的dataflow的告警配置
    while alert_config_index < alert_config_length:
        with auto_meta_sync(using='bkdata_basic'):
            max_index = min(alert_config_index + alert_config_step, alert_config_length)
            for dataflow_id in dataflow_keys[alert_config_index:max_index]:
                flow_id = str(dataflow_id)
                if flow_id in skip_flows:
                    continue
                alert_config, relation, geog_area = create_dataflow_alert_config(dataflow_id, dataflows, projects)
                new_dataflow_set[str(dataflow_id)] = relation
                alert_configs_geog_area[alert_config.id] = geog_area
        alert_config_index += alert_config_step

    # 给所有新创建的告警配置增加地域标签
    geog_area_index = 0
    geog_area_step = 100
    alert_config_ids = list(alert_configs_geog_area.keys())
    geog_area_length = len(alert_config_ids)
    while geog_area_index < geog_area_length:
        with auto_meta_sync(using='bkdata_basic'):
            max_index = min(geog_area_index + geog_area_step, geog_area_length)
            for alert_config_id in alert_config_ids[geog_area_index:max_index]:
                geog_area = alert_configs_geog_area[alert_config_id]
                create_tag_to_target([('alert_config', alert_config_id)], [geog_area])
        geog_area_index += geog_area_step

    return new_raw_data_set, new_dataflow_set


def create_rawdata_alert_config(raw_data_id, raw_datas):
    raw_data = AccessApi.rawdata.retrieve({'raw_data_id': raw_data_id, 'show_display': 0, 'bk_username': 'admin'}).data
    geog_areas = raw_data.get('tags', {}).get('manage', {}).get('geog_area', [])
    if len(geog_areas) > 0:
        geog_area = geog_areas[0].get('code')
    else:
        raise Exception('Can not get geog_area tag about rawdata(%s)' % raw_data_id)
    alert_config, relation = create_alert_config(
        monitor_target={'raw_data_id': raw_data_id, 'data_set_id': None, 'target_type': 'rawdata'},
        target_type='rawdata',
        alert_config_type='all_monitor',
        flow_id=str(raw_data_id),
        geog_area=geog_area,
        created_by=raw_data.get('created_by') or 'admin',
    )
    return alert_config, relation, geog_area


def create_dataflow_alert_config(dataflow_id, dataflows, projects):
    dataflow = dataflows[dataflow_id]
    project_id = dataflow.get('project_id')
    project_info = projects.get(project_id, {})
    geog_areas = project_info.get('tags', {}).get('manage', {}).get('geog_area', [])
    if len(geog_areas) > 0:
        geog_area = geog_areas[0].get('code')
    else:
        raise Exception('Can not get geog_area tag about dataflow(%s)' % dataflow_id)
    alert_config, relation = create_alert_config(
        monitor_target={'flow_id': dataflow_id, 'node_id': None, 'target_type': 'dataflow'},
        target_type='dataflow',
        alert_config_type='all_monitor',
        flow_id=str(dataflow_id),
        geog_area=geog_area,
        created_by=dataflow.get('created_by') or 'admin',
    )
    return alert_config, relation, geog_area


def create_alert_config(
    monitor_target,
    target_type,
    alert_config_type,
    flow_id,
    geog_area='inland',
    monitor_config=None,
    notify_config=None,
    trigger_config=None,
    convergence_config=None,
    created_by='admin',
):
    alert_config = DatamonitorAlertConfig(
        monitor_target=json.dumps([monitor_target]),
        monitor_config=monitor_config or json.dumps(DEFAULT_MONITOR_CONFIG),
        notify_config=notify_config or json.dumps([]),
        trigger_config=trigger_config or json.dumps(DEFAULT_TRIGGER_CONFIG),
        convergence_config=convergence_config or json.dumps(DEFAULT_CONVERGENCE_CONFIG),
        receivers='[]',
        generate_type='user',
        extra='{}',
        active=False,
        created_by=created_by,
    )
    alert_config.save()

    relation = DatamonitorAlertConfigRelation(
        target_type=target_type, flow_id=flow_id, alert_config=alert_config, alert_config_type=alert_config_type
    )
    relation.save()

    return alert_config, relation


def combine_alert_config(new_flow_set, old_flow_set, target_type):
    for flow_id, flow_alert_config_relations in list(old_flow_set.items()):
        new_flow_relation = del_alter_config(flow_id, new_flow_set, target_type, flow_alert_config_relations)
        if new_flow_relation == 'continue':
            continue

        with auto_meta_sync(using='bkdata_basic'):
            # 合并告警策略配置
            monitor_config = copy.copy(DEFAULT_MONITOR_CONFIG)
            notify_config, trigger_config_json, convergence_config_json, receivers_json = None, None, None, None
            alert_config = new_flow_relation.alert_config
            for relation in flow_alert_config_relations:
                monitor_config = json.loads(relation.alert_config.monitor_config)
                if not relation.alert_config.active:
                    for tmp_alert_code in list(monitor_config.keys()):
                        monitor_config[tmp_alert_code]['monitor_status'] = 'off'
                monitor_config.update(monitor_config)
                notify_config = json.loads(relation.alert_config.notify_config)
                trigger_config_json = relation.alert_config.trigger_config
                convergence_config_json = relation.alert_config.convergence_config
                receivers_json = relation.alert_config.receivers
                alert_config.active = alert_config.active or relation.alert_config.active
            alert_config.monitor_config = json.dumps(monitor_config)

            # 更新通知配置
            if isinstance(notify_config, dict):
                alert_config.notify_config = json.dumps(
                    list(
                        map(
                            lambda x: MSG_TYPE_MAPPSINGS.get(x[0], x[0]),
                            [y for y in zip(list(notify_config.keys()), list(notify_config.values())) if y[1]],
                        )
                    )
                )
            else:
                alert_config.notify_config = json.dumps(notify_config)

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


if __name__ == '__main__':
    upgrade_alert_config()

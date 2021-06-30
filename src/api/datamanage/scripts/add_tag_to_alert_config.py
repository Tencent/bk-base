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

from conf import dataapi_settings

from common.transaction import auto_meta_sync
from common.meta.common import create_tag_to_target
from datamanage.utils.api import DataflowApi, MetaApi
from datamanage.lite.dmonitor.models import DatamonitorAlertConfig, DatamonitorAlertConfigRelation


MULTI_GEOG_AREA = getattr(dataapi_settings, 'MULTI_GEOG_AREA', False)


def get_geog_area_by_target_flow(target_type, flow_id):
    try:
        tags = []
        if target_type == 'dataflow':
            flow_info = DataflowApi.flows.retrieve({'flow_id': flow_id}).data
            tags = MetaApi.search_target_tag(
                {
                    'target_type': 'project',
                    'target_filter': json.dumps(
                        [
                            {
                                'criterion': [{'k': 'project_id', 'func': 'eq', 'v': flow_info.get('project_id')}],
                                'condition': 'OR',
                            }
                        ]
                    ),
                }
            ).data
        elif target_type == 'rawdata':
            tags = MetaApi.search_target_tag(
                {
                    'target_type': 'raw_data',
                    'target_filter': json.dumps(
                        [{'criterion': [{'k': 'id', 'func': 'eq', 'v': flow_id}], 'condition': 'OR'}]
                    ),
                }
            ).data
        for tag_info in tags:
            geog_area_tags = tag_info.get('tags', {}).get('manage', {}).get('geog_area', [])
            if len(geog_area_tags) > 0:
                return geog_area_tags[0].get('code')
    except Exception as e:
        print(
            '[ERROR] Can not get geog_area tag for {target_type}({flow_id}). ERROR: {error}'.format(
                target_type=relations[0].target_type, flow_id=relations[0].flow_id, error=e
            )
        )


if __name__ == '__main__':
    if MULTI_GEOG_AREA is True:
        for item in DatamonitorAlertConfig.objects.all():
            relations = DatamonitorAlertConfigRelation.objects.filter(alert_config=item)
            if len(relations) == 1:
                geog_area = get_geog_area_by_target_flow(relations[0].target_type, relations[0].flow_id)
                if geog_area is None:
                    continue
                try:
                    with auto_meta_sync(using='bkdata_basic'):
                        create_tag_to_target([('alert_config', item.id)], [geog_area])
                except Exception as e:
                    print(
                        '[ERROR] Can not add geog_area tag({geog_area}) for {target_type}({flow_id}).'
                        'ERROR: {error}'.format(
                            geog_area=geog_area,
                            target_type=relations[0].target_type,
                            flow_id=relations[0].flow_id,
                            error=e,
                        )
                    )
            else:
                try:
                    res = MetaApi.get_geog_area_tags()
                    all_geog_area_tags = list(res.data.get('supported_areas', {}).keys())
                    with auto_meta_sync(using='bkdata_basic'):
                        create_tag_to_target([('alert_config', item.id)], all_geog_area_tags)
                except Exception as e:
                    print(
                        '[ERROR] Can not add geog_area tag({geog_area}) for {target_type}({flow_id}).'
                        'ERROR: {error}'.format(
                            geog_area=geog_area,
                            target_type=relations[0].target_type,
                            flow_id=relations[0].flow_id,
                            error=e,
                        )
                    )

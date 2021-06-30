# -*-coding: utf-8 -*-
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

from common.local import get_request_username
from common.transaction import auto_meta_sync

from datamanage.utils.api import MetaApi
from datamanage.pro.dstan.models import DmTaskDetailV1
from datamanage.pro.dstan.exceptions import DstanInvalidParam, DstanObjectNotExist, MetaDataNotAsExpected
from datamanage.pro.dstan.constants import StandardType


class StandardManager(object):
    def __init__(self):
        pass

    def bind_dataset_standard_directly(self, dataset, standard, standard_type):
        """
        将数据集与标准直接绑定，目前只支持给 RESULT_TABLE 进行标准化标注

        :param {DataSet} dataset
        :param {DmStandardConfig} standard
        :param {StandardType} standard_type 标准类型，主要说明处于标准化的什么环节
        """
        latest_version = standard.get_latest_online_version()

        if dataset.data_set_type not in ['result_table']:
            raise DstanInvalidParam(message_kv={'k': 'dataset.data_set_type', 'v': dataset.data_set_type})

        result_table = MetaApi.result_tables.retrieve(
            {'result_table_id': dataset.data_set_id}, raise_exception=True
        ).data
        if 'result_table_id' not in result_table:
            raise DstanObjectNotExist(message_kv={'object_id': dataset.data_set_id})

        bk_biz_id = result_table['bk_biz_id']
        project_id = result_table['project_id']

        tags = self.list_standard_tag(standard)

        # 平台以及平台合作团队标准化的表，都需要带上 mark_standard 标签
        tags.append('mark_standard')

        # 不同标准化环节，需要打上不同的标签
        map_standard_type_tag = {StandardType.DETAILDATA: 'details', StandardType.INDICATOR: 'summarized'}
        tags.append(map_standard_type_tag[standard_type])

        params = {
            'tag_targets': [
                {
                    'target_id': dataset.data_set_id,
                    'target_type': dataset.data_set_type,
                    'tags': [{'tag_code': tag_code} for tag_code in tags],
                }
            ]
        }
        MetaApi.tagged(params, raise_exception=True)

        with auto_meta_sync(using='bkdata_basic'):
            # 直接进行标准绑定是不存在标准化任务的，所以在此场景下，标准化任务 ID = 0
            if (
                DmTaskDetailV1.objects.filter(
                    task_id=0,
                    standard_version_id=latest_version.id,
                    data_set_type=dataset.data_set_type,
                    data_set_id=dataset.data_set_id,
                ).count()
                == 0
            ):
                standard_relation = DmTaskDetailV1(
                    task_id=0,
                    task_content_id=0,
                    standard_version_id=latest_version.id,
                    bk_biz_id=bk_biz_id,
                    project_id=project_id,
                    data_set_type=dataset.data_set_type,
                    data_set_id=dataset.data_set_id,
                    task_type=standard_type.value,
                    created_by=get_request_username(),
                    updated_by=get_request_username(),
                )
                standard_relation.save()

    def list_standard_tag(self, standard):
        """
        获取数据标准的标签
        """
        data = MetaApi.query_tag_targets(
            {
                'target_type': 'standard',
                'target_filter': json.dumps(
                    [{"criterion": [{"k": "id", "func": "eq", "v": standard.id}], 'condition': 'OR'}]
                ),
            },
            raise_exception=True,
        ).data

        if data['count'] == 0:
            return []

        if data['count'] > 1:
            raise MetaDataNotAsExpected(message_kv={'message': "出现多个相同的数据标准（{}）".format(standard.id)})

        # 目前仅把标准中应用、系统、业务三类标签给同步至 RT 表
        categories = ['application', 'business', 'system']
        tags = data['content'][0]['tags']
        tag_codes = []
        for cate in categories:
            tag_codes.extend([tag['code'] for tag in tags[cate]])

        return tag_codes

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

from datamanage.tests import BaseTestCase


class StandardManagerTestCase(BaseTestCase):

    # @mock.patch('common.api.MetaApi.sync_hook')
    # @mock.patch('datamanage.utils.api.meta.metaapi.tagged')
    # @mock.patch('datamanage.utils.api.meta.metaapi.result_tables.retrieve')
    # @mock.patch('datamanage.utils.api.meta.metaapi.query_tag_targets')
    def test_bind_dataset_standard_directly(self):
        pass
        # patch_query_tag_targets.return_value = DataResponse({
        #     'message': "ok",
        #     'code': "1500200",
        #     'data': {
        #         'count': 1,
        #         'content': [{
        #             'category_id': 42,
        #             'id': 215,
        #             'tags': {
        #                 'application': [{'code': 'predict'}],
        #                 'business': [{'code': 'login'}],
        #                 'system': [],
        #                 'manage': [],
        #                 'desc': []
        #             }
        #         }]
        #     }
        # })

        # patch_retrieve.return_value = DataResponse({
        #     'data': {
        #         'bk_biz_id': 765,
        #         'result_table_type': None,
        #         'result_table_name_alias': "desc",
        #         'project_name': None,
        #         'count_freq': 0,
        #         'updated_by': "admin",
        #         'sensitivity': "private",
        #         'result_table_name': "system_mem",
        #         'created_at': "2019-08-07 16:27:09",
        #         'processing_type': "stream",
        #         'updated_at': "2019-08-14 15:06:41",
        #         'created_by': "admin",
        #         'count_freq_unit': "S",
        #         'platform': "bk_data",
        #         'generate_type': "user",
        #         'data_category': "UTF8",
        #         'is_managed': 1,
        #         'project_id': 4299,
        #         'result_table_id': "765_system_mem",
        #         'description': "desc"
        #     },
        #     'result': True
        # })

        # dataset = DataSet('result_table', '765_system_mem')
        # standard = DmStandardConfig.objects.get(id=1)
        # standard_type = StandardType('indicator')

        # StandardManager().bind_dataset_standard_directly(dataset, standard, standard_type)
        # self.assertEqual(DmTaskDetailV1.objects.filter(
        #     standard_version_id=1, data_set_id='765_system_mem').count(), 1)
        # self.assertEqual(patch_tagged.call_count, 1)
        # self.assertEqual(patch_sync_hook.call_count, 1)

# # -*- coding: utf-8 -*-
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

# from __future__ import absolute_import, print_function, unicode_literals

# from unittest import mock

# from common.api.base import DataResponse
# from auth.models import UserRole
# from auth.tasks.sync_cc import sync_business
# from auth.tasks.sync_cc_clouds import sync_business as sync_business_clouds
# from auth.models.base_models import RoleConfig
# from auth.tasks.sync_tdm_auth import sync_tdm_auth
# from auth.tests.utils import BaseTestCase


# class TaskTestCase(BaseTestCase):

#     @mock.patch('auth.api.tof.TOFApi.get_staff_info')
#     def test_sync_cc(self, patch_get_staff_info):
#         patch_get_staff_info.return_value = DataResponse({
#             'result': True,
#             'data': {
#                 'StatusName': '在职',
#                 'StatusId': '1',
#                 'TypeId': '2',
#                 'PostName': '安全产品组员工',
#                 'WorkDeptId': '35074',
#                 'OfficialId': '8',
#                 'TypeName': '正式',
#                 'WorkDeptName': 'XX市场部',
#                 'Gender': '女',
#                 'OfficialName': '普通员工',
#                 'RTX': 'user01',
#                 'LoginName': 'user01',
#                 'DepartmentName': 'XX市场部',
#                 'Enabled': 'true',
#                 'GroupId': '111'
#             }
#         })

#         current_count = UserRole.objects.count()
#         sync_business()
#         self.assertTrue(UserRole.objects.count() > current_count)

#     def test_sync_cc_clouds(self):
#         current_count = UserRole.objects.count()
#         sync_business_clouds()
#         self.assertTrue(UserRole.objects.count() > current_count)

#     def test_sync_tdm_auth(self):

#         O_TDM_MANAGER = RoleConfig.objects.get(pk='bkdata.tdm_manager')
#         O_RAW_DATA_VIEWER = RoleConfig.objects.get(pk='raw_data.viewer')
#         O_RESULT_TABLE_VIEWER = RoleConfig.objects.get(pk='result_table.viewer')

#         UserRole.objects.create(user_id='user01', role_id=O_TDM_MANAGER.role_id)
#         UserRole.objects.create(user_id='user02', role_id=O_TDM_MANAGER.role_id)
#         sync_tdm_auth(gap=False)
#         raw_data_viewers = UserRole.objects.filter(role_id=O_RAW_DATA_VIEWER.role_id,
#                                                    scope_id='4').values_list('user_id', flat=True)

#         result_table_viewers = UserRole.objects.filter(role_id=O_RESULT_TABLE_VIEWER.role_id,
#                                                        scope_id='591_test_rt3').values_list('user_id', flat=True)

#         self.assertIn('user01', raw_data_viewers)
#         self.assertIn('user02', raw_data_viewers)
#         self.assertIn('user01', result_table_viewers)
#         self.assertIn('user02', result_table_viewers)

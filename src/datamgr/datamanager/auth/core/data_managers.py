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
from __future__ import absolute_import, print_function, unicode_literals

from auth.core.permission import RoleController
from auth.exceptions import IvalidDataSetErr
from auth.handlers.lineage import DataSetType, makeup_identifier, task_apart_identifier


class DataManagerController(object):
    def __init__(self):
        self._role_controller = RoleController()

    def get_raw_data_managers(self, session):
        managers = self._role_controller.get_role_members_with_all_scope(
            session, "raw_data.manager"
        )

        return {
            makeup_identifier(DataSetType.RAW_DATA, scope_id): manager
            for scope_id, manager in managers.items()
        }

    def get_managers(self, session, identifier):
        data_set_type, data_set_id = task_apart_identifier(identifier)
        role_id = switch_manage_role_id(data_set_type)
        return self._role_controller.get_role_members(session, role_id, data_set_id)

    def get_managers_interset(self, session, identifiers, cache_managers=None):
        if cache_managers is not None:
            manager_sets = [
                set(cache_managers[identifier])
                if identifier in cache_managers
                else set()
                for identifier in identifiers
            ]

            if len(manager_sets) > 0:
                interset = manager_sets[0]
                for manager_set in manager_sets[1:]:
                    interset = interset.intersection(manager_set)
            else:
                interset = set()

            return list(interset)

        raise NotImplementedError("Haha...")

    def update_managers(self, session, identifier, managers):
        data_set_type, data_set_id = task_apart_identifier(identifier)
        role_id = switch_manage_role_id(data_set_type)
        return self._role_controller.update_role_members(
            session, role_id, data_set_id, managers
        )


def switch_manage_role_id(data_set_type):
    if data_set_type in [DataSetType.RESULT_TABLE, DataSetType.RAW_DATA]:
        return "{}.manager".format(data_set_type.value)

    raise IvalidDataSetErr(message_kv={"dataset_type": data_set_type})

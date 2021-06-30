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

from datetime import datetime, timedelta

import pytest
from datahub.access.tests import db_helper


@pytest.fixture
def task_add():

    task_id = 1

    with db_helper.open_cursor("mapleleaf") as cur:

        db_helper.insert(
            cur,
            "databus_hdfs_import_tasks",
            result_table_id="101_xxx_test",
            data_dir="test_data_dir",
            hdfs_conf_dir="test_hdfs_conf_dir",
            kafka_bs="test_kafka_bs",
            finished=0,
            status="t",
            created_at="2018-10-29 16:42:52",
            created_by="",
            updated_by="",
            description="",
            hdfs_custom_property="",
            id=task_id,
        )

    try:
        yield task_id
    finally:
        _task_delete(task_id)


@pytest.fixture
def task_add_multi():
    with db_helper.open_cursor("mapleleaf") as cur:
        #
        # 'UNFINISHED_TASKS': 0,
        # 'EMPTY_DATA_DIR': 0,
        # 'BAD_DATA_FILE': 0,
        # 'FILE_TOO_BIG': 0}
        del_date = datetime.now() - timedelta(minutes=50)
        db_helper.insert(
            cur,
            "databus_hdfs_import_tasks",
            result_table_id="101_xxx_test",
            data_dir="test_data_dir",
            hdfs_conf_dir="test_hdfs_conf_dir",
            kafka_bs="test_kafka_bs",
            finished=1,
            status="EMPTY_DATA_DIR_test",
            created_at=del_date,
            created_by="",
            updated_by="",
            description="",
            hdfs_custom_property="",
            id=1000,
        )

        db_helper.insert(
            cur,
            "databus_hdfs_import_tasks",
            result_table_id="101_xxx_test",
            data_dir="test_data_dir",
            hdfs_conf_dir="test_hdfs_conf_dir",
            kafka_bs="test_kafka_bs",
            finished=0,
            status="test_BAD_DATA_FILE",
            created_at=del_date,
            created_by="",
            updated_by="",
            description="",
            hdfs_custom_property="",
            id=1001,
        )

        db_helper.insert(
            cur,
            "databus_hdfs_import_tasks",
            result_table_id="101_xxx_test",
            data_dir="test_data_dir",
            hdfs_conf_dir="test_hdfs_conf_dir",
            kafka_bs="test_kafka_bs",
            finished=0,
            status="FILE_TOO_BIG_test",
            created_at=del_date,
            created_by="",
            updated_by="",
            description="",
            hdfs_custom_property="",
            id=1002,
        )

    try:
        yield 1000
    finally:
        _task_delete_all()


def _task_delete_all():
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            """
        DELETE FROM databus_hdfs_import_tasks
        """,
        )


def _task_delete(task_id):
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            """
        DELETE FROM databus_hdfs_import_tasks WHERE id = %(task_id)s
        """,
            task_id=task_id,
        )


@pytest.fixture
def task_add_finished():
    task_id = 2

    with db_helper.open_cursor("mapleleaf") as cur:

        db_helper.insert(
            cur,
            "databus_hdfs_import_tasks",
            result_table_id="101_xxx_test2",
            data_dir="test_data_dir2",
            hdfs_conf_dir="test_hdfs_conf_dir2",
            kafka_bs="test_kafka_bs2",
            finished=1,
            status="t",
            created_at="2018-10-29 16:42:52",
            created_by="",
            updated_by="",
            description="",
            hdfs_custom_property="",
            id=task_id,
        )

    try:
        yield task_id
    finally:
        _task_delete_finished(task_id)


def _task_delete_finished(task_id):
    with db_helper.open_cursor("mapleleaf") as cur:
        db_helper.execute(
            cur,
            """
        DELETE FROM databus_hdfs_import_tasks WHERE id = %(task_id)s
        """,
            task_id=task_id,
        )

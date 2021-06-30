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

from datetime import datetime

import pytest
from datalab.tests import db_helper
from datalab.tests.constants import BK_USERNAME


@pytest.fixture
def insert_notebook_task():
    with db_helper.open_cursor("default") as cur:
        db_helper.insert(
            cur,
            "datalab_notebook_task_info",
            notebook_id=1,
            notebook_name="test_notebook",
            project_id=1,
            project_type="personal",
            content_name="test_content",
            notebook_url="http://datalab.com/user/%s/notebooks/Untitled.ipynb" % BK_USERNAME,
            lock_user=BK_USERNAME,
            lock_time=datetime.now(),
            created_by=BK_USERNAME,
            description="test",
        )

    try:
        yield
    finally:
        delete_notebook_task()


def delete_notebook_task():
    with db_helper.open_cursor("default") as cur:
        db_helper.execute(cur, """ DELETE FROM datalab_notebook_task_info""")

# -*- coding:utf-8 -*-
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

import click
from dm_engine import dm_task

from lifecycle.create_lifecycle import (
    create_asset_value,
    create_cost,
    create_heat,
    create_heat_related_metric,
    create_importance,
    create_lifecycle,
    create_range,
)
from lifecycle.delete_lifecycle_entity import (
    delete_extra_heat_entity,
    delete_extra_lifecycle_entity,
    delete_extra_range_entity,
)

click.disable_unicode_literals_warning = True


@click.group()
def entry():
    pass


entry.add_command(
    click.command("delete-extra-heat-entity")(dm_task(delete_extra_heat_entity))
)
entry.add_command(
    click.command("delete-extra-range-entity")(dm_task(delete_extra_range_entity))
)
entry.add_command(
    click.command("delete-extra-lifecycle-entity")(
        dm_task(delete_extra_lifecycle_entity)
    )
)
entry.add_command(click.command("create-importance")(dm_task(create_importance)))
entry.add_command(click.command("create-asset-value")(dm_task(create_asset_value)))
entry.add_command(click.command("create-cost")(dm_task(create_cost)))
entry.add_command(click.command("create-lifecycle")(dm_task(create_lifecycle)))
entry.add_command(click.command("create-range")(dm_task(create_range)))
entry.add_command(click.command("create-heat")(dm_task(create_heat)))
entry.add_command(
    click.command("create-heat-related-metric")(dm_task(create_heat_related_metric))
)

if __name__ == "__main__":
    entry()

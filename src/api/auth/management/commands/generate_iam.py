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
from __future__ import absolute_import, unicode_literals

from django.core.management import BaseCommand
from mako.template import Template

from auth.models.base_models import ActionConfig

ACTIONS_TEMP = Template(
    """
{
    "system_id": "bk_bkdata",
    "operations": [
        % for action in actions:
        {
            "operation": "upsert_action",
            "data": {
                "id": "${action.action_id.replace('.', '-')}",
                "name": "${action.action_name}",
                "name_en": "${action.action_name_en}",
                "description": "${action.action_name}",
                "description_en": "${action.action_name_en}",
                "type": "view",
                "related_resource_types": [
                    {
                        "system_id": "bk_bkdata",
                        "id": "${action.object_class.object_class}",
                        "related_instance_selections": [],
                        "selection_mode": "all"
                    }
                ],
                "related_actions": []
            }
        },
        % endfor
    ]
}
"""
)


class Command(BaseCommand):
    help = "This command is used to generate the iam configuration"

    def add_arguments(self, parser):
        # Named (optional) arguments
        parser.add_argument("-t", "--target", help="生成 IAM 对象，可选有 resource_type|action")
        parser.add_argument("-i", "--typeid", help="资源类型ID")

    def handle(self, *args, **options):
        target = options["target"]
        resource_group_id = options["typeid"]

        if target == "resource_group":
            print("目前仅支持 target==action")
            return

        iam_content = "init...."
        if target == "action":
            actions = ActionConfig.objects.filter(object_class=resource_group_id)
            iam_content = ACTIONS_TEMP.render(actions=actions)

        print(iam_content)

    def get_version(self):
        return "1.0.0"

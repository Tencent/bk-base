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


from auth.bkiam.sync import RoleSync
from django.core.management import BaseCommand


class Command(BaseCommand):
    help = "This command used to re-sync user-role authorization to IAM"

    def __init__(self, stdout=None, stderr=None, no_color=False):
        super().__init__(stdout=None, stderr=None, no_color=False)

    def add_arguments(self, parser):
        # Named (optional) arguments
        parser.add_argument("-u", "--userid", help="Resync UserID")
        parser.add_argument("-r", "--roleid", help="Resync RoleID")
        parser.add_argument("-s", "--scopeid", help="Resync ScopeID having same ObjectClass with Role")
        parser.add_argument(
            "-o",
            "--operate",
            help="Grant or revoke, meaning add or remove member for RoleGroup",
            choices=["grant", "revoke"],
        )

    def handle(self, *args, **options):
        user_id = options["userid"]
        role_id = options["roleid"]
        scope_id = options["scopeid"]
        operate = options["operate"]

        sync_tool = RoleSync()
        sync_tool.sync(user_id, role_id, scope_id, operate)

    def get_version(self):
        return "1.0.0"

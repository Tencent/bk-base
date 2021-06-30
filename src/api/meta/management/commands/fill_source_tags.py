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


from codecs import open

from django.core.management.base import BaseCommand
from gevent.pool import Pool

from meta.management.modules.self import MetaApi


class Command(BaseCommand):
    help = "Fill source tags."

    def handle(self, *args, **options):
        pool = Pool(200)
        with open("tag_target_config.txt", encoding="utf8") as fr:
            for n, line in enumerate(fr):
                pool.spawn(self.fill, line)
                if not n % 1000:
                    pool.join()
                    self.stderr.write(self.style.SUCCESS("Filled {} source tags.".format(n)))

    def fill(self, line):
        info_lst = line.strip().split("\t")
        if len(info_lst) == 14:
            (
                id,
                target_id,
                target_type,
                tag_code,
                tag_type,
                checked,
                scope,
                active,
                probability,
                description,
                created_by,
                created_at,
                updated_by,
                updated_at,
            ) = info_lst
            data = {
                "bk_username": created_by,
                "tag_targets": [
                    {
                        "target_id": target_id,
                        "target_type": target_type,
                        "tags": [
                            {
                                "tag_code": tag_code,
                                "probability": probability,
                                "checked": checked,
                                "description": description,
                                "tag_type": tag_type,
                                "scope": scope,
                                "created_by": created_by,
                                "updated_by": updated_by,
                            }
                        ],
                    }
                ],
            }
            if not (scope and scope != "NULL"):
                data["scope"] = ""
            ret = MetaApi.create_tag_target(data)

            if not ret.is_success():
                self.stderr.write(self.style.ERROR("Fail to add source tag {}.".format(ret.response)))
        else:
            self.stderr.write(self.style.WARNING("Invalid info: {}".format(info_lst)))

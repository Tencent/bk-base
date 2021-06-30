# coding=utf-8
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

import json
import logging
import os

import click
import requests
from metadata_client import DEFAULT_SETTINGS, MetadataClient
from more_itertools import chunked

from conf.settings import metadata_settings

logger = logging.getLogger(__name__)


class BipGradeManager(object):

    CHUNK_SLICE_LIMIT = 50

    def __init__(self):
        self.settings = DEFAULT_SETTINGS.copy()
        self.settings.update(metadata_settings)
        self.metadata_client = MetadataClient(self.settings)
        self.bk_app_secret = self.settings.BK_APP_SECRET
        self.biz_id_list = []
        self.bip_grade_dict = {}
        self.target_path = (
            os.path.abspath(os.path.dirname(__file__)) + "/bip_grade.json"
        )

    def sync_bip_grade_info(self):
        statement = "{get_biz_list(func: has(BKBiz.id)){BKBiz.id}}"
        local_biz_lst = self.metadata_client.query(statement)
        biz_ret_list = local_biz_lst.get("data", {}).get("get_biz_list", [])
        for biz_item in biz_ret_list:
            self.biz_id_list.append(biz_item.get("BKBiz.id"))

        if self.biz_id_list:
            bip_grade_url = self.settings.CC_BIP_GRADE
            bip_grade_header = {"content-type": "application/json"}
            bip_grade_query_obj = {
                "bk_app_code": "data",
                "bk_app_secret": self.bk_app_secret,
                "bk_username": "data",
                "bk_supplier_account": "tencent",
                "bk_biz_ids": [],
                "fields": ["bk_biz_id", "bk_bip_grade_id", "bk_bip_grade_name"],
            }
            biz_id_chunked_list = chunked(self.biz_id_list, self.CHUNK_SLICE_LIMIT)
            for biz_id_chunk in biz_id_chunked_list:
                bip_grade_query_obj["bk_biz_ids"] = biz_id_chunk
                bip_grade_query_data = json.dumps(bip_grade_query_obj)
                bip_grade_ret = requests.post(
                    bip_grade_url, data=bip_grade_query_data, headers=bip_grade_header
                )
                bip_grade_ret_obj = json.loads(bip_grade_ret.text)
                # print('biz obj ret is {}'.format(bip_grade_ret_obj))
                bip_grade_ret_list = bip_grade_ret_obj.get("data", {}).get("info", [])
                for info_item in bip_grade_ret_list:
                    self.bip_grade_dict[info_item["bk_biz_id"]] = info_item

        if self.bip_grade_dict:
            json_file_path = self.target_path
            with open(json_file_path, "w") as wp:
                wp.write(json.dumps(self.bip_grade_dict))


@click.command()
def sync_bip_grade_info():
    BipGradeManager().sync_bip_grade_info()

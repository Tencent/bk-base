# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""

from django.utils.translation import ugettext as _

from codecheck.exceptions.codecheck_execptions import CodeCheckConfigException
from codecheck.pizza_settings import CODE_CHECK_SERVER_ADDR
from common.api.base import DataAPI
from common.exceptions import CommonCode


class _CodeCheckApi(object):
    def __init__(self):
        domain = CODE_CHECK_SERVER_ADDR
        if not domain:
            raise CodeCheckConfigException(message=_("没有CodeCheck Server Addr 配置"), code=CommonCode.HTTP_500)
        if not domain.startswith("http://"):
            self.codecheck_url = "http://" + domain
        else:
            self.codecheck_url = domain

    def codecheck(self, code_language, check_content, parser_group_name, blacklist_group_name, check_flag=False):
        args = {
            "code_language": code_language,
            "check_content": check_content,
            "parser_group_name": parser_group_name,
            "blacklist_group_name": blacklist_group_name,
            "check_flag": check_flag,
        }
        codecheck_verify_api = DataAPI(
            url=self.codecheck_url + "/codecheck", method="POST", module="codecheck",
            description="获取codecheck校验结果"
        )
        return codecheck_verify_api(args)

    def parser_group_add(self, parser_group_name, lib_dir=None, source_dir=None):
        args = {
            "parser_op": "add",
            "parser_group_name": parser_group_name,
            "lib_dir": lib_dir,
            "source_dir": source_dir,
        }
        codecheck_parser_api = DataAPI(
            url=self.codecheck_url + "/parser_groups", method="POST", module="codecheck",
            description="添加parser group信息"
        )
        return codecheck_parser_api(args)

    def parser_group_delete(self, parser_group_name):
        args = {"parser_op": "delete", "parser_group_name": parser_group_name}
        codecheck_parser_api = DataAPI(
            url=self.codecheck_url + "/parser_groups", method="POST", module="codecheck",
            description="删除parser group信息"
        )
        return codecheck_parser_api(args)

    def parser_group_list(self, parser_group_name):
        args = {"parser_op": "list", "parser_group_name": parser_group_name}
        codecheck_parser_api = DataAPI(
            url=self.codecheck_url + "/parser_groups", method="GET", module="codecheck",
            description="获取parser group信息"
        )
        return codecheck_parser_api(args)

    def blacklist_group_add(self, blacklist_group_name, blacklist):
        args = {"blacklist_op": "add", "blacklist_group_name": blacklist_group_name, "blacklist": blacklist}
        codecheck_blacklist_api = DataAPI(
            url=self.codecheck_url + "/blacklist_groups",
            method="POST",
            module="codecheck",
            description="添加blacklist group信息",
        )
        return codecheck_blacklist_api(args)

    def blacklist_group_delete(self, blacklist_group_name):
        args = {"blacklist_op": "delete", "blacklist_group_name": blacklist_group_name}
        codecheck_blacklist_api = DataAPI(
            url=self.codecheck_url + "/blacklist_groups",
            method="POST",
            module="codecheck",
            description="删除blacklist group信息",
        )
        return codecheck_blacklist_api(args)

    def blacklist_group_list(self, blacklist_group_name):
        args = {"blacklist_op": "list", "blacklist_group_name": blacklist_group_name}
        codecheck_blacklist_api = DataAPI(
            url=self.codecheck_url + "/blacklist_groups",
            method="GET",
            module="codecheck",
            description="获取blacklist group信息",
        )
        return codecheck_blacklist_api(args)


CodeCheckApi = _CodeCheckApi()

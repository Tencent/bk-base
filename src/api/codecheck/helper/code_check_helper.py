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
import json

from codecheck.api.code_check_api import CodeCheckApi
from codecheck.exceptions.codecheck_execptions import CodeCheckException


class CodeCheckHelper(object):
    @staticmethod
    def codecheck_verify(code_language, check_content, parser_group_name, blacklist_group_name, check_flag):
        code_check_res = CodeCheckApi.codecheck(
            code_language, check_content, parser_group_name, blacklist_group_name, check_flag
        )

        if not code_check_res or not code_check_res.is_success():
            raise CodeCheckException(message=code_check_res.message)
        return code_check_res.data

    @staticmethod
    def parser_group_add(parser_group_name, lib_dir, source_dir):
        code_check_res = CodeCheckApi.parser_group_add(parser_group_name, lib_dir, source_dir)
        if not code_check_res or not code_check_res.is_success():
            raise CodeCheckException(message=code_check_res.message)
        return code_check_res.data

    @staticmethod
    def parser_group_delete(parser_group_name):
        code_check_res = CodeCheckApi.parser_group_delete(parser_group_name)
        if not code_check_res or not code_check_res.is_success():
            raise CodeCheckException(message=code_check_res.message)
        return code_check_res.data

    @staticmethod
    def parser_group_list(parser_group_name):
        code_check_res = CodeCheckApi.parser_group_list(parser_group_name)
        if not code_check_res or not code_check_res.is_success():
            raise CodeCheckException(message=code_check_res.message)
        return code_check_res.data

    @staticmethod
    def blacklist_group_add(blacklist_group_name, blacklist):
        code_check_res = CodeCheckApi.blacklist_group_add(blacklist_group_name, blacklist)
        if not code_check_res or not code_check_res.is_success():
            raise CodeCheckException(message=code_check_res.message)
        return code_check_res.data

    @staticmethod
    def blacklist_group_delete(blacklist_group_name):
        code_check_res = CodeCheckApi.blacklist_group_delete(blacklist_group_name)
        if not code_check_res or not code_check_res.is_success():
            raise CodeCheckException(message=code_check_res.message)
        return code_check_res.data

    @staticmethod
    def blacklist_group_list(blacklist_group_name):
        code_check_res = CodeCheckApi.blacklist_group_list(blacklist_group_name)
        if not code_check_res or not code_check_res.is_success():
            raise CodeCheckException(message=code_check_res.message)
        return code_check_res.data

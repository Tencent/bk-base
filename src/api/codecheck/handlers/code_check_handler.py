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
import re

from codecheck.exceptions.codecheck_execptions import CodeCheckException
from codecheck.handlers.python_code_handler import PythonCodeHandler
from codecheck.helper.code_check_helper import CodeCheckHelper
from codecheck.shared.log import codecheck_logger


class _CodeCheckHandler(object):
    @staticmethod
    def verify(params):
        check_flag = params.get("check_flag", True)
        code_language = params.get("code_language")
        check_content = None
        code = params.get("check_content")
        parser_group_name = params.get("parser_group_name")
        blacklist_group_name = params.get("blacklist_group_name")

        python_imports_dict = None
        try:
            if code_language == "python":
                # python_imports = None
                # try:
                #     python_imports = PythonCodeHandler.get_python_imports(code)
                # except CodeCheckException as e:
                #     raise e
                #
                # python_imports_dict = PythonCodeHandler.get_python_imports_dict(python_imports)

                python_imports_dict = None
                try:
                    python_imports_dict = PythonCodeHandler.get_python_imports_and_funcs(code)
                except CodeCheckException as e:
                    raise e
                check_content = ";".join(list(python_imports_dict.keys()))
            elif code_language == "java":
                check_content = code

            code_check_res = CodeCheckHelper.codecheck_verify(
                code_language, check_content, parser_group_name, blacklist_group_name, check_flag
            )

            if code_language == "python":
                if "parse_result" in code_check_res:
                    code_check_res["parse_result"] = PythonCodeHandler.python_parse_result_extend(
                        code_check_res["parse_result"], python_imports_dict
                    )
        except Exception as e:
            codecheck_logger.warning("parse code ex(%s)" % (str(e)))
            # extract line no form ex.message when exception happens
            if len(e.args) >= 1:
                syntax_ex_pattern = re.compile(r"(?<=line\s)\d+")
                syntax_ex_line_no = syntax_ex_pattern.findall(e.args[0])
                syntax_ex_line_no_res = None
                if syntax_ex_line_no:
                    syntax_ex_line_no_res = int(syntax_ex_line_no[-1])
                return {
                    "parse_status": "fail",
                    "parse_message": e.args[0],
                    "parse_result": [{"exception_message": e.args[0], "line_no": syntax_ex_line_no_res}],
                }
            else:
                return {
                    "parse_status": "fail",
                    "parse_message": "unexpected exception",
                    "parse_result": [{"exception_message": str(e), "line_no": -1}],
                }
        return code_check_res

    def get_parser_group(self, parser_group_name="default"):
        return CodeCheckHelper.parser_group_list(parser_group_name=parser_group_name)

    def list_parser_group(self):
        return CodeCheckHelper.parser_group_list(parser_group_name=None)

    def add_parser_group(self, lib_dir, source_dir, parser_group_name="default"):
        CodeCheckHelper.parser_group_add(parser_group_name, lib_dir, source_dir)

    def delete_parser_group(self, parser_group_name="default"):
        CodeCheckHelper.parser_group_delete(parser_group_name)

    def get_blacklist_group(self, blacklist_group_name="default"):
        return CodeCheckHelper.blacklist_group_list(blacklist_group_name=blacklist_group_name)

    def list_blacklist_group(self):
        return CodeCheckHelper.blacklist_group_list(blacklist_group_name=None)

    def add_blacklist_group(self, blacklist, blacklist_group_name="default"):
        CodeCheckHelper.blacklist_group_add(blacklist_group_name, blacklist)

    def delete_blacklist_group(self, blacklist_group_name="default"):
        CodeCheckHelper.blacklist_group_delete(blacklist_group_name)


CodeCheckHandler = _CodeCheckHandler()

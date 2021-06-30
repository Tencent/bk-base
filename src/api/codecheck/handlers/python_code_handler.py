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
import ast
import sys
from collections import namedtuple

from codecheck.exceptions.codecheck_execptions import CodeCheckException
from codecheck.utils.java_base64_util import decode_java_urlsafe_base64
from codecheck.shared.log import codecheck_logger


class Analyzer(ast.NodeVisitor):
    def __init__(self):
        self.stats = {"import": [], "func": []}
        self.alias_dict = {}
        self.code_and_line = {}

    def process_import(self, node):
        if isinstance(node, ast.Import):
            module = []
        elif isinstance(node, ast.ImportFrom):
            module = node.module.split(".")

        for n in node.names:
            if n.asname:
                self.alias_dict[n.asname] = n.name
            full_name = ".".join(module + n.name.split("."))
            lineno = node.lineno
            self.generic_visit(node)
            if module:
                self.alias_dict[n.name] = full_name
            self.stats["import"].append(full_name)
            if full_name not in self.code_and_line:
                self.code_and_line[full_name] = []
            self.code_and_line[full_name].append(lineno)

    def visit_Import(self, node):
        self.process_import(node)

    def visit_ImportFrom(self, node):
        self.process_import(node)

    def visit_Call(self, node):
        full_name = None
        if hasattr(node.func, "attr") and hasattr(node.func, "value"):
            if hasattr(node.func.attr, "id") or hasattr(node.func.value, "id"):
                if node.func.value.id in self.alias_dict:
                    module_name = self.alias_dict[node.func.value.id]
                else:
                    module_name = node.func.value.id
            else:
                codecheck_logger.error("can not parse node (%s)" % (ast.dump(node)))
                self.generic_visit(node)
                return
            full_name = module_name + "." + node.func.attr
        elif hasattr(node.func, "id"):
            full_name = node.func.id
            if full_name in self.alias_dict:
                full_name = self.alias_dict[full_name]
        else:
            self.generic_visit(node)
            return
        lineno = node.lineno
        self.stats["func"].append(full_name)
        self.generic_visit(node)
        if full_name not in self.code_and_line:
            self.code_and_line[full_name] = []
        self.code_and_line[full_name].append(lineno)

    def report(self):
        # pprint(self.stats)
        # pprint(self.code_and_line)
        pass

    def get_code_and_line(self):
        return self.code_and_line


ImportTuple = namedtuple("ImportTuple", ["module", "name", "alias", "lineno"])


class PythonCodeHandler(object):
    @classmethod
    def get_python_imports(cls, code):
        try:
            code = decode_java_urlsafe_base64(code)
            root = ast.parse(code)

            for node in ast.iter_child_nodes(root):
                if isinstance(node, ast.Import):
                    module = []
                elif isinstance(node, ast.ImportFrom):
                    module = node.module.split(".")
                else:
                    continue

                for n in node.names:
                    yield ImportTuple(module, n.name.split("."), n.asname, node.lineno)

        except Exception as e:
            ex_type, ex_value, ex_traceback = sys.exc_info()
            error_msg = str(type(e).__name__) + ", " + str(ex_value)
            raise CodeCheckException(error_msg)

    @classmethod
    def get_python_imports_and_funcs(cls, code):
        try:
            code = decode_java_urlsafe_base64(code)
            tree = ast.parse(code)
            # print(astunparse.dump(tree))

            analyzer = Analyzer()
            analyzer.visit(tree)
            # analyzer.report()
            return analyzer.get_code_and_line()
        except Exception as e:
            ex_type, ex_value, ex_traceback = sys.exc_info()
            error_msg = str(type(e).__name__) + ", " + str(ex_value)
            raise Exception(error_msg)

    @classmethod
    def get_python_imports_dict(cls, python_import_list):
        python_imports = {}
        for one_import in python_import_list:
            one_check_content = ".".join(one_import.module + one_import.name)
            if one_check_content:
                if one_check_content not in python_imports:
                    python_imports[one_check_content] = []
                python_imports.get(one_check_content).append(one_import.lineno)
        return python_imports

    @staticmethod
    def python_parse_result_extend(parse_result, python_imports_dict):
        modified_parse_result = []
        for one_parse_result in parse_result:
            one_parse_lines = python_imports_dict[one_parse_result]
            for one_line in one_parse_lines:
                modified_parse_result.append({"import_name": one_parse_result, "line_no": int(one_line)})
        return modified_parse_result

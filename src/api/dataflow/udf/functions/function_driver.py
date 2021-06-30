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

import json
import os
import re
import subprocess
from datetime import datetime

from common.auth.objects import is_sys_scopes
from common.auth.perms import UserPerm
from common.local import get_request_username
from django.db import transaction
from django.utils.translation import ugettext as _

from dataflow.batch.config.hdfs_config import ConfigHDFS
from dataflow.shared.component.component_helper import ComponentHelper
from dataflow.shared.handlers import processing_udf_info, processing_udf_job
from dataflow.shared.language import translate_based_on_language_config as translate
from dataflow.shared.log import udf_logger as logger
from dataflow.stream.utils.FtpServer import FtpServer
from dataflow.udf.ast_check.policy import UdfPolicy
from dataflow.udf.exceptions.comp_exceptions import (
    DebugMavenRunError,
    ExampleSqlError,
    FtpServerInfoError,
    FuncLanguageNotSupportError,
    FunctionDeleteError,
    FunctionDevLockError,
    FunctionExistsError,
    FunctionIllegalDeleteError,
    FunctionNameRegexError,
    FunctionNotExistsError,
    ListFunctionsError,
    PackageError,
    SecurityCheckError,
)
from dataflow.udf.handlers import (
    bksql_function_config,
    bksql_function_dev_config,
    bksql_function_patameter_config,
    bksql_function_sandbox_config,
    bksql_function_update_log,
)
from dataflow.udf.helper.api_helper import BksqlHelper
from dataflow.udf.settings import (
    DEPLOY_POM,
    FUNC_DEV_VERSION,
    FUNC_JAVA_RELATIVE_PATH,
    FUNC_PYTHON_RELATIVE_PATH,
    UDF_CORE_RELATIVE_PATH,
    UDF_FUNC_PYTHON_RELATIVE_PATH,
    UDF_JEP_PATH,
    UDF_LD_PRELOAD,
    UDF_MAVEN_PATH,
    UDF_PYTHON_SCRIPT_PATH,
    UDF_WORKSPACE,
    USER_DEFINED_FUNCTION,
    USER_DEPENDENCIES_REPLACE_STRING,
    CalculationType,
    FuncLanguage,
)
from dataflow.udf.templates import get_java_udf_prefix, load_java_function_template, load_python_function_template


class FunctionHandler(object):
    def __init__(self, function_name):
        self.function_name = function_name
        self.clusters = []
        self.data_prefix = "/app/udf/%s" % self.function_name

    def list_clusters(self):
        if not self.clusters:
            cluster_groups = FtpServer.list_cluster_groups()
            if not cluster_groups:
                raise FtpServerInfoError(_("FTP服务器列表为空，请联系管理员处理."))
            self.clusters = cluster_groups
        return self.clusters

    def build_tmp_path(self):
        return "{}/dev/{}.jar".format(self.data_prefix, self.function_name)

    def upload_dev_package(self):
        udf_jar = os.path.abspath(
            os.path.join(
                UDF_WORKSPACE,
                self.function_name,
                "udf/udf-core/target",
                "%s.jar" % self.function_name,
            )
        )
        udf_jar_stream = open(udf_jar, "rb")
        udf_path = self.build_tmp_path()
        for cluster in self.list_clusters():
            logger.info("Start to upload udf %s to hdfs" % udf_path)
            res_data = ComponentHelper.hdfs_upload(cluster, udf_path, udf_jar_stream)
            logger.info("Upload udf {}.jar to hdfs, result is {}.".format(self.function_name, res_data))

    def upload_release_package(self, release_version):
        from_path = self.build_tmp_path()
        to_path = "/app/udf/{}/{}/{}.jar".format(
            self.function_name,
            release_version,
            self.function_name,
        )
        for cluster in self.list_clusters():
            res_data = ComponentHelper.hdfs_check_to_path_before_move(cluster, from_path, to_path)
            logger.info(
                "mv hdfs udf %s jar with version %s to release, result is %s."
                % (self.function_name, release_function, res_data)
            )

    @transaction.atomic()
    def delete_function(self):
        # 检测函数是否为用户自定义函数，如果不是不能删除
        if not self.function_name.startswith("udf_"):
            raise FunctionIllegalDeleteError()
        # 检测是否可以删除，当processing 或者job有使用此function时，禁止删除
        if processing_udf_job.exists(udf_name=self.function_name) or processing_udf_info.exists(
            udf_name=self.function_name
        ):
            raise FunctionDeleteError()
        # 删除hdfs上jar包
        paths = [self.data_prefix]
        for cluster in self.list_clusters():
            ComponentHelper.hdfs_clean(cluster, paths, True, user_name=get_request_username())
        # 删除udf元数据信息
        # todo 删除函数时，需要删除对应的权限数据
        bksql_function_dev_config.delete(func_name=self.function_name)
        bksql_function_config.delete(self.function_name)
        bksql_function_patameter_config.delete(self.function_name)
        bksql_function_update_log.delete(func_name=self.function_name)


def _exist_release_version(func_name):
    return bksql_function_dev_config.exists_no_dev_version(FUNC_DEV_VERSION, func_name)


def is_modify_with_udf_released(processing_id):
    """
    判断 processing 是否需要自动更新
    processing 包含 udf，并且 processing 中使用的udf版本和udf最新版本不一致，则需要自动更新，否则不需要更新

    :param processing_id:
    :return:
    """
    if not processing_udf_info.exists(processing_id=processing_id):
        return False
    processing_infos = processing_udf_info.where(processing_id=processing_id)
    for processing_info in processing_infos:
        current_udf_version = json.loads(processing_info.udf_info)["version"]
        latest_udf_version = bksql_function_config.get(
            func_name=processing_info.udf_name, func_type=USER_DEFINED_FUNCTION
        ).version
        if current_udf_version != latest_udf_version:
            return True
    return False


@transaction.atomic()
def create_function(args):
    func_name_regex = r"^[a-z][a-z0-9_]*$"
    if not re.match(func_name_regex, args["func_name"]):
        raise FunctionNameRegexError()
    args["version"] = "dev"
    if bksql_function_dev_config.exists_dev_version(args["version"], args["func_name"]):
        raise FunctionExistsError()
    args["created_by"] = get_request_username()
    args["input_type"] = json.dumps(args["input_type"])
    args["return_type"] = json.dumps(args["return_type"])
    args["code_config"] = json.dumps(args["code_config"])
    bksql_function_dev_config.save(**args)

    is_release = True if _exist_release_version(args.get("func_name")) else False
    return {"func_name": args.get("func_name"), "is_release": is_release}


def update_function(function_name, args):
    # todo check java class name
    args["updated_by"] = get_request_username()
    args["updated_at"] = datetime.now()
    args["input_type"] = json.dumps(args["input_type"])
    args["return_type"] = json.dumps(args["return_type"])
    args["code_config"] = json.dumps(args["code_config"])
    bksql_function_dev_config.update_version_config(function_name, FUNC_DEV_VERSION, **args)
    is_release = True if _exist_release_version(function_name) else False
    data = {"func_name": function_name, "is_release": is_release}
    return data


def get_dev_function_info(function_name):
    condition = {"func_name": function_name, "version": FUNC_DEV_VERSION}
    func_dev_config = bksql_function_dev_config.get(**condition)
    return {
        "func_name": func_dev_config.__name__,
        "func_alias": func_dev_config.func_alias,
        "func_language": func_dev_config.func_language,
        "func_udf_type": func_dev_config.func_udf_type,
        "input_type": json.loads(func_dev_config.input_type),
        "return_type": json.loads(func_dev_config.return_type),
        "explain": func_dev_config.explain,
        "example": func_dev_config.example,
        "example_return_value": func_dev_config.example_return_value,
        "code_config": json.loads(func_dev_config.code_config),
        "locked": func_dev_config.locked,
        "locked_at": func_dev_config.locked_at,
        "locked_by": func_dev_config.locked_by,
    }


def lock_function(function_name):
    filter_condition = {"func_name": function_name, "version": FUNC_DEV_VERSION}
    if bksql_function_dev_config.get(**filter_condition).locked == 1:
        function_dev_log = bksql_function_dev_config.get(**filter_condition)
        raise FunctionDevLockError(function_dev_log.locked_by, function_dev_log.locked_at)
    args = {
        "locked": 1,
        "locked_by": get_request_username(),
        "locked_at": datetime.now(),
    }
    bksql_function_dev_config.update_version_config(function_name, FUNC_DEV_VERSION, **args)


def unlock_function(function_name):
    bksql_function_dev_config.update_version_config(function_name, FUNC_DEV_VERSION, locked=0)


def parse_sql(sql, geog_area_code, check_params=False):
    """

    :param sql: "select a(x) as a, b(x) as b from ...",
    :param geog_area_code: inland
    :param check_params:[optional] True
    :return:
        [{
            "name": "a",
            "version": "v1",
            "language": "java",
            "type": "udf",
            "hdfs_path": "/app/udf/a/v1/a.jar",
            "func_params": [{
                    "type": "column",
                    "value": "x"
                        }
                    ]
                }
            ]
    """
    functions = parse_sql_function(sql, check_params)
    data = []
    config_hdfs = ConfigHDFS(FtpServer.get_ftp_server_cluster_group(geog_area_code))
    for func_name in functions:
        if not bksql_function_config.exists(func_name=func_name, func_type=USER_DEFINED_FUNCTION):
            continue
        func_config = bksql_function_config.get(func_name=func_name, func_type=USER_DEFINED_FUNCTION)
        data.append(
            {
                "name": func_config.__name__,
                "version": func_config.version,
                "language": func_config.func_language,
                "type": func_config.func_udf_type,
                "hdfs_path": "%s/app/udf/%s/%s/%s.jar"
                % (
                    config_hdfs.get_url(),
                    func_config.__name__,
                    func_config.version,
                    func_config.__name__,
                ),
                "local_path": os.path.join(UDF_PYTHON_SCRIPT_PATH, "udf_jar"),
            }
        )
    return data


def parse_sql_function(sql, check_params=False):
    """

    :param sql:
    :param check_params:
    :return:
    """
    params = {"sql": sql, "properties": {"check_params": check_params}}
    return BksqlHelper.parse_sql_function(params)


def _get_release_version(function_name):
    version_num = bksql_function_dev_config.get_max_version(function_name)
    if version_num and version_num["max_version"]:
        return "v%s" % str(version_num["max_version"] + 1)
    else:
        return "v1"


def _save_function_config(function_name, release_version):
    func_dev_config = bksql_function_dev_config.get(func_name=function_name, version=release_version)
    bksql_function_config.delete(function_name)
    func_config = {
        "func_name": func_dev_config.__name__,
        "func_alias": func_dev_config.func_alias,
        "func_group": "Other Function",
        "explain": func_dev_config.explain,
        "func_type": "User-defined Function",
        "func_udf_type": func_dev_config.func_udf_type,
        "func_language": func_dev_config.func_language,
        "example": func_dev_config.example,
        "example_return_value": func_dev_config.example_return_value,
        "version": func_dev_config.version,
        "support_framework": func_dev_config.support_framework,
        "created_by": func_dev_config.created_by,
    }
    bksql_function_config.save(**func_config)
    bksql_function_patameter_config.delete(function_name)
    bksql_function_patameter_config.save(
        func_name=func_dev_config.__name__,
        input_type=func_dev_config.input_type,
        return_type=func_dev_config.return_type,
    )


def _save_function_release_log(func_name, version, release_log):
    bksql_function_update_log.save(
        func_name=func_name,
        version=version,
        release_log=release_log,
        updated_by=get_request_username(),
    )


@transaction.atomic()
def release_function(function_name, release_log):
    release_version = _get_release_version(function_name)
    release_func = bksql_function_dev_config.get(func_name=function_name, version="dev")
    release_func.released = 1
    release_func.save()

    release_func.version = release_version
    release_func.id = None

    # save release version function
    logger.info("To save function {} version {}".format(function_name, release_version))
    release_func.save()

    # save function config
    logger.info("To save function %s to bksql_function_config_v3" % function_name)
    _save_function_config(function_name, release_version)

    # save function release log
    logger.info("To save function %s release log." % function_name)
    _save_function_release_log(function_name, release_version, release_log)

    # release udf jar
    logger.info("release udf %s's jar" % function_name)
    FunctionHandler(function_name).upload_release_package(release_version)


def generate_code_frame(function_name, args):
    """
    产生函数模板
    @param function_name:
    @param args:
            {
                "func_language": "java",
                "func_udf_type": "udf",
                "input_type": ["string", "string"],
                "return_type": ["string"]
            }
    @return:
    """
    if args["func_language"] == FuncLanguage.java.name:
        return load_java_function_template(
            _underline_to_camel(function_name),
            args["func_udf_type"],
            args["input_type"],
            args["return_type"],
        )
    elif args["func_language"] == FuncLanguage.python.name:
        return load_python_function_template(
            function_name,
            args["func_udf_type"],
            args["input_type"],
            args["return_type"],
        )
    else:
        raise FuncLanguageNotSupportError()


def _underline_to_camel(underline_format):
    """
    Underlined naming format hump naming format

    :param underline_format: abc_bcd
    :return: AbcBcd
    """
    camel_format = ""
    if isinstance(underline_format, str):
        for _s_ in underline_format.split("_"):
            camel_format += _s_.capitalize()
    return camel_format


def _replace_pom(workspace, dependencies):
    user_dependencies = ""
    for dependency in dependencies:
        line = (
            "<dependency>\n\t\t\t<groupId>%s</groupId>\n\t\t\t<artifactId>%s</artifactId>"
            "\n\t\t\t<version>%s</version>\n\t\t</dependency>\n"
            % (dependency["group_id"], dependency["artifact_id"], dependency["version"])
        )
        user_dependencies += line
    udf_pom = os.path.abspath(os.path.join(workspace, DEPLOY_POM))
    pom_str = ""
    r_open = open(udf_pom, "r")
    for pom_line in r_open:
        if re.search(USER_DEPENDENCIES_REPLACE_STRING, pom_line):
            pom_line = re.sub(USER_DEPENDENCIES_REPLACE_STRING, user_dependencies, pom_line)
            pom_str += pom_line
        else:
            pom_str += pom_line
    w_open = open(udf_pom, "w")
    w_open.write(pom_str)
    r_open.close()
    w_open.close()


def package(func_name, calculation_type):
    """
    package udf

    :param func_name: func name
    :param calculation_type: stream,batch
    :return:
    """
    # func workspace
    workspace = os.path.abspath(os.path.join(UDF_WORKSPACE, func_name, UDF_CORE_RELATIVE_PATH))
    func_config = bksql_function_dev_config.get(func_name=func_name, version=FUNC_DEV_VERSION)
    code_config = json.loads(func_config.code_config)
    # get udf filename and path
    if func_config.func_language == FuncLanguage.java.name:
        func_file_name = _underline_to_camel(func_name)
        func_path = os.path.abspath(os.path.join(workspace, FUNC_JAVA_RELATIVE_PATH, "%s.java" % func_file_name))
        if "dependencies" in code_config and len(code_config["dependencies"]) > 0:
            _replace_pom(workspace, code_config["dependencies"])
        code_config["code"] = get_java_udf_prefix(func_config.func_udf_type.lower()) + code_config["code"]
    elif func_config.func_language == FuncLanguage.python.name:
        func_file_name = func_name
        func_path = os.path.abspath(os.path.join(workspace, FUNC_PYTHON_RELATIVE_PATH, "%s.py" % func_file_name))
        # 将python目录下common名字改为udf名字, 方便后台使用多个函数时区分开
        os.rename(
            os.path.join(workspace, FUNC_PYTHON_RELATIVE_PATH, "common"),
            os.path.join(workspace, FUNC_PYTHON_RELATIVE_PATH, "%s_util" % func_file_name),
        )
    else:
        raise PackageError("Not support the language %s" % func_config.func_language)
    # generate function file
    with open(func_path, "w") as f:
        f.write(code_config["code"])
        f.flush()
        f.close()

    if func_config.func_language == FuncLanguage.python.name:
        subprocess.run(
            ["python", "-m", "py_compile", "%s.py" % func_file_name],
            cwd=os.path.abspath(os.path.join(workspace, FUNC_PYTHON_RELATIVE_PATH)),
        )

    try:
        # generate code
        input_type = ",".join(json.loads(func_config.input_type))
        output_type = ",".join(json.loads(func_config.return_type))

        # 获取 cpython 包, 异常时获取 cpython 包列表为空
        cpython_packages = []
        try:
            python_install_packages = json.loads(
                bksql_function_sandbox_config.get(sandbox_name=func_config.sandbox_name).python_install_packages
            )
            for key, value in list(python_install_packages.items()):
                if value["is_cpython"]:
                    cpython_packages.append(key)
        except Exception as e:
            logger.warning("Failed to get python install package, and exception is {}".format(e))

        tmp_command = (
            "export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:"
            + "/sbin:/bin:/root/bin:/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin && "
            "export LD_PRELOAD=%s && "
            "cd %s && %s clean scala:compile install -f %s -Dmaven.text.skip=true "
            "-Dudf.codeLanguage=%s -Dudf.type=%s -Dudf.name=%s -Dudf.inputTypes=%s -Dudf.outputTypes=%s "
            "-Dudf.roleType=%s -Dudf.userFuncFileName=%s -Dudf.jepPath=%s "
            "-Dudf.pythonScriptPath=%s -Dudf.cpythonPackages=%s"
            % (
                UDF_LD_PRELOAD,
                workspace,
                UDF_MAVEN_PATH,
                DEPLOY_POM,
                func_config.func_language,
                func_config.func_udf_type,
                func_config.__name__,
                input_type,
                output_type,
                calculation_type,
                func_file_name,
                UDF_JEP_PATH,
                UDF_PYTHON_SCRIPT_PATH,
                ",".join(cpython_packages),
            )
        )

        logger.info(tmp_command)

        code, output = subprocess.getstatusoutput(tmp_command)

        if code != 0:
            raise DebugMavenRunError(output)

        logger.info("udf-debug-package: generate udf {}, result is {}".format(func_name, output))
        # package
        package_command = [
            "%s" % UDF_MAVEN_PATH,
            "clean",
            "scala:compile",
            "package",
            "-f",
            DEPLOY_POM,
            "-Dmaven.text.skip=true",
            "-Dudf.name=%s" % func_config.__name__,
        ]
        logger.info("udf-package: workspace {}, and command {}".format(workspace, package_command))
        package_re = subprocess.check_output(package_command, cwd=workspace, stderr=subprocess.STDOUT)
        logger.info("udf-debug-package: package udf {}, result is {}".format(func_name, package_re))
    except subprocess.CalledProcessError as e:
        logger.error("udf-debug-package: generate or package udf {} error, output is {}".format(func_name, e.output))
        raise DebugMavenRunError("udf-debug: mvn err, output is {}, exception is {}".format(e.output, e))
    except Exception as e1:
        logger.exception("udf-debug-package: error is %s" % e1)
        raise PackageError("udf {} package error {}".format(func_name, e1))
    # upload udf jar to hdfs
    logger.info("begin to upload %s.jar to hdfs" % func_name)
    FunctionHandler(func_name).upload_dev_package()


def security_check(func_name, sql, debug_data):
    """
    security check

    :param func_name: udf
    :param sql:  select udf(a,b) as cc from table
    :param debug_data:
        {
            'schema': [{
                    field_name: a,
                    field_type: string
                }, {
                    field_name: b,
                    field_type: string
                }
            ],
            'value': [
                [xx, 2]
            ]
        }
    :return:
    """
    parse_re = parse_sql_function(sql, True)
    # one input data
    field_names = []
    for one_schema in debug_data["schema"]:
        field_names.append(one_schema["field_name"])
    field_values = []
    for one_value in debug_data["value"][0]:
        field_values.append(one_value)

    input_data = dict(list(zip(field_names, field_values)))

    input_args = []
    for field in parse_re[func_name]:
        if field["type"] == "constant":
            input_args.append(str(field["value"]))
        elif field["type"] == "column":
            input_args.append(str(input_data[field["value"]]))

    func_config = bksql_function_dev_config.get(func_name=func_name, version=FUNC_DEV_VERSION)
    if func_config.func_language == FuncLanguage.java.name:
        _java_code_security_check(func_name, input_args, func_config)
    elif func_config.func_language == FuncLanguage.python.name:
        _python_code_security_check(func_name, func_config)
    else:
        raise SecurityCheckError("The udf {} not support language {}".format(func_name, func_config.func_language))


def _python_code_security_check(func_name, func_config):
    workspace = os.path.abspath(
        os.path.join(
            UDF_WORKSPACE,
            func_name,
            UDF_FUNC_PYTHON_RELATIVE_PATH,
            "%s.py" % func_name,
        )
    )
    sandbox_config = bksql_function_sandbox_config.get(sandbox_name=func_config.sandbox_name)
    disable_imports = list(
        map(
            lambda _str: _str.encode("utf-8").strip(),
            sandbox_config.python_disable_imports.split(","),
        )
    )

    for imp in UdfPolicy().get_imports(workspace):
        if imp.module:
            check_import = imp.module[0]
        else:
            check_import = imp.name[0]
        if check_import in disable_imports:
            raise SecurityCheckError("Don't support %s" % check_import)


def _java_code_security_check(func_name, input_args, func_config):
    workspace = os.path.abspath(os.path.join(UDF_WORKSPACE, func_name, "udf/udf-core/target"))
    model_jar = os.path.abspath(os.path.join(UDF_WORKSPACE, func_name, "udf/udf-core/src/main/resources/lib/*"))
    try:
        input_type = ",".join(json.loads(func_config.input_type))
        command = [
            "java",
            "-cp",
            "{}.jar:{}".format(func_name, model_jar),
            "com.tencent.bk.base.dataflow.udf.security.JavaSandBoxCheck",
            "%s" % _underline_to_camel(func_name),
            "%s" % input_type,
        ]
        command.extend(input_args)
        logger.info("udf-security-check-command: %s" % command)
        check_re = subprocess.check_output(command, cwd=workspace, stderr=subprocess.STDOUT)
        logger.info("udf-security-check: function_name {}, result is {}".format(func_name, check_re))
    except subprocess.CalledProcessError as e:
        logger.error("udf-security-check: function {}, output is {}".format(func_name, e.output))
        raise SecurityCheckError(
            "udf-security-check: function {} security check error, exception is {}".format(func_name, e)
        )
    except Exception as e1:
        logger.exception("udf-security-check: error is %s" % e1)
        raise SecurityCheckError("udf %s security check error" % func_name)


def list_functions(args):
    env = args.get("env", "product")
    function_name_str = args.get("function_name", None)
    function_name = function_name_str.split(",") if function_name_str else []
    if env == "dev":
        _check_function_name(function_name)
        return _list_dev_functions(function_name)
    elif env == "product":
        return _list_product_functions(function_name)
    else:
        raise ListFunctionsError("Not support the %s env." % env)


def _list_dev_functions(function_name):
    functions = []
    function_config = bksql_function_dev_config.get(func_name=function_name[0], version="dev")

    parameter_types = [
        {
            "input_types": json.loads(function_config.input_type),
            "output_types": json.loads(function_config.return_type),
        }
    ]

    one_function = {
        "name": function_config.__name__,
        "udf_type": function_config.func_udf_type,
        "parameter_types": parameter_types,
    }
    functions.append(one_function)
    return functions


def _list_product_functions(function_name):
    function_configs = bksql_function_config.filter(func_name__in=function_name)
    functions = []
    for function_config in function_configs:
        parameter_types = _get_parameter_types(function_config.__name__)
        one_function = {
            "name": function_config.__name__,
            "udf_type": function_config.func_udf_type,
            "parameter_types": parameter_types,
        }
        functions.append(one_function)
    return functions


def _get_parameter_types(func_name):
    function_parameters = bksql_function_patameter_config.filter(func_name)
    parameter_types = []

    for parameter in function_parameters:
        one_parameter_type = {
            "input_types": json.loads(parameter.input_type),
            "output_types": json.loads(parameter.return_type),
        }
        parameter_types.append(one_parameter_type)
    return parameter_types


def _check_function_name(function_name):
    if len(function_name) != 1:
        raise ListFunctionsError("If you chose dev, must have the function_name param, and only one.")


# bksql_function_patameter_config 获取函数的用法信息
def get_all_function_usages():
    all_function_patameter = bksql_function_patameter_config.list_all()
    all_function_patameter_map = {}
    for patameter in all_function_patameter:
        func_name = patameter.__name__
        # 不存在则构造一个list
        if func_name not in all_function_patameter_map:
            all_function_patameter_map[func_name] = []

        input_type = json.loads(patameter.input_type)
        return_type = json.loads(patameter.return_type)
        # case when 无参数和返回值
        if func_name == "case when":
            new_str = "CASE WHEN a THEN b [WHEN c THEN d]*[ELSE e] END"
        else:
            param = ",".join(i for i in input_type).upper()
            return_param = ",".join(i for i in return_type).upper()
            new_str = return_param + " " + func_name + "(" + param + ")"

        all_function_patameter_map[func_name].append(new_str)

    return all_function_patameter_map


# 获取所有函数的信息(内置函数 + udf
def get_all_function_data():
    # 数学,字符串,条件,聚合
    all_function_data_map = {"Internal Function": {}, "User-defined Function": {}}
    all_bksql_function_config = bksql_function_config.list_all()
    for config in all_bksql_function_config:
        # first key 'Internal Function'
        func_type = config.func_type
        # second key 'Aggregate Function'
        func_group = config.func_group
        node_value = {
            "func_name": config.__name__,
            "example": config.example,
            "explain": translate(config.explain),
            "example_return_value": config.example_return_value,
            "support_framework": config.support_framework,
        }
        func_group_info = all_function_data_map[func_type].setdefault(func_group, [])
        func_group_info.append(node_value)
    return all_function_data_map


# 构造内置函数组
def build_internal_group(all_function_data_map, all_function_usage_map):
    internal_func_group_list = []
    for func_group_key in all_function_data_map["Internal Function"]:
        # func_group_key = Mathematical Function,...
        func_group_data_list = all_function_data_map["Internal Function"][func_group_key]
        func_list = []
        for func_group_data in func_group_data_list:
            name = func_group_data["func_name"]
            func = {
                "usages": all_function_usage_map[name],
                "name": name,
                "usage_case": func_group_data["example"],
                "explain": func_group_data["explain"],
                "example_return_value": func_group_data["example_return_value"],
                "support_framework": translate(func_group_data["support_framework"]),
                "released": 1,
                "editable": False,
            }
            func_list.append(func)

        internal_func_group = {
            "group_name": translate(func_group_key),
            "display": translate(bksql_function_config.get_group_display(func_group_key)),
            "func": func_list,
        }
        internal_func_group_list.append(internal_func_group)

    return {
        "id": 0,
        "type_name": translate("Internal Function"),
        "display": translate("Internal Function"),
        "func_groups": internal_func_group_list,
    }


# 构造udf函数组 分为管理员和普通用户
def build_udf_group(all_function_data_map, all_function_usage_map):
    # 顺序1:用户正在开发的dev函数 2:用户自己已发布的函数 3：其它人已发布的函数(可共享使用
    # 返回当前开发者未发布的函数 只展示函数名和编辑状态
    operator = get_request_username()
    scopes = UserPerm(operator).list_scopes("function.develop")
    # 可操作函数列表
    operate_function = []
    # 是否超级管理员
    is_super_manager = False
    for scope in scopes:
        if "function_id" in scope:
            operate_function.append(scope["function_id"])
        elif "*" in scope:
            is_super_manager = True

    func_group_data_list = (
        all_function_data_map["User-defined Function"]["Other Function"]
        if "Other Function" in all_function_data_map["User-defined Function"]
        else []
    )
    # 返回函数列表
    func_list = []
    if is_super_manager:
        # 超级管理员查询所有正在开发的和已发布的函数
        all_function_dev_config = bksql_function_dev_config.filter(version="dev", released=0)
        for dev_node in all_function_dev_config:
            func = {
                "name": dev_node.__name__,
                "released": 0,
                "editable": True,
                "type_name": "udf",
            }
            func_list.append(func)

        for func_group_data in func_group_data_list:
            name = func_group_data["func_name"]
            func = {
                "usages": all_function_usage_map[name],
                "name": name,
                "usage_case": func_group_data["example"],
                "explain": translate(func_group_data["explain"]),
                "example_return_value": func_group_data["example_return_value"],
                "support_framework": translate(func_group_data["support_framework"]),
                "released": 1,
                "editable": True,
                "type_name": "udf",
            }
            func_list.append(func)
    else:
        # 不是超级管理员 可以看到有权限未发布dev的函数
        user_function_dev_config = bksql_function_dev_config.filter(
            func_name__in=operate_function, version="dev", released=0
        )
        for dev_node in user_function_dev_config:
            func = {
                "name": dev_node.__name__,
                "released": 0,
                "editable": True,
                "type_name": "udf",
            }
            func_list.append(func)
            # 可操作函数 = dev中的函数 + 已发布的函数
            # 剩下的是用户已发布的函数
            if dev_node.__name__ in operate_function:
                operate_function.remove(dev_node.__name__)

        # 已发布的有权限到函数可以查看，也可以编辑
        user_own_function_list = []
        for func_group_data in func_group_data_list:
            name = func_group_data["func_name"]
            # 用户已发布的函数 可编辑。其他人发布的函数不可编辑，不可查看
            if name in operate_function:
                editable = True
            else:
                editable = False
            func = {
                "usages": all_function_usage_map[name],
                "name": name,
                "usage_case": func_group_data["example"],
                "explain": translate(func_group_data["explain"]),
                "example_return_value": func_group_data["example_return_value"],
                "support_framework": translate(func_group_data["support_framework"]),
                "released": 1,
                "editable": editable,
                "type_name": "udf",
            }
            if name in operate_function:
                user_own_function_list.append(func)

        func_list.extend(user_own_function_list)

    return {
        "id": 1,
        "type_name": translate("User-defined Function"),
        "display": translate("User-defined Function"),
        "func": func_list,
    }


def list_function_doc():
    # 返回函数列表信息
    function_doc = []
    # 1.将函数用法usages准备好 func_name:["DOUBLE floor(INT)",...]
    all_function_usage_map = get_all_function_usages()
    # 2.所有的函数信息
    all_function_data_map = get_all_function_data()
    # 3.返回内置函数(因为内置函数和自定义函数逻辑差别较大,不好全部一起返回
    internal_function_group = build_internal_group(all_function_data_map, all_function_usage_map)
    function_doc.append(internal_function_group)
    # 4.返回udf函数
    udf_function_group = build_udf_group(all_function_data_map, all_function_usage_map)
    function_doc.append(udf_function_group)
    return function_doc


def list_function_info():
    """
    获取发布函数列表，先通过权限接口获取有权限的列表，再通过求交集的方式或有权限的函数列表
    1. 未发布函数的版本展示为 '-'
    2. 发布函数的状态为 released；未发布函数为开发中的函数，状态为 developing

    @return:
    """
    scopes = UserPerm(get_request_username()).list_scopes("function.develop")
    function_ids = []
    if is_sys_scopes(scopes):
        # 已发布函数
        released_function = bksql_function_config.filter(active=1, func_type="User-defined Function").order_by(
            "-updated_at"
        )
        # 未发布函数
        unreleased_function = bksql_function_dev_config.filter(version="dev", released=0).order_by("-updated_at")
    else:
        for scope in scopes:
            if "function_id" in scope:
                function_ids.append(scope["function_id"])
        # 已发布函数
        released_function = bksql_function_config.filter(
            func_name__in=function_ids, active=1, func_type="User-defined Function"
        ).order_by("-updated_at")
        # 未发布函数
        unreleased_function = bksql_function_dev_config.filter(
            func_name__in=function_ids, version="dev", released=0
        ).order_by("-updated_at")
    udf_function = []

    # 首先返回发布函数
    for function in released_function:
        one_function = {
            "func_name": function.__name__,
            "func_alias": function.func_alias,
            "version": function.version,
            "updated_at": function.updated_at,
            "created_by": function.created_by,
            "status": "released",
        }
        udf_function.append(one_function)
    # 再次返回未发布函数
    for function in unreleased_function:
        one_function = {
            "func_name": function.__name__,
            "func_alias": function.func_alias,
            "version": "-",
            "updated_at": function.updated_at,
            "created_by": function.created_by,
            "status": "developing",
        }
        udf_function.append(one_function)
    return udf_function


def get_function_info(func_name, **condition):
    """
    @param func_name:
    @param condition
    @return:
    """
    condition.update({"func_name": func_name})
    func_config = bksql_function_config.get(**condition)
    return {
        "func_name": func_config.__name__,
        "func_alias": func_config.func_alias,
        "func_group": func_config.func_group,
        "func_language": func_config.func_language,
        "func_udf_type": func_config.func_udf_type,
        "func_type": func_config.func_type,
        "version": func_config.version,
        "support_framework": func_config.support_framework,
        "explain": func_config.explain,
        "example": func_config.example,
        "example_return_value": func_config.example_return_value,
    }


def get_function_related_info(func_name):
    """
    @param func_name:
    @return:
        {
            'processing_id': {
                'status': 'normal',
                'version': 'v3'
            }
        }
    """
    udf_processings = {}
    udf_processing_objects = processing_udf_info.where(udf_name=func_name).only("processing_id", "udf_info")
    for udf_processing_object in udf_processing_objects:
        udf_processings[udf_processing_object.processing_id] = {
            "version": json.loads(udf_processing_object.udf_info).get("version", None)
        }

    udf_jobs = {}
    udf_job_objects = processing_udf_job.where(udf_name=func_name).only("processing_id", "udf_info")
    for udf_job_object in udf_job_objects:
        udf_jobs[udf_job_object.processing_id] = {"version": json.loads(udf_job_object.udf_info).get("version", None)}

    function_related_info = {}
    for processing in set(list(udf_processings.keys()) + list(udf_jobs.keys())):
        status = "normal"
        # job 信息有，processing 信息没有
        if processing not in udf_processings:
            status = "warning"
        processing_info = udf_jobs.get(processing, {}) or udf_processings.get(processing, {})
        version = processing_info.get("version", None)
        function_related_info[processing] = {"status": status, "version": version}
    return function_related_info


def list_function_log(func_name, **condition):
    condition.update({"func_name": func_name})
    func_log_info = bksql_function_update_log.filter(**condition).order_by("-updated_at")
    _function_log = []
    for _log_obj in func_log_info:
        _function_log.append(
            {
                "func_name": _log_obj.__name__,
                "version": _log_obj.version,
                "release_log": _log_obj.release_log,
                "updated_by": _log_obj.updated_by,
                "updated_at": _log_obj.updated_at,
            }
        )
    return _function_log


def _get_func_usage(func_name):
    function_parameters = bksql_function_patameter_config.filter(func_name)
    usage = []
    for parameter in function_parameters:
        usage.append(
            func_name + "(" + ", ".join(_str.upper().strip() for _str in parameter.input_type.split(",")) + ")"
        )
    return usage


def get_support_python_packages():
    config = bksql_function_sandbox_config.get(sandbox_name="default")
    try:
        return list(json.loads(config.python_install_packages).values())
    except Exception as e:
        logger.warning("Failed to load python install packages, and exception {}".format(e))
        return []


def set_support_python_packages(name, version, description, is_cpython):
    config = bksql_function_sandbox_config.get(sandbox_name="default")
    support_packages = config.python_install_packages
    if support_packages:
        support_packages = json.loads(support_packages)
    else:
        support_packages = {}

    # 支持 python package 信息更新
    support_packages[name] = {
        "name": name,
        "version": version,
        "is_cpython": is_cpython,
        "description": description,
    }
    bksql_function_sandbox_config.update("default", python_install_packages=json.dumps(support_packages))


def check_example_sql(function_name, sql):
    try:
        result = parse_sql_function(sql)
    except Exception:
        raise ExampleSqlError()
    if function_name not in result:
        raise ExampleSqlError(function_name)


def get_function_config(function_name, geog_area_code):
    config_hdfs = ConfigHDFS(FtpServer.get_ftp_server_cluster_group(geog_area_code))
    if not bksql_function_config.exists(func_name=function_name, func_type=USER_DEFINED_FUNCTION):
        raise FunctionNotExistsError()
    func_config = bksql_function_config.get(func_name=function_name, func_type=USER_DEFINED_FUNCTION)
    support_framework = [x.strip() for x in func_config.support_framework.split(",")]
    # get class name
    class_name = {}
    for one in support_framework:
        if one == CalculationType.stream.name:
            class_name[CalculationType.stream.name] = _get_stream_function_class_name(
                func_config.func_language, function_name
            )
        if one == CalculationType.batch.name:
            class_name[CalculationType.batch.name] = _get_batch_function_class_name(
                func_config.func_language, function_name
            )

    return {
        "name": func_config.__name__,
        "version": func_config.version,
        "language": func_config.func_language,
        "type": func_config.func_udf_type,
        "path": "%s/app/udf/%s/%s/%s.jar"
        % (
            config_hdfs.get_url(),
            func_config.__name__,
            func_config.version,
            func_config.__name__,
        ),
        "support_framework": support_framework,
        "class_name": class_name,
    }


def _get_stream_function_class_name(func_language, function_name):
    prefix = {
        FuncLanguage.java.name: "com.tencent.bk.base.dataflow.udf.codegen.flink.FlinkJava",
        FuncLanguage.python.name: "com.tencent.bk.base.dataflow.udf.codegen.flink.FlinkPy",
    }
    return prefix[func_language] + _underline_to_camel(function_name)


def _get_batch_function_class_name(func_language, function_name):
    prefix = {
        FuncLanguage.java.name: "com.tencent.bk.base.dataflow.udf.codegen.hive.HiveJava",
        FuncLanguage.python.name: "com.tencent.bk.base.dataflow.udf.codegen.hive.HivePy",
    }
    return prefix[func_language] + _underline_to_camel(function_name)

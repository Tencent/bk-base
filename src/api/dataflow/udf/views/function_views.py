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

from common.decorators import detail_route, list_route, params_valid

from dataflow.udf.functions.function_driver import *
from dataflow.udf.functions.function_serializers import GetConfigSerializer, SetSupportPythonPackages
from dataflow.udf.views.base_views import BaseViewSet


class FunctionViewSet(BaseViewSet):
    """
    function rest api
    """

    lookup_field = "function_name"

    def create(self, request):
        """
        @api {post} /dataflow/udf/functions/ 创建function
        @apiName functions
        @apiGroup udf
        @apiParam {string} func_name  函数名称
        @apiParam {string} func_alias  函数中文名
        @apiParam {string} func_language 函数编程语言
        @apiParam {string} func_udf_type udf类型
        @apiParam {string} input_type 输入参数
        @apiParam {string} return_type 返回参数
        @apiParam {string} explain 函数说明
        @apiParam {string} example 使用样例
        @apiParam {string} example_return_value 样例返回
        @apiParam {string} code_config 代码配置
        @apiParamExample {json} 参数样例:
            {
              "func_name": "abc",
              "func_alias": "阿比西",
              "func_language": "java",
              "func_udf_type": "udf",
              "input_type": ["string", "string"],
              "return_type": ["string"],
              "explain": "abc函数的用法",
              "example": "select abc(field_a) as cc from table",
              "example_return_value": "return_value"
              "code_config": {"dependencies":[], "code":"public ..."}
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "func_name": "abc",
                        "is_release": true(发布),false(未发布)
                    },
                    "result": true
                }
        """
        args = request.data
        data = create_function(args)
        return self.return_ok(data)

    def update(self, request, function_name):
        """
        @api {put} /dataflow/udf/functions/:function_name 更新function
        @apiName functions
        @apiGroup udf
        @apiParam {string} func_alias  函数中文名
        @apiParam {string} func_language 函数编程语言
        @apiParam {string} func_udf_type udf类型
        @apiParam {string} input_type 输入参数
        @apiParam {string} return_type 返回参数
        @apiParam {string} explain 函数说明
        @apiParam {string} example 使用样例
        @apiParam {string} example_return_value 样例返回
        @apiParam {string} code_config 代码配置
        @apiParamExample {json} 参数样例:
            {
              "func_alias": "阿比西",
              "func_language": "java",
              "func_udf_type": "udf",
              "input_type": ["string", "string"],
              "return_type": ["string"],
              "explain": "abc函数的用法",
              "example": "select abc(field_a) as cc from table",
              "example_return_value": "return_value"
              "code_config": {"dependencies":[], "code":"public ..."}
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "func_name": "abc",
                        "is_release": true(发布),false(未发布)
                    },
                    "result": true
                }
        """
        args = request.data
        data = update_function(function_name, args)
        return self.return_ok(data)

    def retrieve(self, request, function_name):
        """
        @api {get} /dataflow/udf/functions/:function_name/ 获取编辑状态的函数
        @apiName functions/:function_name/
        @apiGroup udf
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "func_name": "abc",
                        "func_alias": "阿比西",
                        "func_language": "java",
                        "func_udf_type": "udf",
                        "input_type": ["string", "string"],
                        "return_type": ["string"],
                        "explain": "abc函数的用法",
                        "example": "select abc(field_a) as cc from table",
                        "example_return_value": "return_value"
                        "code_config": {"dependencies":[], "code":"public ..."},
                        "locked": 0,,
                        "locked_at": "xxx",
                        "locked_by": "xxx"
                    },
                    "result": true
                }
        """
        data = get_dev_function_info(function_name)
        return self.return_ok(data)

    def list(self, request):
        """
        @api {get} /dataflow/udf/functions/ 批量获取函数信息
        @apiName functions/:function_name/lock
        @apiGroup udf
        @apiParam {string} env 环境 env/product 【可选】默认为product
        @apiParam {string} function_name 函数名称 【可选】不选会返回所有的函数，配合env参数使用
        @apiParamExample {json} 参数样例:
            {
                "env": "dev",
                "function_name": "your_udf_name"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": [{
                            'udf_type': 'udtf',
                            'name': 'zip',
                            'parameter_types': [{
                                    'input_types': ['string', 'string...'],
                                    'output_types': ['string']
                                }
                            ],
                            'version': 'dev'

                        }, {
                            'udf_type': 'udf',
                            'name': 'if',
                            'parameter_types': [{
                                    'input_types': ['boolean', 'string', 'string'],
                                    'output_types': ['string']
                                }, {
                                    'input_types': ['boplean', 'int', 'int'],
                                    'output_types': ['int']
                                }
                            ],
                            'version': null
                        }
                    ],
                    "result": true
                }
        """
        args = request.query_params
        result = list_functions(args)
        return self.return_ok(result)

    def destroy(self, request, function_name):
        """
        @api {delete} /dataflow/udf/functions/:function_name/processings/ 删除stream processing
        @apiName processings
        @apiGroup Stream
        @apiParamExample {json} 参数样例:
            {
                "with_data": False  可选
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": null,
                    "result": true
                }
        """
        FunctionHandler(function_name).delete_function()
        return self.return_ok()

    @list_route(methods=["get"], url_path="my_function")
    def my_function(self, request):
        """
        @api {get} /dataflow/udf/functions/my_function
        @apiName functions/my_function
        @apiGroup udf
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                [{
                "func_name": "abc",
                "func_alias": "阿比西",
                "managers": "xx,xx",
                "developers": "xx,xx",
                "version": "",
                "updated_at": "2018-01-01 11:00:00"
            }]
        """
        return self.return_ok(list_function_info())

    @detail_route(methods=["post"], url_path="lock")
    def lock(self, request, function_name):
        """
        @api {post} /dataflow/udf/functions/:function_name/lock 对function上锁
        @apiName functions/:function_name/lock
        @apiGroup udf
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": null,
                    "result": true
                }
        """
        lock_function(function_name)
        return self.return_ok()

    @detail_route(methods=["post"], url_path="unlock")
    def unlock(self, request, function_name):
        """
        @api {post} /dataflow/udf/functions/:function_name/unlock 对function解锁
        @apiName functions/:function_name/unlock
        @apiGroup udf
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": null,
                    "result": true
                }
        """
        unlock_function(function_name)
        return self.return_ok()

    @detail_route(methods=["get"], url_path="code_frame")
    def code_frame(self, request, function_name):
        """
        @api {get} /dataflow/udf/functions/:function_name/code_frame 获取udf代码框架
        @apiName functions/:function_name/code_frame
        @apiGroup udf
        @apiParamExample {json} 参数样例:
            {
                "func_language": "java",
                "func_udf_type": "udf",
                "input_type": ["string", "string"],
                "return_type": ["string"]
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": "the code",
                    "result": true
                }
        """
        data = generate_code_frame(function_name, request.query_params)
        return self.return_ok(data)

    @list_route(methods=["post"], url_path="parse")
    def parse(self, request):
        """
        @api {post} /dataflow/udf/functions/parse 根据sql获取udf信息
        @apiName functions/parse
        @apiGroup udf
        @apiParam {String} sql
        @apiParamExample {json} 参数样例:
            {
                "sql": "select udf(a) from table"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": [{
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
                            ],
                    "result": true
                }
        """
        args = request.data
        sql = args["sql"]
        geog_area_code = args["geog_area_code"]
        check_params = args["check_params"] or False
        data = parse_sql(sql, geog_area_code, check_params)
        return self.return_ok(data)

    @list_route(methods=["get"], url_path="get_support_python_packages")
    def get_support_python_packages(self, request):
        """
        @api {get} /dataflow/udf/functions/support_python_packages
        @apiName functions/support_python_packages
        @apiGroup udf
        @apiParamExample {json} 参数样例:
            {
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": [{
                            "name": "pandas",
                            "version": "0.25.0",
                            "description": "分析结构化数据的工具集"
                                }
                            ],
                    "result": true
                }
        """
        data = get_support_python_packages()
        return self.return_ok(data)

    @list_route(methods=["post"], url_path="support_python_packages")
    @params_valid(serializer=SetSupportPythonPackages)
    def support_python_packages(self, request, params):
        """
        @api {post} /dataflow/udf/functions/support_python_packages
        @apiName functions/support_python_packages
        @apiGroup udf
        @apiParam {String} name
        @apiParam {String} version
        @apiParam {String} description
        @apiParamExample {json} 参数样例:
            {
                "name": "pandas",
                "version": "0.25.0",
                "is_cpython": "true or false",
                "description": "分析结构化数据的工具集"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": "",
                    "result": true
                }
        """
        name = params["name"]
        version = params["version"]
        description = params["description"]
        is_cpython = params["is_cpython"]
        data = set_support_python_packages(name, version, description, is_cpython)
        return self.return_ok(data)

    @detail_route(methods=["post"], url_path="release")
    def release(self, request, function_name):
        """
        @api {post} /dataflow/udf/functions/:function_name/release 发布函数
        @apiName functions/:function_name/release
        @apiGroup udf
        @apiParam {String} release_log 函数发布日志
        @apiParamExample {json} 参数样例:
            {
                "release_log": "the version release log."
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": null,
                    "result": true
                }
        """
        args = request.data
        release_log = args["release_log"]
        release_function(function_name, release_log)
        return self.return_ok()

    @detail_route(methods=["post"], url_path="package")
    def package(self, request, function_name):
        """
        @api {post} /dataflow/udf/functions/:function_name/package 代码打包，包含生成udf代码
        @apiName functions/:function_name/package
        @apiGroup udf
        @apiParam {string} calculation_type  计算类型
        @apiParamExample {json} 参数样例:
            {
                "calculation_type": "stream"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": null,
                    "result": true
                }
        """
        args = request.data
        package(function_name, args["calculation_type"])
        return self.return_ok()

    @detail_route(methods=["post"], url_path="security_check")
    def security_check(self, request, function_name):
        """
        @api {post} /dataflow/udf/functions/:function_name/security_check udf安全检测
        @apiName functions/:function_name/security_check
        @apiGroup udf
        @apiParamExample {json} 参数样例:
            {
                "sql": "select ****",
                "debug_data": {
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
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": null,
                    "result": true
                }
        """
        args = request.data
        sql = args["sql"]
        debug_data = args["debug_data"]
        security_check(function_name, sql, debug_data)
        return self.return_ok()

    @list_route(methods=["get"], url_path="list_function_doc")
    def list_function_doc(self, request):
        """
        @api {get} /dataflow/udf/functions/list_function_doc 获取所有函数信息
        @apiName functions/list_function_doc
        @apiGroup udf
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": [
                        {
                        'func_groups':[
                            'display':'',
                            'func':[],
                            'group_name':''
                        ],
                        'type_name':'',
                        'display':'',
                        'id':''
                        }
                    ]
                    "result": true
                }
        """
        # 废弃，CommonViewSet会调用这个方法
        # data = list_function_doc()
        return self.return_ok()

    @detail_route(methods=["post"], url_path="check_example_sql")
    def check_example_sql(self, request, function_name):
        """
        @api {post} /dataflow/udf/functions/:function_name/check_example_sql 样例SQL语法检测
        @apiName functions/:function_name/check_example_sql
        @apiGroup udf
        @apiParamExample {json} 参数样例:
            {
                "sql": "select ****"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": null,
                    "result": true
                }
        """
        args = request.data
        sql = args["sql"]
        check_example_sql(function_name, sql)
        return self.return_ok()

    @detail_route(methods=["get"], url_path="config")
    @params_valid(serializer=GetConfigSerializer)
    def get_config(self, request, function_name, params):
        """
        @api {get} /dataflow/udf/functions/:function_name/config 获取自定义函数的配置
        @apiName functions/:function_name/config
        @apiGroup udf
        @apiParam {String} geog_area_code 地域信息
        @apiParamExample {json} 参数样例:
            {
                "geog_area_code": "inland"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200",
                    "data": {
                        "path": "xxxx",
                        "version": "v11",
                        "language": "python"
                        "type": "udaf",
                        "name": "function_name",
                        "support_framework": ['stream', 'batch'],
                        "class_name": {
                            "batch": "xxxx",
                            "stream": "xxxx"
                        }
                    },
                    "result": true
                }
        """
        geog_area_code = params["geog_area_code"]
        data = get_function_config(function_name, geog_area_code)
        return self.return_ok(data)

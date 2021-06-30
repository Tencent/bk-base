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

from rest_framework.decorators import list_route
from rest_framework.response import Response

from codecheck.handlers.code_check_handler import CodeCheckHandler
from codecheck.serializer.serializers import (
    CodeCheckVerifySerializer,
    CodeCheckAddBlacklistGroupSerializer,
    CodeCheckAddParserGroupSerializer,
    CodeCheckDeleteAndGetBlacklistGroupSerializer,
    CodeCheckDeleteAndGetParserGroupSerializer,
)
from common.decorators import params_valid
from common.views import APIViewSet
from codecheck.shared.log import codecheck_logger


class CodeCheckVerifyViewSet(APIViewSet):
    # lookup_field = "parser_group_name"
    # 由英文字母、数字、下划线、小数点组成，且需以字母开头，不需要增加^表示开头和加$表示结尾
    # lookup_value_regex = "[a-zA-Z]+(_|[a-zA-Z0-9]|\\.)*"

    @list_route(methods=["post"], url_path="check")
    @params_valid(serializer=CodeCheckVerifySerializer)
    def verify(self, request, params):
        """
        @api {post} /verify/check/ 代码安全检测，支持python/java
        @apiName verify
        @apiGroup CodeCheck
        @apiDescription 代码安全检测，支持python/java。返回命中的黑名单结果信息。
        @apiParam {string} code_language 代码语言，java/python
        @apiParam {string} check_content 校验的完整代码内容，base64编码字符串（必须urlsafe）
        @apiParam {string} parser_group_name 校验的lib库的组名称，对java有效，python不需要
        @apiParam {string} blacklist_group_name 校验的黑名单组名称
        @apiParam {boolean} check_flag 是否校验，默认true。若为true，只返回命中黑名单的结果。
            若为false，对python返回所有import列表，对java返回所有函数调用列表。
        @apiParamExample {json} 请求参数示例
        {
            "code_language": "java",
            "check_content": "cHVibG7CiAgICAgICAgU3lzdGVtLmV4aX",
            "parser_group_name": "flink_1_7_2",
            "blacklist_group_name": "flink_1_7_2",
            "check_flag": true
        }
        @apiSuccessExample {json} 代码安全检测返回结果
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "parse_message": "java codecheck success",
                "parse_status": "success",
                "parse_result": [
                    {
                        "message": null,
                        "method_name": "java.io.PrintStream.println",
                        "line_no": 3,
                        "file_name": "HelloWorld2"
                    },
                    {
                        "message": null,
                        "method_name": "java.lang.System.exit",
                        "line_no": 4,
                        "file_name": "HelloWorld2"
                    }
                ]
            },
            "result": true
        }
        """
        code_language = params.get("code_language")
        if code_language != "python" and code_language != "java":
            codecheck_logger.warning("code_language(%s) not supported" % (code_language))
            return Response(data={"parse_status": "fail", "parse_message": "only python/java supported"})

        ret_data = CodeCheckHandler.verify(params)
        return Response(data=ret_data)


class CodeCheckParserGroupViewSet(APIViewSet):
    lookup_field = "parser_group_name"

    # 由英文字母、数字、下划线、小数点组成，且需以字母开头，不需要增加^表示开头和加$表示结尾
    # lookup_value_regex = "[a-zA-Z]+(_|[a-zA-Z0-9]|\\.)*"

    @params_valid(serializer=CodeCheckAddParserGroupSerializer)
    def create(self, request, params):
        """
        @api {post} /parser_groups/ 添加代码检测的lib库组
        @apiName parser_groups
        @apiGroup CodeCheck
        @apiDescription 添加代码检测的lib库，只对java生效，python无意义。
        @apiParam {string} parser_op 操作类型，'add'
        @apiParam {string} parser_group_name 校验的lib库的组名称，对java有效，python不需要
        @apiParam {string} lib_dir 校验的lib库的本地lib目录（在api机器的本地目录，包含所需的.jar文件），会递归加载该目录下所有jar文件
        @apiParam {string} source_dir 校验的lib库的本地source目录（.java文件），一般设置为空""
        @apiParamExample {json} 请求参数示例
        {
            "parser_op": "add",
            "parser_group_name": "flink_1_7_4",
            "lib_dir": "/xxx/flink_lib/",
            "source_dir": ""
        }
        @apiSuccessExample {json} 添加代码检测的lib库组的返回结果
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": null,
            "result": true
        }
        """
        code_check_res = CodeCheckHandler.add_parser_group(
            params.get("lib_dir"), params.get("source_dir"), params.get("parser_group_name")
        )
        return Response(data=code_check_res)

    @params_valid(serializer=CodeCheckDeleteAndGetParserGroupSerializer)
    def destroy(self, request, parser_group_name, params):
        """
        @api {delete} /parser_groups/:parser_group_name/ 删除代码检测的lib库
        @apiName parser_groups
        @apiGroup CodeCheck
        @apiDescription 删除代码检测的lib库，只对java生效，python无意义。
        @apiParam {string} parser_op 操作类型，'add'
        @apiParam {string} parser_group_name 校验的lib库的组名称，对java有效，python不需要
        @apiParamExample {json} 请求参数示例
        {
        }
        @apiSuccessExample {json} 删除代码检测的lib库信息返回结果
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": null,
            "result": true
        }
        """
        code_check_res = CodeCheckHandler.delete_parser_group(parser_group_name)
        return Response(data=code_check_res)

    @params_valid(serializer=CodeCheckDeleteAndGetParserGroupSerializer)
    def retrieve(self, request, parser_group_name, params):
        """
        @api {get} /parser_groups/:parser_group_name/ 获取代码检测的lib库信息
        @apiName parser_groups
        @apiGroup CodeCheck
        @apiDescription 获取代码检测的lib库信息，只对java生效，python无意义。
        @apiParam {string} parser_group_name 校验的lib库的组名称
        @apiParamExample {json} 请求参数示例
        {
        }
        @apiSuccessExample {json} 获取代码检测的lib库组信息返回结果
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "status": "success",
                "parser_group_name": "flink_1_7_4",
                "parser_op": "list",
                "result": {
                    "flink_1_7_4": {
                        "lib_dir": [
                            "/xxx/cclib2/"
                        ],
                        "source_dir": ""
                    }
                }
            },
            "result": true
        }
        """
        code_check_res = CodeCheckHandler.get_parser_group(parser_group_name)
        return Response(data=code_check_res)

    @params_valid(serializer=CodeCheckDeleteAndGetParserGroupSerializer)
    def list(self, request, params):
        """
        @api {get} /parser_groups/ 获取所有代码检测的lib库组信息
        @apiName parser_groups
        @apiGroup CodeCheck
        @apiDescription 获取代码检测的lib库组信息，只对java生效，python无意义。
        @apiParamExample {json} 请求参数示例
        {
        }
        @apiSuccessExample {json} 获取所有代码检测的lib库组信息返回结果
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "status": "success",
                "parser_group_name": null,
                "parser_op": "list",
                "result": {
                    "flink_1_7_4": {
                        "lib_dir": [
                            "/xxx/cclib4/"
                        ],
                        "source_dir": ""
                    },
                    "flink_1_7_2": {
                        "lib_dir": [
                            "/xxx/cclib2/"
                        ],
                        "source_dir": ""
                    },
                    "flink_1_7_3": {
                        "lib_dir": [
                            "/xxx/cclib3/"
                        ],
                        "source_dir": ""
                    }
                }
            },
            "result": true
        }
        """
        code_check_res = CodeCheckHandler.list_parser_group()
        return Response(data=code_check_res)


class CodeCheckBlacklistGroupViewSet(APIViewSet):
    lookup_field = "blacklist_group_name"

    # 由英文字母、数字、下划线、小数点组成，且需以字母开头，不需要增加^表示开头和加$表示结尾
    # lookup_value_regex = "[a-zA-Z]+(_|[a-zA-Z0-9]|\\.)*"

    @params_valid(serializer=CodeCheckAddBlacklistGroupSerializer)
    def create(self, request, params):
        """
        @api {post} /blacklist_groups/ 添加黑名单组
        @apiName blacklist_groups
        @apiGroup CodeCheck
        @apiDescription 添加黑名单组，对java为'函数调用'黑名单，对python为'import 包名'黑名单。
        @apiParam {string} blacklist_op 操作类型，'add'
        @apiParam {string} blacklist_group_name 黑名单组名称
        @apiParam {string} blacklist 黑名单列表，多个黑名单以英文分号分隔；如果黑名单中包含英文分号，则只能单个添加；
            对java为完整对包路径+函数名称；对python为完整import对包名。支持正则表达式；
        @apiParamExample {json} 请求参数示例
        {
            "blacklist_op": "add",
            "blacklist_group_name": "flink_1_7_4",
            "blacklist": "java.io.PrintStream.println;java.util.ArrayList.*;java.util.Map.*"
        }
        @apiSuccessExample {json} 添加黑名单组返回结果
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": null,
            "result": true
        }
        """
        code_check_res = CodeCheckHandler.add_blacklist_group(
            params.get("blacklist"), params.get("blacklist_group_name")
        )
        return Response(data=code_check_res)

    @params_valid(serializer=CodeCheckDeleteAndGetBlacklistGroupSerializer)
    def destroy(self, request, blacklist_group_name, params):
        """
        @api {delete} /blacklist_groups/:blacklist_group_name/ 删除黑名单组
        @apiName blacklist_groups
        @apiGroup CodeCheck
        @apiDescription 删除黑名单组
        @apiParam {string} blacklist_group_name 黑名单组名称
        @apiParamExample {json} 请求参数示例
        {
        }
        @apiSuccessExample {json} 删除黑名单组返回结果
        {
           "errors": null,
           "message": "ok",
           "code": "1500200",
           "data": null,
           "result": true
        }
        """
        code_check_res = CodeCheckHandler.delete_blacklist_group(blacklist_group_name)
        return Response(data=code_check_res)

    @params_valid(serializer=CodeCheckDeleteAndGetBlacklistGroupSerializer)
    def retrieve(self, request, blacklist_group_name, params):
        """
        @api {get} /blacklist_groups/:blacklist_group_name/ 获取黑名单组信息
        @apiName blacklist_groups
        @apiGroup CodeCheck
        @apiDescription 获取黑名单组信息
        @apiParamExample {json} 请求参数示例
        {
        }
        @apiSuccessExample {json} 获取黑名单组信息返回结果
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "status": "success",
                "blacklist_op": "list",
                "result": {
                    "flink_1_7_2": [
                        "java.io.PrintStream.println",
                        "java.util.ArrayList.*",
                        "java.util.Map.*"
                    ]
                },
                "blacklist_group_name": "flink_1_7_2"
            },
            "result": true
        }
        """
        code_check_res = CodeCheckHandler.get_blacklist_group(blacklist_group_name)
        return Response(data=code_check_res)

    @params_valid(serializer=CodeCheckDeleteAndGetBlacklistGroupSerializer)
    def list(self, request, params):
        """
        @api {get} /blacklist_groups/ 获取所有黑名单组信息
        @apiName blacklist_groups
        @apiGroup CodeCheck
        @apiDescription 获取所有黑名单组信息
        @apiParamExample {json} 请求参数示例
        {
        }
        @apiSuccessExample {json} 获取所有黑名单组信息返回结果
        {
            "errors": null,
            "message": "ok",
            "code": "1500200",
            "data": {
                "status": "success",
                "blacklist_op": "list",
                "result": {
                    "flink_1_7_2": [
                        "java.io.PrintStream.println",
                        "java.util.ArrayList.*",
                        "java.util.Map.*"
                    ],
                    "flink_1_7_3": [
                        "java.io.PrintStream.println",
                        "java.util.ArrayList.*",
                        "java.util.Map.*"
                    ]
                },
                "blacklist_group_name": null
            },
            "result": true
        }
        """
        code_check_res = CodeCheckHandler.list_blacklist_group()
        return Response(data=code_check_res)

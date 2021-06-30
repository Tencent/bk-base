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
from common.local import get_request_username
from common.views import APIViewSet
from django.utils.translation import ugettext as _
from rest_framework.response import Response

from dataflow.component.error_code.errorcodes import DataapiCommonCode as common_err_codes
from dataflow.component.exceptions.base_exception import CommonException
from dataflow.component.exceptions.comp_execptions import HDFSException
from dataflow.component.hdfs.hdfs_driver import get_hdfs_result_table_newline
from dataflow.component.serializer.serializers import (
    HDFSCleanSerializer,
    HDFSMoveSerializer,
    HDFSUploadSerializer,
    HDFSUtilSerializer,
)
from dataflow.component.utils.hdfs_util import HDFS
from dataflow.shared.log import component_logger


class NameNodeViewSet(APIViewSet):
    @list_route(methods=["get"], url_path="check")
    def check(self, request):
        """
        @api {get} /dataflow/component/hdfs/namenode/check/ 检查所有namenode状态
        @apiName check_nn_status
        @apiGroup Batch
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                        "message": "ok",
                        "code": "1500200"
                        "data": {
                            "default": {
                                "xxxx-01": "standby",
                                "xxxx-02": "active"
                            },
                            "security": {
                                "xxxx-03": "standby",
                                "xxxx-04": "active"
                            },
                            "tgpa": {
                                "xxxx-05": "standby",
                                "xxxx-06": "active"
                            }
                        },
                        "result": true
                    }

        """
        return Response(HDFS.check_nn_status())


class ResultTableViewSet(APIViewSet):
    lookup_field = "result_table_id"
    lookup_value_regex = r"\w+"

    @list_route(methods=["get"], url_path="new_line")
    def new_line(self, request, result_table_id):
        """
        @api {get} /dataflow/component/hdfs/result_tables/:result_table_id/new_line/ 获取结果表在hdfs上的最新数据
        @apiName hdfs_new_line
        @apiGroup Batch
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": "xxx",
                    "result": true
                }
        """
        new_line = get_hdfs_result_table_newline(result_table_id)
        return Response(new_line)


class HDFSUtilViewSet(APIViewSet):
    lookup_field = "hdfs_ns_id"

    @detail_route(methods=["get"], url_path="list_status")
    @params_valid(serializer=HDFSUtilSerializer)
    def list_status(self, request, hdfs_ns_id, params):
        """
        @api {get} /dataflow/component/hdfs/:hdfs_ns_id/list_status/?path=xxx 列出目录状态
        @apiName list_status
        @apiGroup Batch
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": {
                        'FileStatuses': {
                            'FileStatus': [{
                                u'group': u'root',
                                u'permission': u'755',
                                u'blockSize': 0,
                                u'accessTime': 0,
                                u'pathSuffix': u'api',
                                u'modificationTime': 1491668166162,
                                u'replication': 0,
                                u'length': 0,
                                u'childrenNum': 5,
                                u'owner': u'root',
                                u'storagePolicy': 0,
                                u'type': u'DIRECTORY',
                                u'fileId': 88820}]
                        }
                    },
                    "message": "ok",
                    "code": "1500200",
                }
        """
        try:
            hdfs = HDFS(hdfs_ns_id)
            rtn = hdfs.list_status(params["path"])
            return Response(rtn)
        except CommonException as e:
            component_logger.exception(e)
            raise e
        except Exception as e:
            component_logger.exception(e)
            raise HDFSException(
                message=_("HDFS异常: {}".format(e)),
                code=common_err_codes.INNER_SERVER_EX,
            )

    @detail_route(methods=["post"], url_path="upload")
    @params_valid(serializer=HDFSUploadSerializer)
    def upload(self, request, hdfs_ns_id, params):
        """
        @api {post} /dataflow/component/hdfs/:hdfs_ns_id/upload 上传文件到HDFS指定目录
        @apiName upload
        @apiGroup Batch
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": {'md5': xxx},
                    "message": "ok",
                    "code": "1500200",
                }
        """
        file_path = params["path"]
        uploaded_file = request.FILES["file"]
        try:
            hdfs = HDFS(hdfs_ns_id)
            # 默认切成 65536 字节分块上传
            md5_value = hdfs.create_and_write_large_file(file_path, uploaded_file, is_overwrite=True)
            return Response({"md5": md5_value})
        except CommonException as e:
            component_logger.exception(e)
            raise e
        except Exception as e:
            component_logger.exception(e)
            raise HDFSException(
                message=_("HDFS异常: {}".format(e)),
                code=common_err_codes.INNER_SERVER_EX,
            )
        finally:
            uploaded_file.close()

    @detail_route(methods=["post"], url_path="move")
    @params_valid(serializer=HDFSMoveSerializer)
    def move_file(self, request, hdfs_ns_id, params):
        """
        @api {post} /dataflow/component/hdfs/:hdfs_ns_id/move 上传文件到HDFS指定目录
        @apiName move_file
        @apiGroup Batch
        @apiParamExample {json} 参数样例:
            {
                "is_overwrite": True,
                "from_path": "",
                "to_path": ""
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": {
                        "status": "ignore" | "success"
                    },
                    "message": "ok",
                    "code": "1500200",
                }
        """
        is_overwrite = params["is_overwrite"]
        from_path = params["from_path"]
        to_path = params["to_path"]
        status = "success"
        try:
            hdfs = HDFS(hdfs_ns_id)
            # from_path 对应的文件必须存在
            if not hdfs.is_file_exists(from_path):
                raise CommonException(_("源文件(%s)不存在") % from_path)
            if not is_overwrite and hdfs.is_file_exists(to_path):
                status = "ignore"
            else:
                hdfs.rename(from_path, to_path)
            return Response({"status": status})
        except CommonException as e:
            component_logger.exception(e)
            raise e
        except Exception as e:
            component_logger.exception(e)
            raise HDFSException(
                message=_("HDFS异常: {}".format(e)),
                code=common_err_codes.INNER_SERVER_EX,
            )

    @detail_route(methods=["post"], url_path="clean")
    @params_valid(serializer=HDFSCleanSerializer)
    def clean(self, request, hdfs_ns_id, params):
        """
        @api {post} /dataflow/component/hdfs/:hdfs_ns_id/clean 删除 HDFS 指定目录列表
        @apiName clean
        @apiGroup Batch
        @apiParamExample {json} 参数样例:
            {
                "paths": [],
                "is_recursive": False,
                "user_name": "root"
            }
        @apiSuccessExample {json} Success-Response:
            HTTP/1.1 200 OK
                {
                    "result": true,
                    "data": {},
                    "message": "ok",
                    "code": "1500200",
                }
        """
        paths = params["paths"]
        is_recursive = params.get("is_recursive", False)
        user_name = params.get("user_name", None)
        component_logger.info(
            "用户:{} 使用集群租户:{} 递归删除:{} 目录:{}".format(get_request_username(), user_name, is_recursive, paths)
        )
        try:
            hdfs = HDFS(hdfs_ns_id)
            for path in paths:
                if path == "/":
                    raise HDFSException(
                        message=_("HDFS异常: %s") % "不允许删除根目录",
                        code=common_err_codes.INNER_SERVER_EX,
                    )
                # return {'boolean': True}
                delete_result = hdfs.delete(path, is_recursive, user_name)
                if delete_result and delete_result["boolean"]:
                    component_logger.info("删除:%s 成功" % path)
                else:
                    component_logger.info("删除:%s 失败" % path)
            return Response({})
        except Exception as e:
            component_logger.exception(e)
            raise HDFSException(
                message=_("HDFS异常: {}".format(e)),
                code=common_err_codes.INNER_SERVER_EX,
            )

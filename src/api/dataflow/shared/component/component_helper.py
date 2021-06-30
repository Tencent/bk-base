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

from dataflow.shared.api.modules.component import ComponentAPi
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util
from dataflow.shared.api.util.api_driver import retry_deco


class ComponentHelper(object):
    @staticmethod
    @retry_deco(3)
    def hdfs_upload(hdfs_ns_id, file_path, file):
        """
        文件上传操作成本较高，增加重试
        @param hdfs_ns_id:
        @param file_path:
        @param file:
        @return:
        """
        # 加入重试机制需要重置文件指针到初始位置
        file.seek(0)
        request_params = {"hdfs_ns_id": hdfs_ns_id, "path": file_path}
        res = ComponentAPi.hdfs.upload(request_params, files={"file": file})
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def hdfs_list_status(hdfs_ns_id, path):
        request_params = {"hdfs_ns_id": hdfs_ns_id, "path": path}
        res = ComponentAPi.hdfs.list_status(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def hdfs_mv(hdfs_ns_id, from_path, to_path, is_overwrite=True):
        request_params = {
            "hdfs_ns_id": hdfs_ns_id,
            "from_path": from_path,
            "to_path": to_path,
            "is_overwrite": is_overwrite,
        }
        res = ComponentAPi.hdfs.move(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def hdfs_check_to_path_before_move(hdfs_ns_id, from_path, to_path):
        """
        TODO: 目前 HDFS 不支持拷贝操作，只能结合 move 接口代替
        @param hdfs_ns_id:
        @param from_path:
        @param to_path:
        @return:
        """
        if not ComponentHelper.hdfs_list_status(hdfs_ns_id, from_path) and ComponentHelper.hdfs_list_status(
            hdfs_ns_id, to_path
        ):
            return {"status": "ignore"}
        else:
            return ComponentHelper.hdfs_mv(hdfs_ns_id, from_path, to_path)

    @staticmethod
    def hdfs_clean(hdfs_ns_id, paths, is_recursive=False, user_name=None):
        """
        删除hdfs指定目录
        @param hdfs_ns_id: hdfs ns id
        @param paths: 待删除的目录
        @param is_recursive: 是否递归删除
        @param user_name: 认证用户
        @return:
        """
        request_params = {
            "hdfs_ns_id": hdfs_ns_id,
            "paths": paths,
            "is_recursive": is_recursive,
            "user_name": user_name,
        }
        res = ComponentAPi.hdfs.clean(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

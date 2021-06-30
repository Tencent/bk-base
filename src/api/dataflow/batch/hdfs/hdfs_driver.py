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
import time

from django.utils.translation import ugettext as _

from dataflow.batch.exceptions.comp_execptions import BatchUnexpectedException, InnerHDFSServerException
from dataflow.batch.settings import BKDATA_DIR, DATA_TIME_ZONE
from dataflow.batch.utils import time_util
from dataflow.batch.utils.hdfs_util import HDFS, ConfigHDFS
from dataflow.batch.utils.parquet_util import read_line
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import batch_logger
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper


def get_hdfs_result_table_newline(result_table_id):
    result_table = ResultTableHelper.get_result_table(result_table_id)
    result_table_type = result_table["processing_type"]
    if result_table_type not in [
        "batch",
        "stream",
        "clean",
        "batch_model",
        "stream_model",
        "transform",
    ]:
        raise BatchUnexpectedException(message=_("不支持的结果表类型 - %s") % result_table_type)

    storage_param = ResultTableHelper.get_result_table_storage(result_table_id, "hdfs")
    if "data_type" in storage_param["hdfs"]:
        data_type = storage_param["hdfs"]["data_type"]
        if str(data_type).lower() == "iceberg":
            iceberg_data = StorekitHelper.sample_records(result_table_id, data_type)
            if len(iceberg_data) > 0:
                return json.dumps(iceberg_data[0])
            else:
                raise Exception("尚未发现最近写入数据")

    geog_area_code = result_table["tags"]["manage"]["geog_area"][0]["code"]
    jobnavi_cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
    hdfs_cluster = get_real_hdfs_cluster_type(geog_area_code, get_hdfs_cluster_group(result_table_id))
    hdfs = HDFS(hdfs_cluster)
    TMP_DIR = BKDATA_DIR + "/logs/tmp"
    if not os.path.exists(TMP_DIR):
        os.mkdir(TMP_DIR)
    table_path = get_hdfs_path_by_result_table_id(result_table_id, result_table_type)
    for i in range(0, 24):
        time_path = time.strftime(
            "%Y/%m/%d/%H",
            time_util.timetuple_timezone(time.time() - 60 * 60 * i, DATA_TIME_ZONE),
        )
        hdfs_path = "{}/{}".format(table_path, time_path)
        path_status = hdfs.list_status(hdfs_path)
        if path_status and path_status["FileStatuses"]["FileStatus"]:
            file_status = path_status["FileStatuses"]["FileStatus"][-1]
            path_suffix = file_status["pathSuffix"]
            if path_suffix and path_suffix != "_READ":
                if (
                    result_table_type == "stream"
                    or result_table_type == "clean"
                    or result_table_type == "stream_model"
                    or path_suffix.endswith(".parquet")
                ):
                    try:
                        path = "{}/{}/{}".format(
                            hdfs.config_hdfs.get_url(),
                            hdfs_path,
                            path_suffix,
                        )
                        line = read_line(geog_area_code, jobnavi_cluster_id, path)
                        return line
                    except Exception as e:
                        batch_logger.exception(e)
                else:
                    return __open_and_read_hdfs(hdfs, "{}/{}".format(hdfs_path, path_suffix)).strip().split("\n")[-1]


def get_hdfs_path_by_result_table_id(result_table_id, result_table_type=None):
    if not result_table_type:
        result_table_type = ResultTableHelper.get_processing_type(result_table_id)
        if not result_table_type:
            raise BatchUnexpectedException(message=_("结果表%s不存在") % result_table_id)
    data = StorekitHelper.get_physical_table_name(result_table_id, result_table_type)
    return data["hdfs"]


def get_real_hdfs_cluster_type(geog_area_code, storage_cluster_group):
    # 此处仅是为了获取所有可用的 hdfs 信息
    if storage_cluster_group in ConfigHDFS.get_all_cluster():
        return storage_cluster_group
    else:
        return StorekitHelper.get_default_storage("hdfs", geog_area_code)["cluster_group"]


def get_hdfs_cluster_group(result_table_id):
    storage_info = ResultTableHelper.get_result_table_storage(result_table_id, cluster_type="hdfs")
    if not storage_info:
        raise BatchUnexpectedException(message=_("结果表%s不存在") % result_table_id)
    return storage_info["hdfs"]["storage_cluster"]["cluster_group"]


def __open_and_read_hdfs(hdfs, path):
    return __open_and_read_hdfs_with_retry(hdfs, path, 1)


def __open_and_read_hdfs_with_retry(hdfs, path, retry_time):
    if retry_time > 5:
        raise InnerHDFSServerException(message="Inner Error")
    try:
        return hdfs.open_and_read(path)
    except Exception as e:
        batch_logger.exception(e)
        time.sleep(retry_time)
        return __open_and_read_hdfs_with_retry(hdfs, path, retry_time + 1)


def __save_to_tmp_file(res, tmp_file_path):
    tmp_file = None
    try:
        tmp_file = open(tmp_file_path, "w")
        tmp_file.write(res)
    finally:
        if tmp_file:
            tmp_file.flush()
            tmp_file.close()


def __remove_tmp_file(tmp_file_path):
    os.remove(tmp_file_path)

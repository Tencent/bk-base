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

import hashlib

import requests
from common.log import logger
from datahub.access.api import StoreKitApi
from datahub.access.exceptions import CollectorError, CollerctorCode
from datahub.common.const import CLUSTER_TYPE, DATALAB, HDFS, INLAND, TAGS


def get_default_storage():
    res = StoreKitApi.cluster_config.list({CLUSTER_TYPE: HDFS, TAGS: [INLAND, DATALAB]})
    if res.is_success() and len(res.data) > 0:
        return res.data[0]
    else:
        logger.error("failed to get default hdfs-cluster from storekit!")
        raise CollectorError(error_code=CollerctorCode.API_REQ_ERR)


def get_hdfs_upload_path_chunk(biz_id, file_name, chunk_id=None):
    """
    get hdfs chunk_split file name or folder name
    /data/{biz_id}/{file_name}/{id}.data
    """
    if chunk_id:
        chunk_id = chunk_id.rjust(10, "0")
        return "/data/{}/tmp_{}/{}/".format(biz_id, file_name, chunk_id)
    else:
        return "/data/{}/tmp_{}/".format(biz_id, file_name)


def get_hdfs_upload_path(biz_id, file_name):
    """
    /data/{biz_id}/{file_name}/{id}.data
    """
    return "/data/{}/{}/".format(biz_id, file_name)


def get_raw_data_upload_path(biz_id, raw_data_id, file_name):
    """
    /data/{biz_id}/{file_name}/{id}.data
    """
    return "/data/{}/{}/{}/".format(biz_id, raw_data_id, file_name)


def create_and_write_file(hosts, port, file_path, data):
    """
    将单个分片上传到相应路径下
    """
    for host in hosts.split(","):
        hdfs_path = "http://{}:{}/webhdfs/v1{}?op=CREATE&user.name=root&overwrite=true".format(host, port, file_path)
        logger.info(hdfs_path)
        res = requests.put(hdfs_path, data=data)
        if res.status_code == 200 or res.status_code == 201:
            return True
        else:
            logger.warning("{} request error! try another, code:{} ,info:{}".format(host, res.status_code, res.text))

    logger.error("use these hosts send hdfs request failed {}:{} path:{}".format(hosts, port, file_path))
    raise CollectorError(error_code=CollerctorCode.API_REQ_ERR)


def load_and_merge_file(hosts, port, file_path, tmp_folder_path, chunk_size):
    active_host = get_active_namenode(hosts, port)
    # step1: get part_file list from hdfs
    list_dir_url = "http://{}:{}/webhdfs/v1{}?op=LISTSTATUS&user.name=root".format(
        active_host,
        port,
        tmp_folder_path,
    )
    list_res = requests.get(list_dir_url)
    if list_res.status_code != 200 and list_res.status_code != 201:
        logger.error("get hdfs file list failed! request:{} path:{}".format(list_dir_url, tmp_folder_path))
        raise CollectorError(error_code=CollerctorCode.API_REQ_ERR)

    file_list = [file_status["pathSuffix"] for file_status in list_res.json()["FileStatuses"]["FileStatus"]]
    file_list.sort()
    if len(file_list) != chunk_size:
        logger.error("hdfs file list size, %s != %d" % (chunk_size, len(file_list)))
        raise CollectorError(error_code=CollerctorCode.API_REQ_ERR)

    # step2: read and merge file
    md5_updater = hashlib.md5()
    for index, file_name in enumerate(file_list):
        read_file_url = "http://{}:{}/webhdfs/v1{}{}?op=OPEN&user.name=root".format(
            active_host,
            port,
            tmp_folder_path,
            file_name,
        )
        read_res = requests.get(read_file_url)
        if read_res.status_code != 200 and read_res.status_code != 201:
            logger.error("get hdfs file list failed! {}:{} path:{}".format(active_host, port, tmp_folder_path))
            raise CollectorError(error_code=CollerctorCode.API_REQ_ERR)

        # update md5
        md5_updater.update(read_res.content)

        if index != 0:
            write_hdfs_url = "http://{}:{}/webhdfs/v1{}?op=APPEND&user.name=root".format(
                active_host,
                port,
                file_path,
            )
            res = requests.post(write_hdfs_url, data=read_res.content)
        else:
            write_hdfs_url = "http://{}:{}/webhdfs/v1{}?op=CREATE&user.name=root&overwrite=true".format(
                active_host,
                port,
                file_path,
            )
            res = requests.put(write_hdfs_url, data=read_res.content)

        if res.status_code != 200 and res.status_code != 201:
            logger.error("{} http request error! code: {} ,info: {}".format(active_host, res.status_code, res.text))
            raise CollectorError(error_code=CollerctorCode.API_REQ_ERR)

    return md5_updater.hexdigest()


def get_active_namenode(hosts, port):
    active_host = ""
    for host in hosts.split(","):
        check_url = "http://{}:{}/webhdfs/v1/?op=LISTSTATUS&user.name=root".format(
            host,
            port,
        )
        check_res = requests.get(check_url)
        if check_res.status_code == 200:
            active_host = host
            break

    if active_host == "":
        logger.error("get hdfs active namenode failed! request:{} path:{}".format(check_url, hosts))
        raise CollectorError(error_code=CollerctorCode.API_REQ_ERR)

    return active_host


def remove_dir(hosts, port, file_path):
    active_host = get_active_namenode(hosts, port)
    rm_url = "http://{}:{}/webhdfs/v1{}?op=DELETE&recursive=true&user.name=root".format(
        active_host,
        port,
        file_path,
    )
    res = requests.delete(rm_url)
    if res.status_code != 200 and res.status_code != 201:
        logger.error("{} request error! url:{} code:{} ,info:{}".format(active_host, rm_url, res.status_code, res.text))
        raise CollectorError(error_code=CollerctorCode.API_REQ_ERR)


def rename_file(hosts, port, old_path, new_path):
    rename_url = "http://{}:{}/webhdfs/v1{}?op=RENAME&destination={}&user.name=root&createParent=true".format(
        hosts,
        port,
        old_path,
        new_path,
    )
    res = requests.put(rename_url)
    if res.status_code != 200 and res.status_code != 201:
        print(res.text)
        logger.error(
            "{} http request error! url:{} code: {},info:{}".format(old_path, new_path, res.status_code, res.text)
        )
        raise CollectorError(error_code=CollerctorCode.API_REQ_ERR)
